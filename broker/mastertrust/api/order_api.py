"""Master Trust orders, books, smart orders and bulk actions."""

from broker.mastertrust.api.baseurl import api_request, get_client_id
from broker.mastertrust.api.data import BrokerData
from broker.mastertrust.mapping.transform_data import (
    OrderRejected,
    from_broker_quantity,
    map_product_type,
    reverse_map_product_type,
    transform_data,
    transform_modify_order_data,
)
from database.token_db import get_symbol, get_token
from utils.logging import get_logger
from utils.position_read import read_position_book, refuse_smart_order_on_read_failure
from utils.smart_order_guard import PositionBookCache, SymbolLocks

logger = get_logger(__name__)

NO_CLIENT_ID = {
    "status": "error",
    "message": "Could not read your Master Trust client ID. Please log in again.",
}
FINAL_STATUSES = ("complete", "cancel", "reject")


class _Response:
    """Status holder for callers that read .status (services check res.status == 200)."""

    def __init__(self, status):
        self.status = status
        self.status_code = status


def _get(path, auth, **params):
    client_id = get_client_id(auth)
    if not client_id:
        return NO_CLIENT_ID
    return api_request("GET", path, auth, params={"client_id": client_id, **params})[1]


def get_order_book(auth):
    """Pending and completed orders in one list (Tradelab serves them separately)."""
    orders = []
    for kind in ("pending", "completed"):
        body = _get("/api/v1/orders", auth, type=kind)
        if body.get("status") != "success":
            return body
        orders.extend((body.get("data") or {}).get("orders") or [])
    return {"status": "success", "data": orders}


def get_trade_book(auth):
    body = _get("/api/v1/trades", auth)
    if body.get("status") != "success":
        return body
    return {"status": "success", "data": (body.get("data") or {}).get("trades") or []}


def get_positions(auth):
    # Netwise ("historical") carries forward overnight positions; daywise does not.
    body = _get("/api/v1/positions", auth, type="historical")
    if body.get("status") != "success":
        return body
    return {"status": "success", "data": body.get("data") or []}


def get_holdings(auth):
    body = _get("/api/v1/holdings", auth)
    if body.get("status") != "success":
        return body
    return {"status": "success", "data": (body.get("data") or {}).get("holdings") or []}


# One smart order per symbol at a time, each against a fresh position book
# (utils/smart_order_guard.py).
_SMART_ORDER_LOCKS = SymbolLocks()
_POSITION_BOOK = PositionBookCache()


def _position_book_ok(positions):
    return isinstance(positions, dict) and positions.get("status") == "success"


def _cached_positions(auth):
    return _POSITION_BOOK.get(
        auth,
        lambda: read_position_book("mastertrust", lambda: get_positions(auth), _position_book_ok),
    )


def get_open_position(tradingsymbol, exchange, product, auth):
    """Net quantity (OpenAlgo units, as a string) for one symbol and product."""
    token = str(get_token(tradingsymbol, exchange))
    for position in _cached_positions(auth).get("data", []):
        if (
            str(position.get("instrument_token") or position.get("token")) == token
            and position.get("exchange") == exchange
            and position.get("product") == product
        ):
            return str(from_broker_quantity(position.get("net_quantity"), tradingsymbol, exchange))
    return "0"


def _last_price(data, auth):
    if str(data.get("pricetype", "")).upper() != "MARKET":
        return None
    try:
        return BrokerData(auth).get_quotes(data["symbol"], data["exchange"]).get("ltp")
    except Exception:
        logger.exception(f"Quote for MARKET protection failed: {data['exchange']}:{data['symbol']}")
        return None


def place_order_api(data, auth):
    client_id = get_client_id(auth)
    if not client_id:
        return _Response(401), NO_CLIENT_ID, None
    try:
        payload = transform_data(data, client_id, ltp=_last_price(data, auth))
    except OrderRejected as e:
        return _Response(400), {"status": "error", "message": str(e)}, None
    logger.debug(f"Master Trust order payload: {payload}")
    status, body = api_request("POST", "/api/v1/orders", auth, payload=payload)
    orderid = (
        (body.get("data") or {}).get("oms_order_id") if body.get("status") == "success" else None
    )
    return _Response(200 if orderid else (status if status != 200 else 400)), body, orderid


@refuse_smart_order_on_read_failure
def place_smartorder_api(data, auth):
    symbol, exchange, product = data.get("symbol"), data.get("exchange"), data.get("product")
    with _SMART_ORDER_LOCKS.hold(symbol, exchange, product) as held:
        if not held:
            return SymbolLocks.busy(symbol)
        position_size = int(data.get("position_size", "0"))
        current = int(get_open_position(symbol, exchange, map_product_type(product), auth))

        if position_size == 0 and current == 0:
            action, quantity = data.get("action", "BUY").upper(), int(data.get("quantity", "0"))
        else:
            delta = position_size - current
            action, quantity = ("BUY" if delta > 0 else "SELL"), abs(delta)

        if quantity == 0:
            return (
                None,
                {"status": "success", "message": "No action needed. Position already matched."},
                None,
            )

        order = {**data, "action": action, "quantity": str(quantity)}
        res, body, orderid = place_order_api(order, auth)
        _POSITION_BOOK.invalidate(auth)
        return res, body, orderid


def close_all_positions(current_api_key, auth):
    positions = get_positions(auth)
    if positions.get("status") != "success":
        return {
            "status": "error",
            "message": positions.get("message", "Could not read positions"),
        }, 500

    failures = []
    for position in positions["data"]:
        exchange = position.get("exchange", "")
        token = str(position.get("instrument_token") or position.get("token"))
        symbol = get_symbol(token, exchange)
        if not symbol:
            failures.append(
                f"{position.get('trading_symbol')} ({exchange}): not in the master contract"
            )
            continue
        net = from_broker_quantity(position.get("net_quantity"), symbol, exchange)
        if net == 0:
            continue
        order = {
            "apikey": current_api_key,
            "strategy": "Squareoff",
            "symbol": symbol,
            "exchange": exchange,
            "action": "SELL" if net > 0 else "BUY",
            "quantity": str(abs(net)),
            "pricetype": "MARKET",
            "product": reverse_map_product_type(position.get("product", "")),
        }
        res, body, orderid = place_order_api(order, auth)
        if res.status != 200 or not orderid:
            failures.append(f"{symbol} ({exchange}): {body.get('message', 'order was refused')}")

    if failures:
        return {
            "status": "error",
            "message": f"{len(failures)} position(s) could not be squared off and are still open: "
            + "; ".join(failures),
        }, 500
    return {"status": "success", "message": "All Open Positions SquaredOff"}, 200


def cancel_order(orderid, auth):
    client_id = get_client_id(auth)
    if not client_id:
        return NO_CLIENT_ID, 401
    status, body = api_request(
        "DELETE", f"/api/v1/orders/{orderid}", auth, params={"client_id": client_id}
    )
    if body.get("status") == "success":
        return {"status": "success", "orderid": str(orderid)}, 200
    return {
        "status": "error",
        "message": body.get("message", "Failed to cancel order"),
    }, status if status != 200 else 400


def modify_order(data, auth):
    client_id = get_client_id(auth)
    if not client_id:
        return NO_CLIENT_ID, 401
    try:
        payload = transform_modify_order_data(data, client_id)
    except OrderRejected as e:
        return {"status": "error", "message": str(e)}, 400
    status, body = api_request("PUT", "/api/v1/orders", auth, payload=payload)
    if body.get("status") == "success":
        return {"status": "success", "orderid": str(data["orderid"])}, 200
    return {
        "status": "error",
        "message": body.get("message", "Failed to modify order"),
    }, status if status != 200 else 400


def cancel_all_orders_api(data, auth):
    body = _get("/api/v1/orders", auth, type="pending")
    if body.get("status") != "success":
        return [], []
    canceled, failed = [], []
    for order in (body.get("data") or {}).get("orders") or []:
        status = str(order.get("order_status", "")).lower()
        if any(word in status for word in FINAL_STATUSES):
            continue
        orderid = order.get("oms_order_id")
        _, code = cancel_order(orderid, auth)
        (canceled if code == 200 else failed).append(orderid)
    return canceled, failed
