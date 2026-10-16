"""Master Trust book rows -> OpenAlgo order/trade/position/holding rows.

map_* does the whole normalisation (symbols, enums, MCX lots -> units) and
transform_* only orders the fields, so statistics run on normalised rows.
"""

from datetime import datetime, timedelta, timezone

from broker.mastertrust.mapping.transform_data import from_broker_quantity, reverse_map_order_type
from database.token_db import get_oa_symbol, get_symbol
from utils.logging import get_logger

logger = get_logger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))


def _f(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _symbol(row):
    """OpenAlgo symbol by instrument token, falling back to the trading symbol."""
    exchange = row.get("exchange", "")
    token = row.get("instrument_token") or row.get("token")
    if token is None and isinstance(row.get("instrument_details"), dict):
        token = row["instrument_details"].get("instrument_token")
    symbol = get_symbol(str(token), exchange) if token is not None else None
    return (
        symbol
        or get_oa_symbol(row.get("trading_symbol", ""), exchange)
        or row.get("trading_symbol", "")
    )


def _timestamp(epoch):
    try:
        return (
            datetime.fromtimestamp(int(epoch), IST).strftime("%Y-%m-%d %H:%M:%S") if epoch else ""
        )
    except (TypeError, ValueError, OSError):
        return str(epoch)


def map_order_status(status):
    status = str(status or "").lower()
    if "complete" in status:
        return "complete"
    if "reject" in status:
        return "rejected"
    if "cancel" in status:
        return "cancelled"
    if "trigger" in status:
        return "trigger pending"
    return "open"


def _rows(data):
    if isinstance(data, dict):
        data = data.get("data")
    return data if isinstance(data, list) else []


def map_order_data(order_data):
    orders = []
    for row in _rows(order_data):
        symbol, exchange = _symbol(row), row.get("exchange", "")
        status = map_order_status(row.get("order_status"))
        orders.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "action": str(row.get("order_side", "")).upper(),
                "quantity": from_broker_quantity(
                    row.get("quantity"), symbol, exchange, row.get("lot_size")
                ),
                "price": _f(row.get("price")),
                "trigger_price": _f(row.get("trigger_price")),
                "pricetype": reverse_map_order_type(row.get("order_type", "")),
                "product": str(row.get("product", "")).upper(),
                "orderid": str(row.get("oms_order_id", "")),
                # Trigger-pending stops are live and cancellable: the order book shows them as open.
                "order_status": "open" if status == "trigger pending" else status,
                "timestamp": _timestamp(row.get("order_entry_time")),
                "average_price": _f(row.get("average_trade_price") or row.get("average_price")),
                "rejection_reason": row.get("rejection_reason", "") if status == "rejected" else "",
            }
        )
    return orders


def calculate_order_statistics(order_data):
    stats = {
        "total_buy_orders": 0,
        "total_sell_orders": 0,
        "total_completed_orders": 0,
        "total_open_orders": 0,
        "total_rejected_orders": 0,
    }
    for order in order_data or []:
        if order["action"] == "BUY":
            stats["total_buy_orders"] += 1
        elif order["action"] == "SELL":
            stats["total_sell_orders"] += 1
        key = {
            "complete": "total_completed_orders",
            "open": "total_open_orders",
            "rejected": "total_rejected_orders",
        }.get(order["order_status"])
        if key:
            stats[key] += 1
    return stats


def transform_order_data(orders):
    return orders


def map_trade_data(trade_data):
    trades = []
    for row in _rows(trade_data):
        symbol, exchange = _symbol(row), row.get("exchange", "")
        quantity = from_broker_quantity(
            row.get("trade_quantity") or row.get("filled_quantity"), symbol, exchange
        )
        price = _f(row.get("trade_price"))
        trades.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "product": str(row.get("product", "")).upper(),
                "action": str(row.get("order_side", "")).upper(),
                "quantity": quantity,
                "average_price": price,
                "trade_value": round(quantity * price, 2),
                "orderid": str(row.get("oms_order_id", "")),
                "timestamp": _timestamp(row.get("trade_time") or row.get("exchange_time")),
            }
        )
    return trades


def transform_tradebook_data(trades):
    return trades


def map_position_data(position_data):
    positions = []
    for row in _rows(position_data):
        symbol, exchange = _symbol(row), row.get("exchange", "")
        net = int(float(row.get("net_quantity") or 0))
        ltp = _f(row.get("ltp"))
        multiplier = _f(row.get("multiplier")) or 1.0
        # Tradelab amounts are in rupees; net open quantity is valued at LTP.
        pnl = _f(row.get("sell_amount")) - _f(row.get("buy_amount")) + net * ltp * multiplier
        if net > 0:
            average = _f(row.get("average_buy_price"))
        elif net < 0:
            average = _f(row.get("average_sell_price"))
        else:
            average = 0.0
        positions.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "product": str(row.get("product", "")).upper(),
                "quantity": from_broker_quantity(net, symbol, exchange),
                "pnl": round(pnl, 2),
                "average_price": f"{average:.2f}",
                "ltp": round(ltp, 2),
            }
        )
    return positions


def transform_positions_data(positions):
    return positions


def map_portfolio_data(portfolio_data):
    holdings = []
    for row in _rows(portfolio_data):
        quantity = int(float(row.get("quantity") or 0))
        if quantity == 0:
            continue
        # actual_buy_avg is the purchase cost; buy_avg is marked to the previous close.
        average = _f(row.get("actual_buy_avg")) or _f(row.get("buy_avg"))
        ltp = _f(row.get("ltp"))
        pnl = (ltp - average) * quantity if average and ltp else 0.0
        holdings.append(
            {
                "symbol": _symbol(row),
                "exchange": row.get("exchange", ""),
                "quantity": quantity,
                "product": str(row.get("product", "CNC")).upper(),
                "average_price": round(average, 2),
                "ltp": round(ltp, 2),
                "pnl": round(pnl, 2),
                "pnlpercent": round((ltp - average) / average * 100, 2) if average and ltp else 0.0,
            }
        )
    return holdings


def calculate_portfolio_statistics(holdings):
    holding_value = sum(h["ltp"] * h["quantity"] for h in holdings)
    invested = sum(h["average_price"] * h["quantity"] for h in holdings)
    pnl = sum(h["pnl"] for h in holdings)
    return {
        "totalholdingvalue": round(holding_value, 2),
        "totalinvvalue": round(invested, 2),
        "totalprofitandloss": round(pnl, 2),
        "totalpnlpercentage": round(pnl / invested * 100, 2) if invested else 0.0,
    }


def transform_holdings_data(holdings):
    return holdings
