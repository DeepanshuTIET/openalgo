"""Master Trust funds: /api/v1/funds/view rows plus day P&L from the position book."""

from broker.mastertrust.api.baseurl import api_request, get_client_id
from broker.mastertrust.api.order_api import get_positions
from utils.logging import get_logger

logger = get_logger(__name__)


def _f(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def get_margin_data(auth_token):
    client_id = get_client_id(auth_token)
    if not client_id:
        return {}
    status, body = api_request(
        "GET", "/api/v1/funds/view", auth_token, params={"client_id": client_id, "type": "all"}
    )
    if body.get("status") != "success":
        logger.error(f"Master Trust funds failed ({status}): {body.get('message')}")
        return {}
    # data.values is [[description, value], ...]
    values = {
        str(row[0]).strip().lower(): _f(row[1])
        for row in (body.get("data") or {}).get("values", [])
        if isinstance(row, list) and len(row) >= 2
    }

    realized = unrealized = 0.0
    positions = get_positions(auth_token)
    for row in positions.get("data", []) if positions.get("status") == "success" else []:
        net = _f(row.get("net_quantity"))
        total = (
            _f(row.get("sell_amount"))
            - _f(row.get("buy_amount"))
            + net * _f(row.get("ltp")) * (_f(row.get("multiplier")) or 1.0)
        )
        realized_row = _f(row.get("realized_mtm"))
        realized += realized_row
        unrealized += total - realized_row

    return {
        "availablecash": f"{values.get('available', 0.0):.2f}",
        "collateral": f"{values.get('collateral', 0.0):.2f}",
        "m2munrealized": f"{unrealized:.2f}",
        "m2mrealized": f"{realized:.2f}",
        "utiliseddebits": f"{values.get('margin used', 0.0):.2f}",
    }
