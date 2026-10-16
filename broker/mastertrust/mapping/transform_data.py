"""OpenAlgo order -> Master Trust (Tradelab) order payload, and the enum maps both ways."""

import math
import time

from database.token_db import get_symbol_info, get_token
from utils.logging import get_logger
from utils.mpp_slab import (
    calculate_protected_price,
    get_instrument_type_from_symbol,
    get_mpp_percentage,
)

logger = get_logger(__name__)

ORDER_TYPES = {"MARKET": "MARKET", "LIMIT": "LIMIT", "SL": "SL", "SL-M": "SLM"}
REVERSE_ORDER_TYPES = {
    "MARKET": "MARKET",
    "MKT": "MARKET",
    "LIMIT": "LIMIT",
    "L": "LIMIT",
    "SL": "SL",
    "SLM": "SL-M",
    "SL-M": "SL-M",
}


class OrderRejected(Exception):
    """An order refused before it reaches the broker; the message is for the trader."""


def map_order_type(pricetype):
    return ORDER_TYPES.get(str(pricetype).upper(), "MARKET")


def reverse_map_order_type(order_type):
    return REVERSE_ORDER_TYPES.get(str(order_type).upper(), str(order_type).upper())


def map_product_type(product):
    # Tradelab speaks the OpenAlgo names directly.
    return str(product).upper() if str(product).upper() in ("CNC", "NRML", "MIS") else "MIS"


def reverse_map_product_type(product):
    return str(product).upper()


def _lot_size(symbol, exchange):
    info = get_symbol_info(symbol, exchange)
    return int(info.lotsize) if info and info.lotsize else 1


def to_broker_quantity(quantity, symbol, exchange):
    """OpenAlgo quantity is units. Tradelab counts MCX in lots (CRUDEOIL qty 1 = 100 units)."""
    quantity = int(quantity)
    if exchange != "MCX":
        return quantity
    lot = _lot_size(symbol, exchange)
    if quantity % lot:
        raise OrderRejected(
            f"{symbol} trades in multiples of {lot}. Quantity {quantity} is not a multiple."
        )
    return quantity // lot


def from_broker_quantity(quantity, symbol, exchange, lot=None):
    """Inverse of to_broker_quantity, for books read back from the broker."""
    quantity = int(float(quantity or 0))
    if exchange != "MCX":
        return quantity
    return quantity * (lot or _lot_size(symbol, exchange))


def _protected_limit(data, quote_ltp):
    """MARKET -> crossing LIMIT under the shared SEBI MPP slabs, as Pocketful (same platform) does."""
    symbol_info = get_symbol_info(data["symbol"], data["exchange"])
    return calculate_protected_price(
        price=quote_ltp,
        action=data["action"].upper(),
        symbol=data["symbol"],
        instrument_type=get_instrument_type_from_symbol(data["symbol"]),
        tick_size=symbol_info.tick_size if symbol_info else None,
    )


def transform_data(data, client_id, ltp=None):
    """Build the POST /api/v1/orders body.

    ltp: last price for MARKET emulation; when missing the order goes as a
    native MARKET with the slab protection percentage.
    """
    token = get_token(data["symbol"], data["exchange"])
    if not token:
        raise OrderRejected(f"Symbol {data['symbol']} is not available on {data['exchange']}.")
    order_type = map_order_type(data["pricetype"])
    price = float(data.get("price") or 0)
    trigger_price = float(data.get("trigger_price") or 0)
    instrument_type = get_instrument_type_from_symbol(data["symbol"])
    protection = get_mpp_percentage(trigger_price or price or (ltp or 0), instrument_type)

    if order_type == "MARKET" and ltp and ltp > 0:
        price = _protected_limit(data, ltp)
        order_type = "LIMIT"
    elif order_type == "MARKET":
        logger.warning(
            f"No LTP for {data['symbol']}; sending native MARKET with {protection}% protection"
        )
        price = 0.0

    payload = {
        "client_id": client_id,
        "exchange": data["exchange"],
        "instrument_token": str(token),
        "order_side": data["action"].upper(),
        "order_type": order_type,
        "price": price,
        "trigger_price": trigger_price,
        "product": map_product_type(data["product"]),
        "quantity": to_broker_quantity(data["quantity"], data["symbol"], data["exchange"]),
        "disclosed_quantity": int(data.get("disclosed_quantity") or 0),
        "validity": "DAY",
        "user_order_id": int(time.time() * 1000) % 1_000_000_000,
    }
    # Tradelab answered "market_protection_percentage is invalid" to 2.0 on a LIMIT;
    # Pocketful (same platform) never sends it. Only native MARKET carries it, as an int.
    if order_type == "MARKET":
        payload["market_protection_percentage"] = math.ceil(protection)
    return payload


def transform_modify_order_data(data, client_id):
    """Build the PUT /api/v1/orders body."""
    token = get_token(data["symbol"], data["exchange"])
    if not token:
        raise OrderRejected(f"Symbol {data['symbol']} is not available on {data['exchange']}.")
    return {
        "client_id": client_id,
        "oms_order_id": str(data["orderid"]),
        "exchange": data["exchange"],
        "instrument_token": int(token),
        "order_side": data["action"].upper(),
        "order_type": map_order_type(data["pricetype"]),
        "price": float(data.get("price") or 0),
        "trigger_price": float(data.get("trigger_price") or 0),
        "product": map_product_type(data["product"]),
        "quantity": to_broker_quantity(data["quantity"], data["symbol"], data["exchange"]),
        "disclosed_quantity": int(data.get("disclosed_quantity") or 0),
        "validity": "DAY",
    }
