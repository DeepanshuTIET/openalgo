"""Tradelab binary market-data feed: packet decoding and one-shot snapshots.

Offsets were verified against a live capture and the REST quote for the same
instruments. All integers are big-endian; prices are paise (divide by 100) on
NSE, BSE, NFO, BFO and MCX.

  mode 1 "marketdata"     106 bytes: LTP, OHLC, volume, best bid/ask, OI
  mode 4 "full_snapquote" 170 bytes: five-level book, OHLC, volume
The server answers every subscribe with the current packet at once, even when
the market is closed, so a snapshot is: connect, subscribe, read, close.
"""

import json
import struct
import time

import websocket

from broker.mastertrust.api.baseurl import WS_URL
from utils.logging import get_logger

logger = get_logger(__name__)

# OpenAlgo exchange -> Tradelab feed exchange code.
EXCHANGE_CODES = {
    "NSE": 1,
    "NSE_INDEX": 1,
    "NFO": 2,
    "MCX": 4,
    "MCX_INDEX": 4,
    "BSE": 6,
    "BSE_INDEX": 6,
    "BFO": 7,
}
MODE_MARKETDATA, MODE_SNAPQUOTE = 1, 4
MODE_NAMES = {MODE_MARKETDATA: "marketdata", MODE_SNAPQUOTE: "full_snapquote"}
# Instruments per subscribe frame. Tradelab publishes no limit; 200 matches the
# Zerodha adapter and keeps a 3000-instrument resubscribe to small frames.
SUBSCRIBE_CHUNK = 200

_MARKETDATA = struct.Struct(">bbIIIIIIIIIQQ" + "I" * 12)  # 102 bytes used of 106
_SNAPQUOTE = struct.Struct(">bbI" + "I" * 30 + "IIIIIQQI")  # 166 bytes used of 170


def decode(packet):
    """Return (mode, exchange_code, token, fields) or None for anything else."""
    if not isinstance(packet, bytes) or len(packet) < 6:
        return None
    mode = packet[0]
    if mode == MODE_MARKETDATA and len(packet) >= _MARKETDATA.size:
        (
            _,
            exch,
            token,
            ltp,
            ltt,
            ltq,
            volume,
            bid,
            bid_qty,
            ask,
            ask_qty,
            tbq,
            tsq,
            atp,
            _exch_ts,
            open_,
            high,
            low,
            close,
            _yh,
            _yl,
            _ldpr,
            _hdpr,
            oi,
            _ioi,
        ) = _MARKETDATA.unpack_from(packet)
        return (
            mode,
            exch,
            token,
            {
                "ltp": ltp / 100,
                "ltt": ltt,
                "last_trade_quantity": ltq,
                "volume": volume,
                "bid": bid / 100,
                "bid_qty": bid_qty,
                "ask": ask / 100,
                "ask_qty": ask_qty,
                "total_buy_quantity": tbq,
                "total_sell_quantity": tsq,
                "average_price": atp / 100,
                "open": open_ / 100,
                "high": high / 100,
                "low": low / 100,
                "close": close / 100,
                "oi": oi,
            },
        )
    if mode == MODE_SNAPQUOTE and len(packet) >= _SNAPQUOTE.size:
        v = _SNAPQUOTE.unpack_from(packet)
        exch, token = v[1], v[2]
        buyers, bid_p, bid_q, sellers, ask_p, ask_q = (v[3 + i * 5 : 8 + i * 5] for i in range(6))
        return (
            mode,
            exch,
            token,
            {
                "depth": {
                    "buy": [
                        {"price": p / 100, "quantity": q, "orders": o}
                        for p, q, o in zip(bid_p, bid_q, buyers, strict=True)
                    ],
                    "sell": [
                        {"price": p / 100, "quantity": q, "orders": o}
                        for p, q, o in zip(ask_p, ask_q, sellers, strict=True)
                    ],
                },
                "total_buy_quantity": v[38],
                "total_sell_quantity": v[39],
                "volume": v[40],
            },
        )
    return None


def subscribe_message(action, mode, instruments):
    """instruments: iterable of (exchange_code, token)."""
    return json.dumps(
        {"a": action, "v": [[c, int(t)] for c, t in instruments], "m": MODE_NAMES[mode]}
    )


def chunked(instruments, size=SUBSCRIBE_CHUNK):
    instruments = list(instruments)
    for i in range(0, len(instruments), size):
        yield instruments[i : i + size]


def feed_url(client_id, auth):
    return f"{WS_URL}?login_id={client_id}&access_token={auth}"


def _connect(url, timeout, attempts=3):
    """Open the feed socket, retrying a 5xx handshake.

    Master Trust's nginx answers the upgrade with a bare 502 now and then
    (2026-10-14: one optionsmultiorder lost its underlying LTP to it); the
    next attempt succeeds. 4xx means bad credentials and is not retried.
    """
    for attempt in range(1, attempts + 1):
        try:
            return websocket.create_connection(url, timeout=timeout)
        except websocket.WebSocketBadStatusException as e:
            if (e.status_code or 0) < 500:
                raise
            logger.warning(
                f"Master Trust feed handshake returned {e.status_code} "
                f"(attempt {attempt}/{attempts})"
            )
            if attempt == attempts:
                raise Exception(
                    "Master Trust market data is temporarily unavailable. Try again in a moment."
                ) from e
            time.sleep(0.5 * attempt)


def snapshot(client_id, auth, instruments, depth=False, timeout=6.0):
    """Current packets for each (exchange_code, token): {(code, token): merged fields}.

    Instruments that sent nothing before the deadline are simply absent.
    """
    wanted = {(int(c), int(t)) for c, t in instruments}
    modes = (MODE_MARKETDATA, MODE_SNAPQUOTE) if depth else (MODE_MARKETDATA,)
    seen = {mode: set() for mode in modes}
    result = {}
    deadline = time.monotonic() + timeout
    ws = _connect(feed_url(client_id, auth), timeout)
    try:
        for mode in modes:
            for part in chunked(wanted):
                ws.send(subscribe_message("subscribe", mode, part))
        while any(len(s) < len(wanted) for s in seen.values()):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            ws.settimeout(remaining)
            try:
                decoded = decode(ws.recv())
            except websocket.WebSocketTimeoutException:
                break
            if not decoded:
                continue
            mode, code, token, fields = decoded
            key = (code, token)
            if key in wanted and mode in seen:
                seen[mode].add(key)
                result.setdefault(key, {}).update(fields)
    finally:
        ws.close()
    if len(result) < len(wanted):
        logger.warning(f"Master Trust snapshot: {len(result)}/{len(wanted)} instruments answered")
    return result
