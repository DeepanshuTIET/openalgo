"""Master Trust market-data adapter: Tradelab binary feed -> ZeroMQ.

One socket carries every subscription. Each instrument is subscribed to the
"marketdata" stream (LTP, OHLC, volume, OI); instruments with a depth
subscriber are also subscribed to "full_snapquote" (five-level book). The two
packet kinds are merged per instrument so a DEPTH tick carries LTP as well.
Runs in the websocket proxy process, so plain threads are real threads here.

Subscribe and unsubscribe only record the wanted state and start a short timer.
The flush sends the difference between what is wanted and what the broker
already has, in frames of SUBSCRIBE_CHUNK, so a client subscribing to 500
symbols one call at a time costs a few frames instead of 500. A reconnect is
the same flush with nothing sent yet.
"""

import threading
import time

import websocket

from broker.mastertrust.api.baseurl import get_client_id
from broker.mastertrust.streaming.feed import (
    EXCHANGE_CODES,
    MODE_MARKETDATA,
    MODE_SNAPQUOTE,
    chunked,
    decode,
    feed_url,
    subscribe_message,
)
from database.auth_db import get_auth_token
from utils.logging import get_logger
from websocket_proxy.base_adapter import BaseBrokerWebSocketAdapter
from websocket_proxy.mapping import SymbolMapper

MODE_TOPICS = {1: "LTP", 2: "QUOTE", 3: "DEPTH"}


class MastertrustWebSocketAdapter(BaseBrokerWebSocketAdapter):
    HEARTBEAT_SECONDS = 15
    MAX_BACKOFF = 60
    # Long enough to gather a proxy's per-symbol subscribe loop, short enough
    # that a single click is not noticeably delayed.
    BATCH_DELAY = 0.1

    def __init__(self):
        super().__init__()
        self.logger = get_logger("mastertrust_websocket")
        self.user_id = None
        self.ws = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        # (exchange_code, token) -> {"symbol", "exchange", "modes": set, "last": merged fields}
        self.instruments = {}
        # What the broker holds on sent_ws, per feed mode. Touched only under flush_lock.
        self.flush_lock = threading.Lock()
        self.sent_ws = None
        self.sent = {MODE_MARKETDATA: set(), MODE_SNAPQUOTE: set()}
        self.batch_timer = None

    def initialize(self, broker_name, user_id, auth_data=None):
        self.user_id = user_id
        if not get_auth_token(user_id, bypass_cache=True):
            raise ValueError(f"No Master Trust session for user {user_id}. Please log in again.")

    def connect(self):
        with self.lock:
            # A thread still winding down after disconnect() is not a live feed:
            # it exits on its own stop_event, so start a fresh one beside it.
            if self.thread and self.thread.is_alive() and not self.stop_event.is_set():
                return
            self.stop_event = threading.Event()
            self.thread = threading.Thread(
                target=self._run, args=(self.stop_event,), daemon=True, name="mastertrust-feed"
            )
            self.thread.start()

    def disconnect(self):
        with self.lock:
            self.stop_event.set()
            self.connected = False
            ws, self.ws = self.ws, None
            timer, self.batch_timer = self.batch_timer, None
        if timer:
            timer.cancel()
        if ws:
            ws.close()
        self.cleanup_zmq()

    def _run(self, stop_event):
        backoff = 2
        while not stop_event.is_set():
            # Re-read the token every time: it rolls over daily around 3 AM IST.
            auth = get_auth_token(self.user_id, bypass_cache=True)
            if auth:
                ws = websocket.WebSocketApp(
                    feed_url(get_client_id(auth) or "", auth),
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=lambda _ws, error: self.logger.error(
                        f"Master Trust feed error: {error}"
                    ),
                    on_close=self._on_close,
                )
                with self.lock:
                    if stop_event.is_set():
                        break
                    self.ws = ws
                started = time.monotonic()
                ws.run_forever(ping_interval=20, ping_timeout=10)
                if time.monotonic() - started > 60:
                    backoff = 2  # it was a working connection, not a failing one
            else:
                self.logger.error("No Master Trust token; retrying the feed connection later")
            if stop_event.wait(backoff):
                break
            backoff = min(backoff * 2, self.MAX_BACKOFF)

    def _on_open(self, ws):
        with self.lock:
            if ws is not self.ws:
                ws.close()
                return
            self.connected = True
            stop_event = self.stop_event
            count = len(self.instruments)
        self.logger.info(f"Master Trust feed connected; resubscribing {count} instruments")
        self._flush()
        threading.Thread(
            target=self._heartbeat, args=(ws, stop_event), daemon=True, name="mastertrust-heartbeat"
        ).start()

    def _heartbeat(self, ws, stop_event):
        while not stop_event.wait(self.HEARTBEAT_SECONDS) and ws is self.ws:
            try:
                ws.send('{"a": "h"}')
            except Exception:
                return

    def _on_close(self, ws, code=None, message=None):
        self.logger.info(f"Master Trust feed closed: {code} {message}")
        with self.lock:
            if ws is self.ws:
                self.connected = False

    def _schedule_flush(self):
        # Fixed window from the first change: later calls join it rather than
        # pushing it back, so a steady stream of subscribes still flushes.
        with self.lock:
            if self.batch_timer:
                return
            self.batch_timer = threading.Timer(self.BATCH_DELAY, self._flush)
            self.batch_timer.daemon = True
            self.batch_timer.start()

    def _flush(self):
        with self.flush_lock:
            with self.lock:
                self.batch_timer = None
                ws = self.ws if self.connected else None
                want = {
                    MODE_MARKETDATA: set(self.instruments),
                    MODE_SNAPQUOTE: {k for k, v in self.instruments.items() if 3 in v["modes"]},
                }
            if not ws:
                return  # _on_open flushes everything once connected
            if ws is not self.sent_ws:
                # A new socket starts with no subscriptions at the broker.
                self.sent_ws = ws
                self.sent = {MODE_MARKETDATA: set(), MODE_SNAPQUOTE: set()}
            for mode in (MODE_MARKETDATA, MODE_SNAPQUOTE):
                sent = self.sent[mode]
                for action, keys, apply in (
                    ("unsubscribe", sent - want[mode], sent.difference_update),
                    ("subscribe", want[mode] - sent, sent.update),
                ):
                    for part in chunked(keys):
                        try:
                            ws.send(subscribe_message(action, mode, part))
                        except Exception:
                            self.logger.exception(
                                f"Master Trust feed {action} failed; "
                                "it is retried on the next change or reconnect"
                            )
                            return
                        apply(part)

    def subscribe(self, symbol, exchange, mode=2, depth_level=5):
        if mode not in MODE_TOPICS:
            return self._create_error_response(
                "INVALID_MODE", "Mode must be 1 (LTP), 2 (Quote) or 3 (Depth)"
            )
        token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
        if not token_info or exchange not in EXCHANGE_CODES:
            return self._create_error_response(
                "SYMBOL_NOT_FOUND", f"Symbol {symbol} not found on {exchange}"
            )
        key = (EXCHANGE_CODES[exchange], int(token_info["token"]))
        with self.lock:
            entry = self.instruments.setdefault(
                key, {"symbol": symbol, "exchange": exchange, "modes": set(), "last": {}}
            )
            entry["modes"].add(mode)
            self.subscriptions[f"{symbol}_{exchange}_{mode}"] = {
                "symbol": symbol,
                "exchange": exchange,
                "mode": mode,
            }
        self._schedule_flush()
        return self._create_success_response(
            f"Subscribed to {symbol}.{exchange}",
            symbol=symbol,
            exchange=exchange,
            mode=mode,
            requested_depth=depth_level,
            actual_depth=5,
            is_fallback=depth_level != 5,
        )

    def unsubscribe(self, symbol, exchange, mode=2):
        token_info = SymbolMapper.get_token_from_symbol(symbol, exchange)
        if not token_info or exchange not in EXCHANGE_CODES:
            return self._create_error_response(
                "SYMBOL_NOT_FOUND", f"Symbol {symbol} not found on {exchange}"
            )
        key = (EXCHANGE_CODES[exchange], int(token_info["token"]))
        with self.lock:
            self.subscriptions.pop(f"{symbol}_{exchange}_{mode}", None)
            entry = self.instruments.get(key)
            if not entry or mode not in entry["modes"]:
                return self._create_success_response(f"{symbol}.{exchange} was not subscribed")
            entry["modes"].discard(mode)
            if not entry["modes"]:
                del self.instruments[key]
        self._schedule_flush()
        return self._create_success_response(
            f"Unsubscribed from {symbol}.{exchange}", symbol=symbol, exchange=exchange, mode=mode
        )

    def _on_message(self, ws, message):
        decoded = decode(message)
        if not decoded:
            return
        packet_mode, code, token, fields = decoded
        entry = self.instruments.get((code, token))  # dict read is atomic; no lock per tick
        if not entry:
            return
        last = entry["last"]
        last.update(fields)
        symbol, exchange, modes = entry["symbol"], entry["exchange"], tuple(entry["modes"])
        now = int(time.time() * 1000)
        base = {
            "symbol": symbol,
            "exchange": exchange,
            "ltp": last.get("ltp", 0),
            "ltt": last.get("ltt", 0),
            "timestamp": now,
        }
        close = last.get("close", 0)
        quote = {
            **base,
            "open": last.get("open", 0),
            "high": last.get("high", 0),
            "low": last.get("low", 0),
            "close": close,
            "volume": last.get("volume", 0),
            "last_quantity": last.get("last_trade_quantity", 0),
            "average_price": last.get("average_price", 0),
            "total_buy_quantity": last.get("total_buy_quantity", 0),
            "total_sell_quantity": last.get("total_sell_quantity", 0),
            "oi": last.get("oi", 0),
            "change": round(last.get("ltp", 0) - close, 2) if close else 0,
            "change_percent": round((last.get("ltp", 0) - close) / close * 100, 2) if close else 0,
        }
        for mode in modes:
            if mode == 3:
                if "depth" not in last:
                    continue
                payload = {**quote, "depth": last["depth"]}
            elif packet_mode != MODE_MARKETDATA:
                continue  # a book update changes nothing an LTP or Quote subscriber sees
            else:
                payload = base if mode == 1 else quote
            self.publish_market_data(
                f"{exchange}_{symbol}_{MODE_TOPICS[mode]}", {**payload, "mode": mode}
            )
