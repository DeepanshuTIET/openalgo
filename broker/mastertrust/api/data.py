"""Master Trust market data.

Quotes, multi-quotes and depth come from a one-shot snapshot on the Tradelab
feed socket (the REST quote has no OI and no depth). History comes from
/api/v1/charts/tdv. NSE and BSE index candles are served only under
NSE_INDICES and BSE_INDICES: the plain NSE/BSE codes the master contract
stores answer an index token with an empty list. MCX indices work under MCX.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd

from broker.mastertrust.api.baseurl import api_request, get_client_id
from broker.mastertrust.streaming.feed import EXCHANGE_CODES, snapshot
from database.token_db import get_brexchange, get_token, get_tokens_bulk
from utils.logging import get_logger

logger = get_logger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
HISTORY_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "oi"]
HISTORY_INDEX_EXCHANGES = {"NSE_INDEX": "NSE_INDICES", "BSE_INDEX": "BSE_INDICES"}


def _quote(fields):
    return {
        "ask": fields.get("ask", 0),
        "bid": fields.get("bid", 0),
        "ask_qty": fields.get("ask_qty", 0),
        "bid_qty": fields.get("bid_qty", 0),
        "high": fields.get("high", 0),
        "low": fields.get("low", 0),
        "ltp": fields.get("ltp", 0),
        "open": fields.get("open", 0),
        "prev_close": fields.get("close", 0),
        "volume": fields.get("volume", 0),
        "oi": fields.get("oi", 0),
    }


class BrokerData:
    def __init__(self, auth_token):
        self.auth_token = auth_token
        # OpenAlgo interval -> (candletype, data_duration). candletype 1 is
        # minutes aggregated by data_duration, 2 is hourly, 3 is daily.
        self.timeframe_map = {
            "1m": (1, 1),
            "3m": (1, 3),
            "5m": (1, 5),
            "10m": (1, 10),
            "15m": (1, 15),
            "30m": (1, 30),
            "1h": (2, None),
            "D": (3, None),
        }

    def _snapshot(self, keyed, depth=False):
        """keyed: {(exchange_code, token): anything} -> {(code, token): fields}."""
        return snapshot(
            get_client_id(self.auth_token) or "", self.auth_token, keyed.keys(), depth=depth
        )

    def _instrument(self, symbol, exchange):
        token = get_token(symbol, exchange)
        if not token or exchange not in EXCHANGE_CODES:
            raise Exception(f"Symbol {symbol} is not available on {exchange} for Master Trust")
        return EXCHANGE_CODES[exchange], int(token)

    def get_quotes(self, symbol, exchange):
        key = self._instrument(symbol, exchange)
        fields = self._snapshot({key: None}).get(key)
        if fields is None:
            raise Exception(f"Master Trust sent no quote for {exchange}:{symbol}. Try again.")
        return _quote(fields)

    def get_multiquotes(self, symbols):
        results, wanted = [], {}
        tokens = get_tokens_bulk([(s["symbol"], s["exchange"]) for s in symbols])
        for item, token in zip(symbols, tokens, strict=True):
            code = EXCHANGE_CODES.get(item["exchange"])
            if not token or code is None:
                results.append({**item, "error": "Symbol not found"})
                continue
            wanted.setdefault((code, int(token)), []).append(item)
        if wanted:
            data = self._snapshot(wanted)
            for key, items in wanted.items():
                for item in items:
                    fields = data.get(key)
                    entry = {"symbol": item["symbol"], "exchange": item["exchange"]}
                    entry.update(
                        {"data": _quote(fields)} if fields else {"error": "No data received"}
                    )
                    results.append(entry)
        return results

    def get_depth(self, symbol, exchange):
        key = self._instrument(symbol, exchange)
        fields = self._snapshot({key: None}, depth=True).get(key)
        if fields is None:
            raise Exception(
                f"Master Trust sent no market depth for {exchange}:{symbol}. Try again."
            )
        depth = fields.get("depth", {"buy": [], "sell": []})
        empty = {"price": 0, "quantity": 0, "orders": 0}
        bids = (depth["buy"][:5] + [empty] * 5)[:5]
        asks = (depth["sell"][:5] + [empty] * 5)[:5]
        return {
            "bids": bids,
            "asks": asks,
            "high": fields.get("high", 0),
            "low": fields.get("low", 0),
            "ltp": fields.get("ltp", 0),
            "ltq": fields.get("last_trade_quantity", 0),
            "oi": fields.get("oi", 0),
            "open": fields.get("open", 0),
            "prev_close": fields.get("close", 0),
            "totalbuyqty": fields.get("total_buy_quantity", 0),
            "totalsellqty": fields.get("total_sell_quantity", 0),
            "volume": fields.get("volume", 0),
        }

    get_market_depth = get_depth

    def get_history(self, symbol, exchange, interval, start_date, end_date):
        if interval not in self.timeframe_map:
            raise Exception(
                f"Interval {interval} is not supported. Use one of: {', '.join(self.timeframe_map)}"
            )
        token = get_token(symbol, exchange)
        if not token:
            raise Exception(f"Symbol {symbol} is not available on {exchange} for Master Trust")
        candletype, duration = self.timeframe_map[interval]
        brexchange = (
            HISTORY_INDEX_EXCHANGES.get(exchange) or get_brexchange(symbol, exchange) or exchange
        )

        start = datetime.strptime(str(start_date)[:10], "%Y-%m-%d").replace(tzinfo=IST)
        end = datetime.strptime(str(end_date)[:10], "%Y-%m-%d").replace(tzinfo=IST) + timedelta(
            days=1
        )
        # ponytail: the server has no published range cap; chunks only keep payloads modest.
        chunk = timedelta(days={1: 60, 2: 180, 3: 2000}[candletype])

        candles = []
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + chunk, end)
            params = {
                "exchange": brexchange,
                "token": token,
                "candletype": candletype,
                "starttime": int(chunk_start.timestamp()),
                "endtime": int(chunk_end.timestamp()) - 1,
            }
            if duration:
                params["data_duration"] = duration
            status, body = api_request(
                "GET", "/api/v1/charts/tdv", self.auth_token, params=params, timeout=30.0
            )
            if body.get("status") != "success":
                raise Exception(
                    f"Master Trust history request failed: {body.get('message', status)}"
                )
            candles.extend((body.get("data") or {}).get("candles") or [])
            chunk_start = chunk_end

        if not candles:
            return pd.DataFrame(columns=HISTORY_COLUMNS)
        df = pd.DataFrame([c[:6] for c in candles], columns=HISTORY_COLUMNS[:6])
        ts = pd.to_datetime(df["timestamp"], format="%Y-%m-%dT%H:%M:%S%z", utc=True)
        if candletype == 3:
            # Daily candles land on IST midnight (the zerodha convention): the
            # IST trade date read as a UTC midnight.
            ts = pd.to_datetime((ts + pd.Timedelta(hours=5, minutes=30)).dt.date).dt.tz_localize(
                "UTC"
            )
        df["timestamp"] = (ts - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(seconds=1)
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
        # tdv candles are [ts, o, h, l, c, v] on every exchange, F&O included
        # (probed 2026-10-14 on NIFTY and CRUDEOIL futures): there is no OI.
        df["oi"] = 0
        df = df.sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)
        return _repair_candles(df, exchange, symbol, interval)

    def get_intervals(self):
        return list(self.timeframe_map)


def _repair_candles(df, exchange, symbol, interval):
    """Make every candle satisfy low <= open, close <= high with volume >= 0.

    Chart clients reject the whole series over one bad bar ("History contains
    an invalid candle"). tdv serves such bars: NSE TCS 5m 2026-09-14 came back
    with volume -5,031,805, and 2026-09-21 09:15 with open 2135.1 above high
    2131.3. Same repair as broker/kotak/api/data.py::_repair_candles: widen the
    wick to cover the traded open/close, zero an impossible volume, drop rows
    missing a price.
    """
    ohlc = ["open", "high", "low", "close"]
    df[ohlc] = df[ohlc].apply(pd.to_numeric, errors="coerce")
    usable = df[ohlc].notna().all(axis=1)
    if not usable.all():
        logger.warning(
            f"HISTORY API - Dropping {int((~usable).sum())} candle(s) with missing "
            f"OHLC for {exchange}:{symbol} {interval}"
        )
        df = df[usable].reset_index(drop=True)
    fixed = int(((df[ohlc].max(axis=1) != df["high"]) | (df[ohlc].min(axis=1) != df["low"])).sum())
    negative = int((df["volume"] < 0).sum())
    if fixed or negative:
        logger.info(
            f"HISTORY API - Repaired {fixed} wick(s), zeroed {negative} negative "
            f"volume(s) for {exchange}:{symbol} {interval}"
        )
    df["high"] = df[ohlc].max(axis=1)
    df["low"] = df[ohlc].min(axis=1)
    df["volume"] = df["volume"].clip(lower=0)
    return df
