import websocket_proxy  # noqa: F401  isort:skip

import json

from broker.mastertrust.streaming import mastertrust_adapter as adapter_module
from broker.mastertrust.streaming.feed import SUBSCRIBE_CHUNK
from broker.mastertrust.streaming.mastertrust_adapter import MastertrustWebSocketAdapter


class FakeWS:
    def __init__(self):
        self.frames = []

    def send(self, message):
        self.frames.append(json.loads(message))


def _adapter(monkeypatch):
    monkeypatch.setattr(
        adapter_module.SymbolMapper,
        "get_token_from_symbol",
        staticmethod(lambda symbol, exchange: {"token": symbol[3:]}),
    )
    adapter = MastertrustWebSocketAdapter()
    adapter.ws = FakeWS()
    adapter.connected = True
    # Flush by hand instead of on the timer.
    monkeypatch.setattr(adapter, "_schedule_flush", lambda: None)
    return adapter


def _keys(frames, action, mode):
    return [tuple(k) for f in frames if f["a"] == action and f["m"] == mode for k in f["v"]]


def test_burst_of_subscribes_goes_out_in_chunked_frames(monkeypatch):
    adapter = _adapter(monkeypatch)
    for i in range(450):
        adapter.subscribe(f"SYM{i}", "NSE", mode=2)
    for i in range(3):
        adapter.subscribe(f"SYM{i}", "NSE", mode=3)
    adapter._flush()

    frames = adapter.ws.frames
    assert len(frames) == 4  # 3 marketdata frames of <=200, 1 snapquote frame
    assert all(len(f["v"]) <= SUBSCRIBE_CHUNK for f in frames)
    assert len(set(_keys(frames, "subscribe", "marketdata"))) == 450
    assert sorted(_keys(frames, "subscribe", "full_snapquote")) == [(1, 0), (1, 1), (1, 2)]

    # Nothing changed: a second flush sends nothing.
    adapter._flush()
    assert len(adapter.ws.frames) == 4


def test_subscribe_then_unsubscribe_in_one_window_sends_nothing(monkeypatch):
    adapter = _adapter(monkeypatch)
    adapter.subscribe("SYM7", "NSE", mode=1)
    adapter.unsubscribe("SYM7", "NSE", mode=1)
    adapter._flush()
    assert adapter.ws.frames == []


def test_unsubscribe_and_reconnect_resend_from_wanted_state(monkeypatch):
    adapter = _adapter(monkeypatch)
    adapter.subscribe("SYM1", "NSE", mode=3)
    adapter.subscribe("SYM2", "NSE", mode=2)
    adapter._flush()
    adapter.unsubscribe("SYM1", "NSE", mode=3)
    adapter._flush()
    frames = adapter.ws.frames
    assert _keys(frames, "unsubscribe", "full_snapquote") == [(1, 1)]
    assert _keys(frames, "unsubscribe", "marketdata") == [(1, 1)]

    # A new socket holds nothing, so everything still wanted is sent again.
    adapter.ws = FakeWS()
    adapter._flush()
    assert adapter.ws.frames == [{"a": "subscribe", "v": [[1, 2]], "m": "marketdata"}]


def test_failed_send_is_retried_on_next_flush(monkeypatch):
    adapter = _adapter(monkeypatch)
    adapter.subscribe("SYM5", "NSE", mode=2)

    class Broken:
        def send(self, _message):
            raise OSError("socket closed")

    good = adapter.ws
    adapter.ws = Broken()
    adapter._flush()
    adapter.ws = good
    adapter._flush()
    assert good.frames == [{"a": "subscribe", "v": [[1, 5]], "m": "marketdata"}]


def test_connect_right_after_disconnect_starts_a_new_feed(monkeypatch):
    import threading

    release = threading.Event()
    runs = []

    def fake_run(self, stop_event):
        runs.append(stop_event)
        release.wait(2)  # old thread is still winding down

    monkeypatch.setattr(MastertrustWebSocketAdapter, "_run", fake_run)
    monkeypatch.setattr(MastertrustWebSocketAdapter, "cleanup_zmq", lambda self: None)
    adapter = MastertrustWebSocketAdapter()
    adapter.connect()
    adapter.connect()  # live feed: no second thread
    adapter.disconnect()
    adapter.connect()
    release.set()

    assert len(runs) == 2
    assert runs[0].is_set() and not runs[1].is_set()
