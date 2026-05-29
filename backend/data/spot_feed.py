"""
BTC Spot WebSocket feed.
Streams: btcusdt@bookTicker + btcusdt@aggTrade + btcusdt@depth20@100ms
"""

import logging
import time
from backend.data.ws_manager import WSConnection
from backend.message_bus import bus, TICK_SPOT, DEPTH_SPOT

log = logging.getLogger("spot_feed")

_URL = ("wss://stream.binance.com:9443/stream?streams="
        "btcusdt@bookTicker/btcusdt@aggTrade/btcusdt@depth20@100ms")

_state = {"bid": 0.0, "ask": 0.0, "last_price": 0.0, "ts": 0.0}
_depth = {"bids": [], "asks": [], "ts": 0.0}   # top-20 each side


async def _on_message(msg: dict):
    stream = msg.get("stream", "")
    data   = msg.get("data", msg)

    if "bookTicker" in stream:
        _state["bid"] = float(data.get("b", 0) or 0)
        _state["ask"] = float(data.get("a", 0) or 0)
        _state["ts"]  = time.time()

    elif "aggTrade" in stream:
        _state["last_price"] = float(data.get("p", 0) or 0)

    elif "depth20" in stream:
        _depth["bids"] = [[float(p), float(q)] for p, q in data.get("bids", [])]
        _depth["asks"] = [[float(p), float(q)] for p, q in data.get("asks", [])]
        _depth["ts"]   = time.time()
        await bus.publish(DEPTH_SPOT, dict(_depth), source="spot_feed")

    if _state["bid"]:
        await bus.publish(TICK_SPOT, {
            "bid":        _state["bid"],
            "ask":        _state["ask"],
            "last_price": _state["last_price"] or (_state["bid"] + _state["ask"]) / 2,
            "ts":         _state["ts"],
        }, source="spot_feed")


_conn: WSConnection = None


async def start():
    global _conn
    _conn = WSConnection("spot_feed", _URL, _on_message)
    await _conn.start()
    log.info("Spot feed started (bookTicker + aggTrade + depth20).")


async def stop():
    if _conn:
        await _conn.stop()


def get_state() -> dict:
    return dict(_state)


def get_depth() -> dict:
    return dict(_depth)
