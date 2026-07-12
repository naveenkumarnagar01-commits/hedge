"""
BTC USD-M Futures WebSocket feed.

2-Step Mark Price Verification (production requirement):
  Step 1 — Freshness check: mark_price must have updated within STALE_SEC seconds.
  Step 2 — Sanity check: new mark must be within MAX_DRIFT_PCT of last known good.

If either check fails → fall back to bid/ask mid and flag as stale.
Downstream consumers (executors, options intrinsic) always call get_verified_mark()
so they never act on frozen data.
"""

import json
import logging
import time
import urllib.request
from collections import deque
from backend.data.ws_manager import WSConnection
from backend.message_bus import bus, TICK_FUTURES, CANDLE_CLOSE, DEPTH_FUTURES, FEED_STATUS

log = logging.getLogger("futures_feed")

_URL = ("wss://fstream.binance.com/stream?streams="
        "btcusdt@markPrice@1s/btcusdt@bookTicker/btcusdt@depth20@100ms/"
        "btcusdt@kline_1m/btcusdt@kline_5m/btcusdt@kline_15m/"
        "btcusdt@kline_1h/btcusdt@kline_4h/btcusdt@kline_1d")

# Staleness thresholds
_STALE_SEC      = 6.0    # mark not updated in >6s → stale (REST poller refreshes every 3s)
_MAX_DRIFT_PCT  = 0.02   # mark moved >2% from last good → sanity fail → use mid

_state = {
    "mark_price":     0.0,
    "mark_ts":        0.0,   # when mark_price was last updated
    "mark_good":      0.0,   # last verified-good mark (for drift check)
    "premium":        0.0,
    "bid":            0.0,
    "ask":            0.0,
    "ts":             0.0,
    "bid_ts":         0.0,   # when bid/ask was last updated
    "stale":          False, # True when mark is stale, using fallback
}
_depth  = {"bids": [], "asks": [], "ts": 0.0}
_candles_map: dict[str, deque] = {
    "1m":  deque(maxlen=1000),
    "5m":  deque(maxlen=1000),
    "15m": deque(maxlen=1000),
    "1h":  deque(maxlen=1000),
    "4h":  deque(maxlen=1000),
    "1d":  deque(maxlen=1000),
    "1w":  deque(maxlen=500),   # weekly — REST-only, seeded on first request
}


# ── 2-Step verified mark price ────────────────────────────────────────────────

def get_verified_mark() -> float:
    """
    Step 1: Is mark_price fresh (updated within STALE_SEC)?
    Step 2: Is mark_price within MAX_DRIFT_PCT of last known good?
    If either fails: return bid/ask mid (fresh from bookTicker stream).
    """
    now         = time.time()
    mark        = _state["mark_price"]
    mark_ts     = _state["mark_ts"]
    mark_good   = _state["mark_good"]
    bid         = _state["bid"]
    ask         = _state["ask"]
    bid_ts      = _state["bid_ts"]

    # Step 1: freshness
    mark_age = now - mark_ts
    if mark > 0 and mark_age <= _STALE_SEC:
        # Step 2: sanity — max drift from last known good
        if mark_good > 0:
            drift = abs(mark - mark_good) / mark_good
            if drift > _MAX_DRIFT_PCT:
                # Mark jumped too far — suspect bad tick, use mid
                log.warning(
                    f"Mark price sanity FAIL: {mark_good:.2f} -> {mark:.2f} "
                    f"({drift*100:.2f}% drift > {_MAX_DRIFT_PCT*100}%) -- using bid/ask mid"
                )
                _state["stale"] = True
                if bid > 0 and ask > 0:
                    return round((bid + ask) / 2, 2)
                return mark_good

        # Both checks pass
        _state["mark_good"]  = mark
        _state["stale"]      = False
        return mark

    # Step 1 failed — mark is stale
    if not _state["stale"]:
        age_str = f"{mark_age:.1f}s" if mark_ts else "never"
        log.warning(
            f"Mark price STALE (age={age_str}) -- falling back to bid/ask mid"
        )
    _state["stale"] = True

    # Fallback: use bid/ask mid if fresh
    bid_age = now - bid_ts
    if bid > 0 and ask > 0 and bid_age <= _STALE_SEC:
        return round((bid + ask) / 2, 2)

    # Last resort: return last known good
    return mark_good if mark_good > 0 else mark


def is_mark_stale() -> bool:
    return _state["stale"]


# ── WebSocket message handler ─────────────────────────────────────────────────

async def _on_message(msg: dict):
    stream = msg.get("stream", "").lower()
    data   = msg.get("data", msg)
    now    = time.time()

    if "markprice" in stream:
        raw_mp = float(data.get("p", 0) or 0)
        if raw_mp > 0:
            _state["mark_price"] = raw_mp
            _state["mark_ts"]    = now
        _state["premium"] = float(data.get("P", 0) or 0)
        _state["ts"]      = now

    elif "bookticker" in stream:
        bid = float(data.get("b", 0) or 0)
        ask = float(data.get("a", 0) or 0)
        if bid > 0 and ask > 0:
            _state["bid"]    = bid
            _state["ask"]    = ask
            _state["bid_ts"] = now
            # Bootstrap mark_price if not yet received
            if not _state["mark_price"]:
                _state["mark_price"] = round((bid + ask) / 2, 2)
                _state["mark_ts"]    = now
                _state["mark_good"]  = _state["mark_price"]

    elif "depth20" in stream:
        raw_b = data.get("b", [])
        raw_a = data.get("a", [])
        _depth["bids"] = [[float(p), float(q)] for p, q in raw_b]
        _depth["asks"] = [[float(p), float(q)] for p, q in raw_a]
        _depth["ts"]   = now
        await bus.publish(DEPTH_FUTURES, dict(_depth), source="futures_feed")

    elif "kline" in stream:
        k = data.get("k", {})
        tf = k.get("i", "5m")
        candle = {
            "open":     float(k.get("o", 0)),
            "high":     float(k.get("h", 0)),
            "low":      float(k.get("l", 0)),
            "close":    float(k.get("c", 0)),
            "volume":   float(k.get("v", 0)),
            "ts":       int(k.get("t", 0)),
            "is_final": k.get("x", False),
        }
        if tf in _candles_map:
            if candle["is_final"]:
                _candles_map[tf].append(candle)
            
            # Always publish live candle update (even if not final)
            await bus.publish(CANDLE_CLOSE, {
                "symbol": "BTCUSDT", "tf": tf, "candle": candle,
            }, source="futures_feed")

    # Always publish TICK_FUTURES using the 2-step verified mark
    verified_mark = get_verified_mark()
    if verified_mark:
        await bus.publish(TICK_FUTURES, {
            "mark_price": verified_mark,
            "premium":    _state["premium"],
            "bid":        _state["bid"],
            "ask":        _state["ask"],
            "stale":      _state["stale"],
            "ts":         now,
        }, source="futures_feed")

        # Feed status
        await bus.publish(FEED_STATUS, {
            "feed":       "futures_feed",
            "status":     "stale" if _state["stale"] else "ok",
            "latency_ms": 0,
        }, source="futures_feed")


_conn: WSConnection = None


async def _mark_price_poller():
    """
    Fallback: poll REST every 3s for markPrice when WS mark is stale OR WS is fully down.
    Also publishes TICK_FUTURES so executors receive price ticks even without a WS connection —
    without this, self.current_price stays 0 and _step() returns immediately, blocking all
    OB analysis and trade logic.
    """
    import asyncio as _asyncio
    _last_tick_ts = 0.0   # throttle TICK_FUTURES publish to once per 3s from REST

    while True:
        await _asyncio.sleep(3)
        try:
            mark_age = time.time() - _state["mark_ts"]
            # Run REST fetch if WS mark is stale OR WS is fully down (mark never updated)
            if _state["stale"] or mark_age > 5.0:
                url = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
                loop = _asyncio.get_event_loop()
                raw  = await loop.run_in_executor(None, lambda: _http_get(url))
                import json as _json
                data = _json.loads(raw)
                mp   = float(data.get("markPrice", 0) or 0)
                if mp > 0:
                    now_ts = time.time()
                    _state["mark_price"] = mp
                    _state["mark_ts"]    = now_ts
                    _state["mark_good"]  = mp
                    _state["stale"]      = False
                    log.info(f"Mark price refreshed via REST: {mp:.2f}")

                    # Publish TICK_FUTURES so executors get a price tick even without WS.
                    # This unblocks _step() which guards on `if not self.current_price: return`.
                    await bus.publish(TICK_FUTURES, {
                        "mark_price": mp,
                        "premium":    _state["premium"],
                        "bid":        _state["bid"],
                        "ask":        _state["ask"],
                        "stale":      False,
                        "ts":         now_ts,
                        "source":     "rest_poller",
                    }, source="futures_feed")
                    await bus.publish(FEED_STATUS, {
                        "feed":   "futures_feed",
                        "status": "ok",
                        "latency_ms": 0,
                    }, source="futures_feed")
        except Exception as e:
            log.warning(f"Mark price REST poll failed: {e}")


_mark_poller_task = None


async def start():
    global _conn, _mark_poller_task
    _conn = WSConnection("futures_feed", _URL, _on_message)
    await _conn.start()
    import asyncio as _asyncio

    async def _guarded_poller():
        while True:
            try:
                await _mark_price_poller()
            except _asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"[mark_poller] crashed: {e} — restarting in 5s")
                await _asyncio.sleep(5)

    _mark_poller_task = _asyncio.create_task(_guarded_poller(), name="mark_price_poller")
    log.info("Futures feed: markPrice@1s + bookTicker + kline_5m + depth20 | 2-step verify ON")


async def stop():
    if _conn:
        await _conn.stop()


def get_state() -> dict:
    return dict(_state)


def get_depth() -> dict:
    return dict(_depth)


def get_candles(n: int = 30, tf: str = "5m") -> list:
    if tf not in _candles_map:
        return []
    return list(_candles_map[tf])[-n:]


_TF_MAP = {1:"1m",3:"3m",5:"5m",15:"15m",30:"30m",60:"1h",120:"2h",240:"4h",1440:"1d"}

async def fetch_historical_candles(n: int = 300, tf = "5m") -> list:
    # Accept int (minutes) or string — convert to Binance interval string
    if isinstance(tf, int):
        tf = _TF_MAP.get(tf, "5m")
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval={tf}&limit={n}"
    try:
        import asyncio as _asyncio
        loop = _asyncio.get_event_loop()
        raw  = await loop.run_in_executor(None, lambda: _http_get(url))
        candles = []
        for k in json.loads(raw):
            c = {
                "open": float(k[1]), "high": float(k[2]),
                "low":  float(k[3]), "close": float(k[4]),
                "volume": float(k[5]), "ts": int(k[0]), "is_final": True,
            }
            candles.append(c)
            
        if tf in _candles_map:
            # Combine existing and new, then sort and deduplicate by ts
            combined = list(_candles_map[tf]) + candles
            combined.sort(key=lambda x: x["ts"])
            
            # Deduplicate (keep latest)
            unique_candles = []
            seen = set()
            for c in reversed(combined):
                if c["ts"] not in seen:
                    seen.add(c["ts"])
                    unique_candles.append(c)
            unique_candles.reverse()
            
            _candles_map[tf].clear()
            _candles_map[tf].extend(unique_candles[-1000:])
            
        log.info(f"Seeded {len(candles)} historical {tf} candles.")
        return candles
    except Exception as e:
        log.error(f"Historical candle fetch failed: {e}")
        return []


def _http_get(url: str) -> str:
    with urllib.request.urlopen(url, timeout=10) as r:
        return r.read().decode()
