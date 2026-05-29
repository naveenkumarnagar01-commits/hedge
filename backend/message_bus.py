"""
In-process async message bus + optional Redis pub/sub bridge.

Agents publish typed messages; subscribers receive them instantly.
If Redis is configured, messages also fan out to other processes.
"""

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("message_bus")

# ── Message types ─────────────────────────────────────────────────────────────
# Data layer
TICK_SPOT        = "TICK_SPOT"           # {price, bid, ask, ts}
TICK_FUTURES     = "TICK_FUTURES"        # {mark_price, premium, bid, ask, ts}
TICK_OPTIONS     = "TICK_OPTIONS"        # {symbol, strike, expiry, side, bid, ask, mark, iv, ts}
OPTIONS_CHAIN    = "OPTIONS_CHAIN"       # full chain snapshot {expiry: [{...}]}
CANDLE_CLOSE     = "CANDLE_CLOSE"        # {symbol, tf, ohlcv, ts}
FEED_STATUS      = "FEED_STATUS"         # {feed, status: ok/stale/failed, latency_ms}

# Analyst
ANALYST_LINES    = "ANALYST_LINES"       # {high_line, low_line, log, ts}
ANALYST_RUN_REQ  = "ANALYST_RUN_REQ"    # trigger manual re-run

# Executor
SETUP_DETECTED   = "SETUP_DETECTED"     # {executor, setup_type, trigger_price, ts}
TRADE_REQUEST    = "TRADE_REQUEST"      # {executor, action, params, ts}
TRADE_RESULT     = "TRADE_RESULT"       # {executor, action, status, fill, ts}
POSITION_UPDATE  = "POSITION_UPDATE"    # {executor, state, position, ts}
SQUAREOFF_START  = "SQUAREOFF_START"    # broadcast at 12:45 IST

# Manager
MANAGER_DECISION = "MANAGER_DECISION"   # {executor, request_type, status, reason, ts}

# Config
CONFIG_UPDATE    = "CONFIG_UPDATE"      # {changes: {k: "old→new"}}

# Events (Volatile Event Trader)
EVENT_CALENDAR   = "EVENT_CALENDAR"     # {name, type, ist, impact, country, secs_until, ts_ist}
EVENT_FIRED      = "EVENT_FIRED"        # {name, type, ist, impact, actual, estimate, previous, ts_ist}

# System
DEPTH_SPOT       = "DEPTH_SPOT"         # {bids:[[p,q]…], asks:[[p,q]…], ts}
DEPTH_FUTURES    = "DEPTH_FUTURES"      # {bids:[[p,q]…], asks:[[p,q]…], ts}
EXECUTOR_MONITOR = "EXECUTOR_MONITOR"   # {executor, price, h_line, l_line, distances, …, ts_ist}
SYSTEM_STATUS    = "SYSTEM_STATUS"      # {mode: normal/safe, ts}
LOG_EVENT        = "LOG_EVENT"          # {level, source, message, ts_ist}


class MessageBus:
    def __init__(self):
        self._subs: Dict[str, List[Callable]] = {}
        self._redis = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def connect_redis(self, url: str = "redis://localhost:6379"):
        try:
            import aioredis
            self._redis = await aioredis.from_url(url, encoding="utf-8", decode_responses=True)
            log.info(f"Redis connected: {url}")
            asyncio.create_task(self._redis_listener())
        except Exception as e:
            log.warning(f"Redis unavailable ({e}) — running in-process only")

    def subscribe(self, msg_type: str, callback: Callable):
        self._subs.setdefault(msg_type, []).append(callback)

    def unsubscribe(self, msg_type: str, callback: Callable):
        if msg_type in self._subs:
            self._subs[msg_type] = [c for c in self._subs[msg_type] if c != callback]

    async def publish(self, msg_type: str, data: Any, source: str = ""):
        msg = {
            "type":   msg_type,
            "source": source,
            "data":   data,
            "ts":     time.time(),
        }
        await self._dispatch(msg)
        if self._redis:
            try:
                await self._redis.publish("hedge_bus", json.dumps(msg))
            except Exception as e:
                log.debug(f"Redis publish error: {e}")

    async def _dispatch(self, msg: dict):
        callbacks = self._subs.get(msg["type"], []) + self._subs.get("*", [])
        for cb in callbacks:
            try:
                if asyncio.iscoroutinefunction(cb):
                    asyncio.create_task(cb(msg))
                else:
                    cb(msg)
            except Exception as e:
                log.error(f"Bus dispatch error [{msg['type']}]: {e}")

    async def _redis_listener(self):
        if not self._redis:
            return
        try:
            pub = self._redis.pubsub()
            await pub.subscribe("hedge_bus")
            async for raw in pub.listen():
                if raw["type"] != "message":
                    continue
                try:
                    msg = json.loads(raw["data"])
                    await self._dispatch(msg)
                except Exception as e:
                    log.debug(f"Redis message parse error: {e}")
        except Exception as e:
            log.error(f"Redis listener died: {e}")


# Global singleton
bus = MessageBus()
