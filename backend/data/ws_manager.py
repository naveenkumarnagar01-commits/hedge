"""
Resilient WebSocket connection manager.
Handles: exponential backoff reconnect, stale detection, SAFE MODE trigger.
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional

log = logging.getLogger("ws_manager")

# Import bus lazily to avoid circular imports at module level
_bus = None


def _get_bus():
    global _bus
    if _bus is None:
        from backend.message_bus import bus, FEED_STATUS, SYSTEM_STATUS
        _bus = bus
    return _bus


class WSConnection:
    """
    Manages a single WebSocket connection with exponential backoff reconnect.
    Calls on_message(parsed_dict) for every incoming frame.
    """

    def __init__(self, name: str, url: str, on_message: Callable,
                 on_connect: Optional[Callable] = None,
                 on_disconnect: Optional[Callable] = None):
        self.name         = name
        self.url          = url
        self._on_message  = on_message
        self._on_connect  = on_connect
        self._on_disconnect = on_disconnect
        self._ws          = None
        self._running     = False
        self._last_tick   = 0.0
        self._latency_ms  = 0.0
        self._status      = "disconnected"   # disconnected / ok / stale / failed
        self._task: Optional[asyncio.Task] = None
        self._backoff     = 1

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()

    async def _loop(self):
        from backend.config import cfg
        while self._running:
            try:
                await self._connect()
                self._backoff = 1       # reset on successful connection
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error(f"[{self.name}] Connection error: {e}")
                self._status = "failed"
                await self._report_status("failed")
            if self._running:
                sleep = min(self._backoff, cfg.ws_reconnect_max_sec)
                log.info(f"[{self.name}] Reconnecting in {sleep}s...")
                await asyncio.sleep(sleep)
                self._backoff = min(self._backoff * 2, cfg.ws_reconnect_max_sec)

    async def _connect(self):
        import websockets
        import ssl
        log.info(f"[{self.name}] Connecting -> {self.url}")
        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        async with websockets.connect(
            self.url,
            ping_interval=20,
            ping_timeout=15,
            close_timeout=5,
            ssl=ssl_context,
        ) as ws:
            self._ws = ws
            self._status = "ok"
            self._last_tick = time.time()
            log.info(f"[{self.name}] Connected.")
            await self._report_status("ok")
            if self._on_connect:
                await self._on_connect(ws)

            asyncio.create_task(self._stale_watcher())

            async for raw in ws:
                recv_ts = time.time()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Tick timestamp validation
                tick_ts = _extract_ts(msg)
                if tick_ts and (recv_ts - tick_ts) > 1.0:
                    log.warning(f"[{self.name}] Stale tick: age={recv_ts - tick_ts:.2f}s — dropped")
                    continue

                self._last_tick  = recv_ts
                self._latency_ms = (recv_ts - tick_ts) * 1000 if tick_ts else 0
                self._status     = "ok"

                try:
                    await self._on_message(msg)
                except Exception as e:
                    log.error(f"[{self.name}] on_message error: {e}")

            # Normal close
            self._status = "disconnected"
            if self._on_disconnect:
                await self._on_disconnect()

    async def _stale_watcher(self):
        from backend.config import cfg
        while self._running and self._status == "ok":
            await asyncio.sleep(1)
            age = time.time() - self._last_tick
            if age > cfg.safe_mode_timeout_sec:
                self._status = "stale"
                await self._report_status("stale")
                log.warning(f"[{self.name}] Feed stale for {age:.1f}s -- SAFE MODE triggered")
                from backend.message_bus import bus, SYSTEM_STATUS
                await bus.publish(SYSTEM_STATUS, {
                    "mode": "safe",
                    "feed": self.name,
                    "age_sec": age,
                }, source=self.name)
            elif self._status == "stale" and age < 1:
                self._status = "ok"
                await self._report_status("ok")

    async def _report_status(self, status: str):
        from backend.message_bus import bus, FEED_STATUS
        await bus.publish(FEED_STATUS, {
            "feed":       self.name,
            "status":     status,
            "latency_ms": self._latency_ms,
        }, source=self.name)

    def is_healthy(self) -> bool:
        return self._status == "ok"

    @property
    def latency_ms(self) -> float:
        return self._latency_ms

    @property
    def status(self) -> str:
        return self._status


def _extract_ts(msg: dict) -> Optional[float]:
    """
    Find an event timestamp (ms) from Binance WS message formats.
    Combined streams wrap payload as {"stream":"...","data":{...}}, so we
    check both the top-level and the nested 'data' dict for E/T/t keys.
    """
    for src in (msg, msg.get("data") if isinstance(msg.get("data"), dict) else {}):
        if not src:
            continue
        for key in ("E", "T", "t"):
            val = src.get(key)
            if val and isinstance(val, (int, float)) and val > 1_000_000_000_000:
                return val / 1000.0
    return None
