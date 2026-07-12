"""
Resilient WebSocket connection manager.
Handles: exponential backoff reconnect, stale detection, force-reconnect, SAFE MODE.

Key design decisions:
  - _last_tick updates on EVERY received frame (connection-alive indicator).
    Data freshness is validated downstream by get_verified_mark() — not here.
  - _stale_watcher runs unconditionally (while _running), not just while "ok".
    This allows it to detect recovery after going stale.
  - If feed is stale for >10s, stale_watcher force-closes the WS socket.
    This triggers immediate reconnect via _loop() instead of waiting 35s for
    the websockets library's ping/pong timeout (ping_interval=20 + timeout=15).
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional

log = logging.getLogger("ws_manager")

_bus = None


def _get_bus():
    global _bus
    if _bus is None:
        from backend.message_bus import bus
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
        self.name           = name
        self.url            = url
        self._on_message    = on_message
        self._on_connect    = on_connect
        self._on_disconnect = on_disconnect
        self._ws            = None
        self._running       = False
        self._last_tick     = 0.0
        self._latency_ms    = 0.0
        self._status        = "disconnected"
        self._task: Optional[asyncio.Task]       = None
        self._stale_task: Optional[asyncio.Task] = None
        self._backoff       = 1

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop(), name=f"ws_{self.name}")

    async def stop(self):
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task:
            self._task.cancel()
        if self._stale_task and not self._stale_task.done():
            self._stale_task.cancel()

    async def _loop(self):
        from backend.config import cfg
        while self._running:
            try:
                await self._connect()
                self._backoff = 1       # reset only after a successful connection
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
        log.info(f"[{self.name}] Connecting -> {self.url[:80]}...")

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode    = ssl.CERT_NONE

        async with websockets.connect(
            self.url,
            ping_interval=20,
            ping_timeout=15,
            close_timeout=5,
            ssl=ssl_ctx,
        ) as ws:
            self._ws         = ws
            self._status     = "ok"
            self._last_tick  = time.time()
            log.info(f"[{self.name}] Connected.")
            await self._report_status("ok")

            if self._on_connect:
                await self._on_connect(ws)

            # Cancel any leftover stale watcher from a previous connection
            if self._stale_task and not self._stale_task.done():
                self._stale_task.cancel()
            self._stale_task = asyncio.create_task(
                self._stale_watcher(), name=f"stale_{self.name}"
            )

            async for raw in ws:
                recv_ts = time.time()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Update last_tick on EVERY received frame.
                # This is the connection-alive indicator — do NOT skip stale frames here.
                # Data freshness is validated downstream (get_verified_mark()).
                self._last_tick = recv_ts
                self._status    = "ok"

                tick_ts = _extract_ts(msg)
                if tick_ts:
                    self._latency_ms = (recv_ts - tick_ts) * 1000
                    if self._latency_ms > 2000:
                        log.debug(
                            f"[{self.name}] High latency tick: {self._latency_ms:.0f}ms"
                        )

                try:
                    await self._on_message(msg)
                except Exception as e:
                    log.error(f"[{self.name}] on_message error: {e}")

            # WS closed normally
            self._status = "disconnected"
            if self._on_disconnect:
                await self._on_disconnect()

    async def _stale_watcher(self):
        """
        Runs for the lifetime of ONE connection attempt.
        - Detects when no frames arrive for > safe_mode_timeout_sec
        - Detects recovery when frames resume
        - Force-closes the socket after 10s of stale — triggers immediate reconnect
          instead of waiting 35s for the ping/pong timeout
        """
        from backend.config import cfg
        _stale_since = 0.0

        while self._running:
            await asyncio.sleep(1)

            age = time.time() - self._last_tick

            if age > cfg.safe_mode_timeout_sec:
                # Transition → stale
                if self._status == "ok":
                    self._status  = "stale"
                    _stale_since  = time.time()
                    log.warning(
                        f"[{self.name}] Feed stale ({age:.0f}s) — SAFE MODE"
                    )
                    await self._report_status("stale")
                    from backend.message_bus import bus, SYSTEM_STATUS
                    await bus.publish(SYSTEM_STATUS, {
                        "mode":    "safe",
                        "feed":    self.name,
                        "age_sec": round(age, 1),
                    }, source=self.name)

                # Force reconnect if stale for >10s (don't wait for 35s ping/pong timeout)
                if _stale_since and (time.time() - _stale_since) > 10:
                    log.warning(
                        f"[{self.name}] Stale for "
                        f"{time.time() - _stale_since:.0f}s — force-closing socket"
                    )
                    try:
                        if self._ws:
                            await self._ws.close()
                    except Exception:
                        pass
                    return   # exit watcher; _connect() exits → _loop() reconnects

            elif self._status == "stale":
                # Recovery — frames are arriving again
                self._status = "ok"
                _stale_since = 0.0
                log.info(f"[{self.name}] Feed recovered (age={age:.1f}s)")
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
    Combined streams wrap payload as {"stream":"...","data":{...}}.
    """
    for src in (msg, msg.get("data") if isinstance(msg.get("data"), dict) else {}):
        if not src:
            continue
        for key in ("E", "T", "t"):
            val = src.get(key)
            if val and isinstance(val, (int, float)) and val > 1_000_000_000_000:
                return val / 1000.0
    return None
