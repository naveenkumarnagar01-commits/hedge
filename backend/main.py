"""
Hedge Platform - Backend Entry Point.

Agents:
  - Spot + Futures + Options WebSocket feeds
  - AnalystAgent     : daily line computation (configurable IST time), runs at startup too
  - ManagerAgent     : risk gate for position size
  - BullishExecutor  : paper - long futures + ITM PUT hedge
  - BearishExecutor  : paper - short futures + ITM CALL hedge

Run:
  cd hedge_platform
  python -m backend.main
"""

import asyncio
import logging
import os
import sys

# Fix: python -m does NOT add backend.main to sys.modules (only __main__).
# gateway.py uses lazy 'import backend.main as m' which would re-import it
# fresh (new executor instances, price=0, no state) without this line.
if "backend.main" not in sys.modules:
    sys.modules["backend.main"] = sys.modules[__name__]

import uvicorn

from backend.message_bus import bus
from backend.config import cfg
from backend.state_store import store
from backend.utils import ist_now, ist_now_str

# Force UTF-8 on Windows console to avoid UnicodeEncodeError from log messages
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)-22s] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("main")

# ── Agent singletons ──────────────────────────────────────────────────────────
from backend.agents.analyst          import analyst
from backend.agents.manager          import manager
from backend.agents.bullish_executor import BullishExecutor
from backend.agents.bearish_executor import BearishExecutor
from backend.agents.volatile_trader  import VolatileTrader
from backend.execution.paper_engine  import PaperEngine
from backend.data.event_calendar     import event_calendar

# Each trader has an INDEPENDENT $100k virtual balance — no shared state
_bull_paper = PaperEngine(); _bull_paper._state_key = "paper_engine_state_bull"
_bear_paper = PaperEngine(); _bear_paper._state_key = "paper_engine_state_bear"
_vol_paper  = PaperEngine(); _vol_paper._state_key  = "paper_engine_state_vol"

bullish  = BullishExecutor(is_paper=True, force_window=False, paper_engine=_bull_paper)
bearish  = BearishExecutor(is_paper=True, force_window=False, paper_engine=_bear_paper)
volatile = VolatileTrader( is_paper=True, paper_engine=_vol_paper)


async def _squareoff_broadcaster(executor, h_key: str, m_key: str):
    """
    Fires SQUAREOFF_START for bull/bear at their configured force-close time.
    Uses wall-clock sleep (safe for bull/bear whose windows are within a single
    calendar day). VolatileTrader handles its own squareoff inside _step().
    """
    from backend.message_bus import SQUAREOFF_START
    from datetime import timedelta
    name = executor.name
    while True:
        try:
            now_ist = ist_now()
            sq_h    = int(getattr(cfg, h_key) or 0)   # int() — never falsy-0 issue
            sq_m    = int(getattr(cfg, m_key) or 0)
            sq_time = now_ist.replace(hour=sq_h, minute=sq_m, second=0, microsecond=0)
            if now_ist >= sq_time:
                sq_time += timedelta(days=1)
            wait_sec = (sq_time - now_ist).total_seconds()
            # Clamp: always wait at least 60s to prevent tight-loop on bad config
            wait_sec = max(wait_sec, 60)
            log.info(f"[{name}] Squareoff in {wait_sec/3600:.2f}h at {sq_time.strftime('%H:%M')} IST")
            await asyncio.sleep(wait_sec)
            log.info(f"[{name}] Broadcasting SQUAREOFF_START")
            await bus.publish(SQUAREOFF_START, {"ts_ist": ist_now_str(), "executor": name}, source="main")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"[{name}] Squareoff broadcaster error: {e} — retrying in 60s")
            await asyncio.sleep(60)


async def _state_persister():
    """Persist executor states every 5s for crash recovery."""
    while True:
        await asyncio.sleep(5)
        for ex in (bullish, bearish, volatile):
            try:
                await store.set(f"{ex.name}_state", ex._serialize())
            except Exception as e:
                log.error(f"State persist failed for {ex.name}: {e}")


async def startup():
    from backend import telegram_alert as tg

    log.info("=" * 64)
    log.info("  HEDGE TRADER  -  3-Trader Platform")
    log.info("  Bull | Bear | Volatile — each $100k independent balance")
    log.info(f"  {ist_now_str()}")
    log.info("=" * 64)

    # 1. State store
    db_url = os.environ.get("DATABASE_URL", "")
    await store.connect(db_url)

    # 2. Restore persisted user config
    user_cfg = store.get("user_config")
    if user_cfg:
        for k, v in user_cfg.items():
            if hasattr(cfg, k):
                try:
                    setattr(cfg, k, type(getattr(cfg, k))(v))
                except Exception:
                    setattr(cfg, k, v)
        log.info(f"User config restored: {len(user_cfg)} key(s) from database.")

    # 3. Redis (optional)
    redis_url = os.environ.get("REDIS_URL", "")
    if redis_url:
        await bus.connect_redis(redis_url)

    # 4. Data feeds (seed candles for all TFs used by traders
    from backend.data import spot_feed, futures_feed, options_feed
    await futures_feed.fetch_historical_candles(500, "5m")
    await futures_feed.fetch_historical_candles(500, "15m")
    await futures_feed.fetch_historical_candles(300, "1h")
    await futures_feed.fetch_historical_candles(200, "4h")
    await spot_feed.start()
    await futures_feed.start()
    await options_feed.start()
    log.info("WebSocket data feeds started.")

    # 5. Event calendar (must start before volatile trader subscribes)
    await event_calendar.start()

    # 6. Agents
    await analyst.start()
    await manager.start()
    await bullish.start()
    await bearish.start()
    await volatile.start()
    log.info("All 3 traders started.")

    # 6. Background tasks
    asyncio.create_task(_squareoff_broadcaster(bullish, "bull_force_close_h", "bull_force_close_m"))
    asyncio.create_task(_squareoff_broadcaster(bearish, "bear_force_close_h", "bear_force_close_m"))
    # NOTE: VolatileTrader manages its own squareoff inside _step() — no broadcaster needed
    asyncio.create_task(_state_persister())

    log.info("System fully online.")
    log.info(f"  Bullish : {bullish.name}  (paper, 24/7)")
    log.info(f"  Bearish : {bearish.name}  (paper, 24/7)")
    log.info(f"  Lines   : H={analyst.high_line}  L={analyst.low_line}")
    log.info("=" * 64)

    tg.send(
        f"✅ <b>Hedge Trader ONLINE</b>\n"
        f"H-Line: <b>{analyst.high_line}</b>  |  L-Line: <b>{analyst.low_line}</b>\n"
        f"Time: {ist_now_str()}"
    )


async def shutdown():
    from backend import telegram_alert as tg
    log.info("Shutting down...")
    tg.send(f"🔴 <b>Hedge Trader OFFLINE</b>\nTime: {ist_now_str()}")
    from backend.data import spot_feed, futures_feed, options_feed
    await spot_feed.stop()
    await futures_feed.stop()
    await options_feed.stop()
    await analyst.stop()
    await bullish.stop()
    await bearish.stop()
    await volatile.stop()


# ── FastAPI app ───────────────────────────────────────────────────────────────
from backend.api.gateway import app

@app.on_event("startup")
async def fastapi_startup():
    await startup()

@app.on_event("shutdown")
async def fastapi_shutdown():
    await shutdown()


if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(
        app,          # pass object, not string — prevents double-import of backend.main
        host=host, port=port,
        log_level="info", reload=False,
    )
