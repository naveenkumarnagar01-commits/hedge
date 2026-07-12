"""
Hedge Platform - Backend Entry Point.

Agents:
  - Spot + Futures + Options WebSocket feeds
  - AnalystAgent     : daily line computation (configurable IST time)
  - ManagerAgent     : risk gate for position size
  - BullishExecutor  : paper - long futures + ITM PUT hedge
  - BearishExecutor  : paper - short futures + ITM CALL hedge
  - VolatileTrader   : event-driven straddle

Run:
  python -m backend.main
"""

import asyncio
import logging
import os
import sys
import shutil
import signal
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

# ── Load .env FIRST (before anything else) ──────────────────────────────────
try:
    from dotenv import load_dotenv
    _env_file = Path(__file__).parent.parent / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
        print(f"[main] Loaded .env from {_env_file}")
    else:
        print("[main] No .env file found — using system environment variables.")
except ImportError:
    print("[main] python-dotenv not installed — skipping .env load.")

# ── Timezone: set IST before any time operations ────────────────────────────
if not os.environ.get("TZ"):
    os.environ["TZ"] = "Asia/Kolkata"
try:
    import time as _time
    _time.tzset()
except AttributeError:
    pass  # Windows — TZ env var is enough for most libs

# ── Fix: python -m does NOT add backend.main to sys.modules ─────────────────
if "backend.main" not in sys.modules:
    sys.modules["backend.main"] = sys.modules[__name__]

# ── UTF-8 console on Windows ─────────────────────────────────────────────────
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import uvicorn


# ═══════════════════════════════════════════════════════════════════════════
# LOGGING SETUP — console + daily rotating file
# ═══════════════════════════════════════════════════════════════════════════

def _setup_logging():
    log_level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(name)-22s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handlers = [logging.StreamHandler(sys.stdout)]

    # Daily rotating log file
    log_dir = Path(os.environ.get("LOG_DIR", Path(__file__).parent.parent / "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "hedge_trader.log"
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        interval=1,
        backupCount=30,          # keep 30 days
        encoding="utf-8",
        utc=False,
    )
    file_handler.suffix = "%Y-%m-%d"
    handlers.append(file_handler)

    for h in handlers:
        h.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(log_level)
    for h in root.handlers[:]:
        root.removeHandler(h)
    for h in handlers:
        root.addHandler(h)

    return log_dir

log_dir = _setup_logging()
log = logging.getLogger("main")


# ═══════════════════════════════════════════════════════════════════════════
# AGENT SINGLETONS
# ═══════════════════════════════════════════════════════════════════════════

from backend.message_bus import bus
from backend.config import cfg
from backend.state_store import store
from backend.utils import ist_now, ist_now_str

from backend.agents.analyst          import analyst
from backend.agents.manager          import manager
from backend.agents.bullish_executor import BullishExecutor
from backend.agents.bearish_executor import BearishExecutor
from backend.agents.volatile_trader  import VolatileTrader
from backend.execution.paper_engine  import PaperEngine
from backend.data.event_calendar     import event_calendar
from backend.data.ob_tracker         import ob_tracker

_bull_paper = PaperEngine(); _bull_paper._state_key = "paper_engine_state_bull"
_bear_paper = PaperEngine(); _bear_paper._state_key = "paper_engine_state_bear"
_vol_paper  = PaperEngine(); _vol_paper._state_key  = "paper_engine_state_vol"

bullish  = BullishExecutor(is_paper=True, force_window=False, paper_engine=_bull_paper)
bearish  = BearishExecutor(is_paper=True, force_window=False, paper_engine=_bear_paper)
volatile = VolatileTrader( is_paper=True, paper_engine=_vol_paper)


# ═══════════════════════════════════════════════════════════════════════════
# BACKGROUND TASKS
# ═══════════════════════════════════════════════════════════════════════════

async def _squareoff_broadcaster(executor, h_key: str, m_key: str):
    from backend.message_bus import SQUAREOFF_START
    name = executor.name
    while True:
        try:
            now_ist = ist_now()
            sq_h    = int(getattr(cfg, h_key) or 0)
            sq_m    = int(getattr(cfg, m_key) or 0)
            sq_time = now_ist.replace(hour=sq_h, minute=sq_m, second=0, microsecond=0)
            if now_ist >= sq_time:
                sq_time += timedelta(days=1)
            wait_sec = max((sq_time - now_ist).total_seconds(), 60)
            log.info(f"[{name}] Squareoff in {wait_sec/3600:.2f}h at {sq_time.strftime('%H:%M')} IST")
            await asyncio.sleep(wait_sec)
            log.info(f"[{name}] Broadcasting SQUAREOFF_START")
            await bus.publish(SQUAREOFF_START, {"ts_ist": ist_now_str(), "executor": name}, source="main")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"[{name}] Squareoff broadcaster error: {e} — retrying in 60s")
            await asyncio.sleep(60)


async def _ob_prefetch_task():
    """
    Pre-warm OB candle cache 20 minutes before each trading window opens.
    When the window actually opens, candles are already in memory → OB
    analysis runs instantly with no Binance HTTP delay.
    """
    from backend.data.futures_feed import get_candles, fetch_historical_candles
    from backend.utils import ist_now
    from backend.config import cfg as _cfg

    while True:
        try:
            n    = ist_now()
            ts_h = int(getattr(_cfg, "bull_trade_start_h", 4))
            ts_m = int(getattr(_cfg, "bull_trade_start_m", 0))

            # Compute seconds until (window_open - 20 min)
            import datetime as _dt
            today   = n.date()
            prefetch_time = _dt.datetime.combine(today, _dt.time(ts_h, ts_m),
                                                  tzinfo=n.tzinfo) - _dt.timedelta(minutes=20)
            if n >= prefetch_time:
                prefetch_time += _dt.timedelta(days=1)

            wait_sec = max((prefetch_time - n).total_seconds(), 60)
            log.info(f"[ob-prefetch] Next pre-warm in {wait_sec/60:.1f}min at {prefetch_time.strftime('%H:%M')} IST")
            await asyncio.sleep(wait_sec)

            # Pre-fetch all 4 TFs
            log.info("[ob-prefetch] Pre-warming OB candle cache...")
            n_candles = int(getattr(_cfg, "ob_candle_count", 500))
            for tf in ("5m", "15m", "1h", "4h"):
                try:
                    cached = get_candles(n_candles, tf)
                    if len(cached) < 50:
                        await asyncio.wait_for(fetch_historical_candles(n_candles, tf), timeout=45)
                        log.info(f"[ob-prefetch] {tf} fetched fresh")
                    else:
                        log.info(f"[ob-prefetch] {tf} already cached ({len(cached)} candles)")
                except asyncio.TimeoutError:
                    log.warning(f"[ob-prefetch] {tf} fetch timed out — will retry at window open")
                except Exception as e:
                    log.warning(f"[ob-prefetch] {tf} error: {e}")

            # Mark timestamp so health endpoint can show "pre-loaded Xs ago"
            import backend.main as _self
            _self._ob_prefetch_ts = ist_now().timestamp()
            log.info("[ob-prefetch] OB cache warm — ready for window open.")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"[ob-prefetch] Error: {e}")
            await asyncio.sleep(300)


# Timestamp of last successful OB pre-fetch (read by /api/system-health)
_ob_prefetch_ts: float = 0.0


async def _state_persister():
    """Persist executor states every 5s for crash recovery."""
    while True:
        await asyncio.sleep(5)
        for ex in (bullish, bearish, volatile):
            try:
                await store.set(f"{ex.name}_state", ex._serialize())
            except Exception as e:
                log.error(f"State persist failed for {ex.name}: {e}")


async def _daily_db_backup():
    """
    Every day at 01:00 IST, copy hedge_state.db to backups/ folder.
    Keeps last 30 daily backups.
    """
    backup_dir = Path(os.environ.get("BACKUP_DIR", Path(__file__).parent.parent / "backups"))
    backup_dir.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            now = ist_now()
            # Next 01:00 IST
            target = now.replace(hour=1, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            wait_sec = (target - now).total_seconds()
            log.info(f"[backup] Next DB backup in {wait_sec/3600:.1f}h at {target.strftime('%Y-%m-%d %H:%M IST')}")
            await asyncio.sleep(wait_sec)

            # Do backup
            db_path = store._db_path
            if db_path and Path(db_path).exists():
                date_str  = ist_now().strftime("%Y-%m-%d")
                dest_path = backup_dir / f"hedge_state_{date_str}.db"
                shutil.copy2(db_path, dest_path)
                log.info(f"[backup] DB backed up → {dest_path}")

                # Clean old backups (keep 30)
                backups = sorted(backup_dir.glob("hedge_state_*.db"))
                for old in backups[:-30]:
                    old.unlink()
                    log.info(f"[backup] Deleted old backup: {old.name}")
            else:
                log.warning("[backup] DB file not found — skipping backup")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"[backup] Daily backup failed: {e}")
            await asyncio.sleep(3600)  # retry in 1h


async def _health_monitor():
    """
    Every 30 min: heartbeat log + Telegram if feeds dead.
    Also auto-restarts feeds that have been silent for >5 min.
    """
    from backend.data import futures_feed, spot_feed, options_feed
    from backend import telegram_alert as tg
    import time as _time

    _feed_alert_sent = {"spot": False, "futures": False, "options": False}
    await asyncio.sleep(300)   # first check after 5 min (give feeds time to connect)

    while True:
        try:
            now_str = ist_now_str()
            now_ts  = _time.time()

            # ── Feed staleness check + auto-restart ───────────────────────
            def _feed_age(feed_mod, key):
                st = getattr(feed_mod, "_state", {}) if hasattr(feed_mod, "_state") else {}
                ts = float(st.get("bid_ts") or st.get("ts") or 0)
                return now_ts - ts if ts else 9999

            spot_age    = _feed_age(spot_feed, "spot")
            fut_age     = float(getattr(futures_feed, "_state", {}).get("bid_ts") or 0)
            fut_age     = now_ts - fut_age if fut_age else 9999

            for name, age, feed_mod, alert_key in [
                ("Spot",    spot_age, spot_feed,    "spot"),
                ("Futures", fut_age,  futures_feed,  "futures"),
            ]:
                if age > 300:   # >5 min silent → alert + restart
                    if not _feed_alert_sent[alert_key]:
                        log.error(f"[watchdog] {name} feed DEAD for {age:.0f}s — restarting")
                        tg.send(
                            f"🔴 <b>Feed Dead — Auto-Restart</b>\n"
                            f"{name} feed silent for {int(age//60)}m {int(age%60)}s\n"
                            f"Attempting reconnect...\nTime: {now_str}"
                        )
                        _feed_alert_sent[alert_key] = True
                    try:
                        await feed_mod.stop()
                        await asyncio.sleep(2)
                        await feed_mod.start()
                        log.info(f"[watchdog] {name} feed restarted.")
                    except Exception as re:
                        log.error(f"[watchdog] {name} restart failed: {re}")
                else:
                    if _feed_alert_sent[alert_key]:
                        tg.send(f"✅ <b>{name} Feed Recovered</b>\nTime: {now_str}")
                    _feed_alert_sent[alert_key] = False

            # ── Heartbeat every 30 min ────────────────────────────────────
            spot_price = float(getattr(spot_feed, "_state", {}).get("last_price") or 0)
            states = {
                "bull": bullish.state.value  if hasattr(bullish, "state")  else "?",
                "bear": bearish.state.value  if hasattr(bearish, "state")  else "?",
                "vol":  volatile._state.value if hasattr(volatile, "_state") else "?",
            }
            log.info(
                f"[heartbeat] {now_str} | BTC=${spot_price:,.0f} | "
                f"bull={states['bull']} bear={states['bear']} vol={states['vol']}"
            )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"[heartbeat] Monitor error: {e}")
        await asyncio.sleep(300)   # check every 5 min


# ═══════════════════════════════════════════════════════════════════════════
# STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════════════════

async def startup():
    from backend import telegram_alert as tg

    log.info("=" * 64)
    log.info("  HEDGE TRADER  -  3-Trader Platform")
    log.info("  Bull | Bear | Volatile — each $100k independent balance")
    log.info(f"  Started: {ist_now_str()}")
    log.info(f"  Log directory: {log_dir}")
    log.info("=" * 64)

    # 1. State store
    db_url = os.environ.get("DATABASE_URL", "")
    await store.connect(db_url)
    log.info(f"DB path: {store._db_path}")

    # 2. Restore persisted user config
    user_cfg = store.get("user_config")
    if user_cfg:
        for k, v in user_cfg.items():
            if hasattr(cfg, k):
                try:
                    setattr(cfg, k, type(getattr(cfg, k))(v))
                except Exception:
                    setattr(cfg, k, v)
        log.info(f"User config restored: {len(user_cfg)} key(s)")

    # 3. Redis (optional)
    redis_url = os.environ.get("REDIS_URL", "")
    if redis_url:
        await bus.connect_redis(redis_url)

    # 4. Data feeds
    from backend.data import spot_feed, futures_feed, options_feed
    await futures_feed.fetch_historical_candles(500, "5m")
    await futures_feed.fetch_historical_candles(500, "15m")
    await futures_feed.fetch_historical_candles(300, "1h")
    await futures_feed.fetch_historical_candles(200, "4h")
    await spot_feed.start()
    await futures_feed.start()
    await options_feed.start()
    log.info("WebSocket data feeds started.")

    # 5. Event calendar
    await event_calendar.start()

    # 6. Agents
    await analyst.start()
    await manager.start()
    await bullish.start()
    await bearish.start()
    await volatile.start()
    log.info("All 3 traders started.")
    await ob_tracker.start()

    # 7. Background tasks — wrapped in auto-restart guard so crashes don't silently kill them
    def _guarded(make_coro, name: str):
        """Wrap a coroutine factory: if the task crashes, log and restart it after 10s."""
        async def _wrapper():
            while True:
                try:
                    await make_coro()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.error(f"[guarded:{name}] crashed: {e} — restarting in 10s")
                    await asyncio.sleep(10)
        return asyncio.create_task(_wrapper(), name=name)

    _guarded(lambda: _squareoff_broadcaster(bullish, "bull_force_close_h", "bull_force_close_m"), "squareoff_bull")
    _guarded(lambda: _squareoff_broadcaster(bearish, "bear_force_close_h", "bear_force_close_m"), "squareoff_bear")
    _guarded(lambda: _state_persister(),    "state_persister")
    _guarded(lambda: _daily_db_backup(),    "daily_db_backup")
    _guarded(lambda: _health_monitor(),     "health_monitor")
    _guarded(lambda: _ob_prefetch_task(),   "ob_prefetch")

    log.info("System fully online.")
    log.info(f"  Bullish : {bullish.name}  (paper, 24/7)")
    log.info(f"  Bearish : {bearish.name}  (paper, 24/7)")
    log.info(f"  Volatile: {volatile.name} (paper, event-driven)")
    log.info("=" * 64)

    # ── Full health check on startup ────────────────────────────────────────
    await asyncio.sleep(3)   # let feeds settle
    health_lines = []
    all_ok = True

    # DB
    db_ok = bool(store._db_path)
    health_lines.append(f"{'✅' if db_ok else '❌'} DB: {'SQLite ready' if db_ok else 'NOT CONNECTED'}")
    if not db_ok: all_ok = False

    # PG
    if store._pg_pool:
        health_lines.append("✅ PostgreSQL: KV connected")

    # Price feed
    from backend.data.spot_feed import get_state as _ss
    spot_bid = float(_ss().get("bid") or 0)
    feed_ok  = spot_bid > 0
    health_lines.append(f"{'✅' if feed_ok else '❌'} BTC Feed: {'$' + f'{spot_bid:,.0f}' if feed_ok else 'NO DATA'}")
    if not feed_ok: all_ok = False

    # Options
    from backend.data.options_feed import get_chain as _gc
    opts = len(_gc())
    opts_ok = opts >= 10
    health_lines.append(f"{'✅' if opts_ok else '❌'} Options: {opts} strikes {'ready' if opts_ok else '— LOW'}")

    # Env vars
    missing_env = [k for k in ["BINANCE_API_KEY", "BINANCE_API_SECRET"] if not os.environ.get(k)]
    if missing_env:
        health_lines.append(f"⚠️ Missing env: {', '.join(missing_env)} (paper mode OK)")
    else:
        health_lines.append("✅ Env vars: all set")

    # Next session
    from backend.utils import ist_now as _isn
    from backend.config import cfg as _cfg
    from datetime import timedelta as _td
    _n = _isn()
    _exp_h = int(getattr(_cfg, "session_expiry_h", 13))
    _exp_m = int(getattr(_cfg, "session_expiry_m", 30))
    _expiry = _n.replace(hour=_exp_h, minute=_exp_m, second=0, microsecond=0)
    if _n >= _expiry: _expiry += _td(days=1)
    _mins = int((_expiry - _n).total_seconds() / 60)
    health_lines.append(f"📅 Next expiry: {_expiry.strftime('%d/%m %H:%M IST')} (in {_mins//60}h {_mins%60}m)")

    status_icon = "✅" if all_ok else "⚠️"
    tg.send(
        f"{status_icon} <b>Hedge Trader ONLINE</b>\n"
        f"Time: {ist_now_str()}\n\n"
        + "\n".join(health_lines) +
        f"\n\n<b>Traders:</b>\n"
        f"  Bull: {bullish.name}\n"
        f"  Bear: {bearish.name}\n"
        f"  Vol:  {volatile.name}"
    )


async def shutdown():
    from backend import telegram_alert as tg
    log.info("Shutting down gracefully...")
    tg.send(f"🔴 <b>Hedge Trader OFFLINE</b>\nTime: {ist_now_str()}")
    from backend.data import spot_feed, futures_feed, options_feed
    try:
        await spot_feed.stop()
        await futures_feed.stop()
        await options_feed.stop()
        await analyst.stop()
        await bullish.stop()
        await bearish.stop()
        await volatile.stop()
        # Final state save
        for ex in (bullish, bearish, volatile):
            try:
                await store.set(f"{ex.name}_state", ex._serialize())
            except Exception:
                pass
    except Exception as e:
        log.error(f"Shutdown error: {e}")
    log.info("Shutdown complete.")


# ═══════════════════════════════════════════════════════════════════════════
# GLOBAL EXCEPTION HANDLER — catch all uncaught async exceptions
# ═══════════════════════════════════════════════════════════════════════════

def _handle_exception(loop, context):
    msg = context.get("exception", context["message"])
    log.error(f"[asyncio] Uncaught exception: {msg}", exc_info=context.get("exception"))
    from backend import telegram_alert as tg
    tg.send(
        f"🚨 <b>Hedge Trader ERROR</b>\n"
        f"{str(msg)[:400]}\n"
        f"Time: {ist_now_str()}"
    )


# ═══════════════════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════════════════

from backend.api.gateway import app


@app.on_event("startup")
async def fastapi_startup():
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(_handle_exception)
    await startup()


@app.on_event("shutdown")
async def fastapi_shutdown():
    await shutdown()


# ── Health check endpoint ──────────────────────────────────────────────────
@app.get("/health")
async def health_check():
    from backend.data import futures_feed
    price = getattr(futures_feed, "_last_price", 0)
    return {
        "status":   "ok",
        "ts_ist":   ist_now_str(),
        "btc_price": price,
        "traders": {
            "bull": bullish.state.value  if hasattr(bullish,  "state")  else "?",
            "bear": bearish.state.value  if hasattr(bearish,  "state")  else "?",
            "vol":  volatile._state.value if hasattr(volatile, "_state") else "?",
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))

    log.info(f"Starting uvicorn on {host}:{port}")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=os.environ.get("LOG_LEVEL", "info").lower(),
        reload=False,
        access_log=True,
    )
