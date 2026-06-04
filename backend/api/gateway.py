"""
FastAPI Gateway — serves REST endpoints + WebSocket broadcast to frontend.

WebSocket endpoint: ws://host:8000/ws
  → Streams ALL bus events to connected frontend clients in real-time.

REST endpoints:
  GET  /api/status          → system / feed status
  GET  /api/analyst         → current lines + last run log
  GET  /api/positions       → executor statuses
  GET  /api/forward         → paper trading summary
  GET  /api/manager-logs    → recent manager decisions
  GET  /api/config          → current config
  POST /api/config          → update config params
  POST /api/analyst/run     → trigger manual analyst run
  POST /api/forward/reset   → reset paper account
  GET  /api/candles         → last N candles
  GET  /api/options-chain   → current options chain snapshot
"""

import asyncio
import json
import logging
import os
import time
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from backend.message_bus import bus
from backend.config import cfg, update as cfg_update, as_dict as cfg_dict
from backend.state_store import store
from backend.utils import ist_now_str, session_info_str, is_time_before_in_session, is_time_before_or_equal_in_session

log = logging.getLogger("gateway")

app = FastAPI(title="Hedge Trader API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connected WebSocket clients
_clients: Set[WebSocket] = set()

# Recent event buffer (last 500 events — for clients joining mid-session)
_event_buffer: list = []
_MAX_BUFFER = 500

_TRADER_EXECUTOR_NAMES = {
    "bull": "BullishExecutor_Paper",
    "bear": "BearishExecutor_Paper",
    "vol":  "VolatileTrader_Paper",
}


async def _ledger_closed_pnl(executor_name: str) -> float | None:
    if not executor_name:
        return None
    trades = await store.get_paper_trades(executor_name, 10000)
    if not trades:
        return None
    return round(sum(float(t.get("pnl") or 0.0) for t in trades), 2)


async def _reconcile_flat_summary(trader_key: str, summary: dict, paper_engine=None) -> dict:
    """When an account is flat, the persistent trade ledger is authoritative."""
    has_open = bool(summary.get("open_positions")) or bool(summary.get("option_positions"))
    if has_open:
        return summary
    ledger_pnl = await _ledger_closed_pnl(_TRADER_EXECUTOR_NAMES.get(trader_key, ""))
    if ledger_pnl is None:
        return summary
    if abs(ledger_pnl - float(summary.get("total_pnl") or 0.0)) <= 0.01:
        return summary
    fixed = dict(summary)
    start = float(summary.get("starting_balance") or cfg.virtual_balance_usdt)
    fixed["balance"] = round(start + ledger_pnl, 2)
    fixed["equity"] = fixed["balance"]
    fixed["unrealized_pnl"] = 0.0
    fixed["open_pnl"] = 0.0
    fixed["total_pnl"] = ledger_pnl
    fixed["realized_pnl"] = ledger_pnl
    fixed["session_pnl"] = ledger_pnl
    fixed["ledger_reconciled"] = True
    if paper_engine is not None:
        paper_engine.balance = fixed["balance"]
        paper_engine.session_start_balance = start
        paper_engine._realized_pnl = ledger_pnl
        await paper_engine.save_state()
    return fixed


async def _broadcast(msg: dict):
    """Send message to all connected WebSocket clients. Iterates over a snapshot to avoid set-mutation errors."""
    payload = json.dumps(msg)
    disconnected = set()
    for ws in list(_clients):          # ← list() snapshot prevents RuntimeError
        try:
            await ws.send_text(payload)
        except Exception:
            disconnected.add(ws)
    _clients.difference_update(disconnected)


def _bus_to_ws(msg: dict):
    """Bridge: every bus message → WebSocket broadcast."""
    _event_buffer.append(msg)
    if len(_event_buffer) > _MAX_BUFFER:
        _event_buffer.pop(0)
    asyncio.create_task(_broadcast(msg))


# ── WebSocket endpoint ─────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _clients.add(ws)
    log.info(f"WS client connected. Total: {len(_clients)}")

    # Replay recent buffer so client gets current state immediately
    for msg in _event_buffer[-100:]:
        try:
            await ws.send_text(json.dumps(msg))
        except Exception:
            break

    try:
        while True:
            # Keep alive — client can send pings
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text(json.dumps({"type": "pong", "ts": time.time()}))
    except WebSocketDisconnect:
        pass
    finally:
        _clients.discard(ws)
        log.info(f"WS client disconnected. Total: {len(_clients)}")


# ── REST: Status ───────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    from backend.data.spot_feed    import get_state as spot_state, get_depth as spot_depth
    from backend.data.futures_feed import get_state as fut_state,  get_depth as fut_depth
    from backend.data.options_feed import get_chain
    return {
        "ts_ist":        ist_now_str(),
        "spot":          spot_state(),
        "futures":       fut_state(),
        "options_count": len(get_chain()),
        "ws_clients":    len(_clients),
    }

@app.get("/api/session-info")
async def get_session_info():
    expiry_h = int(getattr(cfg, "session_expiry_h", 13))
    expiry_m = int(getattr(cfg, "session_expiry_m", 30))
    return {**session_info_str(expiry_h, expiry_m), "ts_ist": ist_now_str()}


@app.get("/api/orderbook")
async def get_orderbook():
    from backend.data.spot_feed    import get_depth as spot_depth
    from backend.data.futures_feed import get_depth as fut_depth
    return {
        "spot":    spot_depth(),
        "futures": fut_depth(),
        "ts_ist":  ist_now_str(),
    }


@app.get("/api/executor-monitor")
async def get_executor_monitor():
    import backend.main as m
    return {
        "bullish":       m.bullish.get_status()      if hasattr(m, "bullish")       else {},
        "bearish":       m.bearish.get_status()      if hasattr(m, "bearish")       else {},
        "bullish_paper": m.bullish_paper.get_status() if hasattr(m, "bullish_paper") else {},
        "bearish_paper": m.bearish_paper.get_status() if hasattr(m, "bearish_paper") else {},
        "ts_ist":        ist_now_str(),
    }


# ── REST: Analyst ──────────────────────────────────────────────────────────

@app.get("/api/analyst")
async def get_analyst(n: int = 300, tf: str = "5m"):
    from backend.agents.analyst import analyst
    from backend.data.futures_feed import get_candles, fetch_historical_candles
    lines = analyst.get_lines()
    
    candles = get_candles(n, tf)
    if len(candles) < n:  # Always fetch historical when buffer is short
        candles = await fetch_historical_candles(n, tf)
        
    # Return both the global trading lines and the candles for the requested TF
    return {
        "high_line":     lines.get("high_line"),
        "low_line":      lines.get("low_line"),
        "last_run":      lines.get("last_run", 0),
        "run_time":      lines.get("run_time", analyst.run_time_str if hasattr(analyst, 'run_time_str') else ist_now_str()),
        "analysis_tf":   lines.get("tf_minutes", 5),
        "analysis_n":    lines.get("candles_n", 300),
        "analyst_h":     lines.get("analyst_h", 4),
        "analyst_m":     lines.get("analyst_m", 0),
        "cutoff_ist":    lines.get("cutoff_ist"),
        "analysis_from_ist": lines.get("analysis_from_ist"),
        "analysis_to_ist":   lines.get("analysis_to_ist"),
        "candidates":    lines.get("candidates", []),
        "touches":       lines.get("touches", []),
        "candle_detail": lines.get("candle_detail", []),
        "candles":       candles,
        "ts_ist":        ist_now_str(),
    }


@app.post("/api/config/reset-defaults")
async def reset_defaults():
    """Reset all trading parameters to spec defaults."""
    from backend.config import Config
    import dataclasses
    defaults = {f.name: f.default for f in dataclasses.fields(Config)
                if not callable(f.default) and f.default is not dataclasses.MISSING}
    changes = await cfg_update(defaults)
    # Persist the reset config
    from backend.config import as_dict as cfg_dict_fn
    await store.set("user_config", cfg_dict_fn())
    return {"status": "reset", "changes": changes, "ts_ist": ist_now_str()}


@app.post("/api/options/reload")
async def reload_options():
    """Force re-fetch of options symbol list and immediately poll tickers."""
    from backend.data.options_feed import _fetch_target_btc_symbols, _fast_poll_loop
    try:
        symbols, label, same_day = await _fetch_target_btc_symbols()
        return {"status": "ok", "expiry": label, "symbols": len(symbols), "ts_ist": ist_now_str()}
    except Exception as e:
        return {"status": "error", "detail": str(e), "ts_ist": ist_now_str()}


@app.post("/api/analyst/run")
async def manual_analyst_run(body: dict = None):
    from backend.agents.analyst import analyst
    count = body.get("count") if body else None
    tf    = body.get("tf")    if body else None
    
    # We trigger it directly as an async task so API returns immediately
    asyncio.create_task(analyst.run_analysis(count=count, tf=tf))
    
    return {"status": "queued", "count": count, "tf": tf, "ts_ist": ist_now_str()}


@app.post("/api/set-lines")
async def set_lines(body: dict):
    """
    Manually push high_line / low_line to all executors.
    Use this outside 04:00 IST window for testing, or to override analyst output.
    Also saves them so executors survive restart.
    """
    h = body.get("high_line")
    l = body.get("low_line")
    if not h or not l:
        raise HTTPException(status_code=400, detail="high_line and low_line required")
    h, l = float(h), float(l)
    from backend.agents.base_executor import SET_LINES
    await bus.publish(SET_LINES, {"high_line": h, "low_line": l}, source="api")
    # Also persist so restart picks them up
    from backend.state_store import store
    await store.set("analyst_lines", {"high_line": h, "low_line": l, "run_time": ist_now_str()})
    # Broadcast as ANALYST_LINES so panel chart updates too
    from backend.message_bus import ANALYST_LINES
    await bus.publish(ANALYST_LINES, {
        "high_line": h, "low_line": l,
        "run_time": ist_now_str(), "candles_n": 0,
        "candidates": [], "touches": [],
    }, source="api")
    return {"status": "set", "high_line": h, "low_line": l, "ts_ist": ist_now_str()}


# ── REST: Candles ──────────────────────────────────────────────────────────

_TF_SECONDS = {"1m":60,"3m":180,"5m":300,"15m":900,"30m":1800,"1h":3600,"4h":14400,"1d":86400,"1w":604800}

@app.get("/api/candles")
async def get_candles_endpoint(n: int = 500, tf: str = "5m"):
    from backend.data.futures_feed import get_candles as gc, fetch_historical_candles
    candles = gc(n, tf)
    tf_secs = _TF_SECONDS.get(tf, 300)
    # Refresh from Binance if: buffer too small, OR latest candle is older than 2 periods
    latest_ts = candles[-1]["ts"] / 1000 if candles else 0
    stale = (time.time() - latest_ts) > 2 * tf_secs
    if len(candles) < n or stale:
        candles = await fetch_historical_candles(n, tf)
    return {"candles": candles, "ts_ist": ist_now_str()}


# ── REST: Options chain ────────────────────────────────────────────────────

@app.get("/api/options-chain")
async def get_options_chain():
    from backend.data.options_feed import get_chain, get_feed_info
    chain = sorted(get_chain().values(), key=lambda x: (x["side"], x["strike"]))
    info  = get_feed_info()
    return {
        "chain":      chain,
        "expiry":     info.get("expiry"),
        "hours_left": info.get("hours_left"),
        "source":     info.get("source"),
        "ts_ist":     ist_now_str()
    }


# ── REST: Event Calendar ──────────────────────────────────────────────────

@app.get("/api/events")
async def get_events(days: int = 30):
    """Return upcoming macro events + last fired event for the Volatile tab."""
    from backend.data.event_calendar import event_calendar
    return {
        "upcoming":      event_calendar.get_upcoming(days=days),
        "all":           event_calendar.get_all(),
        "recent_fired":  event_calendar.recent_fired,
        "ts_ist":        ist_now_str(),
    }


@app.get("/api/events/report")
async def get_events_report(months: int = 3, refresh: bool = False):
    """
    Past macro event reports with BTC price reaction (Binance) + actual data (FMP).
    Results cached 2h. Use ?refresh=true to force a new fetch.
    """
    from backend.data.event_research import event_research
    return await event_research.get_past_reports(months_back=months,
                                                  force_refresh=refresh)


@app.get("/api/orderblocks")
async def get_order_blocks(n: int = 500, tf: str = "5m"):
    """
    Detect active demand/supply order block zones from recent BTC candles.
    Returns fresh zones scoring ≥58 (grade B or better): A+ ≥86, A ≥74, B ≥58.
    """
    from backend.data.futures_feed import get_candles as gc, fetch_historical_candles
    from backend.data.order_blocks import detect
    candles = gc(n, tf)
    tf_secs = _TF_SECONDS.get(tf, 300)
    latest_ts = candles[-1]["ts"] / 1000 if candles else 0
    if len(candles) < n or (time.time() - latest_ts) > 2 * tf_secs:
        candles = await fetch_historical_candles(n, tf)
    result = detect(candles)
    result["ts_ist"] = ist_now_str()
    result["tf"] = tf
    return result


@app.get("/api/events/forecast")
async def get_events_forecast(refresh: bool = False):
    """
    Upcoming events with FMP consensus estimates + Claude AI directional forecast.
    Results cached 4h. Use ?refresh=true to force a new fetch.
    """
    from backend.data.event_research import event_research
    return await event_research.get_upcoming_forecast(force_refresh=refresh)


# ── REST: Positions ────────────────────────────────────────────────────────

@app.get("/api/positions")
async def get_positions():
    import backend.main as m
    return {
        "bullish":  m.bullish.get_status()  if hasattr(m, "bullish")  else {},
        "bearish":  m.bearish.get_status()  if hasattr(m, "bearish")  else {},
        "volatile": m.volatile.get_status() if hasattr(m, "volatile") else {},
        "ts_ist":   ist_now_str(),
    }


@app.get("/api/executor-monitor")
async def get_executor_monitor_v2():
    import backend.main as m
    return {
        "bullish": m.bullish.get_status() if hasattr(m, "bullish") else {},
        "bearish": m.bearish.get_status() if hasattr(m, "bearish") else {},
        "ts_ist":  ist_now_str(),
    }


# ── REST: Manager logs ─────────────────────────────────────────────────────

@app.get("/api/manager-logs")
async def get_manager_logs(limit: int = 100):
    logs = await store.get_manager_logs(limit)
    return {"logs": logs, "ts_ist": ist_now_str()}


# ── Force-close time validation ────────────────────────────────────────────

_FIELD_RANGES = {
    "_h": (0, 23),
    "_m": (0, 59),
    "_qty": (0.001, 10.0),
    "_premium": (10.0, 20000.0),
    "_target": (10.0, 20000.0),
    "_premium_max": (10.0, 20000.0),
    "_time_value": (10.0, 20000.0),
    "price_diff_percent": (0.0, 100.0),
}

def _validate_config_params(body: dict) -> list[str]:
    errors = []
    for k, v in body.items():
        if isinstance(v, (int, float)):
            for suffix, (min_val, max_val) in _FIELD_RANGES.items():
                if k.endswith(suffix):
                    if not (min_val <= float(v) <= max_val):
                        errors.append(f"{k} must be between {min_val} and {max_val}")

    expiry_h = int(body.get("session_expiry_h", getattr(cfg, "session_expiry_h", 13)))
    expiry_m = int(body.get("session_expiry_m", getattr(cfg, "session_expiry_m", 30)))

    _time_keys = {"trade_start_h", "trade_start_m", "trade_end_h", "trade_end_m",
                  "force_close_h", "force_close_m"}

    for prefix in ["bull_", "bear_", "vol_"]:
        # Only validate time ordering when this prefix has at least one time key in the body.
        # Without this guard, unchanged defaults (e.g. 04:00-18:30) trigger false 422s.
        if not any(f"{prefix}{k}" in body for k in _time_keys):
            continue

        ts_h = int(body.get(f"{prefix}trade_start_h", getattr(cfg, f"{prefix}trade_start_h", 4)))
        ts_m = int(body.get(f"{prefix}trade_start_m", getattr(cfg, f"{prefix}trade_start_m", 0)))
        te_h = int(body.get(f"{prefix}trade_end_h", getattr(cfg, f"{prefix}trade_end_h", 18)))
        te_m = int(body.get(f"{prefix}trade_end_m", getattr(cfg, f"{prefix}trade_end_m", 30)))
        fc_h = int(body.get(f"{prefix}force_close_h", getattr(cfg, f"{prefix}force_close_h", 18)))
        fc_m = int(body.get(f"{prefix}force_close_m", getattr(cfg, f"{prefix}force_close_m", 30)))

        if not is_time_before_in_session(ts_h, ts_m, te_h, te_m, expiry_h, expiry_m):
            errors.append(f"{prefix} window start ({ts_h:02}:{ts_m:02}) must be before end ({te_h:02}:{te_m:02}) within the session.")
        if not is_time_before_or_equal_in_session(te_h, te_m, fc_h, fc_m, expiry_h, expiry_m):
            errors.append(f"{prefix} window end ({te_h:02}:{te_m:02}) must be before or equal to force close ({fc_h:02}:{fc_m:02}).")

    return errors

def _check_force_close_before_expiry(body: dict):
    """
    Guard: any force_close_h/m being set must resolve to a time (today in IST)
    that is strictly BEFORE the current options contract expiry.
    After expiry you cannot sell options, so a force_close set past expiry is
    guaranteed to fail for the hedge leg.
    Raises HTTPException(400) with a human-readable message if the check fails.
    Silently passes when expiry is not yet loaded (0) to avoid blocking startup.
    """
    from backend.data.options_feed import get_feed_info
    from backend.utils import ist_now
    import datetime

    feed_info  = get_feed_info()
    expiry_ms  = feed_info.get("expiry_ms", 0)
    if not expiry_ms:
        return  # options feed not yet loaded — skip

    now_ist = ist_now()
    for prefix in ("bull_", "bear_", "vol_"):
        h_key = f"{prefix}force_close_h"
        m_key = f"{prefix}force_close_m"
        if h_key not in body and m_key not in body:
            continue
        fc_h = int(body.get(h_key, getattr(cfg, h_key, 18)))
        fc_m = int(body.get(m_key, getattr(cfg, m_key, 30)))
        try:
            fc_dt  = now_ist.replace(hour=fc_h, minute=fc_m, second=0, microsecond=0)
        except ValueError:
            raise HTTPException(status_code=400,
                                detail=f"Invalid force close time {fc_h:02d}:{fc_m:02d}")
        fc_ms  = int(fc_dt.timestamp() * 1000)
        if fc_ms >= expiry_ms:
            exp_dt  = datetime.datetime.fromtimestamp(expiry_ms / 1000, tz=now_ist.tzinfo)
            exp_str = exp_dt.strftime("%H:%M IST")
            trader  = prefix.rstrip("_").upper()
            raise HTTPException(
                status_code=400,
                detail=(
                    f"{trader} force close {fc_h:02d}:{fc_m:02d} IST must be BEFORE "
                    f"options contract expiry at {exp_str} "
                    f"({feed_info.get('expiry', 'unknown')}). "
                    f"Options cannot be sold after expiry."
                )
            )


# ── REST: Config ───────────────────────────────────────────────────────────

@app.get("/api/config")
async def get_config():
    return cfg_dict()


@app.post("/api/config")
async def post_config(body: dict):
    errors = _validate_config_params(body)
    if errors:
        raise HTTPException(status_code=422, detail={"errors": errors})
    _check_force_close_before_expiry(body)
    changes = await cfg_update(body)
    if changes:
        # Persist the full config so settings survive restarts
        from backend.config import as_dict as cfg_dict_fn
        await store.set("user_config", cfg_dict_fn())
        from backend.message_bus import CONFIG_UPDATE
        await bus.publish(CONFIG_UPDATE, {"changes": changes}, source="api")
    return {"changes": changes, "ts_ist": ist_now_str()}


# ── REST: Forward testing ──────────────────────────────────────────────────

@app.get("/api/forward")
async def get_forward(trader: str = ""):
    """
    trader = "" → all 3 combined summary
    trader = "bull" | "bear" | "vol" → specific trader's paper engine
    """
    import backend.main as m
    engine_map = {
        "bull": getattr(m, "_bull_paper", None),
        "bear": getattr(m, "_bear_paper", None),
        "vol":  getattr(m, "_vol_paper",  None),
    }
    if trader and trader in engine_map and engine_map[trader]:
        summary = await _reconcile_flat_summary(trader, engine_map[trader].get_summary(), engine_map[trader])
        return {**summary, "ts_ist": ist_now_str()}
    # Combined: sum balances, merge trade histories
    summaries = [e.get_summary() for e in engine_map.values() if e]
    if not summaries:
        from backend.execution.paper_engine import paper
        return {**paper.get_summary(), "ts_ist": ist_now_str()}
    combined = {
        "balance":        sum(s["balance"]        for s in summaries),
        "unrealized_pnl": sum(s["unrealized_pnl"] for s in summaries),
        "equity":         sum(s["equity"]         for s in summaries),
        "session_pnl":    sum(s["session_pnl"]    for s in summaries),
        "trade_count":    sum(s["trade_count"]     for s in summaries),
        "recent_trades":  sorted(
            [t for s in summaries for t in s.get("recent_trades", [])],
            key=lambda t: t.get("ts_ist", ""), reverse=True
        )[:100],
        "equity_curve":   summaries[0].get("equity_curve", []),
        "ts_ist":         ist_now_str(),
    }
    return combined


@app.post("/api/forward/reset")
async def reset_forward(trader: str = ""):
    import backend.main as m
    engine_map = {
        "bull": getattr(m, "_bull_paper", None),
        "bear": getattr(m, "_bear_paper", None),
        "vol":  getattr(m, "_vol_paper",  None),
    }
    if trader and trader in engine_map and engine_map[trader]:
        await engine_map[trader].reset()
        return {"status": "reset", "trader": trader, "ts_ist": ist_now_str()}
    # Reset all
    for e in engine_map.values():
        if e: await e.reset()
    return {"status": "all_reset", "ts_ist": ist_now_str()}


# ── Volatile Trader — event management + control ───────────────────────────

def _get_vol():
    import backend.main as m
    vol = getattr(m, "volatile", None)
    if not vol:
        raise HTTPException(status_code=503, detail="VolatileTrader not running")
    return vol


@app.get("/api/volatile/events")
async def volatile_get_events():
    """Return all saved events (past + upcoming)."""
    return {"events": _get_vol().get_events(), "ts_ist": ist_now_str()}


@app.post("/api/volatile/events")
async def volatile_add_event(body: dict):
    """
    Add a new event.
    Body: { "name": "FOMC Rate Decision", "event_time": "YYYY-MM-DDTHH:MM" }
    """
    name = body.get("name", "").strip()
    event_time = body.get("event_time", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    if not event_time:
        raise HTTPException(status_code=400, detail="event_time required (YYYY-MM-DDTHH:MM)")
    vol = _get_vol()
    before = len(vol.get_events())
    event = await vol.add_event(name, event_time)
    after  = len(vol.get_events())
    status = "saved" if after > before else "duplicate"
    return {"status": status, "event": event, "ts_ist": ist_now_str()}


@app.delete("/api/volatile/events/{event_id}")
async def volatile_delete_event(event_id: str):
    """Delete an event by its id."""
    removed = await _get_vol().remove_event(event_id)
    if not removed:
        raise HTTPException(status_code=404, detail=f"Event '{event_id}' not found")
    return {"status": "deleted", "id": event_id, "ts_ist": ist_now_str()}


@app.put("/api/volatile/events/{event_id}")
async def volatile_update_event(event_id: str, body: dict):
    """Update an event's name and/or event_time."""
    name       = body.get("name", "").strip()
    event_time = body.get("event_time", "").strip()
    if not event_time:
        raise HTTPException(status_code=400, detail="event_time required")
    updated = await _get_vol().update_event(event_id, name, event_time)
    if not updated:
        raise HTTPException(status_code=404, detail=f"Event '{event_id}' not found")
    return {"status": "updated", "event": updated, "ts_ist": ist_now_str()}


@app.post("/api/volatile/reset")
async def volatile_reset():
    """Reset Volatile Trader to SLEEP (events list preserved)."""
    _get_vol().reset()
    return {"status": "reset", "ts_ist": ist_now_str()}

@app.post("/api/volatile/force-close")
async def volatile_force_close():
    await _get_vol().force_close()
    return {"status": "force_close_done", "ts_ist": ist_now_str()}

@app.post("/api/volatile/clear-memory")
async def volatile_clear_memory():
    await _get_vol().clear_memory()
    return {"status": "memory_cleared", "ts_ist": ist_now_str()}


# ── Per-trader REST endpoints ───────────────────────────────────────────────

_TRADER_EXECUTOR_MAP = {
    "bull": "BullishExecutor_Paper",
    "bear": "BearishExecutor_Paper",
    "vol":  "VolatileTrader_Paper",
}
_TRADER_PREFIX_MAP = {
    "bull": "bull_",
    "bear": "bear_",
    "vol":  "vol_",
}


@app.get("/api/trader/{trader}/status")
async def get_trader_status(trader: str):
    import backend.main as m
    ex_map = {
        "bull": getattr(m, "bullish",  None),
        "bear": getattr(m, "bearish",  None),
        "vol":  getattr(m, "volatile", None),
    }
    paper_map = {
        "bull": getattr(m, "_bull_paper", None),
        "bear": getattr(m, "_bear_paper", None),
        "vol":  getattr(m, "_vol_paper",  None),
    }
    ex    = ex_map.get(trader)
    paper = paper_map.get(trader)
    return {
        "executor": ex.get_status()      if ex    else {},
        "paper":    paper.get_summary()  if paper else {},
        "ts_ist":   ist_now_str(),
    }


@app.get("/api/trader/{trader}/trades")
async def get_trader_trades(trader: str, limit: int = 200):
    """Persistent trades from SQLite (survive restarts)."""
    ex_name = _TRADER_EXECUTOR_MAP.get(trader, "")
    trades  = await store.get_paper_trades(ex_name, limit)
    return {"trades": trades, "ts_ist": ist_now_str()}


@app.get("/api/trader/{trader}/live-trades")
async def get_trader_live_trades(trader: str):
    """Current-session trades from in-memory paper engine only.
    These are cleared at the start of each new trading window — no cross-session history."""
    import backend.main as m
    paper_map = {
        "bull": getattr(m, "_bull_paper", None),
        "bear": getattr(m, "_bear_paper", None),
        "vol":  getattr(m, "_vol_paper",  None),
    }
    paper = paper_map.get(trader)
    if not paper:
        return {"trades": [], "session_trade_count": 0, "ts_ist": ist_now_str()}
    trades = list(reversed(paper.trade_history[-200:]))
    return {
        "trades":              trades,
        "session_trade_count": len(paper.trade_history),
        "ts_ist":              ist_now_str(),
    }


@app.get("/api/trader/{trader}/sessions")
async def get_trader_sessions(trader: str, limit: int = 50):
    ex_name  = _TRADER_EXECUTOR_MAP.get(trader, "")
    sessions = await store.get_sessions(ex_name, limit)
    return {"sessions": sessions, "ts_ist": ist_now_str()}


@app.post("/api/trader/{trader}/clear-position")
async def clear_trader_position(trader: str):
    """
    Force-clear all position memory for a trader (executor state + paper engine positions).
    Does NOT affect balance. Does NOT send any exchange orders.
    """
    import backend.main as m
    # Volatile Trader: use its own reset()
    if trader == "vol":
        vol = getattr(m, "volatile", None)
        if vol:
            vol.reset()
        return {"status": "cleared", "trader": trader, "ts_ist": ist_now_str()}
    ex_map = {
        "bull": getattr(m, "bullish",  None),
        "bear": getattr(m, "bearish",  None),
    }
    ex = ex_map.get(trader)
    if not ex:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Unknown trader: {trader}")
    await ex.clear_position()
    return {"status": "cleared", "trader": trader, "ts_ist": ist_now_str()}


@app.post("/api/trader/{trader}/force-close")
async def force_close_trader(trader: str):
    """
    Manually trigger squareoff for a trader.
    For Volatile Trader: resets to SLEEP (options-only, no futures to close).
    """
    import backend.main as m
    # Volatile Trader
    if trader == "vol":
        vol = getattr(m, "volatile", None)
        if vol:
            await vol.force_close()
        return {"status": "force_close_done", "trader": trader, "ts_ist": ist_now_str()}
    ex_map = {
        "bull": getattr(m, "bullish",  None),
        "bear": getattr(m, "bearish",  None),
    }
    ex = ex_map.get(trader)
    if not ex:
        raise HTTPException(status_code=400, detail=f"Unknown trader: {trader}")
    from backend.agents.base_executor import ExState
    has_position = (
        ex.state not in (ExState.SLEEP, ExState.FORCE_CLOSE)
        or bool(ex.hedge_symbol and ex.hedge_qty)
    )
    if not has_position:
        return {"status": "no_position", "trader": trader,
                "state": ex.state.value, "ts_ist": ist_now_str()}
    await ex._do_force_close()
    return {"status": "force_close_done", "trader": trader,
            "state": ex.state.value, "ts_ist": ist_now_str()}


@app.post("/api/trader/{trader}/reset-analysis")
async def reset_trader_analysis(trader: str):
    """
    Clear S/R analysis memory without touching positions (bull/bear only).
    """
    if trader == "vol":
        return {"status": "not_applicable", "trader": trader, "ts_ist": ist_now_str()}
    import backend.main as m
    ex_map = {
        "bull": getattr(m, "bullish",  None),
        "bear": getattr(m, "bearish",  None),
    }
    ex = ex_map.get(trader)
    if not ex:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"Unknown trader: {trader}")
    await ex.reset_analysis()
    return {"status": "analysis_reset", "trader": trader, "ts_ist": ist_now_str()}


@app.get("/api/trader/{trader}/balance")
async def get_trader_balance(trader: str):
    import backend.main as m
    paper_map = {
        "bull": getattr(m, "_bull_paper", None),
        "bear": getattr(m, "_bear_paper", None),
        "vol":  getattr(m, "_vol_paper",  None),
    }
    paper = paper_map.get(trader)
    if not paper:
        raise HTTPException(status_code=404, detail=f"Unknown trader: {trader}")
    s = paper.get_summary()
    return {
        "balance":       s["balance"],
        "equity":        s["equity"],
        "unrealized_pnl":s["unrealized_pnl"],
        "realized_pnl":  s["realized_pnl"],
        "total_pnl":     s["total_pnl"],
        "session_pnl":   s["session_pnl"],
        "ts_ist": ist_now_str(),
    }


@app.post("/api/trader/{trader}/clear-history")
async def clear_trader_history(trader: str):
    """Delete all trade history and sessions for a trader, reset virtual balance to 100k."""
    import backend.main as m
    paper_map = {
        "bull": getattr(m, "_bull_paper", None),
        "bear": getattr(m, "_bear_paper", None),
        "vol":  getattr(m, "_vol_paper",  None),
    }
    ex_name = _TRADER_EXECUTOR_MAP.get(trader, "")
    if not ex_name:
        raise HTTPException(status_code=400, detail=f"Unknown trader: {trader}")

    # Delete all DB history for this trader
    await store.delete_trader_history(ex_name)

    # Reset paper balance to 100k
    paper = paper_map.get(trader)
    if paper:
        await paper.reset()

    # Reset executor session memory
    ex_map = {
        "bull": getattr(m, "bullish", None),
        "bear": getattr(m, "bearish", None),
        "vol":  getattr(m, "volatile", None),
    }
    ex = ex_map.get(trader)
    if ex:
        if trader == "vol":
            ex._session_pnl = 0.0
        else:
            ex.session_realized_futures_pnl = 0.0
            ex.session_realized_hedge_pnl = 0.0
            ex.session_start_ts = 0.0

    return {"status": "history_cleared", "trader": trader, "new_balance": 100000.0, "ts_ist": ist_now_str()}


@app.post("/api/trader/{trader}/config/save-defaults")
async def save_trader_config_defaults(trader: str, body: dict):
    """
    Save trader-specific config as persistent defaults.
    These are loaded at startup and override hard-coded defaults.
    """
    prefix = _TRADER_PREFIX_MAP.get(trader, "")
    if not prefix:
        raise HTTPException(status_code=400, detail=f"Unknown trader: {trader}")

    # Guard: force_close time must be before options contract expiry
    errors = _validate_config_params(body)
    if errors:
        raise HTTPException(status_code=422, detail={"errors": errors})
    _check_force_close_before_expiry(body)

    # Apply to live config
    changes = await cfg_update(body)

    # Persist trader-specific keys as defaults
    trader_cfg = {k: v for k, v in body.items() if k.startswith(prefix)}
    if trader_cfg:
        await store.save_trader_config(_TRADER_EXECUTOR_MAP.get(trader, trader), trader_cfg)

    # Also save full config so all settings survive restarts
    from backend.config import as_dict as cfg_dict_fn
    await store.set("user_config", cfg_dict_fn())

    return {"status": "saved", "trader": trader, "changes": changes, "ts_ist": ist_now_str()}


@app.get("/api/trader/{trader}/config/defaults")
async def get_trader_config_defaults(trader: str):
    """Return the saved defaults for a specific trader."""
    ex_name = _TRADER_EXECUTOR_MAP.get(trader, trader)
    saved   = await store.get_trader_config(ex_name)
    return {"config": saved or {}, "ts_ist": ist_now_str()}


# ── REST: Live trade history (legacy endpoint kept for compat) ─────────────

@app.get("/api/trades")
async def get_trades(limit: int = 200, executor: str = ""):
    """
    Returns trades from persistent SQLite paper_trades table (survives restarts).
    Falls back to in-memory paper engine if DB has no records yet.
    """
    if executor:
        # Try persistent DB first
        db_trades = await store.get_paper_trades(executor, limit)
        if db_trades:
            return {"trades": db_trades, "ts_ist": ist_now_str()}
        # Fallback: in-memory paper engine
        import backend.main as m
        ex_to_trader = {v: k for k, v in _TRADER_EXECUTOR_MAP.items()}
        trader_key = ex_to_trader.get(executor)
        if trader_key:
            engine_map = {
                "bull": getattr(m, "_bull_paper", None),
                "bear": getattr(m, "_bear_paper", None),
                "vol":  getattr(m, "_vol_paper",  None),
            }
            paper = engine_map.get(trader_key)
            if paper:
                trades = paper.get_trades_by_executor(executor)[:limit]
                return {"trades": trades, "ts_ist": ist_now_str()}
    return {"trades": [], "ts_ist": ist_now_str()}


@app.get("/api/triggers")
async def get_triggers(executor: str = ""):
    if not executor:
        return {"triggers": [], "ts_ist": ist_now_str()}
    history = store.get(f"{executor}_triggers", [])
    return {"triggers": list(reversed(history)), "ts_ist": ist_now_str()}


# ── Journal Endpoints ──────────────────────────────────────────────────────

@app.get("/api/journal/sessions")
async def get_journal_sessions(trader: str = "all", from_date: str = "", to_date: str = "", limit: int = 100):
    ex_name = _TRADER_EXECUTOR_MAP.get(trader, trader) if trader != "all" else "all"
    sessions = await store.get_sessions_filtered(ex_name, from_date, to_date, limit)
    return {"sessions": sessions, "ts_ist": ist_now_str()}

@app.get("/api/journal/sessions/{session_id}/trades")
async def get_journal_session_trades(session_id: str):
    trades = await store.get_trades_by_session(session_id)
    return {"trades": trades, "ts_ist": ist_now_str()}

@app.get("/api/journal/summary")
async def get_journal_summary(trader: str = "all", from_date: str = "", to_date: str = ""):
    ex_name = _TRADER_EXECUTOR_MAP.get(trader, trader) if trader != "all" else "all"
    sessions = await store.get_sessions_filtered(ex_name, from_date, to_date, limit=10000)
    total_sessions = len(sessions)
    wins = sum(1 for s in sessions if s.get("total_pnl", 0) > 0)
    losses = sum(1 for s in sessions if s.get("total_pnl", 0) <= 0)
    win_rate = (wins / total_sessions * 100) if total_sessions > 0 else 0
    net_pnl = sum(s.get("total_pnl", 0) for s in sessions)
    avg_pnl = net_pnl / total_sessions if total_sessions > 0 else 0
    best_pnl = max([s.get("total_pnl", 0) for s in sessions], default=0)
    worst_pnl = min([s.get("total_pnl", 0) for s in sessions], default=0)
    
    return {
        "summary": {
            "total_sessions": total_sessions,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "net_pnl": net_pnl,
            "avg_pnl": avg_pnl,
            "best_pnl": best_pnl,
            "worst_pnl": worst_pnl,
        },
        "ts_ist": ist_now_str()
    }


# ── REST: Accounts (per-trader virtual balance + PnL breakdown) ───────────────

@app.get("/api/accounts")
async def get_accounts():
    """
    Per-trader virtual account snapshot: balance, equity, unrealized PnL,
    session realized PnL (closed trades this session, from executor state),
    and overall PnL (equity − $100k start).

    session_realized_pnl differs from the paper engine's session_pnl:
    - session_realized_pnl = sum of closed-trade PnL for the current active position
    - paper engine session_pnl = equity − session_start_balance (includes unrealized)
    """
    import backend.main as m
    STARTING = cfg.virtual_balance_usdt

    traders: dict = {}

    for key, ex_attr, paper_attr in [
        ("bull", "bullish", "_bull_paper"),
        ("bear", "bearish", "_bear_paper"),
    ]:
        ex    = getattr(m, ex_attr, None)
        paper = getattr(m, paper_attr, None)
        if not paper:
            continue
        summary  = await _reconcile_flat_summary(key, paper.get_summary(), paper)
        has_fut  = bool(summary.get("open_positions"))
        has_opt  = bool(summary.get("option_positions"))
        has_pos  = has_fut or has_opt

        position = None
        if ex and has_pos:
            position = {}
            if ex.futures_remaining_qty > 0:
                position["futures"] = {
                    "side":    ex._futures_side,
                    "qty":     ex.futures_remaining_qty,
                    "entry":   ex.futures_entry_price,
                    "current": ex.current_price,
                }
            if ex.hedge_symbol:
                position["option"] = {
                    "symbol": ex.hedge_symbol,
                    "qty":    ex.hedge_qty,
                    "entry":  ex.hedge_fill_price,
                }

        session_realized = 0.0
        if ex:
            session_realized = round(
                ex.session_realized_futures_pnl + ex.session_realized_hedge_pnl, 2)

        traders[key] = {
            "name":                 ex.name if ex else key,
            "state":                ex.state.value if ex else "UNKNOWN",
            "starting_balance":     STARTING,
            "balance":              summary["balance"],
            "has_position":         has_pos,
            "position":             position,
            "unrealized_pnl":       summary["unrealized_pnl"],
            "session_realized_pnl": session_realized,
            "overall_pnl":          summary["total_pnl"],
            "equity":               summary["equity"],
        }

    vol       = getattr(m, "volatile", None)
    vol_paper = getattr(m, "_vol_paper", None)
    if vol_paper:
        summary    = await _reconcile_flat_summary("vol", vol_paper.get_summary(), vol_paper)
        vol_status = vol.get_status() if vol else {}
        has_pos    = (vol._state.value == "MANAGING") if vol else False

        position = None
        if vol and has_pos:
            position = {
                "put":  vol._put_leg  or {},
                "call": vol._call_leg or {},
            }

        traders["vol"] = {
            "name":                 vol.name if vol else "VolatileTrader_Paper",
            "state":                vol._state.value if vol else "UNKNOWN",
            "starting_balance":     STARTING,
            "balance":              summary["balance"],
            "has_position":         has_pos,
            "position":             position,
            "unrealized_pnl":       round(vol_status.get("unrealized_pnl", 0.0), 2),
            "session_realized_pnl": round(vol._session_pnl, 2) if vol else 0.0,
            "overall_pnl":          summary["total_pnl"],
            "equity":               summary["equity"],
        }

    combined = {}
    if traders:
        combined = {
            "starting_balance":       STARTING * len(traders),
            "equity":                 round(sum(r["equity"]               for r in traders.values()), 2),
            "overall_pnl":            round(sum(r["overall_pnl"]          for r in traders.values()), 2),
            "total_unrealized":       round(sum(r["unrealized_pnl"]       for r in traders.values()), 2),
            "total_session_realized": round(sum(r["session_realized_pnl"] for r in traders.values()), 2),
        }

    return {"traders": traders, "combined": combined, "ts_ist": ist_now_str()}


# ── Panel HTML (no Node.js / build step needed) ───────────────────

_PANEL = os.path.join(
    os.path.dirname(__file__), "..", "..", "panel.html"
)

@app.get("/")
@app.get("/panel")
async def serve_panel():
    path = os.path.abspath(_PANEL)
    if not os.path.exists(path):
        return {"error": "panel.html not found"}
    with open(path, "rb") as f:
        content = f.read()
    return Response(
        content=content,
        media_type="text/html",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ── Startup: wire bus → WS ────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup():
    bus.subscribe("*", _bus_to_ws)
    log.info("Gateway started — bus bridged to WebSocket.")
