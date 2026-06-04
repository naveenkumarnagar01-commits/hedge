"""
BaseExecutor — self-contained trading engine.

Each executor is fully independent:
  - Fetches nearest Order Block zone at window open (demand for bull, supply for bear)
  - Manages own $100k virtual balance via dedicated paper engine
  - Tracks position lifecycle: SLEEP → VERIFY → EXECUTE → MANAGING → SQUAREOFF

Key rules:
  - Trading window: Mon–Fri only (weekends auto-skip)
  - Options are NEVER sold before squareoff time
  - Squareoff sequence: harvest profitable leg first → then other (both limit/market)
  - Partial booking: sell 50% futures when fut_pnl >= premium × ratio
  - Rebuy limit: avg ± 2×TV (only fills when price crosses that level)
  - Full TP: based on session_pnl_target (futures only, adjusts after partial)
  - Per-trader config via self._cfg(key) which prepends bull_/bear_/vol_ prefix
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Optional

from backend.message_bus import (
    bus,
    TICK_FUTURES,
    POSITION_UPDATE, LOG_EVENT, SQUAREOFF_START,
    EXECUTOR_MONITOR,
)
from backend.config import cfg
from backend.state_store import store
from backend.utils import (ist_now, ist_now_str,
                           is_in_session_range, has_reached_session_time,
                           get_session_day)

SET_LINES = "SET_LINES"


class ExState(Enum):
    SLEEP             = "SLEEP"
    CHECK_ELIGIBILITY = "CHECK_ELIGIBILITY"
    WAIT_TRIGGER      = "WAIT_TRIGGER"
    VERIFY_HEDGE_LOOP = "VERIFY_HEDGE_LOOP"
    EXECUTE           = "EXECUTE"
    MANAGING_POSITION = "MANAGING_POSITION"
    PARTIAL_BOOKING   = "PARTIAL_BOOKING"
    FORCE_CLOSE       = "FORCE_CLOSE"


class BaseExecutor:
    """
    Subclasses must implement:
      _futures_side          -> "BUY" | "SELL"
      _option_side           -> "P"   | "C"
      _cfg_prefix            -> "bull_" | "bear_" | "vol_"
      _is_eligible(price, target_line)   -> bool
    """

    def __init__(self, name: str, direction: str,
                 is_paper: bool = False, force_window: bool = False,
                 paper_engine=None):
        self.name         = name
        self.direction    = direction
        self.is_paper     = is_paper
        self.force_window = force_window
        self.log          = logging.getLogger(name)

        # Each executor has its own paper engine instance ($100k independent balance)
        self._paper = paper_engine

        # Zone boundary lines — set from OB zone at window open (top/bottom for display)
        self.high_line: Optional[float] = None
        self.low_line:  Optional[float] = None

        # Live price
        self.mark_price:    float = 0.0
        self.current_price: float = 0.0

        # State machine
        self.state = ExState.SLEEP

        # Eligibility (reset daily)
        self.eligible_today: Optional[bool] = None
        self._eligibility_date: str = ""

        # Trigger info
        self.trigger_type:  str   = ""
        self.triggered:     bool  = False
        self.trigger_time:  str   = ""
        self.trigger_line:  float = 0.0
        self._loop_iter:    int   = 0
        self._far_check:    Optional[bool] = None

        # Position data
        self.hedge_symbol:              str   = ""
        self.hedge_fill_price:          float = 0.0
        self.hedge_qty:                 float = 0.0
        self.hedge_premium_paid:        float = 0.0
        self.hedge_intrinsic_at_entry:  float = 0.0
        self.hedge_tv_at_entry:         float = 0.0
        self.futures_entry_price:       float = 0.0
        self.futures_qty:               float = 0.0
        self.futures_remaining_qty:     float = 0.0
        self.partial_done:              bool  = False
        self._rebuy_order_price:        float = 0.0

        # Pending rebuy limit order (paper: price-triggered, live: exchange limit)
        self.pending_rebuy_price: float = 0.0
        self.pending_rebuy_qty:   float = 0.0

        # Pre-calculated price levels for display (updated on every position change)
        self.partial_trigger_price: float = 0.0  # futures price at which 50% books
        self.full_close_price:      float = 0.0  # approx futures price for full TP

        # Session realized PnL — accumulated as each leg closes
        # Persists until daily reset so post-squareoff display shows correct final values
        self.session_realized_futures_pnl: float = 0.0
        self.session_realized_hedge_pnl:   float = 0.0

        # Full analysis report — stored after self-analysis, shown in "Check Details"
        self.analysis_report: dict = {}

        # Zone context — WHY this line was locked (set at eligibility check)
        # e.g. "above_h"→H-line retest | "between"→L/H bounce | "below_l"→L-line retest
        self.entry_zone:      str = ""
        self.zone_price_snap: float = 0.0  # price at the moment zone was evaluated

        # Verify hedge loop tracking
        self._verify_start_time: float = 0.0
        self._prox_bad_ticks:    int   = 0

        # Locked lines — set once when trading window opens, cleared on daily reset
        self.locked_high_line: Optional[float] = None
        self.locked_low_line:  Optional[float] = None

        # Session start timestamp — set when window opens, used to filter trade history
        self.session_start_ts: float = 0.0
        # Candle-fetch failure cooldown — retry analysis after this timestamp (epoch)
        self._analysis_retry_after: float = 0.0
        # True while _run_ob_analysis is awaited — shown as "CALCULATING" in panel
        self._is_analyzing: bool = False

        # Execution timestamp — set in _execute(), must be initialized to prevent
        # AttributeError when _serialize() is called after restart in MANAGING_POSITION
        self.execution_time_ist: str = ""

        # Guard: prevents concurrent double-entry into _do_force_close()
        self._is_force_closing: bool = False

        # Tasks
        self._main_task: Optional[asyncio.Task] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self):
        bus.subscribe(TICK_FUTURES,    self._on_price)
        bus.subscribe(SQUAREOFF_START, self._on_squareoff_broadcast)
        bus.subscribe(SET_LINES,       self._on_set_lines)

        # Crash recovery from SQLite
        saved = store.get(f"{self.name}_state")
        if saved:
            self._restore_state(saved)

        # Sync open position into paper engine so PnL tracks correctly after restart
        if self._paper is not None:
            self._paper.load_state(getattr(self._paper, "_state_key", "paper_engine_state"))

            # If executor is SLEEP (no active trade), remove any stale positions that
            # belong to this executor from the paper engine. This handles the case where
            # force_close closed the positions but the paper engine's background save_state
            # task didn't complete before a server restart — leaving ghost positions.
            _active_states = {"MANAGING_POSITION", "PARTIAL_BOOKING", "FORCE_CLOSE",
                              "VERIFY_HEDGE_LOOP", "EXECUTE"}
            if (saved.get("state", "SLEEP") if saved else "SLEEP") not in _active_states:
                self._paper.clear_executor_positions(self.name)
                await self._paper.save_state()
                self.log.info(f"{self.name}: cleared stale paper engine positions (executor is SLEEP)")

            self.sync_position_to_paper()

        # Auto-reset if the hedge option has already expired (server was down at force-close time)
        if self._is_option_expired():
            self.log.warning(
                f"Hedge option {self.hedge_symbol} has EXPIRED. "
                f"Forcing full reset — stale position cleared."
            )
            # Realize the full premium loss in the paper engine BEFORE clearing state.
            # Without this, the premium was deducted from balance at buy time but
            # _realized_pnl was never updated → gap between Realized PnL and Total PnL.
            if self.is_paper and self._paper and self.hedge_symbol and self.hedge_qty > 0:
                self._paper.executor = self.name
                from backend.utils import ist_now_str as _isn, utc_now as _utn
                await self._paper.sell_option(
                    self.hedge_symbol, self.hedge_qty, 0.0, action="EXPIRED_AT_STARTUP")
                await store.save_paper_trade(
                    self.name, "EXPIRED_AT_STARTUP", self.hedge_symbol, "SELL",
                    self.hedge_qty, 0.0,
                    -(self.hedge_fill_price * self.hedge_qty),
                    notes=f"Expired while server offline — entry={self.hedge_fill_price:.2f}",
                    ts_ist=_isn(), ts_utc=_utn())
            self._reset_position()
            await self._reset_daily()
            await self._save_state()

        # Check if any pending limit orders would have filled while we were offline
        if saved and self.state == ExState.MANAGING_POSITION and self.is_paper:
            await self._check_missed_fills_on_reconnect()

        self._main_task = asyncio.create_task(self._main_loop())
        self.log.info(f"{self.name} started. is_paper={self.is_paper} force_window={self.force_window}")
        await self._broadcast_position()

    async def stop(self):
        if self._main_task:
            self._main_task.cancel()

    # ── Event handlers ─────────────────────────────────────────────────────

    async def _on_set_lines(self, msg: dict):
        """Manual override — sets the locked target line directly."""
        d = msg.get("data", {})
        h, l = d.get("high_line"), d.get("low_line")
        if h and l:
            self.locked_high_line, self.locked_low_line = float(h), float(l)
            await self._log(f"Lines manually overridden: H={float(h):.2f}  L={float(l):.2f}")

    async def _on_price(self, msg: dict):
        d = msg.get("data", {})
        p = float(d.get("mark_price", 0) or 0)
        if p:
            self.mark_price = p
            self.current_price = p

    async def _on_squareoff_broadcast(self, msg):
        data   = msg.get("data", {}) if isinstance(msg, dict) else {}
        target = data.get("executor", "")
        if target and target != self.name:
            return  # squareoff fired for a different executor
        _has_open_hedge = bool(self.hedge_symbol and self.hedge_qty)
        # Fire for active states OR for SLEEP when option is still held after full futures TP
        _active = self.state not in (ExState.SLEEP, ExState.FORCE_CLOSE)
        _sleep_hedge = self.state == ExState.SLEEP and _has_open_hedge
        if _active or _sleep_hedge:
            await self._log("Squareoff broadcast received — initiating force close.")
            await self._do_force_close()

    # ── Per-trader config accessor ─────────────────────────────────────────

    def _cfg(self, key: str):
        """Read per-trader config: prepends bull_ or bear_ prefix."""
        full_key = self._cfg_prefix + key
        return getattr(cfg, full_key, None)

    # ── Main loop ──────────────────────────────────────────────────────────

    async def _main_loop(self):
        _last_broadcast = 0.0
        while True:
            try:
                await self._step()
                # Broadcast position every 5s when in position, else every 15s
                now = time.time()
                interval = 5 if self.state in (ExState.MANAGING_POSITION, ExState.PARTIAL_BOOKING) else 15
                if now - _last_broadcast >= interval:
                    _last_broadcast = now
                    await self._broadcast_position()
            except Exception as e:
                self.log.error(f"Main loop error: {e}", exc_info=True)
            await asyncio.sleep(1)

    async def _step(self):
        p = self.current_price
        if not p:
            return

        now_ist  = ist_now()
        now_time = now_ist.time()
        pfx      = self._cfg_prefix
        today    = now_ist.strftime("%Y-%m-%d")
        _exp_h   = int(getattr(cfg, "session_expiry_h", 13))
        _exp_m   = int(getattr(cfg, "session_expiry_m", 30))
        _now_h, _now_m = now_ist.hour, now_ist.minute
        # Session day: same for the entire session window even across midnight.
        # e.g. June 1 22:10 AND June 2 02:00 both return "2026-06-01" when
        # the session opened June 1 13:31. Prevents false midnight reset.
        session_day = get_session_day(now_ist, _exp_h, _exp_m)

        # ── New Session Reset (NOT calendar-day — session-aware) ────────────
        if self._eligibility_date and self._eligibility_date != session_day:
            if self.state not in (ExState.MANAGING_POSITION, ExState.PARTIAL_BOOKING):
                await self._log(f"New session detected ({session_day}). Resetting state to SLEEP.")
                await self._reset_daily()

        # ── FORCE_CLOSE recovery (crash-recovery guard) ────────────────────
        # If server crashed mid-force-close, state is restored as FORCE_CLOSE
        # but the position is already zero. Detect this and escape to SLEEP.
        if self.state == ExState.FORCE_CLOSE:
            if not self.futures_remaining_qty and not self.hedge_symbol:
                await self._log(
                    "FORCE_CLOSE: no open position detected (crash-recovery) — resetting to SLEEP.",
                    level="WARNING"
                )
                self._reset_position()
                await self._reset_daily()
                await self._save_state()
                await self._broadcast_position()
            else:
                # Position still open after crash — complete the close now
                await self._log("FORCE_CLOSE: open position found on restart — completing close.", level="WARNING")
                await self._do_force_close()
            await self._publish_monitor(p, False)
            return

        # None-safe integer config reader — treats 0 as a valid value (not falsy default)
        def _ci(key, default): v = self._cfg(key); return int(v) if v is not None else default

        # ── FORCE CLOSE check (session-aware: handles cross-midnight) ─────────
        _fc_h = _ci("force_close_h", 18)
        _fc_m = _ci("force_close_m", 30)
        _has_open_hedge = bool(self.hedge_symbol and self.hedge_qty)
        _force_reached = has_reached_session_time(_now_h, _now_m, _fc_h, _fc_m, _exp_h, _exp_m)
        if _force_reached:
            if self.state in (ExState.MANAGING_POSITION, ExState.PARTIAL_BOOKING):
                # Only fire if the position was entered BEFORE force_close in session order.
                # Prevents spurious force-close for trades opened after the force_close time
                # (e.g. evening-window trade with a next-day 04:00 force_close).
                _should_force = True
                if self.execution_time_ist:
                    try:
                        _tp = self.execution_time_ist.split(" ")[1]  # "HH:MM:SS"
                        _eh, _em, _ = [int(x) for x in _tp.split(":")]
                        # If trade was entered AFTER the force_close point in session order,
                        # this session's force_close hasn't arrived yet — skip.
                        _entry_after_fc = (
                            has_reached_session_time(_eh, _em, _fc_h, _fc_m, _exp_h, _exp_m)
                            and (_eh, _em) != (_fc_h, _fc_m)
                        )
                        if _entry_after_fc:
                            _should_force = False
                    except Exception:
                        pass
                if _should_force:
                    await self._do_force_close()
                    return
            elif self.state in (ExState.VERIFY_HEDGE_LOOP, ExState.CHECK_ELIGIBILITY,
                                 ExState.EXECUTE, ExState.WAIT_TRIGGER):
                await self._log(
                    f"Force-close time reached during {self.state.value} — aborting pre-entry, going SLEEP.",
                    level="WARNING"
                )
                await self._reset_daily()
                await self._save_state()
                return
            elif self.state == ExState.SLEEP and _has_open_hedge:
                await self._do_force_close()
                return

        # ── Window bounds (session-aware: handles cross-midnight) ────────────
        _ts_h = _ci("trade_start_h", 4)
        _ts_m = _ci("trade_start_m", 0)
        _te_h = _ci("trade_end_h",  18)
        _te_m = _ci("trade_end_m",  30)
        in_window = self.force_window or is_in_session_range(
            _now_h, _now_m, _ts_h, _ts_m, _te_h, _te_m, _exp_h, _exp_m
        )

        # ── SLEEP ────────────────────────────────────────────────────────────
        if self.state == ExState.SLEEP:
            if in_window:
                # If already determined INELIGIBLE today, stay SLEEP — no re-check
                # UNLESS price has since moved into eligibility range (e.g. OB approached)
                if self._eligibility_date == session_day and self.eligible_today is False:
                    tl = self.locked_high_line or self.locked_low_line
                    if tl and self._is_eligible(p, tl):
                        pass  # price now in range — fall through to re-evaluate
                    else:
                        await self._publish_monitor(p, in_window)
                        return
                # If analysis failed (no candle data), wait for retry cooldown
                retry_after = getattr(self, "_analysis_retry_after", 0)
                if retry_after and time.time() < retry_after:
                    await self._publish_monitor(p, in_window)
                    return
                # Mark session start — clears previous session's trades (in-memory only)
                today_date = ist_now().date()
                from backend.utils import is_blackout_day
                _skip_val = self._cfg("skip_weekends")
                skip_wk = True if _skip_val is None else bool(_skip_val)
                _blk_val = self._cfg("blackout_dates")
                blk_dates = "" if _blk_val is None else str(_blk_val)
                if is_blackout_day(today_date, skip_wk, blk_dates):
                    await self._log(f"Calendar blackout: {today_date.strftime('%A %Y-%m-%d')} — staying SLEEP")
                    self.eligible_today = False
                    self._eligibility_date = today_date.isoformat()
                    await self._publish_monitor(p, False)
                    return

                if self.session_start_ts == 0.0:
                    self.session_start_ts = time.time()
                    if self._paper is not None:
                        self._paper.clear_session_trades()
                self.state = ExState.CHECK_ELIGIBILITY
            await self._publish_monitor(p, in_window)
            return

        # ── CHECK_ELIGIBILITY ────────────────────────────────────────────────
        if self.state == ExState.CHECK_ELIGIBILITY:
            # Guard: window must still be open — window can close in the same tick
            if not in_window:
                await self._log("Window closed before eligibility completed. Back to SLEEP.")
                await self._reset_daily()
                await self._publish_monitor(p, False)
                return
            await self._do_eligibility_check(p, in_window)
            await self._broadcast_position()   # push fresh analysis_report to panel immediately
            await self._publish_monitor(p, in_window)
            return

        # ── WAIT_TRIGGER ─────────────────────────────────────────────────────
        # Reached via crash-recovery from DB (hedge-fill timeout previously set this).
        # Safely redirect back to VERIFY or CHECK depending on state.
        if self.state == ExState.WAIT_TRIGGER:
            if not in_window:
                await self._log("Window closed (WAIT_TRIGGER). Back to SLEEP.")
                await self._reset_daily()
                await self._publish_monitor(p, False)
                return
            if self.triggered and self.trigger_line:
                dist = abs(p - self.trigger_line)
                max_dist = self._cfg("max_distance_from_line") or 100
                if dist <= max_dist:
                    await self._log("WAIT_TRIGGER: price near line, re-entering VERIFY_HEDGE_LOOP.")
                    self.state = ExState.VERIFY_HEDGE_LOOP
                    self._verify_start_time = time.time()
                    self._prox_bad_ticks    = 0
                # else: stay in WAIT_TRIGGER until price comes back
            else:
                self.state = ExState.CHECK_ELIGIBILITY
            await self._publish_monitor(p, in_window)
            return

        # ── VERIFY_HEDGE_LOOP ─────────────────────────────────────────────────
        if self.state == ExState.VERIFY_HEDGE_LOOP:
            if not in_window:
                await self._log("Window closed during hedge validation. Back to SLEEP.")
                await self._reset_daily()
                await self._publish_monitor(p, False)
                return
            await self._do_verify_hedge(p, in_window)
            return

        # ── MANAGING_POSITION ─────────────────────────────────────────────────
        if self.state == ExState.MANAGING_POSITION:
            await self._do_manage(p)
            await self._publish_monitor(p, in_window)
            return

        # PARTIAL_BOOKING / EXECUTE / FORCE_CLOSE handled in-place
        await self._publish_monitor(p, in_window)

    # ── State handlers ─────────────────────────────────────────────────────

    async def _do_eligibility_check(self, price: float, in_window: bool = False):
        """
        Self-contained analysis: fetch N candles, find dominant S/R line,
        determine zone, set target_line. No analyst dependency.
        """
        # Guard: only run if still in CHECK_ELIGIBILITY (prevents double-execution race)
        if self.state != ExState.CHECK_ELIGIBILITY:
            return
        _exp_h = int(getattr(cfg, "session_expiry_h", 13))
        _exp_m = int(getattr(cfg, "session_expiry_m", 30))
        session_day = get_session_day(ist_now(), _exp_h, _exp_m)
        self._eligibility_date = session_day
        self.zone_price_snap   = price

        # ── OB-based analysis (once per session at window open) ──────────────
        today_analyzed = (
            self.analysis_report.get("analysis_time", "")[:10] == session_day
            and self.analysis_report.get("ob_zone") is not None
            and self.locked_high_line is not None
        )
        if not today_analyzed:
            self._is_analyzing = True
            zone_type = "demand" if self.direction == "BULLISH" else "supply"
            await self._log(
                f"Fetching Order Block zones at window open | "
                f"direction={self.direction} → nearest {zone_type} zone"
            )
            await self._publish_monitor(price, in_window)
            try:
                target_line = await self._run_ob_analysis()
                if not target_line:
                    error_msg = self.analysis_report.get("error", "no zone found")
                    no_candles = "candle" in error_msg.lower()
                    if no_candles:
                        await self._log(
                            f"OB analysis failed: {error_msg}. Will retry in 5 minutes.",
                            level="WARNING"
                        )
                        self._analysis_retry_after = time.time() + 300
                    else:
                        await self._log(
                            f"No qualifying {zone_type} OB zone: {error_msg}. "
                            f"Ineligible today.",
                            level="WARNING"
                        )
                    self._eligibility_date = session_day
                    self.eligible_today    = False
                    self.locked_high_line  = None
                    self.locked_low_line   = None
                    self.state = ExState.SLEEP
                    return
            finally:
                self._is_analyzing = False

            zone = self.analysis_report.get("ob_zone", {})
            # locked lines = mid (the trigger reference level)
            self.locked_high_line = target_line
            self.locked_low_line  = target_line
            # display lines = zone boundaries so chart shows full zone band
            self.high_line = float(zone.get("top",    target_line))
            self.low_line  = float(zone.get("bottom", target_line))
            await self._log(
                f"OB analysis complete → {zone_type.upper()} zone "
                f"${zone.get('bottom', 0):.0f}–${zone.get('top', 0):.0f} "
                f"| mid={target_line:.0f} | {zone.get('grade', '')} "
                f"(score={zone.get('score', 0)}) | age={zone.get('age_bars', 0)} bars"
            )
        else:
            zone        = self.analysis_report.get("ob_zone", {})
            target_line = float(zone.get("mid", self.locked_high_line))
            await self._log(
                f"OB analysis already done today → reusing mid={target_line:.2f}"
            )

        target = self.locked_high_line   # this IS the target line for this trader

        # Eligibility: is price in the correct zone for this direction?
        eligible = self._is_eligible(price, target)
        self.eligible_today = eligible

        if not eligible:
            zone_reason = self._ineligible_reason(price, target)
            await self._log(
                f"INELIGIBLE: {zone_reason}  Price={price:.2f}  Line={target:.2f}"
            )
            self.state = ExState.SLEEP
            return

        # Determine zone label for display
        zone, zone_desc = self._zone_info(price, target)

        self.entry_zone   = zone
        self.trigger_type = "target_line"
        self.trigger_line = target
        self._loop_iter   = 0
        self._far_check   = None
        self.triggered    = True
        self.trigger_time = ist_now_str()
        self._verify_start_time = time.time()
        self._prox_bad_ticks    = 0
        self.state = ExState.VERIFY_HEDGE_LOOP

        await self._log(
            f"ELIGIBLE | {zone_desc} | "
            f"Watching {target:.2f} ±{self._cfg('max_distance_from_line')}pts | "
            f"All 4 hedge conditions must pass simultaneously."
        )
        history = store.get(f"{self.name}_triggers", [])
        history.append({
            "ts":           self.trigger_time,
            "trigger_type": self.trigger_type,
            "entry_zone":   zone,
            "trigger_line": round(target, 2),
            "price":        round(price, 2),
            "result":       "pending",
        })
        if len(history) > 100:
            history = history[-100:]
        await store.set(f"{self.name}_triggers", history)

    async def _run_ob_analysis(self) -> Optional[float]:
        """
        Fetch Order Block zones at window open (once per session).
        Bull: returns nearest demand zone mid as target_line.
        Bear: returns nearest supply zone mid as target_line.
        Stores full zone data in self.analysis_report.
        """
        from backend.data.futures_feed import get_candles, fetch_historical_candles
        from backend.data.order_blocks import detect

        n      = int(getattr(cfg, "ob_candle_count", 500))
        tf_str = str(self._cfg("ob_tf") or getattr(cfg, "ob_tf", "15m"))

        candles = get_candles(n, tf_str)
        if len(candles) < max(n // 2, 10):
            candles = await fetch_historical_candles(n, tf_str)
        if not candles:
            self.analysis_report = {"error": "No candle data for OB analysis"}
            return None

        result    = detect(candles)
        zone_type = "demand" if self.direction == "BULLISH" else "supply"
        zones     = result.get(zone_type, [])

        if not zones:
            self.analysis_report = {
                "error":         f"No {zone_type} zone detected",
                "ob_zone":       None,
                "ob_type":       zone_type,
                "current_price": result.get("current_price", 0),
                "tf":            tf_str,
                "analysis_time": ist_now_str(),
            }
            return None

        zone = zones[0]   # nearest zone (detect() sorts by distance_pct)
        self.analysis_report = {
            "ob_zone":       zone,
            "ob_type":       zone_type,
            "selected_line": zone["mid"],
            "current_price": result.get("current_price", 0),
            "candle_count":  result.get("candle_count", 0),
            "tf":            tf_str,
            "all_demand":    result.get("demand", []),
            "all_supply":    result.get("supply", []),
            "analysis_time": ist_now_str(),
        }
        return float(zone["mid"])

    def _zone_info(self, price: float, target: float):
        """Returns (zone_key, zone_description) for display."""
        zone_type = "demand" if self.direction == "BULLISH" else "supply"
        max_dist  = self._cfg("max_distance_from_line") or 100.0
        dist      = abs(price - target)
        direction = "▲ above" if price > target else "▼ below"
        return (
            "near_ob",
            f"Price ${price:.0f} within {dist:.0f}pts of "
            f"{zone_type} OB mid ${target:.0f} ({direction}, ±{max_dist:.0f}pt window)"
        )

    def _ineligible_reason(self, price: float, target: float) -> str:
        zone_type = "demand" if self.direction == "BULLISH" else "supply"
        max_dist  = self._cfg("max_distance_from_line") or 100.0
        dist      = abs(price - target)
        return (
            f"Price {price:.0f} is {dist:.0f}pts from "
            f"{zone_type} OB mid {target:.0f} — "
            f"need price within ±{max_dist:.0f}pts"
        )

    async def _do_verify_hedge(self, price: float, in_window: bool):
        """
        Continuous 1s loop. All 4 conditions must pass simultaneously to execute:
          1. Proximity  : price within max_distance_from_line pts of trigger line
          2. Premium    : option ask ≤ max_premium
          3. Time Value : option TV ≤ max_time_value
          4. Spread     : ask/mark spread ≤ price_diff_percent %

        No abort on individual condition fail — just keep waiting.
        Only abort on: timeout or window close.
        """
        max_dist = self._cfg("max_distance_from_line")
        timeout  = cfg.verify_hedge_timeout_sec
        dist     = abs(price - self.trigger_line)
        self._loop_iter += 1

        # Timeout — only hard abort besides window close
        elapsed = time.time() - self._verify_start_time
        if elapsed > timeout:
            await self._log(
                f"VERIFY TIMEOUT: no conditions fully met in {elapsed:.0f}s "
                f"(limit={timeout}s). Back to SLEEP.",
                level="WARNING"
            )
            self._update_last_trigger_result("aborted-timeout")
            await self._reset_daily()
            await self._publish_monitor(price, in_window, prox_ok=False)
            return

        # Condition 1 — Proximity (simple check, no abort on fail)
        prox_ok = dist <= max_dist

        # Condition 2/3/4 — Option eligibility
        from backend.data.options_feed import get_nearest_itm_put, get_nearest_itm_call
        itm = (get_nearest_itm_put(price) if self._option_side == "P"
               else get_nearest_itm_call(price))

        max_prem = self._cfg("max_premium")
        max_tv   = self._cfg("max_time_value")
        max_sprd = self._cfg("price_diff_percent")

        prem_ok = spread_ok = tv_ok = None
        if itm:
            ask  = float(itm.get("ask") or 0)
            mark = float(itm.get("mark") or 0)
            intr = float(itm.get("intrinsic") or 0)
            tv   = float(itm.get("time_value") or max(ask - intr, 0))
            sprd = abs(ask - mark) / mark * 100 if mark > 0 else 100.0
            prem_ok   = ask > 0 and ask <= max_prem
            tv_ok     = tv <= max_tv
            spread_ok = sprd <= max_sprd

        # All 4 must pass at same moment → execute
        hedge_valid = bool(itm and prox_ok and prem_ok and tv_ok and spread_ok)
        await self._publish_monitor(
            price, in_window,
            itm=itm, prem_ok=prem_ok, tv_ok=tv_ok, spread_ok=spread_ok,
            prox_ok=prox_ok, hedge_valid=hedge_valid, dist_from_line=dist,
            loop_iter=self._loop_iter,
        )

        if not hedge_valid:
            return   # keep looping

        # All conditions met → EXECUTE
        # Guard: if state changed externally (e.g. concurrent call), don't double-execute
        if self.state != ExState.VERIFY_HEDGE_LOOP:
            return
        self.state = ExState.EXECUTE
        await self._log(
            f"All hedge conditions met! Executing:  "
            f"symbol={itm['symbol']}  ask={itm['ask']:.2f}  "
            f"intrinsic={itm.get('intrinsic', 0):.2f}  TV={tv:.2f}  spread={sprd:.2f}%"
        )
        await self._execute(itm, price)

    async def _do_manage(self, price: float):
        """MANAGING_POSITION: watch partial profit trigger, pending rebuy, and full close target."""
        if not self.futures_entry_price or not self.hedge_premium_paid:
            return

        # Recompute display levels every tick so panel reflects any config change immediately
        self._recalc_price_levels()

        # Futures unrealized PnL (mark-to-market, Binance style)
        fut_pnl = self._futures_unrealized_pnl(price, self.futures_remaining_qty)

        # Hedge unrealized PnL — Binance style: mark → bid → intrinsic
        from backend.data.options_feed import get_chain
        chain     = get_chain()
        opt       = chain.get(self.hedge_symbol, {})
        mark_p    = float(opt.get("mark",      0) or 0)
        bid_p     = float(opt.get("bid",       0) or 0)
        intr_p    = float(opt.get("intrinsic", 0) or 0)
        cur_val   = mark_p if mark_p > 0 else (bid_p if bid_p > 0 else intr_p)
        hedge_pnl = (cur_val - self.hedge_fill_price) * self.hedge_qty
        total_pnl = fut_pnl + hedge_pnl

        # Session PnL target is FUTURES ONLY (option is never sold before squareoff)
        # full_close_target is fallback if session_pnl_target not set
        session_target = (self._cfg("session_pnl_target")
                          or self._cfg("full_close_target") or 600.0)
        already_realized = self.session_realized_futures_pnl
        remaining_needed = session_target - already_realized
        full_target      = remaining_needed   # hit this with remaining futures position

        # ── Check pending REBUY limit order (price-triggered, paper simulation) ──────
        if self.pending_rebuy_price > 0 and self.pending_rebuy_qty > 0:
            rebuy_crossed = (
                (self.direction == "BULLISH" and price <= self.pending_rebuy_price) or
                (self.direction == "BEARISH" and price >= self.pending_rebuy_price)
            )
            if rebuy_crossed:
                # Rebuy limit filled — update avg entry price
                rbx_px  = self.pending_rebuy_price
                rbx_qty = self.pending_rebuy_qty
                old_qty = self.futures_remaining_qty
                new_qty = old_qty + rbx_qty
                new_avg = (self.futures_entry_price * old_qty + rbx_px * rbx_qty) / new_qty
                self.futures_entry_price   = round(new_avg, 2)
                self.futures_qty           = new_qty
                self.futures_remaining_qty = new_qty
                self.pending_rebuy_price   = 0.0
                self.pending_rebuy_qty     = 0.0

                paper = self._paper
                paper.executor = self.name
                await paper.place_futures_order("BTCUSDT", self._futures_side,
                                                rbx_qty, rbx_px, action="PARTIAL_REBUY")
                await store.log_trade(self.name, "PARTIAL_REBUY", "BTCUSDT",
                                      self._futures_side, rbx_qty, rbx_px, 0.0, "FILLED", {}, self.is_paper)
                from backend.utils import ist_now_str as _isn, utc_now as _utn
                await store.save_paper_trade(
                    self.name, "PARTIAL_REBUY", "BTCUSDT", self._futures_side,
                    rbx_qty, rbx_px, 0.0,
                    notes=f"rebuy limit filled | new_avg={new_avg:.2f}",
                    ts_ist=_isn(), ts_utc=_utn())
                self._recalc_price_levels()  # TP levels update with new avg
                await self._log(
                    f"REBUY LIMIT FILLED @ {rbx_px:.2f}  new_avg={new_avg:.2f}  "
                    f"qty={new_qty}  new_full_close_price≈{self.full_close_price:.2f}"
                )
                await self._save_state()
                await self._broadcast_position()
                return

        # ── Full close target (futures only — option stays open until squareoff) ────
        if fut_pnl >= full_target:
            # Cancel any pending rebuy before closing
            if self.pending_rebuy_price > 0:
                await self._log(f"TP hit — cancelling pending rebuy @ {self.pending_rebuy_price:.2f}")
                self.pending_rebuy_price = 0.0
                self.pending_rebuy_qty   = 0.0
            await self._log(f"FULL CLOSE TARGET HIT: total_pnl={total_pnl:.2f} >= {full_target:.2f}")
            await self._do_full_close(price)
            return

        # ── Partial booking: one-time trigger ─────────────────────────────────────
        if not self.partial_done:
            partial_trigger = self.hedge_premium_paid * self._cfg("partial_profit_ratio")
            if fut_pnl >= partial_trigger:
                await self._log(
                    f"PARTIAL BOOKING TRIGGER: futures_pnl={fut_pnl:.2f} >= "
                    f"premium×ratio={partial_trigger:.2f}  → selling 50%"
                )
                self.state = ExState.PARTIAL_BOOKING
                await self._do_partial_booking(price)
                return

    # ── Order execution ────────────────────────────────────────────────────

    def _update_last_trigger_result(self, result: str):
        """Mark the most-recent trigger event with its outcome."""
        history = store.get(f"{self.name}_triggers", [])
        if history and history[-1].get("result") == "pending":
            history[-1]["result"] = result
            import asyncio
            asyncio.create_task(store.set(f"{self.name}_triggers", history))

    async def _execute(self, itm: dict, price: float):
        """Step 1: buy hedge (limit @ best ask, confirm fill). Step 2: enter futures."""
        # Idempotency guard: abort if already in a position (prevents double-execution)
        if self.futures_entry_price > 0 or self.state == ExState.MANAGING_POSITION:
            await self._log("_execute called but position already active — skipping.", level="WARNING")
            return

        paper = self._paper

        sym = itm["symbol"]
        ask = itm["ask"]
        qty = self._cfg("contract_qty")

        paper.executor = self.name

        # Reset per-trade realized PnL so the display reflects THIS trade only
        self.session_realized_futures_pnl = 0.0
        self.session_realized_hedge_pnl   = 0.0

        # 1. BUY HEDGE first
        if self.is_paper:
            # Check balance BEFORE attempting — if balance floor would be hit, stop immediately
            cost = ask * qty
            if paper.balance - cost < cfg.min_paper_balance:
                await self._log(
                    f"Insufficient paper balance: ${paper.balance:.2f} - ${cost:.2f} cost "
                    f"< min ${cfg.min_paper_balance:.2f}. Reset balance via panel to continue. "
                    f"Going SLEEP.",
                    level="ERROR"
                )
                self.state = ExState.SLEEP
                self._eligibility_date = ist_now().strftime("%Y-%m-%d")
                self.eligible_today    = False   # block re-entry today
                return
            fill = await paper.buy_option(sym, qty, ask, action="HEDGE_BUY")
        else:
            from backend.execution.binance_client import client
            fill = await client.place_option_order(sym, "BUY", qty, ask,
                                                   timeout_sec=cfg.fill_timeout_sec)

        if not fill or not fill.get("filled"):
            await self._log(f"Hedge order not filled within timeout — re-entering hedge verification.", level="WARNING")
            # Re-enter VERIFY_HEDGE_LOOP (not WAIT_TRIGGER — _do_wait_trigger is undefined)
            self.state = ExState.VERIFY_HEDGE_LOOP
            self._verify_start_time = time.time()
            self._prox_bad_ticks    = 0
            return

        fill_px = float(fill.get("avg_price", ask))
        self.hedge_symbol             = sym
        self.hedge_fill_price         = fill_px
        self.hedge_qty                = qty
        self.hedge_premium_paid       = fill_px * qty
        self.hedge_intrinsic_at_entry = float(itm.get("intrinsic", 0))
        self.hedge_tv_at_entry        = float(
            itm.get("time_value") or max(fill_px - self.hedge_intrinsic_at_entry, 0)
        )

        await self._log(
            f"HEDGE FILLED: {sym}  fill={fill_px:.2f}  "
            f"premium={self.hedge_premium_paid:.2f} USDT  "
            f"intrinsic={self.hedge_intrinsic_at_entry:.2f}  TV={self.hedge_tv_at_entry:.2f}"
        )
        await store.log_trade(self.name, "HEDGE_BUY", sym, "BUY",
                              qty, fill_px, 0.0, "FILLED", {}, self.is_paper)

        # 2. ENTER FUTURES immediately after hedge confirmed
        side            = self._futures_side
        fut_side_action = f"FUTURES_{side}"   # "FUTURES_BUY" or "FUTURES_SELL"
        if self.is_paper:
            fut_fill = await paper.place_futures_order(
                "BTCUSDT", side, qty, price, action=fut_side_action)
        else:
            from backend.execution.binance_client import client
            fut_fill = await client.place_futures_market("BTCUSDT", side, qty)

        if not fut_fill:
            await self._log("Futures entry failed after hedge filled. Manual intervention needed.",
                            level="ERROR")
            if self.is_paper:
                await paper.sell_option(sym, qty, fill_px * 0.9, action="HEDGE_BUY_REVERSED")
            self._reset_position()
            self.state = ExState.SLEEP
            return

        _raw_avg = fut_fill.get("avg_price")
        fut_px   = float(_raw_avg) if _raw_avg else float(price)   # never allow 0 avg cost
        if fut_px <= 0:
            fut_px = float(price)
        self.futures_entry_price   = fut_px
        self.futures_qty           = qty
        self.futures_remaining_qty = qty
        self.partial_done          = False
        self.pending_rebuy_price   = 0.0
        self.pending_rebuy_qty     = 0.0
        self.execution_time_ist    = ist_now_str()
        self._recalc_price_levels()

        await self._log(
            f"FUTURES FILLED: {side} {qty} BTC @ {fut_px:.2f}  "
            f"Partial trigger @ futures_pnl >= {self.hedge_premium_paid * self._cfg('partial_profit_ratio'):.2f}  "
            f"Full close target = {self._cfg('full_close_target'):.2f} USDT"
        )
        await store.log_trade(self.name, fut_side_action, "BTCUSDT", side,
                              qty, fut_px, 0.0, "FILLED", {}, self.is_paper)

        self.state = ExState.MANAGING_POSITION
        self._update_last_trigger_result("executed")
        await self._save_state()
        await self._broadcast_position()

        # Persist ALL trades to structured SQLite paper_trades table
        # Pass the exact execution timestamp so DB records match actual order time.
        from backend.utils import utc_now
        exec_ts_utc = utc_now()
        sid = self.execution_time_ist.replace(" ", "_").replace(":", "-")
        await store.save_paper_trade(
            self.name, "HEDGE_BUY", sym, "BUY", qty, fill_px, 0.0,
            session_id=sid, notes=f"hedge premium=${fill_px*qty:.2f}",
            ts_ist=self.execution_time_ist, ts_utc=exec_ts_utc)
        await store.save_paper_trade(
            self.name, f"FUTURES_{side}", "BTCUSDT", side, qty, fut_px, 0.0,
            session_id=sid, notes=f"target_line=${self.locked_high_line or 0:.0f}",
            ts_ist=self.execution_time_ist, ts_utc=exec_ts_utc)
        # Session record
        await store.save_session(
            session_id=sid, trader_name=self.name,
            target_line=self.locked_high_line, entry_zone=self.entry_zone,
            entry_price=fut_px, entry_ts_ist=self.execution_time_ist,
            status="open",
        )

        from backend import telegram_alert as tg
        tg.send(
            f"⚡ <b>{self.name} — TRADE ENTERED</b>\n"
            f"Hedge: {sym} @ <b>${fill_px:.2f}</b>  (TV=${self.hedge_tv_at_entry:.2f})\n"
            f"Futures {self._futures_side} @ <b>${fut_px:.2f}</b>\n"
            f"Premium paid: <b>${self.hedge_premium_paid:.2f}</b>\n"
            f"Time: {ist_now_str()}"
        )

    async def _do_partial_booking(self, price: float):
        """
        Sell 50% futures → rebuy same qty at avg - 2*TV (=> new_avg = avg - TV).
        Hedge remains untouched.
        """
        paper = self._paper
        paper.executor = self.name

        qty_sell = round(self.futures_remaining_qty * 0.5, 4)
        side_sell = "SELL" if self.direction == "BULLISH" else "BUY"

        # Sell 50%
        if self.is_paper:
            fill = await paper.place_futures_order("BTCUSDT", side_sell, qty_sell, price, action="PARTIAL_SELL")
        else:
            from backend.execution.binance_client import client
            fill = await client.place_futures_market("BTCUSDT", side_sell, qty_sell)

        if not fill:
            await self._log("Partial sell failed. Continuing management.", level="WARNING")
            self.state = ExState.MANAGING_POSITION
            return

        sell_px  = float(fill.get("avg_price", price))
        sell_pnl = self._futures_unrealized_pnl(sell_px, qty_sell)
        self.futures_remaining_qty -= qty_sell
        self.session_realized_futures_pnl += sell_pnl   # accumulate partial realized
        await store.log_trade(self.name, "PARTIAL_SELL", "BTCUSDT", side_sell,
                              qty_sell, sell_px, sell_pnl, "FILLED", {}, self.is_paper)
        from backend.utils import utc_now as _utc_now, ist_now_str as _ist_now_str
        _ts_ist = _ist_now_str(); _ts_utc = _utc_now()
        await store.save_paper_trade(
            self.name, "PARTIAL_SELL", "BTCUSDT", side_sell, qty_sell, sell_px, sell_pnl,
            notes=f"realized=${sell_pnl:+.2f}", ts_ist=_ts_ist, ts_utc=_ts_utc)

        # Rebuy at avg_remaining - 2*TV (results in new_avg = avg - TV)
        tv            = self.hedge_tv_at_entry
        avg_remaining = self.futures_entry_price
        if self.direction == "BULLISH":
            rebuy_price = round(avg_remaining - 2 * tv, 2)
        else:
            rebuy_price = round(avg_remaining + 2 * tv, 2)

        # Store as pending limit — fills when price crosses rebuy_price
        # (live trading: also places exchange limit order)
        self.pending_rebuy_price = rebuy_price
        self.pending_rebuy_qty   = qty_sell

        if not self.is_paper:
            from backend.execution.binance_client import client
            await client.place_futures_limit("BTCUSDT", self._futures_side, qty_sell, rebuy_price)

        await self._log(
            f"PARTIAL SELL DONE: sold {qty_sell} BTC @ {sell_px:.2f}  pnl={sell_pnl:+.2f}  | "
            f"REBUY LIMIT SET @ {rebuy_price:.2f}  (avg − 2×TV={tv:.2f}) — waits for price to cross"
        )
        self._recalc_price_levels()

        self.partial_done = True
        self.state = ExState.MANAGING_POSITION
        await self._save_state()
        await self._broadcast_position()

    async def _do_full_close(self, price: float):
        """
        Futures session target hit — close ALL futures.
        Option is KEPT open until squareoff (rule: options never sold before squareoff).
        State → SLEEP but hedge_symbol/qty remain set until _do_force_close().
        """
        paper = self._paper
        paper.executor = self.name

        qty  = self.futures_remaining_qty
        side = "SELL" if self.direction == "BULLISH" else "BUY"

        if self.is_paper:
            fill = await paper.place_futures_order("BTCUSDT", side, qty, price,
                                                   action="FULL_CLOSE_FUTURES")
        else:
            from backend.execution.binance_client import client
            fill = await client.place_futures_market("BTCUSDT", side, qty)

        if fill:
            fp  = float(fill.get("avg_price", price))
            pnl = self._futures_unrealized_pnl(fp, qty)
            self.futures_remaining_qty = 0.0
            self.futures_qty           = 0.0
            self.pending_rebuy_price   = 0.0
            self.pending_rebuy_qty     = 0.0
            self.session_realized_futures_pnl += pnl
            await self._log(
                f"FULL CLOSE futures: {side} {qty} BTC @ {fp:.2f}  pnl={pnl:+.2f}  "
                f"| Option {self.hedge_symbol} kept open until squareoff."
            )
            await store.log_trade(self.name, "FULL_CLOSE_FUTURES", "BTCUSDT", side,
                                  qty, fp, pnl, "FILLED", {}, self.is_paper)
            from backend.utils import ist_now_str as _isn, utc_now as _utn
            await store.save_paper_trade(
                self.name, "FULL_CLOSE_FUTURES", "BTCUSDT", side, qty, fp, pnl,
                notes=f"session target hit | entry={self.futures_entry_price:.2f}",
                ts_ist=_isn(), ts_utc=_utn())

        # Only reset futures fields — hedge remains open
        self.futures_entry_price   = 0.0
        self.futures_qty           = 0.0
        self.futures_remaining_qty = 0.0
        self.partial_done          = False
        self.partial_trigger_price = 0.0
        self.full_close_price      = 0.0
        self.state = ExState.SLEEP
        await self._log("Futures closed (session target). Option held for squareoff.")
        if self.execution_time_ist:
            sid = self.execution_time_ist.replace(" ", "_").replace(":", "-")
            await store.save_session(
                session_id=sid, trader_name=self.name,
                futures_pnl=self.session_realized_futures_pnl,
                hedge_pnl=self.session_realized_hedge_pnl,
                total_pnl=self.session_realized_futures_pnl + self.session_realized_hedge_pnl,
                status="open",
            )
        await self._save_state()
        await self._broadcast_position()

    async def _do_force_close(self):
        """
        Squareoff sequence (harvest profitable leg first):
          - Determine which leg (option or futures) has higher unrealised PnL
          - Close profitable leg FIRST to lock in gains
          - Options: LIMIT sell at mark price (never market for options)
          - Futures: market order
        """
        if self._is_force_closing:
            self.log.warning("_do_force_close called while already running — skipped (concurrent guard)")
            return
        self._is_force_closing = True
        try:
            self.state = ExState.FORCE_CLOSE
            await self._log("SQUAREOFF initiated (force-close time reached)", level="WARNING")

            paper = self._paper
            paper.executor = self.name
            from backend.data.options_feed import get_chain

            price = self.current_price

            # Compute current PnL for each leg to decide close order
            # (harvest profitable leg first to lock in gains)
            chain    = get_chain()
            opt_now  = chain.get(self.hedge_symbol, {}) if self.hedge_symbol else {}
            mark_opt = float(opt_now.get("mark", 0) or opt_now.get("bid", 0) or 0)
            hedge_pnl_now = ((mark_opt - self.hedge_fill_price) * self.hedge_qty
                             if self.hedge_symbol and self.hedge_fill_price else 0.0)
            fut_pnl_now   = (self._futures_unrealized_pnl(price, self.futures_remaining_qty)
                             if self.futures_remaining_qty else 0.0)

            option_first = hedge_pnl_now >= fut_pnl_now   # harvest the better leg first

            async def _close_option():
                if not self.hedge_symbol or not self.hedge_qty:
                    return
                chain2    = get_chain()
                opt2      = chain2.get(self.hedge_symbol, {})
                mark_px   = float(opt2.get("mark",      0) or 0)
                bid_px    = float(opt2.get("bid",       0) or 0)
                # ALWAYS limit order at mark price for options (never market)
                limit_px  = mark_px if mark_px > 0 else bid_px

                if self.is_paper:
                    opt_fill = await paper.sell_option(
                        self.hedge_symbol, self.hedge_qty,
                        limit_px or self.hedge_fill_price * 0.5,
                        action="FORCE_CLOSE_HEDGE")
                else:
                    from backend.execution.binance_client import client
                    opt_fill = await client.place_option_order(
                        self.hedge_symbol, "SELL", self.hedge_qty,
                        limit_px or 0, timeout_sec=10)
                    if not opt_fill or not opt_fill.get("filled"):
                        opt_fill = await client.place_option_order(
                            self.hedge_symbol, "SELL", self.hedge_qty, 0, order_type="MARKET")

                if opt_fill:
                    fp  = float(opt_fill.get("avg_price", limit_px) or limit_px)
                    pnl = (fp - self.hedge_fill_price) * self.hedge_qty
                    self.session_realized_hedge_pnl += pnl
                    await self._log(f"SQUAREOFF option: SELL {self.hedge_qty} {self.hedge_symbol}"
                                     f" @ {fp:.2f}  pnl={pnl:+.2f}")
                    await store.log_trade(self.name, "FORCE_CLOSE_HEDGE", self.hedge_symbol, "SELL",
                                          self.hedge_qty, fp, pnl, "FILLED", {}, self.is_paper)
                    from backend.utils import ist_now_str as _isn, utc_now as _utn
                    await store.save_paper_trade(
                        self.name, "FORCE_CLOSE_HEDGE", self.hedge_symbol, "SELL",
                        self.hedge_qty, fp, pnl,
                        notes=f"squareoff | entry={self.hedge_fill_price:.2f}",
                        ts_ist=_isn(), ts_utc=_utn())

            async def _close_futures():
                if not self.futures_remaining_qty:
                    return
                side = "SELL" if self.direction == "BULLISH" else "BUY"
                qty  = self.futures_remaining_qty
                if self.is_paper:
                    fill = await paper.place_futures_order("BTCUSDT", side, qty, price,
                                                           action="FORCE_CLOSE_FUTURES")
                else:
                    from backend.execution.binance_client import client
                    fill = await client.place_futures_market("BTCUSDT", side, qty)
                if fill:
                    fp  = float(fill.get("avg_price", price))
                    pnl = self._futures_unrealized_pnl(fp, qty)
                    self.futures_remaining_qty = 0.0
                    self.session_realized_futures_pnl += pnl
                    await self._log(f"SQUAREOFF futures: {side} {qty} BTC @ {fp:.2f}  pnl={pnl:+.2f}")
                    await store.log_trade(self.name, "FORCE_CLOSE_FUTURES", "BTCUSDT", side,
                                          qty, fp, pnl, "FILLED", {}, self.is_paper)
                    from backend.utils import ist_now_str as _isn, utc_now as _utn
                    await store.save_paper_trade(
                        self.name, "FORCE_CLOSE_FUTURES", "BTCUSDT", side, qty, fp, pnl,
                        notes=f"squareoff | entry={self.futures_entry_price:.2f}",
                        ts_ist=_isn(), ts_utc=_utn())

            if option_first:
                await _close_option()
                await _close_futures()
            else:
                await _close_futures()
                await _close_option()

            if self.execution_time_ist:
                sid = self.execution_time_ist.replace(" ", "_").replace(":", "-")
                await store.save_session(
                    session_id=sid, trader_name=self.name,
                    futures_pnl=self.session_realized_futures_pnl,
                    hedge_pnl=self.session_realized_hedge_pnl,
                    total_pnl=self.session_realized_futures_pnl + self.session_realized_hedge_pnl,
                    close_reason="squareoff",
                    close_ts_ist=ist_now_str(),
                    status="closed",
                )
            self._reset_position()
            self.state             = ExState.SLEEP
            # Block re-entry for the rest of this SESSION — force close ends the session
            _n = ist_now()
            _eh = int(getattr(cfg, "session_expiry_h", 13))
            _em = int(getattr(cfg, "session_expiry_m", 30))
            self._eligibility_date = get_session_day(_n, _eh, _em)
            self.eligible_today    = False
            self.locked_high_line  = None
            self.locked_low_line   = None
            self.high_line         = None
            self.low_line          = None
            # Keep analysis_report after squareoff — it's valid historical data for display.
            # eligible_today=False already prevents re-entry, so stale report can't cause harm.
            self.session_start_ts  = 0.0
            await self._log("Squareoff complete. Ineligible for rest of today.")
            # Mark the session as force-closed so it doesn't appear in journal
            if self.execution_time_ist:
                sid = self.execution_time_ist.replace(" ", "_").replace(":", "-")
                await store.mark_session_force_closed(sid)
            # CRITICAL: await paper engine save explicitly so all closed positions are
            # persisted to SQLite BEFORE executor state is saved.
            # Without this, paper engine save is a background create_task that may not
            # complete before a restart — causing the option to reappear in accounts.
            if self._paper is not None:
                await self._paper.save_state()
            await self._save_state()
            await self._broadcast_position()

            from backend import telegram_alert as tg
            tg.send(
                f"🔴 <b>{self.name} — SQUAREOFF COMPLETE</b>\n"
                f"All positions closed.\n"
                f"Time: {ist_now_str()}"
            )
        finally:
            # Always release the guard so crash-recovery can retry if an exception occurred
            self._is_force_closing = False

    async def _sell_hedge_after_futures(self):
        """Sell hedge at best bid (or market if OTM). Called after futures closed."""
        paper = self._paper
        from backend.data.options_feed import get_chain

        if not self.hedge_symbol or not self.hedge_qty:
            return

        chain    = get_chain()
        opt_now  = chain.get(self.hedge_symbol, {})
        intr_now = float(opt_now.get("intrinsic", 0) or 0)
        bid_now  = float(opt_now.get("bid", 0) or 0)
        limit_px = max(bid_now, intr_now * 0.98) if intr_now > 0 else bid_now

        if self.is_paper:
            fill = await paper.sell_option(self.hedge_symbol, self.hedge_qty,
                                           limit_px or self.hedge_fill_price * 0.5,
                                           action="HEDGE_SELL")
        else:
            from backend.execution.binance_client import client
            fill = await client.place_option_order(
                self.hedge_symbol, "SELL", self.hedge_qty,
                limit_px or 0, timeout_sec=10)

        if fill:
            fp  = float(fill.get("avg_price", limit_px) or limit_px)
            pnl = (fp - self.hedge_fill_price) * self.hedge_qty
            self.session_realized_hedge_pnl += pnl   # realized hedge PnL at sell
            await self._log(f"HEDGE SOLD: {self.hedge_symbol}  @ {fp:.2f}  pnl={pnl:+.2f}")
            await store.log_trade(self.name, "HEDGE_SELL", self.hedge_symbol, "SELL",
                                  self.hedge_qty, fp, pnl, "FILLED", {}, self.is_paper)
            from backend.utils import ist_now_str as _isn, utc_now as _utn
            await store.save_paper_trade(
                self.name, "HEDGE_SELL", self.hedge_symbol, "SELL",
                self.hedge_qty, fp, pnl,
                notes=f"hedge sold after futures TP | entry={self.hedge_fill_price:.2f}",
                ts_ist=_isn(), ts_utc=_utn())

    # ── Monitor / broadcast ────────────────────────────────────────────────

    async def _publish_monitor(self, price: float, in_window: bool, *,
                                itm: dict = None, prem_ok=None, tv_ok=None,
                                spread_ok=None, prox_ok=None, hedge_valid=False,
                                dist_from_line=None, loop_iter=0):
        """Publish EXECUTOR_MONITOR every tick for the frontend."""
        H = self.high_line
        L = self.low_line

        # Always compute nearest ITM so the panel shows it in every state
        if itm is None and price:
            try:
                from backend.data.options_feed import get_nearest_itm_put, get_nearest_itm_call
                raw = (get_nearest_itm_put(price) if self._option_side == "P"
                       else get_nearest_itm_call(price))
                if raw:
                    # Recompute intrinsic using futures price (chain uses spot)
                    intr = max(raw["strike"] - price, 0) if self._option_side == "P" \
                           else max(price - raw["strike"], 0)
                    tv   = max((raw.get("ask") or 0) - intr, 0)
                    sprd = (abs((raw.get("ask") or 0) - (raw.get("mark") or 0))
                            / (raw.get("mark") or 1)) * 100
                    itm  = {**raw, "intrinsic": round(intr, 2),
                            "time_value": round(tv, 2), "spread_pct": round(sprd, 2)}
            except Exception:
                pass

        # Compute current hedge/futures PnL if in position
        hedge_pnl = 0.0
        fut_pnl   = 0.0
        if self.hedge_symbol and self.state in (
                ExState.MANAGING_POSITION, ExState.PARTIAL_BOOKING):
            from backend.data.options_feed import get_chain
            opt  = get_chain().get(self.hedge_symbol, {})
            mark = float(opt.get("mark",      0) or 0)
            bid  = float(opt.get("bid",       0) or 0)
            intr = float(opt.get("intrinsic", 0) or 0)
            # Binance-style unrealized: mark → bid → intrinsic
            cur_val = mark if mark > 0 else (bid if bid > 0 else intr)
            if cur_val > 0:
                hedge_pnl = (cur_val - self.hedge_fill_price) * self.hedge_qty
            if self.futures_remaining_qty:
                fut_pnl = self._futures_unrealized_pnl(price, self.futures_remaining_qty)

        mon = {
            "executor":        self.name,
            "direction":       self.direction,
            "state":           self.state.value,
            "is_analyzing":    self._is_analyzing,
            "price":           price,
            "high_line":       H,
            "low_line":        L,
            "locked_high_line": self.locked_high_line,
            "locked_low_line":  self.locked_low_line,
            "in_window":       in_window,
            "ts_ist":          ist_now_str(),
            # Eligibility / trigger / zone
            "eligible":        self.eligible_today,
            "entry_zone":      self.entry_zone,
            "zone_price_snap": self.zone_price_snap,
            "trigger_type":    self.trigger_type,
            "triggered":       self.triggered,
            "trigger_line":    self.trigger_line if self.triggered else None,
            # Hedge validation loop
            "nearest_itm":     itm,
            "prem_ok":         prem_ok,
            "tv_ok":           tv_ok,
            "spread_ok":       spread_ok,
            "prox_ok":         prox_ok,
            "hedge_valid":     hedge_valid,
            "dist_from_line":  dist_from_line,
            "loop_iter":       loop_iter,
            "far_check":       self._far_check,
            # Position PnL (while in position)
            "futures_unrealized_pnl": round(fut_pnl, 2),
            "hedge_unrealized_pnl":   round(hedge_pnl, 2),
        }
        await bus.publish(EXECUTOR_MONITOR, mon, source=self.name)

    async def _broadcast_position(self):
        """Publish full POSITION_UPDATE for the frontend panel."""
        price = self.current_price

        fut_pnl   = 0.0
        hedge_pnl = 0.0
        if self.futures_remaining_qty and price:
            fut_pnl = self._futures_unrealized_pnl(price, self.futures_remaining_qty)
        if self.hedge_symbol and self.hedge_fill_price:
            from backend.data.options_feed import get_chain
            opt  = get_chain().get(self.hedge_symbol, {})
            mark = float(opt.get("mark",      0) or 0)
            bid  = float(opt.get("bid",       0) or 0)
            intr = float(opt.get("intrinsic", 0) or 0)
            # Binance-style: mark → bid → intrinsic
            cur_val = mark if mark > 0 else (bid if bid > 0 else intr)
            if cur_val > 0:
                hedge_pnl = (cur_val - self.hedge_fill_price) * self.hedge_qty

        await bus.publish(POSITION_UPDATE, {
            "executor":                 self.name,
            "direction":                self.direction,
            "state":                    self.state.value,
            "is_analyzing":             self._is_analyzing,
            "mark_price":               price,
            "high_line":                self.high_line,
            "low_line":                 self.low_line,
            "locked_high_line":         self.locked_high_line,
            "locked_low_line":          self.locked_low_line,
            "in_window":                self.force_window or self._in_window(),
            "eligible":                 self.eligible_today,
            "trigger_type":             self.trigger_type,
            "triggered":                self.triggered,
            "trigger_time":             self.trigger_time,
            "trigger_line":             self.trigger_line,
            # Hedge position (full details for panel display)
            "option_symbol":            self.hedge_symbol,
            "hedge_fill_price":         self.hedge_fill_price,
            "hedge_qty":                self.hedge_qty,
            "hedge_premium_paid":       self.hedge_premium_paid,
            "hedge_intrinsic_at_entry": self.hedge_intrinsic_at_entry,
            "hedge_time_value_at_entry":self.hedge_tv_at_entry,
            # Futures position
            "futures_entry":            self.futures_entry_price,
            "futures_qty":              self.futures_qty,
            "futures_remaining_qty":    self.futures_remaining_qty,
            # Live mark-to-market PnL (both legs)
            "futures_unrealized_pnl":          round(fut_pnl, 2),
            "hedge_unrealized_pnl":            round(hedge_pnl, 2),
            # Order price levels (for panel display)
            "partial_trigger_price":           self.partial_trigger_price,
            "full_close_price":                self.full_close_price,

            "pending_rebuy_price":             self.pending_rebuy_price,
            "pending_rebuy_qty":               self.pending_rebuy_qty,
            # Position timing & zone
            "execution_time_ist":       self.execution_time_ist,
            "entry_zone":               self.entry_zone,
            "zone_price_snap":          self.zone_price_snap,
            # Full analysis report for "Check Details" panel
            "analysis_report":          self.analysis_report,
            # Management
            "partial_done":             self.partial_done,
            # Trigger history (last 20, newest first)
            "trigger_history":          list(reversed(
                store.get(f"{self.name}_triggers", [])[-20:]
            )),
            "ts_ist":                   ist_now_str(),
        }, source=self.name)

    # ── Helpers ────────────────────────────────────────────────────────────

    def _in_window(self) -> bool:
        if self.force_window:
            return True
        n = ist_now()
        exp_h = int(getattr(cfg, "session_expiry_h", 13))
        exp_m = int(getattr(cfg, "session_expiry_m", 30))
        def _ci(key, default): v = self._cfg(key); return int(v) if v is not None else default
        return is_in_session_range(
            n.hour, n.minute,
            _ci("trade_start_h", 4),  _ci("trade_start_m", 0),
            _ci("trade_end_h",  18),  _ci("trade_end_m",  30),
            exp_h, exp_m,
        )

    def _futures_unrealized_pnl(self, exit_price: float, qty: float) -> float:
        entry = self.futures_entry_price
        if not entry or entry <= 0 or not exit_price or exit_price <= 0 or not qty or qty <= 0:
            return 0.0
        if self.direction == "BULLISH":
            return (exit_price - entry) * qty
        return (entry - exit_price) * qty

    async def _log(self, message: str, level: str = "INFO"):
        self.log.info(message)
        await bus.publish(LOG_EVENT, {
            "level":   level,
            "source":  self.name,
            "message": message,
            "ts_ist":  ist_now_str(),
        }, source=self.name)

    async def _check_missed_fills_on_reconnect(self):
        """
        On restart after an offline break, check if pending limit orders
        would have filled based on historical candle data.

        Currently handles: pending rebuy limit order (paper trading).
        Checks 1m candles — if price passed through the rebuy level,
        simulates the fill and logs it as a paper trade.
        """
        if not self.pending_rebuy_price or not self.pending_rebuy_qty:
            return

        from backend.data.futures_feed import get_candles, fetch_historical_candles

        candles = get_candles(100, "1m")
        if len(candles) < 5:
            candles = await fetch_historical_candles(100, "1m")
        if not candles:
            return

        rbx_price = self.pending_rebuy_price
        rbx_qty   = self.pending_rebuy_qty
        crossed_at: float = 0.0
        crossed_candle = None

        # Scan candles newest-first: find if price crossed rebuy level
        for c in reversed(candles):
            low, high, close_price = c.get("low",0), c.get("high",0), c.get("close",0)
            if self.direction == "BULLISH":
                # LONG rebuy: fill when price drops to rbx_price
                if low <= rbx_price:
                    crossed_at    = rbx_price
                    crossed_candle = c
                    break
            else:
                # SHORT rebuy: fill when price rises to rbx_price
                if high >= rbx_price:
                    crossed_at    = rbx_price
                    crossed_candle = c
                    break

        if not crossed_at:
            self.log.info(
                f"[reconnect] Pending rebuy @ {rbx_price:.2f} — "
                f"price never crossed while offline. Order still pending."
            )
            return

        # Simulate the fill that happened while offline
        ts_ms  = crossed_candle.get("ts", 0)
        fill_ts = (
            __import__("datetime").datetime.fromtimestamp(ts_ms / 1000)
            .strftime("%Y-%m-%d %H:%M:%S IST")
            if ts_ms else ist_now_str()
        )
        old_qty = self.futures_remaining_qty
        new_qty = old_qty + rbx_qty
        new_avg = (self.futures_entry_price * old_qty + crossed_at * rbx_qty) / new_qty
        self.futures_entry_price   = round(new_avg, 2)
        self.futures_qty           = new_qty
        self.futures_remaining_qty = new_qty
        self.pending_rebuy_price   = 0.0
        self.pending_rebuy_qty     = 0.0
        self._recalc_price_levels()

        paper = self._paper
        paper.executor = self.name
        await paper.place_futures_order(
            "BTCUSDT", self._futures_side, rbx_qty, crossed_at,
            action="PARTIAL_REBUY"
        )
        await store.save_paper_trade(
            self.name, "PARTIAL_REBUY", "BTCUSDT",
            self._futures_side, rbx_qty, crossed_at, 0.0,
            notes=f"Offline fill detected on reconnect — candle ts={ts_ms}"
        )
        await self._save_state()
        self.log.info(
            f"[reconnect] Rebuy OFFLINE FILL detected: {rbx_qty} BTC "
            f"@ {crossed_at:.2f}  new_avg={new_avg:.2f}  ts={fill_ts}"
        )

    def _is_option_expired(self) -> bool:
        """
        Returns True if hedge_symbol is set and its expiry date has passed.
        Symbol format: BTC-YYMMDD-STRIKE-C/P  e.g. BTC-260523-77000-C → 2026-05-23 13:30 IST.
        Called regardless of state so stale DB-restored positions are always caught.
        """
        if not self.hedge_symbol:
            return False
        try:
            parts = self.hedge_symbol.split("-")   # ['BTC', '260523', '77000', 'C']
            if len(parts) < 4:
                return False
            date_str = parts[1]                    # '260523' → YYMMDD
            yy, mm, dd = int(date_str[0:2]), int(date_str[2:4]), int(date_str[4:6])
            expiry = ist_now().replace(
                year=2000 + yy, month=mm, day=dd,
                hour=13, minute=30, second=0, microsecond=0
            )
            return ist_now() > expiry
        except Exception:
            return False

    def sync_position_to_paper(self):
        """
        Called once at startup after executor state is restored from DB.
        If a position is active but paper engine has no record (restart scenario),
        registers the open futures + option positions so PnL/equity tracks correctly.
        """
        paper = self._paper

        active = self.state.value in ("MANAGING_POSITION", "PARTIAL_BOOKING", "FORCE_CLOSE")
        if not active:
            return

        exec_tag = self.name
        paper.executor = exec_tag

        # Register futures position if not already present
        if self.futures_remaining_qty > 0 and self.futures_entry_price > 0:
            pos_key = f"BTCUSDT::{exec_tag}"
            if pos_key not in paper._positions:
                paper._positions[pos_key] = {
                    "side":      "BUY" if self.direction == "BULLISH" else "SELL",
                    "qty":       self.futures_remaining_qty,
                    "avg_price": self.futures_entry_price,
                    "executor":  exec_tag,
                    "symbol":    "BTCUSDT",
                }
                self.log.info(
                    f"[sync→paper] Futures {self.direction} {self.futures_remaining_qty} BTC "
                    f"@ {self.futures_entry_price:.2f} registered."
                )

        # Register option position if not already present
        if self.hedge_symbol and self.hedge_qty > 0 and self.hedge_fill_price > 0:
            opt_key = f"{self.hedge_symbol}::{exec_tag}"
            if opt_key not in paper._option_positions:
                paper._option_positions[opt_key] = {
                    "side":      "BUY",
                    "qty":       self.hedge_qty,
                    "avg_price": self.hedge_fill_price,
                    "ts":        time.time(),
                    "symbol":    self.hedge_symbol,
                    "executor":  exec_tag,
                }
                self.log.info(
                    f"[sync→paper] Hedge {self.hedge_symbol} qty={self.hedge_qty} "
                    f"@ {self.hedge_fill_price:.2f} registered."
                )

        # Positions are registered above for PnL tracking.
        # Synthetic trade history injection removed — live-trades shows only
        # trades that occurred in the current server session.

    async def clear_position(self):
        """
        Full memory wipe for this trader — clears everything:
          - Open positions in paper engine (futures + options)
          - Trade history + equity curve for this session
          - All position fields (entry price, hedge, qty, PnL)
          - S/R analysis lines + eligibility
          - Session realized PnL + session timestamp
        State resets to SLEEP → next window tick = completely fresh start.
        No exchange orders sent. Balance is NOT affected.
        """
        if self._paper is not None:
            self._paper.clear_executor_positions(self.name)
            self._paper.clear_session_trades()            # wipe trade history + equity curve
        # ── Reset all executor state ─────────────────────────────────────
        self.state                        = ExState.SLEEP
        self._reset_position()
        self.session_realized_futures_pnl = 0.0
        self.session_start_ts             = 0.0
        await self._reset_daily()
        self.analysis_report = {}   # explicit wipe — _reset_daily preserves today's report
        # 10-min cooldown before re-analysis — gives user time to see cleared state
        # Does NOT block all day so fresh analysis + re-entry is still possible
        self._analysis_retry_after = time.time() + 600
        self.eligible_today        = None   # allow fresh analysis after cooldown
        # ── Persist both paper engine and executor state to DB ────────────
        if self._paper is not None:
            await self._paper.save_state()                 # ensures cleared positions survive restart
        await self._save_state()
        # Delete DB history and reset virtual balance to fresh 100k
        await store.delete_trader_history(self.name)
        if self._paper is not None:
            await self._paper.reset()
        await self._log(
            f"Full memory wipe by user — all positions, trades, analysis cleared. "
            f"State: SLEEP. Next window open = fresh start.",
            level="WARNING"
        )
        await self._broadcast_position()

    async def reset_analysis(self):
        """
        Clear only S/R analysis memory (lines, eligibility, report).
        Does NOT touch balance, open positions, or trade history.
        Called when user changes analysis parameters (TF / candle count)
        so fresh analysis runs with the new settings.
        """
        self.locked_high_line      = None
        self.locked_low_line       = None
        self.high_line             = None
        self.low_line              = None
        self.analysis_report       = {}
        self._eligibility_date     = ""
        self.eligible_today        = None
        self._analysis_retry_after = 0.0
        # In pre-execution states: go back to SLEEP so analysis re-runs immediately
        _pre_exec = {ExState.SLEEP, ExState.CHECK_ELIGIBILITY,
                     ExState.WAIT_TRIGGER, ExState.VERIFY_HEDGE_LOOP}
        if self.state in _pre_exec:
            self.state        = ExState.SLEEP
            self.triggered    = False
            self.trigger_type = ""
            self.trigger_line = 0.0
            self.entry_zone   = ""
        await self._save_state()
        await self._log(
            f"Analysis memory reset by user — fresh analysis will run at next window tick.",
            level="WARNING"
        )
        await self._broadcast_position()

    def _recalc_price_levels(self):
        """
        Pre-compute exact futures price levels for display.
        Triggered after every position change (execute, partial sell, rebuy fill).
          partial_trigger_price = price where fut_pnl = premium × partial_ratio
          full_close_price      = price where remaining fut_pnl = session_target - realized
        """
        E = self.futures_entry_price
        Q = self.futures_remaining_qty or self.futures_qty or 1
        if not E or not Q:
            self.partial_trigger_price = 0.0
            self.full_close_price      = 0.0
            return

        pnl_partial   = self.hedge_premium_paid * (self._cfg("partial_profit_ratio") or 1.1)
        session_tgt   = (self._cfg("session_pnl_target")
                         or self._cfg("full_close_target") or 600.0)
        remaining_pnl = session_tgt - self.session_realized_futures_pnl

        if self.direction == "BULLISH":
            self.partial_trigger_price = round(E + pnl_partial    / Q, 2)
            self.full_close_price      = round(E + remaining_pnl  / Q, 2)
        else:
            self.partial_trigger_price = round(E - pnl_partial    / Q, 2)
            self.full_close_price      = round(E - remaining_pnl  / Q, 2)

    async def _reset_daily(self):
        """Reset trigger/loop state for a new day or same-day retry."""
        self.triggered           = False
        self.trigger_type        = ""
        self.trigger_line        = 0.0
        self.trigger_time        = ""
        self._loop_iter          = 0
        self._far_check          = None
        self._verify_start_time  = 0.0
        self._prox_bad_ticks     = 0
        self.locked_high_line    = None   # unlock — fresh analysis at next window open
        self.locked_low_line     = None
        self.high_line           = None   # clear display lines too
        self.low_line            = None
        # Preserve analysis_report within the same session so the panel still shows
        # the last known OB zone during brief gaps between retries.
        # Only wipe it when the SESSION changes (not at calendar midnight).
        _n = ist_now()
        _eh = int(getattr(cfg, "session_expiry_h", 13))
        _em = int(getattr(cfg, "session_expiry_m", 30))
        _sday = get_session_day(_n, _eh, _em)
        if self.analysis_report.get("analysis_time", "")[:10] != _sday:
            self.analysis_report = {}
        self._eligibility_date   = ""
        self.eligible_today      = None
        self.entry_zone          = ""
        self.zone_price_snap     = 0.0
        self.session_start_ts    = 0.0
        self.state               = ExState.SLEEP

    def _reset_position(self):
        self.hedge_symbol              = ""
        self.hedge_fill_price          = 0.0
        self.hedge_qty                 = 0.0
        self.hedge_premium_paid        = 0.0
        self.hedge_intrinsic_at_entry  = 0.0
        self.hedge_tv_at_entry         = 0.0
        self.futures_entry_price       = 0.0
        self.futures_qty               = 0.0
        self.futures_remaining_qty     = 0.0
        self.partial_done              = False
        self._rebuy_order_price        = 0.0
        self.pending_rebuy_price       = 0.0
        self.pending_rebuy_qty         = 0.0
        self.partial_trigger_price     = 0.0
        self.full_close_price          = 0.0
        self.triggered                 = False
        self.trigger_type              = ""
        self.trigger_line              = 0.0
        self.trigger_time              = ""
        self._loop_iter                = 0
        self._far_check                = None
        # Keep session_realized_pnl until daily reset so panel shows final result

    async def _save_state(self):
        await store.set(f"{self.name}_state", self._serialize())

    def _serialize(self) -> dict:
        return {
            "state":                    self.state.value,
            "high_line":                self.high_line,
            "low_line":                 self.low_line,
            "locked_high_line":         self.locked_high_line,
            "locked_low_line":          self.locked_low_line,
            "analysis_report":          self.analysis_report,
            "eligible_today":           self.eligible_today,
            "eligibility_date":         self._eligibility_date,
            "trigger_type":             self.trigger_type,
            "triggered":                self.triggered,
            "trigger_time":             self.trigger_time,
            "trigger_line":             self.trigger_line,
            "execution_time_ist":       self.execution_time_ist,
            "entry_zone":               self.entry_zone,
            "zone_price_snap":          self.zone_price_snap,
            "hedge_symbol":             self.hedge_symbol,
            "hedge_fill_price":         self.hedge_fill_price,
            "hedge_qty":                self.hedge_qty,
            "hedge_premium_paid":       self.hedge_premium_paid,
            "hedge_intrinsic_at_entry": self.hedge_intrinsic_at_entry,
            "hedge_tv_at_entry":        self.hedge_tv_at_entry,
            "futures_entry_price":      self.futures_entry_price,
            "futures_qty":                     self.futures_qty,
            "futures_remaining_qty":           self.futures_remaining_qty,
            "partial_done":                    self.partial_done,
            "pending_rebuy_price":             self.pending_rebuy_price,
            "pending_rebuy_qty":               self.pending_rebuy_qty,
            "partial_trigger_price":           self.partial_trigger_price,
            "full_close_price":                self.full_close_price,
            "session_realized_futures_pnl":    self.session_realized_futures_pnl,
            "session_realized_hedge_pnl":      self.session_realized_hedge_pnl,
            "session_start_ts":                self.session_start_ts,
            "analysis_retry_after":            self._analysis_retry_after,
        }

    def _restore_state(self, s: dict):
        try:
            state_val = s.get("state", "SLEEP")
            try:
                self.state = ExState(state_val)
            except ValueError:
                self.state = ExState.SLEEP
            self.high_line                 = s.get("high_line")
            self.low_line                  = s.get("low_line")
            self.locked_high_line          = s.get("locked_high_line")
            self.locked_low_line           = s.get("locked_low_line")
            self.analysis_report           = s.get("analysis_report", {})

            # For non-active states, clear locked lines to force fresh analysis
            # at next window open. Preserve analysis_report for panel display.
            _active = {"MANAGING_POSITION", "PARTIAL_BOOKING", "FORCE_CLOSE",
                       "VERIFY_HEDGE_LOOP", "EXECUTE"}
            if state_val not in _active:
                self.locked_high_line  = None
                self.locked_low_line   = None
                # Keep analysis_report: locked_high_line=None already forces
                # re-analysis. Clearing it hides historical data from the panel.
                self._eligibility_date = ""
                self.eligible_today    = None
            else:
                self.eligible_today        = s.get("eligible_today")
                self._eligibility_date     = s.get("eligibility_date", "")
            self.trigger_type              = s.get("trigger_type", "")
            self.triggered                 = s.get("triggered", False)
            self.trigger_time              = s.get("trigger_time", "")
            self.trigger_line              = s.get("trigger_line", 0.0)
            self.execution_time_ist        = s.get("execution_time_ist", "")
            self.entry_zone                = s.get("entry_zone", "")
            self.zone_price_snap           = s.get("zone_price_snap", 0.0)
            self.hedge_symbol              = s.get("hedge_symbol", "")
            self.hedge_fill_price          = s.get("hedge_fill_price", 0.0)
            self.hedge_qty                 = s.get("hedge_qty", 0.0)
            self.hedge_premium_paid        = s.get("hedge_premium_paid", 0.0)
            self.hedge_intrinsic_at_entry  = s.get("hedge_intrinsic_at_entry", 0.0)
            self.hedge_tv_at_entry         = s.get("hedge_tv_at_entry", 0.0)
            self.futures_entry_price       = s.get("futures_entry_price", 0.0)
            self.futures_qty               = s.get("futures_qty", 0.0)
            self.futures_remaining_qty     = s.get("futures_remaining_qty", 0.0)
            self.partial_done              = s.get("partial_done", False)
            self.pending_rebuy_price       = s.get("pending_rebuy_price", 0.0)
            self.pending_rebuy_qty         = s.get("pending_rebuy_qty", 0.0)
            self.partial_trigger_price     = s.get("partial_trigger_price", 0.0)
            self.full_close_price          = s.get("full_close_price", 0.0)
            self.session_realized_futures_pnl = s.get("session_realized_futures_pnl", 0.0)
            self.session_realized_hedge_pnl   = s.get("session_realized_hedge_pnl", 0.0)
            self.session_start_ts             = s.get("session_start_ts", 0.0)
            self._analysis_retry_after        = s.get("analysis_retry_after", 0.0)
            # If restoring mid-verification, reset timer to NOW so timeout is fresh
            if self.state == ExState.VERIFY_HEDGE_LOOP:
                self._verify_start_time = time.time()
                self._prox_bad_ticks    = 0
            self.log.info(f"State restored: {self.state.value}")
        except Exception as e:
            self.log.error(f"State restore failed: {e}")

    def get_status(self) -> dict:
        return self._serialize() | {
            "name":           self.name,
            "direction":      self.direction,
            "price":          self.current_price,
            "is_analyzing":   self._is_analyzing,
            "in_window":      self._in_window(),
            "session_start_ts": self.session_start_ts,
        }

    # ── Subclass interface ─────────────────────────────────────────────────

    @property
    def _cfg_prefix(self) -> str:
        raise NotImplementedError

    @property
    def _futures_side(self) -> str:
        raise NotImplementedError

    @property
    def _option_side(self) -> str:
        raise NotImplementedError

    def _is_eligible(self, price: float, target_line: float) -> bool:
        """Return True if price is in the correct zone for this direction."""
        raise NotImplementedError

