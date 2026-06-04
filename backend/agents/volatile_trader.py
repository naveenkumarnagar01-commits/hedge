"""
Volatile Event Trader — options-only straddle strategy.

ul  SLEEP → (event time reached) → SEARCHING → EXECUTING → MANAGING → DONE

Rules:
  - Strict ITM pair: PUT_strike > spot > CALL_strike
  - PUT_strike − CALL_strike ≈ vol_strike_gap ± vol_strike_gap_tolerance
  - Both have intrinsic value simultaneously (enforced by above)
  - Combined ask ≤ vol_combined_premium_max
  - Each leg ask_qty ≥ vol_min_ask_qty
  - TP per leg = (put_ask + call_ask) × vol_tp_multiplier
  - Sell when best_bid ≥ target OR mark ≥ target × 1.02
  - Window: event_time → next 6:00 AM IST (configurable via vol_window_close_h/m)
  - Events persisted in DB; past/upcoming tracked
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum

from backend.config import cfg
from backend.message_bus import bus, TICK_FUTURES, POSITION_UPDATE, LOG_EVENT
from backend.state_store import store

log = logging.getLogger("volatile_trader")

_IST = timezone(timedelta(hours=5, minutes=30))


class VState(str, Enum):
    SLEEP = "SLEEP"
    SEARCHING = "SEARCHING"
    EXECUTING = "EXECUTING"
    MANAGING = "MANAGING"
    DONE = "DONE"


class VolatileTrader:
    """Standalone event-driven options straddle trader (no futures, options-only)."""

    EVENTS_KEY = "volatile_events"

    def __init__(self, is_paper: bool = False, paper_engine=None):
        self.name = "VolatileTrader" + ("_Paper" if is_paper else "")
        self.is_paper = is_paper
        self._paper = paper_engine

        self._state = VState.SLEEP
        self._running = False

        # Active event context
        self._active_event: dict | None = None

        # Scan snapshot (for display)
        self._scan_put: dict = {}
        self._scan_call: dict = {}
        self._scan_reason: str = ""

        # Open legs
        self._put_leg: dict | None = None
        self._call_leg: dict | None = None
        self._combined_entry: float = 0.0

        # Session PnL
        self._session_pnl: float = 0.0

        # Log ring-buffer
        self._logs: list = []

        # Current mark/futures price
        self._mark: float = 0.0
        self._step_running: bool = False
        self._closing_session: bool = False
        # _restore() is intentionally NOT called here — store is not yet connected
        # at __init__ time. It is called in start() after store.connect() runs.

    # ─────────────────────────────────────────────────────────────
    # Event management (persisted to DB)
    # ─────────────────────────────────────────────────────────────

    def get_events(self) -> list:
        events = store.get(self.EVENTS_KEY) or []
        now = datetime.now(_IST)
        for e in events:
            try:
                et = datetime.fromisoformat(e["event_time"])
                if et.tzinfo is None:
                    et = et.replace(tzinfo=_IST)
                e["status"] = "passed" if et < now else "upcoming"
            except Exception:
                e["status"] = "unknown"
        return events

    async def add_event(self, name: str, event_time_str: str) -> dict:
        s = event_time_str.strip().replace("T", " ")
        try:
            et = datetime.strptime(s, "%Y-%m-%d %H:%M")
        except ValueError:
            et = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        et = et.replace(tzinfo=_IST)

        event_time_key = et.strftime("%Y-%m-%dT%H:%M")
        event = {
            "id": str(uuid.uuid4())[:8],
            "name": name.strip() or "Event",
            "event_time": event_time_key,
            "status": "upcoming",
            "created_at": datetime.now(_IST).strftime("%Y-%m-%dT%H:%M"),
        }
        events = store.get(self.EVENTS_KEY) or []
        # Deduplicate: skip if same name+time already exists
        dup = any(
            e["name"].strip().lower() == event["name"].lower() and
            e["event_time"] == event_time_key
            for e in events
        )
        if dup:
            log.info(f"{self.name}: duplicate event skipped — '{event['name']}' @ {event_time_key}")
            return next(e for e in events
                        if e["event_time"] == event_time_key and
                           e["name"].strip().lower() == event["name"].lower())
        events.append(event)
        events.sort(key=lambda x: x["event_time"])
        await store.set(self.EVENTS_KEY, events)
        await self._log(f"Event saved to DB: '{event['name']}' @ {event['event_time']}")
        await self._broadcast()          # instant frontend update
        return event

    async def remove_event(self, event_id: str) -> bool:
        events = store.get(self.EVENTS_KEY) or []
        filtered = [e for e in events if e["id"] != event_id]
        if len(filtered) == len(events):
            return False
        await store.set(self.EVENTS_KEY, filtered)
        await self._log(f"Event deleted: id={event_id}")
        await self._broadcast()          # instant frontend update
        return True

    async def update_event(self, event_id: str, name: str, event_time_str: str) -> dict | None:
        """Edit an existing event's name and/or time in-place."""
        events = store.get(self.EVENTS_KEY) or []
        target = next((e for e in events if e["id"] == event_id), None)
        if not target:
            return None
        s = event_time_str.strip().replace("T", " ")
        try:
            et = datetime.strptime(s, "%Y-%m-%d %H:%M")
        except ValueError:
            et = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        et = et.replace(tzinfo=_IST)
        target["name"]       = name.strip() or target["name"]
        target["event_time"] = et.strftime("%Y-%m-%dT%H:%M")
        events.sort(key=lambda x: x["event_time"])
        await store.set(self.EVENTS_KEY, events)
        await self._log(f"Event updated: '{target['name']}' @ {target['event_time']}")
        await self._broadcast()          # instant frontend update
        return target

    # ─────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────

    async def start(self):
        if self._running:
            return
        self._running = True
        bus.subscribe(TICK_FUTURES, self._on_tick)

        # ── Crash recovery (store is connected by the time start() is called) ──
        self._restore()

        # ── Restore paper engine balance + positions from DB ──────────────────
        if self._paper is not None:
            state_key = getattr(self._paper, "_state_key", "paper_engine_state_vol")
            self._paper.load_state(state_key)

            # If trader is SLEEP (no active trade), wipe any stale paper positions
            # that may have been left from a previous crash mid-trade.
            _active = {VState.MANAGING, VState.EXECUTING, VState.SEARCHING}
            if self._state not in _active:
                self._paper.clear_executor_positions(self.name)
                await self._paper.save_state()
                log.info(f"{self.name}: cleared stale paper positions (state=SLEEP)")

        # ── Handle option expired while server was offline ────────────────────
        for leg in (self._put_leg, self._call_leg):
            if leg and not leg.get("closed") and self._is_option_expired(leg["symbol"]):
                log.warning(
                    f"{self.name}: {leg['symbol']} expired while offline — recording loss"
                )
                await self._expire_leg(leg)

        await self._log("Trader started")

    async def stop(self):
        self._running = False
        bus.unsubscribe(TICK_FUTURES, self._on_tick)
        await self._log("Trader stopped")

    def reset(self):
        """Reset trade state. Events list is preserved."""
        self._state = VState.SLEEP
        self._active_event = None
        self._scan_put = {}
        self._scan_call = {}
        self._scan_reason = ""
        self._put_leg = None
        self._call_leg = None
        self._combined_entry = 0.0
        self._session_pnl = 0.0
        log.info(f"{self.name}: reset to SLEEP (events preserved)")

    async def force_close(self):
        """Sell all open legs at current bid, then reset to SLEEP."""
        if self._closing_session:
            return
        self._closing_session = True
        # Capture event id before reset clears it
        _evt_id = (self._active_event or {}).get("id", "")
        if self._state == VState.MANAGING:
            for leg in (self._put_leg, self._call_leg):
                if leg is not None and not leg.get("closed") and not leg.get("sell_placed"):
                    bid = leg.get("live", {}).get("bid", 0)
                    if bid <= 0:
                        bid = leg.get("entry_price", 0) * 0.9
                    await self._sell_leg(leg, bid, "FORCE_CLOSE")
            if self.is_paper and self._paper:
                await self._paper.save_state()
        # Record final PnL before reset() clears _session_pnl
        if _evt_id:
            from backend.utils import ist_now_str as _isn
            await store.save_session(
                session_id=f"vol_{_evt_id}", trader_name=self.name,
                total_pnl=self._session_pnl,
                close_ts_ist=_isn(), status="closed",
            )
        self.reset()
        await self._save_state()
        # Mark any active session as force-closed (don't record in journal)
        if _evt_id:
            from backend.state_store import store as _store
            await _store.mark_session_force_closed(f"vol_{_evt_id}")
        await self._save_state()
        await self._broadcast()
        self._closing_session = False

    async def clear_memory(self):
        """Wipe state without sending any orders. Clears all history and resets balance."""
        self._state = VState.SLEEP
        self._active_event = None
        self._put_leg = None
        self._call_leg = None
        self._combined_entry = 0.0
        self._scan_put = {}
        self._scan_call = {}
        self._scan_reason = ""
        # Delete DB history and reset virtual balance
        from backend.state_store import store
        await store.delete_trader_history(self.name)
        if self._paper is not None:
            await self._paper.reset()
        self._session_pnl = 0.0
        self._logs = []
        await self._save_state()

    # ─────────────────────────────────────────────────────────────
    # Tick handler
    # ─────────────────────────────────────────────────────────────

    async def _on_tick(self, msg: dict):
        if not self._running:
            return
        data = msg.get("data", {})
        price = float(data.get("mark_price") or data.get("price") or self._mark)
        if price > 0:
            self._mark = price
        if self._step_running:
            return
        self._step_running = True
        try:
            await self._step()
        finally:
            self._step_running = False

    # ─────────────────────────────────────────────────────────────
    # State machine
    # ─────────────────────────────────────────────────────────────

    async def _step(self):
        now = datetime.now(_IST)

        if self._state == VState.SLEEP:
            await self._check_for_active_event(now)

        elif self._state == VState.SEARCHING:
            if self._window_closed(now):
                await self._log("Search window closed — returning to SLEEP")
                self._state = VState.SLEEP
                self._active_event = None
                await self._save_state()
                await self._broadcast()
                return
            if self._mark > 0:
                await self._search_entry(self._mark)

        elif self._state == VState.MANAGING:
            # Auto-squareoff: force-close all legs when configured time is reached.
            # Guard: if the active event started AFTER force_close in session order,
            # force_close already passed before this trade — don't auto-close.
            from backend.utils import has_reached_session_time, _session_minutes
            fc_h = int(getattr(cfg, "vol_force_close_h", 18))
            fc_m = int(getattr(cfg, "vol_force_close_m", 30))
            exp_h = int(getattr(cfg, "session_expiry_h", 13))
            exp_m = int(getattr(cfg, "session_expiry_m", 30))
            if has_reached_session_time(now.hour, now.minute, fc_h, fc_m, exp_h, exp_m):
                _skip_fc = False
                try:
                    evt_str = (self._active_event or {}).get("event_time", "")
                    et = datetime.fromisoformat(evt_str)
                    if et.tzinfo is None:
                        et = et.replace(tzinfo=_IST)
                    # If event occurred after force_close in session order → fc already stale
                    _skip_fc = _session_minutes(et.hour, et.minute, exp_h, exp_m) > \
                                _session_minutes(fc_h, fc_m, exp_h, exp_m)
                except Exception:
                    pass
                if not _skip_fc:
                    await self._log(
                        f"Auto-squareoff: force_close time {fc_h:02d}:{fc_m:02d} IST reached — closing all legs",
                        "WARNING",
                    )
                    await self.force_close()
                    return
            await self._monitor_legs()

        elif self._state == VState.DONE:
            pass  # terminal — wait for manual reset

    # ─────────────────────────────────────────────────────────────
    # Event activation
    # ─────────────────────────────────────────────────────────────

    async def _check_for_active_event(self, now: datetime):
        from backend.utils import is_blackout_day
        if is_blackout_day(now.date(), bool(getattr(cfg, "vol_skip_weekends", False)),
                           str(getattr(cfg, "vol_blackout_dates", ""))):
            return

        events = store.get(self.EVENTS_KEY) or []
        for e in events:
            if e.get("traded"):  # already executed once — never re-activate
                continue
            try:
                et = datetime.fromisoformat(e["event_time"])
                if et.tzinfo is None:
                    et = et.replace(tzinfo=_IST)
            except Exception:
                continue
            wc = self._compute_window_close(et)
            if et <= now <= wc:
                self._active_event = {
                    "id": e["id"],
                    "name": e["name"],
                    "event_time": e["event_time"],
                    "window_close": wc.strftime("%Y-%m-%dT%H:%M"),
                }
                self._state = VState.SEARCHING
                await self._save_state()
                await self._log(
                    f"Window open for '{e['name']}' — "
                    f"searching until {wc.strftime('%d %b %H:%M IST')}"
                )
                await self._broadcast()
                return

    def _compute_window_close(self, event_time: datetime) -> datetime:
        """Next configured hour:minute IST after event_time."""
        h = int(getattr(cfg, "vol_window_close_h", 6))
        m = int(getattr(cfg, "vol_window_close_m", 0))
        et = event_time
        if et.tzinfo is None:
            et = et.replace(tzinfo=_IST)
        candidate = et.replace(hour=h, minute=m, second=0, microsecond=0)
        if candidate <= et:
            candidate += timedelta(days=1)
        return candidate

    def _window_closed(self, now: datetime) -> bool:
        if not self._active_event:
            return True
        wc_str = self._active_event.get("window_close", "")
        if not wc_str:
            return True
        try:
            wc = datetime.fromisoformat(wc_str)
            if wc.tzinfo is None:
                wc = wc.replace(tzinfo=_IST)
            return now >= wc
        except Exception:
            return True

    # ─────────────────────────────────────────────────────────────
    # Entry search — strict ITM pair
    # ─────────────────────────────────────────────────────────────

    async def _search_entry(self, spot: float):
        result = self._find_strict_itm_pair(spot)
        if result is None:
            await self._save_state()
            await self._broadcast()
            return

        put, call = result
        put_ask = float(put.get("ask") or 0)
        call_ask = float(call.get("ask") or 0)
        combined = put_ask + call_ask

        max_prem = float(getattr(cfg, "vol_combined_premium_max", 800.0))
        min_qty = float(getattr(cfg, "vol_min_ask_qty", 1.0))
        put_qty = float(put.get("ask_qty") or 0)
        call_qty = float(call.get("ask_qty") or 0)

        if combined > max_prem:
            self._scan_reason = f"Combined {combined:.0f} > max {max_prem:.0f}"
            await self._broadcast()
            return
        if put_qty < min_qty:
            self._scan_reason = f"PUT qty {put_qty:.0f} < min {min_qty:.0f}"
            await self._broadcast()
            return
        if call_qty < min_qty:
            self._scan_reason = f"CALL qty {call_qty:.0f} < min {min_qty:.0f}"
            await self._broadcast()
            return

        self._scan_reason = "All conditions met — executing"
        await self._execute_entry(put, call, combined)

    def _find_strict_itm_pair(self, spot: float):
        """
        Find nearest ITM PUT + ITM CALL where:
          1. PUT_strike > spot > CALL_strike (price strictly between both strikes)
          2. PUT_strike − CALL_strike ≈ vol_strike_gap ± vol_strike_gap_tolerance
        Both conditions guarantee intrinsic value on both legs simultaneously.
        Returns (put_dict, call_dict) or None.
        """
        try:
            from backend.data.options_feed import get_chain
            chain_dict = get_chain()  # returns Dict[symbol, option_dict]
        except Exception as exc:
            self._scan_reason = f"Chain error: {exc}"
            self._scan_put = {}
            self._scan_call = {}
            return None

        if not chain_dict:
            self._scan_reason = "No chain data"
            self._scan_put = {}
            self._scan_call = {}
            return None

        chain = list(chain_dict.values())
        gap = float(getattr(cfg, "vol_strike_gap", 500.0))
        tol = float(getattr(cfg, "vol_strike_gap_tolerance", 50.0))

        # ITM puts: put_strike > spot → intrinsic = put_strike - spot > 0
        itm_puts = [
            (float(o.get("strike") or 0), o)
            for o in chain
            if str(o.get("side") or "").upper() == "P" and float(o.get("strike") or 0) > spot
        ]

        # ITM calls: call_strike < spot → intrinsic = spot - call_strike > 0
        itm_calls = [
            (float(o.get("strike") or 0), o)
            for o in chain
            if str(o.get("side") or "").upper() == "C" and float(o.get("strike") or 0) < spot
        ]

        if not itm_puts:
            self._scan_reason = "No ITM puts in chain"
            self._scan_put = {}
            self._scan_call = {}
            return None
        if not itm_calls:
            self._scan_reason = "No ITM calls in chain"
            self._scan_put = {}
            self._scan_call = {}
            return None

        # Nearest ITM first
        itm_puts.sort(key=lambda x: x[0])            # ascending  → smallest > spot first
        itm_calls.sort(key=lambda x: x[0], reverse=True)  # descending → largest < spot first

        # Show nearest available in scan panel regardless of gap match
        self._scan_put = itm_puts[0][1]
        self._scan_call = itm_calls[0][1]

        # Find pair closest to desired gap
        best_pair = None
        best_diff = float("inf")

        for p_strike, p_opt in itm_puts:
            for c_strike, c_opt in itm_calls:
                actual_gap = p_strike - c_strike
                diff = abs(actual_gap - gap)
                if diff <= tol:
                    if diff < best_diff:
                        best_diff = diff
                        best_pair = (p_opt, c_opt)

        if best_pair is None:
            nearest_gap = itm_puts[0][0] - itm_calls[0][0]
            self._scan_reason = (
                f"No pair with gap {gap:.0f}±{tol:.0f} pts "
                f"(nearest gap={nearest_gap:.0f})"
            )
            return None

        put_opt, call_opt = best_pair
        self._scan_put = put_opt
        self._scan_call = call_opt
        self._scan_reason = ""
        return put_opt, call_opt

    # ─────────────────────────────────────────────────────────────
    # Execution
    # ─────────────────────────────────────────────────────────────

    async def _execute_entry(self, put: dict, call: dict, combined: float):
        self._state = VState.EXECUTING
        await self._broadcast()

        put_ask = float(put.get("ask") or 0)
        call_ask = float(call.get("ask") or 0)
        qty = float(getattr(cfg, "vol_contract_qty", 1.0))
        tp_mult = float(getattr(cfg, "vol_tp_multiplier", 1.10))
        target = combined * tp_mult

        put_sym = put.get("symbol", "")
        call_sym = call.get("symbol", "")

        if self.is_paper and self._paper:
            self._paper.executor = self.name
            await self._paper.buy_option(put_sym, qty, put_ask, action="VOL_PUT_BUY")
            self._paper.executor = self.name
            await self._paper.buy_option(call_sym, qty, call_ask, action="VOL_CALL_BUY")
            await self._paper.save_state()
        else:
            await self._live_buy(put_sym, qty, put_ask)
            await self._live_buy(call_sym, qty, call_ask)

        self._combined_entry = combined

        # Persist both legs to DB so account history shows them from the start
        from backend.utils import ist_now_str, utc_now as _utc_now
        _ts_ist, _ts_utc = ist_now_str(), _utc_now()
        evt_id = (self._active_event or {}).get("id", "")
        await store.save_paper_trade(
            self.name, "VOL_PUT_BUY", put_sym, "BUY", qty, put_ask, 0.0,
            notes=f"straddle entry — combined={combined:.2f} TP={target:.2f}",
            ts_ist=_ts_ist, ts_utc=_ts_utc, session_id=f"vol_{evt_id}")
        await store.save_paper_trade(
            self.name, "VOL_CALL_BUY", call_sym, "BUY", qty, call_ask, 0.0,
            notes=f"straddle entry — combined={combined:.2f} TP={target:.2f}",
            ts_ist=_ts_ist, ts_utc=_ts_utc, session_id=f"vol_{evt_id}")

        self._put_leg = {
            "symbol": put_sym,
            "strike": float(put.get("strike") or 0),
            "entry_price": put_ask,
            "target": target,
            "qty": qty,
            "closed": False,
            "close_price": 0.0,
            "close_reason": "",
            "sell_placed": False,
            "live": {"bid": 0.0, "ask": put_ask, "mark": put_ask},
        }
        self._call_leg = {
            "symbol": call_sym,
            "strike": float(call.get("strike") or 0),
            "entry_price": call_ask,
            "target": target,
            "qty": qty,
            "closed": False,
            "close_price": 0.0,
            "close_reason": "",
            "sell_placed": False,
            "live": {"bid": 0.0, "ask": call_ask, "mark": call_ask},
        }

        await self._log(
            f"ENTERED — PUT {put_sym} @ {put_ask:.2f}  "
            f"CALL {call_sym} @ {call_ask:.2f}  "
            f"combined={combined:.2f}  TP each @ {target:.2f}"
        )
        self._state = VState.MANAGING

        evt_id = (self._active_event or {}).get("id", "")
        if evt_id:
            await store.save_session(
                session_id=f"vol_{evt_id}", trader_name=self.name,
                entry_ts_ist=_ts_ist, status="open",
            )
            # Mark event as traded so it is never re-activated after a reset
            events = store.get(self.EVENTS_KEY) or []
            for ev in events:
                if ev.get("id") == evt_id:
                    ev["traded"] = True
                    break
            await store.set(self.EVENTS_KEY, events)

        await self._save_state()
        await self._broadcast()

    async def _live_buy(self, symbol: str, qty: float, ask: float):
        try:
            from backend.execution.binance_client import client
            fill = await client.place_option_order(
                symbol, "BUY", qty, ask, timeout_sec=int(cfg.fill_timeout_sec)
            )
            if not fill or not fill.get("filled"):
                await self._log(f"Buy not filled within timeout: {symbol}", "WARNING")
        except Exception as exc:
            await self._log(f"Buy error {symbol}: {exc}", "ERROR")

    # ─────────────────────────────────────────────────────────────
    # Position management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _is_option_expired(symbol: str) -> bool:
        """
        Parse the YYMMDD from an option symbol (e.g. BTC-260603-67500-C)
        and return True if current IST time is past 13:30 IST on that date.
        This catches expired options even when the options feed has rolled
        to the next day's chain after a server restart.
        """
        try:
            parts = symbol.split("-")  # ['BTC', '260603', '67500', 'C']
            if len(parts) < 4:
                return False
            ds = parts[1]  # '260603' → YYMMDD
            yy, mm, dd = int(ds[0:2]), int(ds[2:4]), int(ds[4:6])
            expiry = datetime.now(_IST).replace(
                year=2000 + yy, month=mm, day=dd,
                hour=13, minute=30, second=0, microsecond=0
            )
            return datetime.now(_IST) > expiry
        except Exception:
            return False

    async def _monitor_legs(self):
        expiry_ms = None
        chain_snap = {}
        try:
            from backend.data.options_feed import get_feed_info, get_chain
            info = get_feed_info()
            expiry_ms = info.get("expiry_ms")
            chain_snap = get_chain()
        except Exception:
            pass

        now_ms = datetime.now(_IST).timestamp() * 1000
        past_expiry = expiry_ms is not None and now_ms >= expiry_ms

        for leg in (self._put_leg, self._call_leg):
            if leg is None or leg["closed"]:
                continue

            try:
                quote = chain_snap.get(leg["symbol"]) or {}
                bid = float(quote.get("bid") or 0)
                ask_p = float(quote.get("ask") or leg["entry_price"])
                mark = float(quote.get("mark") or 0)
                leg["live"] = {"bid": bid, "ask": ask_p, "mark": mark}
            except Exception:
                bid = 0.0
                mark = 0.0

            # Check expiry: either the feed says past expiry, OR the symbol
            # itself has a date that's already passed (catches restart edge-case)
            if past_expiry or self._is_option_expired(leg["symbol"]):
                await self._expire_leg(leg)
            elif not leg.get("sell_placed"):
                target = leg["target"]
                if bid > 0 and bid >= target:
                    await self._sell_leg(leg, bid, "BID≥TP")
                elif mark > 0 and mark >= target:
                    await self._sell_leg(leg, target, "MARK≥TP")

        await self._broadcast()

        if (self._put_leg and self._call_leg and
                self._put_leg["closed"] and self._call_leg["closed"]):
            self._state = VState.DONE
            await self._log(f"All legs closed — session PnL: {self._session_pnl:+.2f} USDT")
            evt_id = (self._active_event or {}).get("id", "")
            if evt_id:
                from backend.utils import ist_now_str as _isn
                await store.save_session(
                    session_id=f"vol_{evt_id}", trader_name=self.name,
                    total_pnl=self._session_pnl,
                    close_ts_ist=_isn(), status="closed",
                )
            await self._save_state()
            await self._broadcast()

    async def _sell_leg(self, leg: dict, price: float, reason: str):
        if not leg or leg.get("closed") or leg.get("sell_placed"):
            return False
        leg["sell_placed"] = True
        sym = leg["symbol"]
        qty = leg["qty"]
        await self._save_state()

        if self.is_paper and self._paper:
            self._paper.executor = self.name
            fill = await self._paper.sell_option(sym, qty, price, action="VOL_LEG_SELL")
            if not fill or not fill.get("filled"):
                leg["sell_placed"] = False
                await self._save_state()
                await self._log(f"Sell skipped: no open paper position for {sym}", "WARNING")
                return False
            await self._paper.save_state()
        else:
            try:
                from backend.execution.binance_client import client
                fill = await client.place_option_order(
                    sym, "SELL", qty, price, timeout_sec=int(cfg.fill_timeout_sec)
                )
                if not fill or not fill.get("filled"):
                    await self._log(f"Sell not filled: {sym}", "WARNING")
                    leg["sell_placed"] = False
                    await self._save_state()
                    return
            except Exception as exc:
                await self._log(f"Sell error {sym}: {exc}", "ERROR")
                leg["sell_placed"] = False
                await self._save_state()
                return

        pnl = (price - leg["entry_price"]) * qty
        self._session_pnl += pnl
        leg["closed"] = True
        leg["close_price"] = price
        leg["close_reason"] = reason
        await self._log(f"SOLD {sym} @ {price:.2f} [{reason}]  PnL: {pnl:+.2f}")
        from backend.utils import ist_now_str, utc_now as _utc_now
        evt_id = (self._active_event or {}).get("id", "")
        await store.save_paper_trade(
            self.name, "VOL_LEG_SELL", sym, "SELL", qty, price, pnl,
            notes=f"close reason: {reason}  entry={leg['entry_price']:.2f}",
            ts_ist=ist_now_str(), ts_utc=_utc_now(), session_id=f"vol_{evt_id}")
        await self._save_state()
        return True

    async def _expire_leg(self, leg: dict):
        if not leg or leg.get("closed"):
            return
        leg["sell_placed"] = True
        await self._save_state()
        leg["closed"] = True
        leg["close_price"] = 0.0
        leg["close_reason"] = "EXPIRED"
        sym = leg["symbol"]
        qty = leg["qty"]
        loss = -leg["entry_price"] * qty
        self._session_pnl += loss

        # Remove from paper engine at price 0 so:
        #   1. Option removed from _option_positions (unrealized_pnl = 0, not -entry_price)
        #   2. _realized_pnl updated with the full loss
        #   3. Accounts section shows correct total_pnl (green, not red)
        if self.is_paper and self._paper:
            self._paper.executor = self.name
            fill = await self._paper.sell_option(sym, qty, 0.0, action="EXPIRED")
            if fill:
                await self._paper.save_state()
            else:
                self._session_pnl -= loss
                leg["close_reason"] = "STALE_NO_POSITION"
                await self._log(f"Expired leg skipped: no open paper position for {sym}", "WARNING")
                await self._save_state()
                return

        # Save to DB so account history shows the expired trade with full timestamp
        from backend.utils import ist_now_str, utc_now as _utc_now
        evt_id = (self._active_event or {}).get("id", "")
        await store.save_paper_trade(
            self.name, "EXPIRED", sym, "SELL", qty, 0.0, loss,
            notes=f"Expired worthless — premium lost ${abs(loss):.2f}",
            ts_ist=ist_now_str(), ts_utc=_utc_now(), session_id=f"vol_{evt_id}")

        await self._log(f"EXPIRED {sym}  loss: {loss:+.2f}")
        await self._save_state()

    # ─────────────────────────────────────────────────────────────
    # Persistence
    # ─────────────────────────────────────────────────────────────

    def _serialize(self) -> dict:
        return {
            "state": self._state.value,
            "active_event": self._active_event,
            "put_leg": self._put_leg,
            "call_leg": self._call_leg,
            "combined_entry": self._combined_entry,
            "session_pnl": self._session_pnl,
            "logs": self._logs[-40:],
        }

    def _restore(self):
        saved = store.get(f"{self.name}_state")
        if not saved:
            return
        try:
            self._state = VState(saved.get("state", VState.SLEEP))
            self._active_event = saved.get("active_event")
            self._put_leg = saved.get("put_leg")
            self._call_leg = saved.get("call_leg")
            self._combined_entry = float(saved.get("combined_entry", 0.0))
            self._session_pnl = float(saved.get("session_pnl", 0.0))
            self._logs = saved.get("logs", [])
        except Exception as exc:
            log.warning(f"{self.name}: restore failed: {exc}")

    async def _save_state(self):
        await store.set(f"{self.name}_state", self._serialize())

    # ─────────────────────────────────────────────────────────────
    # Status / broadcast
    # ─────────────────────────────────────────────────────────────

    def get_status(self) -> dict:
        unrealized = 0.0
        for leg in (self._put_leg, self._call_leg):
            if leg and not leg["closed"]:
                mark = leg["live"].get("mark", 0.0)
                if mark > 0:
                    unrealized += (mark - leg["entry_price"]) * leg["qty"]

        return {
            "executor": self.name,
            "state": self._state.value,
            "is_paper": self.is_paper,
            "active_event": self._active_event,
            "events": self.get_events(),
            "mark_price": self._mark,
            "combined_entry": self._combined_entry,
            "combined_premium_max": float(getattr(cfg, "vol_combined_premium_max", 800.0)),
            "tp_multiplier": float(getattr(cfg, "vol_tp_multiplier", 1.10)),
            "strike_gap": float(getattr(cfg, "vol_strike_gap", 500.0)),
            "strike_gap_tol": float(getattr(cfg, "vol_strike_gap_tolerance", 50.0)),
            "window_close_h": int(getattr(cfg, "vol_window_close_h", 6)),
            "window_close_m": int(getattr(cfg, "vol_window_close_m", 0)),
            "unrealized_pnl": round(unrealized, 2),
            "session_pnl": round(self._session_pnl, 2),
            "scan": {
                "put_sym": self._scan_put.get("symbol", ""),
                "call_sym": self._scan_call.get("symbol", ""),
                "put_strike": float(self._scan_put.get("strike") or 0),
                "call_strike": float(self._scan_call.get("strike") or 0),
                "put_ask": float(self._scan_put.get("ask") or 0),
                "call_ask": float(self._scan_call.get("ask") or 0),
                "put_qty": float(self._scan_put.get("ask_qty") or 0),
                "call_qty": float(self._scan_call.get("ask_qty") or 0),
                "combined": (
                    float(self._scan_put.get("ask") or 0) +
                    float(self._scan_call.get("ask") or 0)
                ),
                "reason": self._scan_reason,
            },
            "put": self._put_leg or {},
            "call": self._call_leg or {},
            "logs": self._logs[-30:],
        }

    async def _broadcast(self):
        try:
            await bus.publish(POSITION_UPDATE, {"trader": self.name, **self.get_status()})
        except Exception:
            pass

    async def _log(self, msg: str, level: str = "INFO"):
        entry = {
            "ts": datetime.now(_IST).strftime("%H:%M:%S"),
            "level": level,
            "msg": msg,
        }
        self._logs.append(entry)
        if len(self._logs) > 80:
            self._logs = self._logs[-80:]
        getattr(log, level.lower(), log.info)(f"{self.name}: {msg}")
        try:
            await bus.publish(LOG_EVENT, {
                "level": level, "source": self.name, "message": msg
            })
        except Exception:
            pass
