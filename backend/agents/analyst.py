"""
Analyst Agent - runs daily at 04:00 IST, re-runs on relevant config changes.

Output per run:
  - high_line, low_line (float)
  - candle_detail_log   - each candle: time_ist, open, high, low, close, touches
  - candidate_log       - every evaluated price level with stats
  - touch_log           - per-touch detail for selected lines

All logs broadcast via ANALYST_LINES so frontend shows full transparency.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Tuple

from backend.message_bus import bus, ANALYST_LINES, ANALYST_RUN_REQ, LOG_EVENT, CONFIG_UPDATE
from backend.config import cfg
from backend.state_store import store
from backend.utils import ist_now, ist_now_str

log = logging.getLogger("analyst")

_IST = timezone(timedelta(hours=5, minutes=30))

# Config keys that require re-running the analyst when changed
_ANALYSIS_PARAMS = {
    "candle_count", "candle_tf_minutes", "delta_touch",
    "min_touches", "dominance_ratio_high", "dominance_ratio_low",
    "analyst_h", "analyst_m"
}


def _ts_to_ist(ts_ms: int) -> str:
    """Convert millisecond timestamp to IST string."""
    if not ts_ms:
        return "—"
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=_IST)
    return dt.strftime("%Y-%m-%d %H:%M IST")


class AnalystAgent:
    def __init__(self):
        self.high_line: Optional[float] = None
        self.low_line:  Optional[float] = None
        self._last_run: float = 0.0
        self._run_log_candidates:  list = []
        self._run_log_touches:     list = []
        self._run_log_candles:     list = []
        self._analysis_tf:         int  = 5
        self._analysis_count:      int  = 300
        self._analysis_from_ist:   str  = ""
        self._analysis_to_ist:     str  = ""
        self._cutoff_ist:          str  = ""
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        bus.subscribe(ANALYST_RUN_REQ, self._on_manual_run)
        bus.subscribe(CONFIG_UPDATE,   self._on_config_update)
        self._task = asyncio.create_task(self._scheduler())

        # Recover last lines
        saved = store.get("analyst_lines")
        if saved:
            self.high_line = saved.get("high_line")
            self.low_line  = saved.get("low_line")
            self._analysis_tf = saved.get("tf_minutes", 5)
            self._analysis_count = saved.get("candles_n", 300)
            log.info(f"Lines recovered: H={self.high_line} L={self.low_line} (from {self._analysis_tf}m TF)")

        # Run immediately if past scheduled time today and no lines
        now_ist  = ist_now()
        run_time = now_ist.replace(hour=cfg.analyst_h, minute=cfg.analyst_m, second=0, microsecond=0)
        sq_time  = now_ist.replace(hour=13, minute=0, second=0, microsecond=0)
        if run_time <= now_ist <= sq_time and not self.high_line:
            log.info("Running analyst immediately (no saved lines for today).")
            await self.run_analysis()

        log.info(f"Analyst agent started - scheduled daily at {cfg.analyst_h:02d}:{cfg.analyst_m:02d} IST.")

    async def stop(self):
        if self._task:
            self._task.cancel()

    async def _scheduler(self):
        from datetime import timedelta
        while True:
            now_ist = ist_now()
            target  = now_ist.replace(hour=cfg.analyst_h, minute=cfg.analyst_m, second=0, microsecond=0)
            if now_ist >= target:
                target = target + timedelta(days=1)
            wait_sec = (target - now_ist).total_seconds()
            log.info(f"Analyst next run in {wait_sec/3600:.2f}h ({target.strftime('%H:%M IST')})")
            try:
                await asyncio.sleep(wait_sec)
                await self.run_analysis()
            except asyncio.CancelledError:
                break

    async def _on_manual_run(self, _msg):
        log.info("Manual analyst run triggered.")
        await self.run_analysis()

    async def _on_config_update(self, msg: dict):
        changes = msg.get("data", {}).get("changes", {})
        affected = set(changes.keys()) & _ANALYSIS_PARAMS
        if affected:
            await bus.publish(LOG_EVENT, {
                "level":  "INFO",
                "source": "Analyst",
                "message": f"Config changed ({', '.join(affected)}) -> updating analyst.",
                "ts_ist": ist_now_str(),
            }, source="analyst")
            
            # If time changed, restart scheduler
            if "analyst_h" in affected or "analyst_m" in affected:
                if self._task:
                    self._task.cancel()
                self._task = asyncio.create_task(self._scheduler())
            
            # Re-run analysis for param changes
            if affected - {"analyst_h", "analyst_m"}:
                await self.run_analysis()

    # ── Core algorithm ─────────────────────────────────────────────────────

    async def run_analysis(self, count: int = None, tf: int = None):
        from backend.data.futures_feed import get_candles, fetch_historical_candles

        run_time = ist_now_str()
        log.info(f"Analyst running at {run_time}")

        count = count or cfg.candle_count
        tf = tf or cfg.candle_tf_minutes

        interval_map = {1:"1m",3:"3m",5:"5m",15:"15m",30:"30m",60:"1h",120:"2h",240:"4h",1440:"1d"}
        tf_str = interval_map.get(tf, "5m")

        candles = get_candles(count, tf_str)
        if len(candles) < max(count // 2, 5):
            log.info(f"Fetching historical {tf_str} candles...")
            candles = await fetch_historical_candles(count, tf_str)

        if not candles:
            log.error(f"No {tf_str} candle data — analysis aborted.")
            return

        tf_ms = tf * 60 * 1000

        # Cutoff = today's scheduled analysis time in IST.
        # Candle close time (ts + tf_ms) must be <= cutoff.
        # If we are running before today's scheduled time, use yesterday's cutoff.
        now_ist = ist_now()
        cutoff_today = now_ist.replace(
            hour=cfg.analyst_h, minute=cfg.analyst_m, second=0, microsecond=0
        )
        if now_ist < cutoff_today:
            # Before today's slot → use yesterday's slot as cutoff
            cutoff_today = cutoff_today - timedelta(days=1)
        cutoff_ms = cutoff_today.timestamp() * 1000

        candles = [c for c in candles if c.get("ts", 0) + tf_ms <= cutoff_ms + 5000]

        if not candles:
            log.error("No candles at or before scheduled cutoff — analysis aborted.")
            return

        # Build detailed candle log for UI
        candle_detail = []
        for i, c in enumerate(candles):
            ts_ist = _ts_to_ist(c.get("ts", 0))
            candle_detail.append({
                "index":    i + 1,
                "time_ist": ts_ist,
                "open":     c["open"],
                "high":     c["high"],
                "low":      c["low"],
                "close":    c["close"],
                "volume":   round(c.get("volume", 0), 4),
                "is_bull":  c["close"] >= c["open"],
            })

        N = len(candles)
        # Time range actually used for analysis
        oldest_ts = candles[0].get("ts", 0)  if candles else 0
        newest_ts = candles[-1].get("ts", 0) if candles else 0
        analysis_from_ist = _ts_to_ist(oldest_ts)
        analysis_to_ist   = _ts_to_ist(newest_ts + tf_ms)   # candle close time
        cutoff_ist        = cutoff_today.strftime("%Y-%m-%d %H:%M IST")

        high_line, low_line, candidates, touches = self._compute_lines(candles)

        self.high_line = high_line
        self.low_line  = low_line
        self._last_run           = time.time()
        self._analysis_tf        = tf
        self._analysis_count     = count
        self._run_log_candidates = candidates
        self._run_log_touches    = touches
        self._run_log_candles    = candle_detail
        self._analysis_from_ist  = analysis_from_ist
        self._analysis_to_ist    = analysis_to_ist
        self._cutoff_ist         = cutoff_ist

        # Annotate candles with which lines they touched (wick-only; body-between = no touch)
        if high_line or low_line:
            hl = high_line or float('inf')
            ll = low_line  or float('-inf')
            for cd in candle_detail:
                c = candles[cd["index"] - 1]
                bmin = min(c["open"], c["close"])
                bmax = max(c["open"], c["close"])
                body_mid = (high_line and low_line and bmin > ll and bmax < hl)
                cd["touches_high"] = (not body_mid and bool(high_line) and
                    abs(c["high"] - high_line) <= cfg.delta_touch)
                cd["touches_low"]  = (not body_mid and bool(low_line) and
                    abs(c["low"]  - low_line)  <= cfg.delta_touch)

        await store.set("analyst_lines", {
            "high_line": high_line, "low_line": low_line, "run_time": run_time,
            "tf_minutes": tf, "candles_n": count
        })
        await store.log_analyst(high_line, low_line, candidates, touches)

        accepted_h = [c for c in candidates if c.get("accepted_high")]
        accepted_l = [c for c in candidates if c.get("accepted_low")]
        hl_str = f"{high_line:.2f}" if high_line else "—"
        ll_str = f"{low_line:.2f}"  if low_line  else "—"
        await bus.publish(LOG_EVENT, {
            "level":  "INFO",
            "source": "Analyst",
            "message": (
                f"Analysis complete at {run_time} | "
                f"{N} × {tf}-min candles | "
                f"{len(candidates)} candidates | "
                f"H-line candidates: {len(accepted_h)} | "
                f"L-line candidates: {len(accepted_l)} | "
                f"H={hl_str}  L={ll_str}"
            ),
            "ts_ist": run_time,
        }, source="analyst")

        await bus.publish(ANALYST_LINES, {
            "high_line":     high_line,
            "low_line":      low_line,
            "run_time":      run_time,
            "candles_n":     N,
            "tf_minutes":    tf,
            "delta_touch":   cfg.delta_touch,
            "min_touches":   cfg.min_touches,
            "dominance_ratio_high": cfg.dominance_ratio_high,
            "dominance_ratio_low":  cfg.dominance_ratio_low,
            "analyst_h":        cfg.analyst_h,
            "analyst_m":        cfg.analyst_m,
            "analysis_from_ist":analysis_from_ist,
            "analysis_to_ist":  analysis_to_ist,
            "cutoff_ist":       cutoff_ist,
            "candidates":       candidates,
            "touches":          touches,
            "candle_detail":    candle_detail,
        }, source="analyst")

        log.info(f"Lines -> H={high_line}  L={low_line}  ({len(candidates)} candidates)")

    # ── Line computation ────────────────────────────────────────────────────

    def _compute_lines(self, candles: List[dict]) -> Tuple[
            Optional[float], Optional[float], List[dict], List[dict]]:

        delta = cfg.delta_touch

        # Collect candidate price levels from all wicks
        raw_points = []
        for c in candles:
            raw_points.append(c["high"])
            raw_points.append(c["low"])
        raw_points.sort()
        candidates_raw: List[float] = []
        if raw_points:
            last = raw_points[0]
            candidates_raw.append(last)
            for p in raw_points[1:]:
                if p - last > delta * 0.1:
                    candidates_raw.append(p)
                    last = p

        # Pass 1 — all candles, wick-only touches
        cands_p1, touches_p1 = self._count_touches(candles, candidates_raw, delta)
        hl0 = self._best_line(cands_p1, candles, delta, "high")
        ll0 = self._best_line(cands_p1, candles, delta, "low", exclude_level=hl0)

        # Pass 2 — exclude candles whose entire body is between the two found lines
        if hl0 and ll0:
            hi, lo = max(hl0, ll0), min(hl0, ll0)
            valid = [c for c in candles
                     if not (min(c["open"], c["close"]) > lo
                             and max(c["open"], c["close"]) < hi)]
            candidates_log, all_touches_log = self._count_touches(valid, candidates_raw, delta)
            high_line = self._best_line(candidates_log, valid, delta, "high")
            low_line  = self._best_line(candidates_log, valid, delta, "low", exclude_level=high_line)
        else:
            candidates_log  = cands_p1
            all_touches_log = touches_p1
            high_line, low_line = hl0, ll0

        if high_line and low_line and high_line < low_line:
            high_line, low_line = low_line, high_line

        # ── Fallback: if one line is still None, relax min_touches progressively ──
        if high_line is None or low_line is None:
            for relaxed_t in range(max(cfg.min_touches - 1, 1), 0, -1):
                if high_line is None:
                    high_line = self._best_line_relaxed(
                        candidates_log, candles, delta, "high", relaxed_t, exclude_level=low_line)
                if low_line is None:
                    low_line = self._best_line_relaxed(
                        candidates_log, candles, delta, "low", relaxed_t, exclude_level=high_line)
                if high_line and low_line:
                    break

        # Keep high > low and ensure a minimum difference of at least 50 points or 2 * delta
        min_diff = max(50.0, 2 * delta)
        if high_line and low_line:
            if high_line < low_line:
                high_line, low_line = low_line, high_line
            if high_line - low_line < min_diff:
                high_line = round(high_line + min_diff / 2, 2)
                low_line = round(low_line - min_diff / 2, 2)

        return high_line, low_line, candidates_log, all_touches_log

    def _best_line_relaxed(self, all_candidates, _candles, _delta: float,
                           mode: str, min_t: int, exclude_level=None) -> Optional[float]:
        """Like _best_line but with relaxed min_touches; used only as fallback."""
        dom_key    = "high_dom" if mode == "high" else "low_dom"
        dom_thresh = cfg.dominance_ratio_high if mode == "high" else cfg.dominance_ratio_low
        touch_key  = "t_high" if mode == "high" else "t_low"

        candidates = all_candidates
        if exclude_level is not None:
            min_sep = max(50.0, 2 * _delta)
            candidates = [c for c in all_candidates if abs(c["level"] - exclude_level) >= min_sep]

        eligible = [c for c in candidates if c["t_total"] >= min_t and c[dom_key] >= dom_thresh]
        if not eligible:
            # Even looser: any candidate with enough touches in the right direction
            eligible = [c for c in candidates if c.get(touch_key, 0) >= min_t]
        if not eligible:
            return None

        eligible.sort(key=lambda c: (c.get(touch_key, 0), c["t_total"]), reverse=True)
        return eligible[0]["level"]

    def _count_touches(self, candles: List[dict], candidates_raw: List[float],
                       delta: float) -> Tuple[List[dict], List[dict]]:
        """Count wick-only touches for each candidate level from the given candles."""
        candidates_log: List[dict] = []
        all_touches_log: List[dict] = []

        for L in candidates_raw:
            t_total = t_high = t_low = 0
            touches_for_L: List[dict] = []

            for i, c in enumerate(candles):
                dist_h = abs(c["high"] - L)
                dist_l = abs(c["low"]  - L)
                # Only wick (high/low) counts — body (open/close) is excluded
                if min(dist_h, dist_l) > delta:
                    continue
                t_total += 1
                is_hi = dist_h <= delta
                is_lo = dist_l <= delta
                if is_hi: t_high += 1
                if is_lo: t_low  += 1
                touch_type = "both" if is_hi and is_lo else "high" if is_hi else "low"
                touches_for_L.append({
                    "candle_idx": i,
                    "time_ist":   _ts_to_ist(c.get("ts", 0)),
                    "type":       touch_type,
                    "distance":   round(min(dist_h, dist_l), 2),
                })

            if t_total == 0:
                continue

            high_dom = t_high / t_total
            low_dom  = t_low  / t_total
            accepted_h = t_total >= cfg.min_touches and high_dom >= cfg.dominance_ratio_high
            accepted_l = t_total >= cfg.min_touches and low_dom  >= cfg.dominance_ratio_low

            candidates_log.append({
                "level":         round(L, 2),
                "t_total":       t_total,
                "t_high":        t_high,
                "t_low":         t_low,
                "high_dom":      round(high_dom, 4),
                "low_dom":       round(low_dom,  4),
                "accepted_high": accepted_h,
                "accepted_low":  accepted_l,
            })
            if touches_for_L:
                all_touches_log.extend([dict(t, level=round(L, 2)) for t in touches_for_L])

        return candidates_log, all_touches_log

    def _best_line(self, all_candidates, candles, delta, mode, exclude_level=None) -> Optional[float]:
        """
        From all candidates, pick the one with the MOST touches that ALSO meets
        the dominance threshold.  Ties broken by SSE (tightest fit).
        This ensures a high-touch / borderline-dominance line always beats a
        low-touch / perfect-dominance line.
        """
        dom_key    = "high_dom" if mode == "high" else "low_dom"
        dom_thresh = cfg.dominance_ratio_high if mode == "high" else cfg.dominance_ratio_low

        candidates = all_candidates
        if exclude_level is not None:
            min_sep = max(50.0, 2 * delta)
            candidates = [c for c in all_candidates if abs(c["level"] - exclude_level) >= min_sep]

        eligible = [
            c for c in candidates
            if c["t_total"] >= cfg.min_touches and c[dom_key] >= dom_thresh
        ]
        if not eligible:
            return None

        max_t = max(c["t_total"] for c in eligible)
        top   = [c for c in eligible if c["t_total"] == max_t]
        if len(top) == 1:
            return top[0]["level"]

        def sse(cand):
            L = cand["level"]
            return sum(
                (c["high"] - L) ** 2 if mode == "high" else (c["low"] - L) ** 2
                for c in candles
                if abs((c["high"] if mode == "high" else c["low"]) - L) <= delta
            )
        return min(top, key=sse)["level"]

    def get_lines(self) -> dict:
        return {
            "high_line":          self.high_line,
            "low_line":           self.low_line,
            "last_run":           self._last_run,
            "tf_minutes":         self._analysis_tf,
            "candles_n":          self._analysis_count,
            "analysis_from_ist":  self._analysis_from_ist,
            "analysis_to_ist":    self._analysis_to_ist,
            "cutoff_ist":         self._cutoff_ist,
            "analyst_h":          cfg.analyst_h,
            "analyst_m":          cfg.analyst_m,
            "candidates":         self._run_log_candidates,
            "touches":            self._run_log_touches,
            "candle_detail":      self._run_log_candles,
        }


analyst = AnalystAgent()
