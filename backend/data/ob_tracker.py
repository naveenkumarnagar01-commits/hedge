"""
OB Zone Lifecycle Tracker.

On every CANDLE_CLOSE event for 5m / 15m / 1h / 4h:
  - Runs OB detection (same algorithm traders use)
  - For demand AND supply zones:
      • New zone (born_ts not yet in DB): INSERT — records when it first appeared
      • Zone gone (was in DB as active, now absent): marks consumed_ist + consumed_price
      • Zone still present: updates last_seen_ist
  - All zones (active + consumed) stay in ob_zone_lifecycle table permanently

This lets users open the OB tab next day and select e.g. "1h TF" to see:
  - Which zones existed when their trading window opened yesterday
  - When each zone was created (born_ts → created_ist)
  - Whether the zone was consumed (and at what price) before or after the trade
"""

import asyncio
import logging
from typing import Dict, Set

from backend.config import cfg
from backend.message_bus import bus, CANDLE_CLOSE
from backend.state_store import store
from backend.utils import ist_now_str

log = logging.getLogger("ob_tracker")

_TRACKED_TFS = ("5m", "15m", "1h", "4h")

# In-memory cache of active born_ts sets per "tf:zone_type" — avoids DB round-trip on every candle
_active: Dict[str, Set[int]] = {}


def _key(tf: str, zone_type: str) -> str:
    return f"{tf}:{zone_type}"


async def _load_active_from_db():
    """Populate in-memory cache from DB at startup."""
    for tf in _TRACKED_TFS:
        for zt in ("demand", "supply"):
            rows = await store.get_ob_lifecycle(tf, limit=200)
            _active[_key(tf, zt)] = {
                r["born_ts"] for r in rows
                if r["zone_type"] == zt and r["is_active"] == 1
            }


async def _run_for_tf(tf: str, current_price: float = 0.0):
    """Detect OB zones for one TF and update the lifecycle table."""
    from backend.data.futures_feed import get_candles, fetch_historical_candles
    from backend.data.order_blocks import detect

    n = int(getattr(cfg, "ob_candle_count", 500))
    candles = get_candles(n, tf)
    if len(candles) < max(n // 3, 10):
        candles = await fetch_historical_candles(n, tf)
    if not candles:
        return

    result  = detect(candles)
    now_ist = ist_now_str()
    last_close = float(candles[-1].get("close", 0)) if candles else current_price

    # TF duration in ms (needed to back-calculate born_ts from age_bars when missing)
    tf_ms = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}.get(tf, 300_000)
    last_ts = int(candles[-1]["ts"]) if candles else 0

    for zone_type in ("demand", "supply"):
        new_zones = result.get(zone_type, [])

        # Ensure every zone has a born_ts
        for z in new_zones:
            if not z.get("born_ts"):
                try:
                    age = int(z.get("age_bars", 0))
                    z["born_ts"] = int(last_ts - age * tf_ms)
                except Exception:
                    z["born_ts"] = last_ts

        new_born: Set[int] = {int(z["born_ts"]) for z in new_zones}
        k = _key(tf, zone_type)
        prev_born: Set[int] = _active.get(k, set())

        # Zones that disappeared since last run → consumed
        consumed_born = [bts for bts in prev_born if bts not in new_born]

        await store.upsert_ob_lifecycle(
            zones=new_zones,
            tf=tf,
            zone_type=zone_type,
            now_ist=now_ist,
            consumed_born_ts=consumed_born,
            consumed_price=last_close if consumed_born else None,
        )

        _active[k] = new_born


class OBTracker:
    """Subscribes to CANDLE_CLOSE and keeps ob_zone_lifecycle up to date."""

    def __init__(self):
        self._last_candle_ts: Dict[str, int] = {}
        self._last_snapshot_key: str = ""   # "YYYY-MM-DD_HH:MM"

    async def start(self):
        bus.subscribe(CANDLE_CLOSE, self._on_candle_close)
        await _load_active_from_db()
        asyncio.create_task(self._initial_scan())
        asyncio.create_task(self._snapshot_scheduler())
        log.info("OBTracker started — 5m/15m/1h/4h zone lifecycle tracking active.")

    async def _on_candle_close(self, msg: dict):
        d = msg.get("data", {})
        if not d.get("is_final"):
            return
        tf  = d.get("tf", "")
        bts = int(d.get("candle", {}).get("ts", 0))
        if tf not in _TRACKED_TFS:
            return
        if bts <= self._last_candle_ts.get(tf, 0):
            return
        self._last_candle_ts[tf] = bts
        asyncio.create_task(self._safe_run(tf))

    async def _safe_run(self, tf: str):
        try:
            await _run_for_tf(tf)
        except Exception as e:
            log.debug(f"OBTracker {tf}: {e}")

    async def _initial_scan(self):
        await asyncio.sleep(8)   # feeds warm-up buffer
        for tf in _TRACKED_TFS:
            try:
                await _run_for_tf(tf)
            except Exception as e:
                log.debug(f"OBTracker initial {tf}: {e}")
        log.info("OBTracker initial scan complete.")
        # Take a startup snapshot if today's is missing and we're past the configured time
        await self._startup_snapshot_check()

    async def _startup_snapshot_check(self):
        """On startup: if today's configured-time snapshot is missing, take one now."""
        from backend.utils import ist_now
        try:
            now       = ist_now()
            snap_h    = int(getattr(cfg, "ob_snapshot_h", 4))
            snap_m    = int(getattr(cfg, "ob_snapshot_m", 0))
            today_str = now.strftime("%Y-%m-%d")
            snap_key  = f"{today_str}_{snap_h:02d}:{snap_m:02d}"
            now_mins  = now.hour * 60 + now.minute
            snap_mins = snap_h * 60 + snap_m

            if now_mins >= snap_mins:
                existing    = await store.get_ob_snapshot_dates(limit=60)
                today_exists = any(r.get("date") == today_str for r in existing)
                if not today_exists:
                    log.info(f"OBTracker: startup recovery — no snapshot for {today_str}, taking one now")
                    await self._take_snapshot(today_str, now.strftime("%H:%M"))
                    self._last_snapshot_key = snap_key
        except Exception as e:
            log.error(f"OBTracker startup snapshot check error: {e}")

    async def _snapshot_scheduler(self):
        """Poll every 30s. Tracks date+time key so config changes fire a new snapshot."""
        from backend.utils import ist_now
        await asyncio.sleep(20)
        while True:
            try:
                now       = ist_now()
                snap_h    = int(getattr(cfg, "ob_snapshot_h", 4))
                snap_m    = int(getattr(cfg, "ob_snapshot_m", 0))
                today_str = now.strftime("%Y-%m-%d")
                snap_key  = f"{today_str}_{snap_h:02d}:{snap_m:02d}"

                if now.hour == snap_h and now.minute == snap_m:
                    if self._last_snapshot_key != snap_key:
                        await self._take_snapshot(today_str, f"{snap_h:02d}:{snap_m:02d}")
                        self._last_snapshot_key = snap_key
                        log.info(f"OBTracker: snapshot saved — {snap_key}")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"OBTracker snapshot scheduler error: {e}")

            await asyncio.sleep(30)

    async def _take_snapshot(self, date_str: str, time_str: str):
        from backend.data.futures_feed import get_candles, fetch_historical_candles
        from backend.data.order_blocks import detect
        
        log.info(f"Taking daily OB snapshot for {date_str} {time_str} IST...")
        rows = []
        n = int(getattr(cfg, "ob_candle_count", 500))
        
        for tf in _TRACKED_TFS:
            candles = get_candles(n, tf)
            if len(candles) < max(n // 3, 10):
                candles = await fetch_historical_candles(n, tf)
            if not candles:
                continue
                
            result = detect(candles)
            current_price = float(candles[-1].get("close", 0)) if candles else 0.0
            
            for zone_type in ("demand", "supply"):
                zones = result.get(zone_type, [])
                nearest = zones[:1]   # only the nearest zone per type per TF
                for z in nearest:
                    rows.append({
                        "snapshot_date": date_str,
                        "snapshot_time": time_str,
                        "tf": tf,
                        "zone_type": zone_type,
                        "zone_top": z.get("top"),
                        "zone_bottom": z.get("bottom"),
                        "zone_mid": z.get("mid"),
                        "zone_score": z.get("score"),
                        "zone_grade": z.get("grade"),
                        "born_ts": z.get("born_ts"),
                        "age_bars": z.get("age_bars"),
                        "distance_pct": z.get("distance_pct"),
                        "current_price": current_price
                    })
        
        if rows:
            await store.save_ob_daily_snapshot(rows)
            log.info(f"Saved {len(rows)} zones in OB snapshot.")

ob_tracker = OBTracker()
