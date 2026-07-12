"""
Event Calendar — tracks high-impact macro events and fires bus messages.

Data sources:
  1. Hardcoded 2026 schedule (primary) — FOMC, CPI, NFP, PBOC, ECB, PCE
     All times converted to IST. Dates sourced from official schedules:
     - FOMC: federalreserve.gov  (14:00 ET = 23:30 IST)
     - CPI/NFP: bls.gov          (08:30 ET = 18:00 IST)
     - PBOC LPR: 20th monthly    (09:15 Beijing = 06:45 IST)
     - ECB: ecb.europa.eu        (13:15 CET = 17:45 IST)
     - PCE: bea.gov              (08:30 ET = 18:00 IST)
  2. Financial Modeling Prep API (optional) — overlays actual/estimate/previous
     values AFTER event fires. Requires fmp_api_key in config.

Bus messages published:
  EVENT_CALENDAR  — pre-event alert when within vol_pre_event_minutes
  EVENT_FIRED     — fires when event time arrives (±2 min window)
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from backend.message_bus import bus, EVENT_CALENDAR, EVENT_FIRED
from backend.config import cfg
from backend.utils import ist_now, ist_now_str

log = logging.getLogger("event_calendar")

# ── Timezone constants ─────────────────────────────────────────────────────
_IST = timezone(timedelta(hours=5, minutes=30))
_ET  = timezone(timedelta(hours=-5))   # US Eastern Standard (EST); most events use EST
_CST = timezone(timedelta(hours=8))    # China Standard Time
_CET = timezone(timedelta(hours=1))    # Central European Time


def _to_ist(y: int, m: int, d: int, h: int, mi: int, tz=_IST) -> datetime:
    """Build a timezone-aware IST datetime from a local time in `tz`."""
    return datetime(y, m, d, h, mi, tzinfo=tz).astimezone(_IST)


# ── Hardcoded 2026 macro event calendar ────────────────────────────────────

# FOMC Interest Rate Decision (Federal Reserve — 14:00 ET = 23:30 IST)
_FOMC = [
    _to_ist(2026, 1, 29, 14, 0, _ET),
    _to_ist(2026, 3, 18, 14, 0, _ET),
    _to_ist(2026, 5,  6, 14, 0, _ET),
    _to_ist(2026, 6, 17, 14, 0, _ET),
    _to_ist(2026, 7, 29, 14, 0, _ET),
    _to_ist(2026, 9, 16, 14, 0, _ET),
    _to_ist(2026, 11, 4, 14, 0, _ET),
    _to_ist(2026, 12, 16, 14, 0, _ET),
]

# US CPI — Consumer Price Index (BLS — 08:30 ET = 18:00 IST)
_CPI = [
    _to_ist(2026, 1, 14, 8, 30, _ET),
    _to_ist(2026, 2, 11, 8, 30, _ET),
    _to_ist(2026, 3, 11, 8, 30, _ET),
    _to_ist(2026, 4, 10, 8, 30, _ET),
    _to_ist(2026, 5, 12, 8, 30, _ET),
    _to_ist(2026, 6, 11, 8, 30, _ET),
    _to_ist(2026, 7, 15, 8, 30, _ET),
    _to_ist(2026, 8, 12, 8, 30, _ET),
    _to_ist(2026, 9, 10, 8, 30, _ET),
    _to_ist(2026, 10, 14, 8, 30, _ET),
    _to_ist(2026, 11, 12, 8, 30, _ET),
    _to_ist(2026, 12, 10, 8, 30, _ET),
]

# US NFP — Non-Farm Payrolls (BLS — first Friday of month, 08:30 ET = 18:00 IST)
_NFP = [
    _to_ist(2026, 1,  2, 8, 30, _ET),
    _to_ist(2026, 2,  6, 8, 30, _ET),
    _to_ist(2026, 3,  6, 8, 30, _ET),
    _to_ist(2026, 4,  3, 8, 30, _ET),
    _to_ist(2026, 5,  1, 8, 30, _ET),
    _to_ist(2026, 6,  5, 8, 30, _ET),
    _to_ist(2026, 7,  2, 8, 30, _ET),
    _to_ist(2026, 8,  7, 8, 30, _ET),
    _to_ist(2026, 9,  4, 8, 30, _ET),
    _to_ist(2026, 10, 2, 8, 30, _ET),
    _to_ist(2026, 11, 6, 8, 30, _ET),
    _to_ist(2026, 12, 4, 8, 30, _ET),
]

# PBOC LPR — People's Bank of China Loan Prime Rate (20th monthly, 09:15 Beijing = 06:45 IST)
_PBOC = [_to_ist(2026, m, 20, 9, 15, _CST) for m in range(1, 13)]

# US PCE — Personal Consumption Expenditures (BEA — 08:30 ET = 18:00 IST)
_PCE = [
    _to_ist(2026, 1, 30, 8, 30, _ET),
    _to_ist(2026, 2, 27, 8, 30, _ET),
    _to_ist(2026, 3, 27, 8, 30, _ET),
    _to_ist(2026, 4, 24, 8, 30, _ET),
    _to_ist(2026, 5, 29, 8, 30, _ET),
    _to_ist(2026, 6, 26, 8, 30, _ET),
    _to_ist(2026, 7, 31, 8, 30, _ET),
    _to_ist(2026, 8, 28, 8, 30, _ET),
    _to_ist(2026, 9, 25, 8, 30, _ET),
    _to_ist(2026, 10, 30, 8, 30, _ET),
    _to_ist(2026, 11, 25, 8, 30, _ET),
    _to_ist(2026, 12, 31, 8, 30, _ET),
]

# ECB Rate Decision (13:15 CET = 17:45 IST)
_ECB = [
    _to_ist(2026, 1, 30, 13, 15, _CET),
    _to_ist(2026, 3, 19, 13, 15, _CET),
    _to_ist(2026, 4, 30, 13, 15, _CET),
    _to_ist(2026, 6, 11, 13, 15, _CET),
    _to_ist(2026, 7, 23, 13, 15, _CET),
    _to_ist(2026, 9, 10, 13, 15, _CET),
    _to_ist(2026, 10, 29, 13, 15, _CET),
    _to_ist(2026, 12, 10, 13, 15, _CET),
]

# Assemble and sort all events
ALL_EVENTS: List[Dict[str, Any]] = sorted([
    *[{"name": "FOMC Rate Decision", "type": "FOMC", "ist": dt, "impact": "HIGH",   "country": "US"} for dt in _FOMC],
    *[{"name": "US CPI",             "type": "CPI",  "ist": dt, "impact": "HIGH",   "country": "US"} for dt in _CPI],
    *[{"name": "US NFP",             "type": "NFP",  "ist": dt, "impact": "HIGH",   "country": "US"} for dt in _NFP],
    *[{"name": "PBOC LPR",           "type": "PBOC", "ist": dt, "impact": "HIGH",   "country": "CN"} for dt in _PBOC],
    *[{"name": "US PCE",             "type": "PCE",  "ist": dt, "impact": "MEDIUM", "country": "US"} for dt in _PCE],
    *[{"name": "ECB Rate Decision",  "type": "ECB",  "ist": dt, "impact": "MEDIUM", "country": "EU"} for dt in _ECB],
], key=lambda e: e["ist"])


# ── BTC direction logic per event type ────────────────────────────────────
# Used to annotate the pre-event alert so the panel can show expected direction.
EVENT_DIRECTION_HINT = {
    "FOMC": "Rate cut → BTC BULLISH | Rate hike → BTC BEARISH",
    "CPI":  "Above estimate → BTC BEARISH (rate hike fear) | Below → BTC BULLISH",
    "NFP":  "Above estimate → BTC BEARISH (rate hike fear) | Below → BTC BULLISH",
    "PBOC": "Rate cut → BTC BULLISH | Hold/hike → neutral/BEARISH",
    "PCE":  "Above estimate → BTC BEARISH | Below → BTC BULLISH",
    "ECB":  "Rate cut → BTC BULLISH | Rate hike → BTC BEARISH",
}

# FMP keyword mapping for economic calendar search
_FMP_KEYWORDS = {
    "FOMC": "federal funds",
    "CPI":  "consumer price index",
    "NFP":  "nonfarm payrolls",
    "PBOC": "loan prime rate",
    "PCE":  "personal consumption",
    "ECB":  "ecb interest rate",
}


class EventCalendar:
    """
    Background watcher: fires EVENT_CALENDAR pre-alerts and EVENT_FIRED at event time.
    Optionally enriches fired events with actual/estimate from FMP API.
    """

    def __init__(self):
        self._fired_keys: set = set()       # (type, YYYY-MM-DD) already published
        self._task: Optional[asyncio.Task] = None
        self._recent_fired: Optional[Dict] = None
        self._upcoming: Optional[Dict] = None

    async def start(self):
        self._task = asyncio.create_task(self._watch_loop())
        log.info("EventCalendar started — FOMC | CPI | NFP | PBOC | PCE | ECB")

    async def stop(self):
        if self._task:
            self._task.cancel()

    @property
    def recent_fired(self) -> Optional[Dict]:
        return self._recent_fired

    @property
    def upcoming(self) -> Optional[Dict]:
        return self._upcoming

    def get_upcoming(self, days: int = 30) -> List[Dict]:
        """Upcoming events within next `days` days, serialised for API response."""
        now = ist_now()
        cutoff = now + timedelta(days=days)
        result = []
        for ev in ALL_EVENTS:
            if now <= ev["ist"] <= cutoff:
                secs = int((ev["ist"] - now).total_seconds())
                result.append({
                    "name":       ev["name"],
                    "type":       ev["type"],
                    "ist":        ev["ist"].strftime("%Y-%m-%d %H:%M IST"),
                    "impact":     ev["impact"],
                    "country":    ev["country"],
                    "secs_until": secs,
                    "hint":       EVENT_DIRECTION_HINT.get(ev["type"], ""),
                })
        return result

    def get_all(self) -> List[Dict]:
        """Full 2026 calendar for panel display."""
        now = ist_now()
        result = []
        for ev in ALL_EVENTS:
            secs = int((ev["ist"] - now).total_seconds())
            result.append({
                "name":       ev["name"],
                "type":       ev["type"],
                "ist":        ev["ist"].strftime("%Y-%m-%d %H:%M IST"),
                "impact":     ev["impact"],
                "country":    ev["country"],
                "secs_until": secs,
                "hint":       EVENT_DIRECTION_HINT.get(ev["type"], ""),
                "past":       secs < 0,
            })
        return result

    # ── Internal loop ──────────────────────────────────────────────────────

    async def _watch_loop(self):
        while True:
            try:
                await self._tick()
            except Exception as ex:
                log.error(f"EventCalendar tick error: {ex}", exc_info=True)
            await asyncio.sleep(60)

    async def _tick(self):
        now = ist_now()
        pre_min = getattr(cfg, "vol_pre_event_minutes", 30)

        # Find next upcoming event
        self._upcoming = None
        for ev in ALL_EVENTS:
            if ev["ist"] > now:
                self._upcoming = ev
                break

        # Fire events that just passed (within last 2 minutes)
        for ev in ALL_EVENTS:
            key = (ev["type"], ev["ist"].strftime("%Y-%m-%d"))
            if key in self._fired_keys:
                continue
            secs_since = (now - ev["ist"]).total_seconds()
            if 0 <= secs_since < 120:
                await self._fire(ev)
                self._fired_keys.add(key)

        # Pre-event alert for next upcoming event
        if self._upcoming:
            secs_until = (self._upcoming["ist"] - now).total_seconds()
            if 0 < secs_until <= pre_min * 60:
                await self._alert(self._upcoming, secs_until)

    async def _fire(self, ev: Dict):
        fmp = await self._fetch_fmp(ev)
        payload = {
            "name":     ev["name"],
            "type":     ev["type"],
            "ist":      ev["ist"].strftime("%Y-%m-%d %H:%M IST"),
            "impact":   ev["impact"],
            "country":  ev["country"],
            "actual":   fmp.get("actual"),
            "estimate": fmp.get("estimate"),
            "previous": fmp.get("previous"),
            "unit":     fmp.get("unit", ""),
            "hint":     EVENT_DIRECTION_HINT.get(ev["type"], ""),
            "ts_ist":   ist_now_str(),
        }
        self._recent_fired = payload
        log.info(
            f"EVENT_FIRED: {ev['name']} "
            f"actual={fmp.get('actual')} est={fmp.get('estimate')} prev={fmp.get('previous')}"
        )
        await bus.publish(EVENT_FIRED, payload, source="event_calendar")

    async def _alert(self, ev: Dict, secs_until: float):
        payload = {
            "name":       ev["name"],
            "type":       ev["type"],
            "ist":        ev["ist"].strftime("%Y-%m-%d %H:%M IST"),
            "impact":     ev["impact"],
            "country":    ev["country"],
            "secs_until": int(secs_until),
            "hint":       EVENT_DIRECTION_HINT.get(ev["type"], ""),
            "ts_ist":     ist_now_str(),
        }
        log.info(f"EVENT_CALENDAR: {ev['name']} in {secs_until/60:.1f}min")
        await bus.publish(EVENT_CALENDAR, payload, source="event_calendar")

    async def _fetch_fmp(self, ev: Dict) -> Dict:
        """Try FMP API for actual/estimate/previous. Returns {} on any failure."""
        api_key = getattr(cfg, "fmp_api_key", "")
        if not api_key:
            return {}
        date_str = ev["ist"].strftime("%Y-%m-%d")
        kw = _FMP_KEYWORDS.get(ev["type"], ev["name"].lower())
        try:
            import aiohttp
            url = (
                f"https://financialmodelingprep.com/api/v3/economic_calendar"
                f"?from={date_str}&to={date_str}&apikey={api_key}"
            )
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            ) as sess:
                async with sess.get(url) as resp:
                    if resp.status != 200:
                        return {}
                    data = await resp.json()
            for item in data:
                if kw.lower() in item.get("event", "").lower():
                    return {
                        "actual":   item.get("actual"),
                        "estimate": item.get("estimate"),
                        "previous": item.get("previous"),
                        "unit":     item.get("unit", ""),
                    }
        except Exception as ex:
            log.debug(f"FMP fetch skipped: {ex}")
        return {}


# Global singleton
event_calendar = EventCalendar()
