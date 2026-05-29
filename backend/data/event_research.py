"""
Event Research Engine — Macro event analysis using free/open-source APIs.

Three data pipelines (all gracefully degrade when keys are missing):

  1. PAST EVENT REPORTS
     ┌ FRED API (fred_api_key in config) — official US Federal Reserve data
     │   FOMC → DFEDTARU (Federal Funds Upper Target Rate, daily)
     │   CPI  → CPIAUCSL (CPI-U All Items Seasonally Adjusted, monthly)
     │   NFP  → PAYEMS   (Total Nonfarm Payrolls in thousands, monthly)
     │   PCE  → PCEPI    (PCE Price Index, monthly)
     └ Binance public futures API (no auth required)
         → BTC price at event, 1h after, 4h after → move % + direction

  2. UPCOMING EVENT FORECAST
     └ FRED API → most recent actual value shown as "latest reading"

  3. AI ANALYSIS — tries in order, falls back silently:
     1. Groq API  (groq_api_key in config, free 14 400 req/day)
     2. Ollama    (ollama_url in config, local zero-cost LLM)
     3. ""        (empty string — no key/server available)

Free API registration:
  FRED : https://fred.stlouisfed.org/docs/api/api_key.html
  Groq : https://console.groq.com  (14 400 free requests/day)
  Ollama: https://ollama.ai  (local, zero cost — run: ollama pull llama3.2)

Usage:
  from backend.data.event_research import event_research
  report   = await event_research.get_past_reports(months_back=3)
  forecast = await event_research.get_upcoming_forecast()
"""

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from backend.config import cfg
from backend.utils import ist_now, ist_now_str

log = logging.getLogger("event_research")

_IST = timezone(timedelta(hours=5, minutes=30))

# FRED series IDs and display units per event type (US only, monthly/daily precision)
_FRED_SERIES: Dict[str, tuple] = {
    "FOMC": ("DFEDTARU", "%"),      # Federal Funds Upper Target Rate (daily)
    "CPI":  ("CPIAUCSL", "Index"),  # CPI-U All Items Seasonally Adjusted (monthly)
    "NFP":  ("PAYEMS",   "K chg"),  # Total Nonfarm Payrolls — level in thousands
    "PCE":  ("PCEPI",    "Index"),  # PCE Price Index (monthly)
}

# Statistics of the World API indicators (annual data — fills PBOC/ECB gap not covered by FRED)
# Free: statisticsoftheworld.com/api-docs  — no auth for 100 req/day, free key for 1000/day
_SOTW_BASE = "https://statisticsoftheworld.com/api/v1/history"
_SOTW_INDICATORS: Dict[str, tuple] = {
    "FOMC": ("FRED.FEDFUNDS", "USA", "%"),       # US Fed Funds Rate annual avg (FRED fallback)
    "CPI":  ("FP.CPI.TOTL.ZG", "USA", "% YoY"), # US CPI inflation annual
    "NFP":  ("FRED.UNRATE",    "USA", "%"),       # US Unemployment Rate annual
    "PCE":  ("IMF.PCPIPCH",    "USA", "% YoY"),  # US PCE proxy annual
    "PBOC": ("FRED.CBRATE",    "CHN", "%"),       # China central bank rate (annual)
    "ECB":  ("FRED.CBRATE",    "DEU", "%"),       # ECB rate via Germany (annual)
}

# Historical avg BTC move by event type (seed data)
_HISTORICAL_PATTERN: Dict[str, Dict] = {
    "FOMC": {"avg_move_pct": 2.1, "bullish_rate": 0.55, "sample": "Rate cuts favor BTC"},
    "CPI":  {"avg_move_pct": 1.8, "bullish_rate": 0.45, "sample": "Below est → rally"},
    "NFP":  {"avg_move_pct": 1.2, "bullish_rate": 0.42, "sample": "Weak jobs → rate cut hope"},
    "PBOC": {"avg_move_pct": 0.9, "bullish_rate": 0.58, "sample": "China easing bullish"},
    "PCE":  {"avg_move_pct": 1.5, "bullish_rate": 0.44, "sample": "Below est → rally"},
    "ECB":  {"avg_move_pct": 0.8, "bullish_rate": 0.50, "sample": "Dovish stance bullish"},
}


class _Cache:
    def __init__(self, ttl_sec: int = 3600):
        self._data: Dict[str, Any] = {}
        self._ts:   Dict[str, float] = {}
        self._ttl = ttl_sec

    def get(self, key: str) -> Optional[Any]:
        if key in self._data and (time.time() - self._ts.get(key, 0)) < self._ttl:
            return self._data[key]
        return None

    def set(self, key: str, val: Any):
        self._data[key] = val
        self._ts[key]   = time.time()

    def invalidate(self, key: str):
        self._data.pop(key, None)
        self._ts.pop(key, None)


class EventResearch:

    def __init__(self):
        self._past_cache     = _Cache(ttl_sec=7200)   # 2 h — past reports
        self._fcast_cache    = _Cache(ttl_sec=14400)  # 4 h — forecasts
        self._ai_cache       = _Cache(ttl_sec=43200)  # 12 h — AI text
        self._fred_cache     = _Cache(ttl_sec=3600)   # 1 h — FRED series
        self._sotw_cache     = _Cache(ttl_sec=21600)  # 6 h — SOTW annual data
        self._fetching_past  = False
        self._fetching_fcast = False

    # ── Public API ─────────────────────────────────────────────────────────

    async def get_past_reports(self, months_back: int = 3,
                                force_refresh: bool = False) -> Dict:
        key = f"past_{months_back}"
        if not force_refresh:
            cached = self._past_cache.get(key)
            if cached:
                return cached
        if self._fetching_past:
            return {"reports": [], "status": "fetching", "ts_ist": ist_now_str()}
        self._fetching_past = True
        try:
            reports = await self._build_past_reports(months_back)
            result  = {"reports": reports, "status": "ok",
                       "count": len(reports), "ts_ist": ist_now_str()}
            self._past_cache.set(key, result)
            return result
        except Exception as ex:
            log.error(f"Past reports error: {ex}", exc_info=True)
            return {"reports": [], "status": f"error: {ex}", "ts_ist": ist_now_str()}
        finally:
            self._fetching_past = False

    async def get_upcoming_forecast(self, force_refresh: bool = False) -> Dict:
        key = "forecast"
        if not force_refresh:
            cached = self._fcast_cache.get(key)
            if cached:
                return cached
        if self._fetching_fcast:
            return {"events": [], "status": "fetching", "ts_ist": ist_now_str()}
        self._fetching_fcast = True
        try:
            events = await self._build_forecast()
            result = {"events": events, "status": "ok",
                      "count": len(events), "ts_ist": ist_now_str()}
            self._fcast_cache.set(key, result)
            return result
        except Exception as ex:
            log.error(f"Forecast error: {ex}", exc_info=True)
            return {"events": [], "status": f"error: {ex}", "ts_ist": ist_now_str()}
        finally:
            self._fetching_fcast = False

    def invalidate_all(self):
        self._past_cache.invalidate("past_3")
        self._past_cache.invalidate("past_6")
        self._fcast_cache.invalidate("forecast")

    # ── Past reports builder ───────────────────────────────────────────────

    async def _build_past_reports(self, months_back: int) -> List[Dict]:
        from backend.data.event_calendar import ALL_EVENTS
        now      = ist_now()
        start_dt = now - timedelta(days=months_back * 30)

        past_events = [
            ev for ev in ALL_EVENTS
            if ev["ist"] < now and ev["ist"] >= start_dt
        ]
        if not past_events:
            return []

        # Fetch FRED series covering the range (with 90-day lookback for prev values)
        fetch_start = (start_dt - timedelta(days=90)).strftime("%Y-%m-%d")
        fetch_end   = now.strftime("%Y-%m-%d")
        fred_obs: Dict[str, List[Dict]] = {}
        for ev_type, (series_id, _) in _FRED_SERIES.items():
            cache_key = f"fred_{series_id}_{fetch_start}_{fetch_end}"
            hit = self._fred_cache.get(cache_key)
            if hit is not None:
                fred_obs[ev_type] = hit
            else:
                obs = await self._fetch_fred(series_id, fetch_start, fetch_end)
                fred_obs[ev_type] = obs
                self._fred_cache.set(cache_key, obs)

        # Pre-fetch SOTW annual data for PBOC/ECB (FRED has no coverage for these)
        sotw_obs: Dict[str, List[Dict]] = {}
        for ev_type in ("PBOC", "ECB"):
            entry = _SOTW_INDICATORS.get(ev_type)
            if entry:
                ind, country, _ = entry
                sotw_obs[ev_type] = await self._fetch_sotw(ind, country)

        reports = []
        for ev in past_events:
            ev_type = ev["type"]
            ev_ist  = ev["ist"]
            _, unit = _FRED_SERIES.get(ev_type, ("", ""))

            actual = None
            obs_list = fred_obs.get(ev_type, [])
            if obs_list:
                actual = self._get_fred_actual(obs_list, ev_ist, ev_type)

            # PBOC/ECB: no FRED coverage → use SOTW annual data
            note = ""
            if actual is None and ev_type in sotw_obs:
                sw = sotw_obs[ev_type]
                actual = self._get_sotw_actual(sw, ev_ist)
                _, _, sotw_unit = _SOTW_INDICATORS.get(ev_type, (None, None, ""))
                if actual is not None:
                    unit = sotw_unit
                    note = "Annual avg · statisticsoftheworld.com"
            elif actual is None and ev_type in _FRED_SERIES:
                note = "Set fred_api_key for actual data (free: fred.stlouisfed.org)"

            btc  = await self._btc_reaction(ev_ist)

            reports.append({
                "event":         ev["name"],
                "type":          ev_type,
                "country":       ev["country"],
                "ist":           ev_ist.strftime("%Y-%m-%d %H:%M IST"),
                "actual":        actual,
                "estimate":      None,
                "previous":      None,
                "surprise":      None,
                "surprise_dir":  "—",
                "unit":          unit,
                "impact":        ev["impact"],
                "btc_pre":       btc.get("pre_price"),
                "btc_1h":        btc.get("price_1h"),
                "btc_4h":        btc.get("price_4h"),
                "btc_move_1h":   btc.get("move_1h_pct"),
                "btc_move_4h":   btc.get("move_4h_pct"),
                "btc_direction": btc.get("direction_1h", "—"),
                "note":          note,
            })

        reports.sort(key=lambda r: r["ist"], reverse=True)
        return reports

    # ── Upcoming forecast builder ──────────────────────────────────────────

    async def _build_forecast(self) -> List[Dict]:
        from backend.data.event_calendar import ALL_EVENTS
        now = ist_now()

        upcoming = [ev for ev in ALL_EVENTS if ev["ist"] > now][:10]
        if not upcoming:
            return []

        # Fetch latest FRED readings to use as "previous value" for upcoming events
        start_str = (now - timedelta(days=90)).strftime("%Y-%m-%d")
        end_str   = now.strftime("%Y-%m-%d")
        fred_latest: Dict[str, Optional[float]] = {}

        for ev_type, (series_id, _) in _FRED_SERIES.items():
            cache_key = f"fred_latest_{series_id}_{end_str}"
            hit = self._fred_cache.get(cache_key)
            if hit is not None:
                fred_latest[ev_type] = hit
            else:
                obs = await self._fetch_fred(series_id, start_str, end_str)
                val: Optional[float] = None
                if obs:
                    if ev_type == "NFP" and len(obs) >= 2:
                        val = round(obs[-1]["value"] - obs[-2]["value"], 0)
                    else:
                        val = round(obs[-1]["value"], 2)
                fred_latest[ev_type] = val
                self._fred_cache.set(cache_key, val)

        # Fetch SOTW latest for PBOC/ECB (no FRED coverage)
        sotw_latest: Dict[str, Dict] = {}
        for ev_type in ("PBOC", "ECB"):
            entry = _SOTW_INDICATORS.get(ev_type)
            if entry:
                ind, country, sotw_unit = entry
                sw_cache_key = f"sotw_latest_{ind}_{country}"
                hit = self._sotw_cache.get(sw_cache_key)
                if hit is not None:
                    sotw_latest[ev_type] = hit
                else:
                    sw_data = await self._fetch_sotw(ind, country)
                    sw_val  = round(sw_data[-1]["value"], 2) if sw_data else None
                    sotw_latest[ev_type] = {"value": sw_val, "unit": sotw_unit}
                    self._sotw_cache.set(sw_cache_key, sotw_latest[ev_type])

        events = []
        for ev in upcoming:
            ev_type  = ev["type"]
            date_str = ev["ist"].strftime("%Y-%m-%d")
            secs     = int((ev["ist"] - now).total_seconds())
            pattern  = _HISTORICAL_PATTERN.get(ev_type, {})
            _, unit  = _FRED_SERIES.get(ev_type, ("", ""))
            previous = fred_latest.get(ev_type)

            # PBOC/ECB: override with SOTW annual data
            if previous is None and ev_type in sotw_latest:
                sw = sotw_latest[ev_type]
                previous = sw.get("value")
                unit = sw.get("unit", "")

            consensus = {"estimate": None, "previous": previous, "unit": unit}

            ai_cache_key = f"ai_{ev_type}_{date_str}"
            ai_text = self._ai_cache.get(ai_cache_key)
            if ai_text is None:
                ai_text = await self._ai_forecast(ev, consensus, pattern)
                self._ai_cache.set(ai_cache_key, ai_text or "")

            events.append({
                "name":         ev["name"],
                "type":         ev_type,
                "country":      ev["country"],
                "ist":          ev["ist"].strftime("%Y-%m-%d %H:%M IST"),
                "impact":       ev["impact"],
                "secs_until":   secs,
                "estimate":     None,
                "previous":     previous,
                "unit":         unit,
                "avg_move_pct": pattern.get("avg_move_pct"),
                "bullish_rate": pattern.get("bullish_rate"),
                "pattern_note": pattern.get("sample", ""),
                "ai_forecast":  ai_text,
            })

        return events

    # ── Statistics of the World API ───────────────────────────────────────

    async def _fetch_sotw(self, indicator: str, country: str) -> List[Dict]:
        """Fetch annual historical data from statisticsoftheworld.com. No auth for basic usage."""
        cache_key = f"sotw_{indicator}_{country}"
        hit = self._sotw_cache.get(cache_key)
        if hit is not None:
            return hit
        try:
            import aiohttp
        except ImportError:
            return []
        api_key = getattr(cfg, "stat_world_api_key", "") or os.environ.get("STAT_WORLD_API_KEY", "")
        url = f"{_SOTW_BASE}/{indicator}/{country}"
        if api_key:
            url += f"?apikey={api_key}"
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            ) as sess:
                async with sess.get(url) as resp:
                    if resp.status != 200:
                        log.debug(f"SOTW {indicator}/{country} returned {resp.status}")
                        self._sotw_cache.set(cache_key, [])
                        return []
                    data = await resp.json()
                    obs = [
                        {"year": int(d["year"]), "value": float(d["value"])}
                        for d in data.get("data", [])
                        if d.get("value") is not None
                    ]
                    self._sotw_cache.set(cache_key, obs)
                    return obs
        except Exception as ex:
            log.debug(f"SOTW fetch {indicator}/{country} failed: {ex}")
            self._sotw_cache.set(cache_key, [])
            return []

    def _get_sotw_actual(self, obs: List[Dict], ev_ist: datetime) -> Optional[float]:
        """Return the annual value for the year of the event (or most recent prior year)."""
        if not obs:
            return None
        target_year = ev_ist.year
        result = None
        for o in obs:
            if o["year"] <= target_year:
                result = o
        return round(result["value"], 2) if result else None

    # ── FRED API ───────────────────────────────────────────────────────────

    async def _fetch_fred(self, series_id: str, start_date: str, end_date: str) -> List[Dict]:
        """Fetch FRED series observations. Returns [] if no key or on failure."""
        api_key = getattr(cfg, "fred_api_key", "") or os.environ.get("FRED_API_KEY", "")
        if not api_key:
            return []
        try:
            import aiohttp
        except ImportError:
            return []

        url = (
            "https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={api_key}&file_type=json"
            f"&observation_start={start_date}&observation_end={end_date}&sort_order=asc"
        )
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            ) as sess:
                async with sess.get(url) as resp:
                    if resp.status != 200:
                        log.warning(f"FRED {series_id} returned {resp.status}")
                        return []
                    data = await resp.json()
                    return [
                        {"date": o["date"], "value": float(o["value"])}
                        for o in data.get("observations", [])
                        if o.get("value") not in (".", None, "")
                    ]
        except Exception as ex:
            log.debug(f"FRED fetch {series_id} failed: {ex}")
            return []

    def _get_fred_actual(self, obs: List[Dict], ev_ist: datetime,
                          ev_type: str) -> Optional[float]:
        """
        Extract the relevant actual value from FRED observations.

        FOMC (daily series): find the observation on or within 5 days before the
        event date.  FOMC decisions at 2 pm ET = next IST calendar day, so the
        event's IST date may be one day ahead of the FRED observation date.

        CPI / NFP / PCE (monthly series): the event releases the *prior* month's
        data, so an event on 2026-02-12 → looks for the FRED observation dated
        2026-01-01.  For NFP the value shown is the month-over-month change.
        """
        if not obs:
            return None

        ev_date = ev_ist.date()

        if ev_type == "FOMC":
            ev_str = ev_date.strftime("%Y-%m-%d")
            result = None
            for o in obs:
                if o["date"] <= ev_str:
                    result = o
            if result:
                obs_date = datetime.strptime(result["date"], "%Y-%m-%d").date()
                if abs((ev_date - obs_date).days) <= 5:
                    return round(result["value"], 2)
            return None

        # Monthly series
        if ev_date.month == 1:
            ref_year, ref_month = ev_date.year - 1, 12
        else:
            ref_year, ref_month = ev_date.year, ev_date.month - 1
        ref_str = f"{ref_year:04d}-{ref_month:02d}-01"

        curr_obs: Optional[Dict] = None
        for o in obs:
            if o["date"] == ref_str:
                curr_obs = o
                break
        if curr_obs is None:
            for o in obs:
                if o["date"] <= ref_str:
                    curr_obs = o

        if curr_obs is None:
            return None

        if ev_type == "NFP":
            if ref_month > 1:
                prev_str = f"{ref_year:04d}-{ref_month - 1:02d}-01"
            else:
                prev_str = f"{ref_year - 1:04d}-12-01"
            prev_obs = next((o for o in obs if o["date"] == prev_str), None)
            if prev_obs:
                return round(curr_obs["value"] - prev_obs["value"], 0)
            return round(curr_obs["value"], 1)

        return round(curr_obs["value"], 2)

    # ── BTC price reaction from Binance public API ─────────────────────────

    async def _btc_reaction(self, event_ist: datetime) -> Dict:
        """BTC futures price at event, 1h after, 4h after. No auth needed."""
        try:
            import aiohttp
        except ImportError:
            return {}

        ev_ms    = int(event_ist.timestamp() * 1000)
        start_ms = ev_ms - 60 * 60 * 1000
        end_ms   = ev_ms + 5 * 60 * 60 * 1000

        url = (
            "https://fapi.binance.com/fapi/v1/klines"
            f"?symbol=BTCUSDT&interval=15m"
            f"&startTime={start_ms}&endTime={end_ms}&limit=40"
        )
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            ) as sess:
                async with sess.get(url) as resp:
                    if resp.status != 200:
                        return {}
                    candles = await resp.json()
        except Exception as ex:
            log.debug(f"Binance BTC reaction fetch failed: {ex}")
            return {}

        if not candles:
            return {}

        def _close_at(target_ms: int) -> Optional[float]:
            if not candles:
                return None
            closest = min(candles, key=lambda c: abs(int(c[0]) - target_ms))
            return float(closest[4])

        pre_price = _close_at(ev_ms - 15 * 60 * 1000)
        price_1h  = _close_at(ev_ms + 60 * 60 * 1000)
        price_4h  = _close_at(ev_ms + 4 * 60 * 60 * 1000)

        move_1h_pct  = None
        move_4h_pct  = None
        direction_1h = "—"

        if pre_price and price_1h:
            move_1h_pct  = round((price_1h - pre_price) / pre_price * 100, 2)
            direction_1h = ("BULLISH" if move_1h_pct > 0.3 else
                            "BEARISH" if move_1h_pct < -0.3 else "NEUTRAL")
        if pre_price and price_4h:
            move_4h_pct = round((price_4h - pre_price) / pre_price * 100, 2)

        return {
            "pre_price":    round(pre_price, 1) if pre_price else None,
            "price_1h":     round(price_1h,  1) if price_1h  else None,
            "price_4h":     round(price_4h,  1) if price_4h  else None,
            "move_1h_pct":  move_1h_pct,
            "move_4h_pct":  move_4h_pct,
            "direction_1h": direction_1h,
        }

    # ── AI: Groq → Ollama ─────────────────────────────────────────────────

    def _build_prompt(self, ev: Dict, consensus: Dict, pattern: Dict) -> str:
        avg_move  = pattern.get("avg_move_pct", 1.5)
        bull_rate = pattern.get("bullish_rate", 0.5)
        previous  = consensus.get("previous")
        unit      = consensus.get("unit", "")
        ev_ist    = ev.get("ist", "")
        if hasattr(ev_ist, "strftime"):
            ev_ist = ev_ist.strftime("%Y-%m-%d %H:%M IST")
        return (
            "You are a crypto market analyst. Analyze this upcoming macro event for BTC/USDT impact.\n\n"
            f"Event: {ev['name']}\n"
            f"Date/Time: {ev_ist}\n"
            f"Country: {ev['country']}\n"
            f"Impact: {ev['impact']}\n"
            f"Latest Actual Value: {previous if previous is not None else 'N/A'} {unit}\n"
            f"Historical Pattern: BTC avg moves {avg_move:.1f}% on this event type, "
            f"bullish {bull_rate*100:.0f}% of the time\n\n"
            "Provide a concise 3-sentence analysis:\n"
            "1. Expected market reaction (BULLISH/BEARISH/NEUTRAL) and the key reason\n"
            "2. What specific outcome would trigger the strongest BTC move\n"
            "3. Key risk to the base case\n"
            "Be specific, data-driven, no filler."
        )

    async def _ai_forecast(self, ev: Dict, consensus: Dict, pattern: Dict) -> str:
        """Try Groq, then Ollama. Returns '' if neither available."""
        prompt = self._build_prompt(ev, consensus, pattern)

        groq_key = getattr(cfg, "groq_api_key", "") or os.environ.get("GROQ_API_KEY", "")
        if groq_key:
            result = await self._groq_generate(prompt, groq_key)
            if result:
                return result

        ollama_url   = getattr(cfg, "ollama_url",   "http://localhost:11434")
        ollama_model = getattr(cfg, "ollama_model", "llama3.2")
        result = await self._ollama_generate(prompt, ollama_url, ollama_model)
        if result:
            return result

        return ""

    async def _groq_generate(self, prompt: str, api_key: str) -> str:
        try:
            import aiohttp
        except ImportError:
            return ""
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        }
        payload = {
            "model":       "llama-3.1-8b-instant",
            "messages":    [{"role": "user", "content": prompt}],
            "max_tokens":  300,
            "temperature": 0.3,
        }
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            ) as sess:
                async with sess.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers=headers, json=payload,
                ) as resp:
                    if resp.status != 200:
                        log.debug(f"Groq returned {resp.status}")
                        return ""
                    data = await resp.json()
                    return data["choices"][0]["message"]["content"].strip()
        except Exception as ex:
            log.debug(f"Groq generate failed: {ex}")
            return ""

    async def _ollama_generate(self, prompt: str, url: str, model: str) -> str:
        try:
            import aiohttp
        except ImportError:
            return ""
        payload = {
            "model":   model,
            "prompt":  prompt,
            "stream":  False,
            "options": {"num_predict": 300, "temperature": 0.3},
        }
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60)
            ) as sess:
                async with sess.post(f"{url}/api/generate", json=payload) as resp:
                    if resp.status != 200:
                        return ""
                    data = await resp.json()
                    return (data.get("response") or "").strip()
        except Exception as ex:
            log.debug(f"Ollama generate failed: {ex}")
            return ""


# Global singleton
event_research = EventResearch()
