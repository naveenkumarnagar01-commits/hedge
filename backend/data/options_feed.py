"""
BTC Options - WebSocket Depth Feed (100ms updates)
====================================================
Architecture:
  • WebSocket @depth10@100ms  -> Bid/Ask prices + quantities (REAL-TIME, 100ms)
  • REST /eapi/v1/mark        -> Mark Price + IV (every 5 seconds, lightweight)
  • No rate-limit issues      -> Depth stream is FREE (no weight cost)

Subscribes to:
  btc-YYMMDD-STRIKE-c@depth10@100ms   (ATM CALL)
  btc-YYMMDD-STRIKE-p@depth10@100ms   (ATM PUT)

When ATM strike changes (spot moves by >500), resubscribes live via WS
SUBSCRIBE/UNSUBSCRIBE protocol (no reconnect needed).

WebSocket URL: wss://fstream.binance.com/stream?streams=<s1>/<s2>
"""

import asyncio
import json
import logging
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

from backend.message_bus import bus, OPTIONS_CHAIN, FEED_STATUS

log = logging.getLogger("options_feed")

_EAPI     = "https://eapi.binance.com"
_WS_BASE  = "wss://fstream.binance.com/stream"
_IST      = timezone(timedelta(hours=5, minutes=30))
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="opt_mark")

# ── State ─────────────────────────────────────────────────────────────────────
_chain:        Dict[str, dict] = {}       # sym -> ticker dict
_all_strikes:  List[float]     = []
_sym_meta:     Dict[str, dict] = {}
_expiry_label:      str   = "-"
_expiry_ms:         int   = 0
_expiry_6d:         str   = ""
_expiry_hours_left: float = 0.0
_is_same_day:       bool  = False
_running:           bool  = False

# Current symbols in window being tracked via WS
_window_syms: List[str] = []
_itm_call_sym: str = ""
_itm_put_sym:  str = ""
_ws_handle = None  # must be initialized at module level to avoid NameError in stop()

# Cached mark prices (updated by REST every 5s)
_mark_cache: Dict[str, Tuple[float, float]] = {}  # sym -> (mark, iv)

# Configuration
_WINDOW_SIZE = 10  # strikes above/below spot to track


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_spot() -> float:
    from backend.data.spot_feed import get_state
    st = get_state()
    p  = st.get("last_price", 0.0) or st.get("bid", 0.0)
    return p or (st.get("bid", 0) + st.get("ask", 0)) / 2


def _intrinsic(strike: float, spot: float, side: str) -> float:
    return max(spot - strike, 0.0) if side == "C" else max(strike - spot, 0.0)


def _safe(v, d: float = 0.0) -> float:
    try:
        return float(v) if v not in (None, "", "nan", "NaN") else d
    except (TypeError, ValueError):
        return d


def _sym_name(strike: float, side: str) -> str:
    """Uppercase symbol e.g. BTC-260515-80000-C"""
    return f"BTC-{_expiry_6d}-{int(strike)}-{side}"


def _stream_name(sym: str) -> str:
    """Lowercase stream name e.g. btc-260515-80000-c@depth10@100ms"""
    return f"{sym.lower()}@depth10@100ms"


# ── HTTP REST helpers ─────────────────────────────────────────────────────────

def _http_get_sync(url: str) -> Tuple[str, int]:
    req = urllib.request.Request(url, headers={"User-Agent": "HedgeTrader/1.0"})
    with urllib.request.urlopen(req, timeout=10) as r:
        body = r.read().decode()
        wt = 0
        for h, v in r.headers.items():
            if "USED-WEIGHT" in h.upper():
                try:
                    wt = int(v); break
                except:
                    pass
        return body, wt


# ── Expiry Init (REST, runs once) ─────────────────────────────────────────────

async def _fetch_expiry() -> bool:
    global _expiry_label, _expiry_ms, _expiry_6d, _expiry_hours_left, _is_same_day, _sym_meta, _all_strikes

    url = f"{_EAPI}/eapi/v1/exchangeInfo"
    try:
        loop = asyncio.get_event_loop()
        raw, _ = await loop.run_in_executor(_executor, lambda: _http_get_sync(url))
        data   = json.loads(raw)
    except Exception as e:
        log.error(f"exchangeInfo failed: {e}")
        return False

    now_ms    = int(time.time() * 1000)
    today_ist = datetime.now(_IST).date()

    all_btc = [
        s for s in data.get("optionSymbols", [])
        if s.get("underlying") == "BTCUSDT"
        and s.get("status") == "TRADING"
        and s.get("expiryDate", 0) > now_ms
    ]
    if not all_btc:
        log.error("No active BTC options found.")
        return False

    by_exp: Dict[int, List] = {}
    for s in all_btc:
        by_exp.setdefault(s["expiryDate"], []).append(s)
    sorted_exp = sorted(by_exp.keys())

    same_day_ts = next(
        (ts for ts in sorted_exp
         if datetime.fromtimestamp(ts/1000, tz=_IST).date() == today_ist), None)
    target_ts = same_day_ts if same_day_ts else sorted_exp[0]
    raw_syms  = by_exp[target_ts]

    exp_ist   = datetime.fromtimestamp(target_ts / 1000, tz=_IST)
    is_today  = same_day_ts is not None
    exp_str   = exp_ist.strftime("%Y-%m-%d %H:%M IST")
    exp_6d    = exp_ist.strftime("%y%m%d")
    days_away = (exp_ist.date() - today_ist).days

    label = (f"TODAY {exp_str} ({len(raw_syms)} strikes)"
             if is_today else
             f"NEAREST {exp_str} ({days_away}d away, {len(raw_syms)} strikes)")

    _expiry_label = label
    _expiry_ms    = target_ts
    _expiry_6d    = exp_6d
    _is_same_day  = is_today
    _expiry_hours_left = max((target_ts - int(time.time()*1000)) / 3_600_000, 0)

    strikes = sorted(set(float(s["strikePrice"]) for s in raw_syms))
    _all_strikes = strikes

    meta_map = {}
    for s in raw_syms:
        side_c = "C" if s.get("side", "CALL") == "CALL" else "P"
        sym    = s["symbol"]
        meta_map[sym] = {
            "symbol":  sym,
            "strike":  float(s["strikePrice"]),
            "side":    side_c,
            "expiry":  exp_str,
            "min_qty": float(s.get("minQty", 0.01)),
        }
    _sym_meta = meta_map
    
    # Initialize _chain with all symbols (placeholders)
    global _chain
    _chain = {}
    for sym, m in meta_map.items():
        _chain[sym] = {
            "symbol": sym, "strike": m["strike"], "side": m["side"],
            "expiry": m["expiry"], "bid": 0, "ask": 0, "mark": 0, "iv": 0,
            "intrinsic": 0, "source": "REST", "ts": time.time()
        }
    
    log.info(f"Options expiry: {label} | {len(strikes)} strikes initialized")
    return True


# ── ATM selector ──────────────────────────────────────────────────────────────

def _get_itm_syms(spot: float) -> Tuple[Optional[str], Optional[str]]:
    """Returns (nearest_itm_call_sym, nearest_itm_put_sym) for executor targets."""
    if not _all_strikes: return None, None
    below = [s for s in _all_strikes if s < spot]
    above = [s for s in _all_strikes if s > spot]
    if not below or not above: return None, None
    c_strike, p_strike = max(below), min(above)
    call, put = _sym_name(c_strike, "C"), _sym_name(p_strike, "P")
    return call if call in _sym_meta else None, put if put in _sym_meta else None


def _get_window_syms(spot: float) -> List[str]:
    """Returns list of symbols for +/- _WINDOW_SIZE strikes around spot."""
    if not _all_strikes: return []
    
    # Find index of ATM strike
    idx = 0
    for i, s in enumerate(_all_strikes):
        if s >= spot:
            idx = i
            break
    
    start = max(0, idx - _WINDOW_SIZE)
    end   = min(len(_all_strikes), idx + _WINDOW_SIZE + 1)
    window = _all_strikes[start:end]
    
    syms = []
    for s in window:
        c = _sym_name(s, "C")
        p = _sym_name(s, "P")
        if c in _sym_meta: syms.append(c)
        if p in _sym_meta: syms.append(p)
    return syms


# ── Mark Price REST loop (every 5 seconds) ────────────────────────────────────

async def _mark_loop():
    """Fetch mark price + IV for ALL symbols for the current expiry every 5s via REST."""
    global _expiry_hours_left
    while _running:
        await asyncio.sleep(5)
        if _expiry_ms:
            _expiry_hours_left = max((_expiry_ms - int(time.time()*1000)) / 3_600_000, 0)
        
        # Bulk mark price fetch
        url = f"{_EAPI}/eapi/v1/mark"
        try:
            loop = asyncio.get_event_loop()
            raw, _ = await loop.run_in_executor(_executor, lambda: _http_get_sync(url))
            data = json.loads(raw)
            if isinstance(data, list):
                new_cache = {}
                for m in data:
                    sym = m.get("symbol")
                    if sym in _sym_meta:
                        new_cache[sym] = (
                            _safe(m.get("markPrice")),
                            _safe(m.get("markIV")),
                        )
                _mark_cache.update(new_cache)
                log.debug(f"Bulk mark update: {len(new_cache)} symbols cached")
        except Exception as e:
            log.warning(f"Bulk mark fetch failed: {e}")


# ── WebSocket depth stream ────────────────────────────────────────────────────

def _build_depth_msg(data: dict, sym_upper: str) -> None:
    """Process a depth update from WS and update _chain."""
    bids = data.get("b", [])
    asks = data.get("a", [])

    if not bids and not asks:
        return

    meta = _sym_meta.get(sym_upper, {})
    if not meta:
        return

    strike = meta["strike"]
    side   = meta["side"]
    spot   = _get_spot()

    bid     = _safe(bids[0][0]) if bids else 0.0
    bid_qty = _safe(bids[0][1]) if bids else 0.0
    ask     = _safe(asks[0][0]) if asks else 0.0
    ask_qty = _safe(asks[0][1]) if asks else 0.0

    mark, iv = _mark_cache.get(sym_upper, (0.0, 0.0))
    intr = _intrinsic(strike, spot, side)
    prem = ask if ask > 0 else mark
    tv   = max(prem - intr, 0.0)

    _chain[sym_upper] = {
        "symbol":       sym_upper,
        "strike":       strike,
        "side":         side,
        "expiry":       meta.get("expiry", ""),
        "bid":          round(bid, 2),
        "bid_qty":      round(bid_qty, 3) if bid_qty > 0 else None,
        "ask":          round(ask, 2),
        "ask_qty":      round(ask_qty, 3) if ask_qty > 0 else None,
        "mark":         round(mark, 2),
        "premium":      round(prem, 2),
        "iv":           round(iv, 6),
        "intrinsic":    round(intr, 2),
        "time_value":   round(tv, 2),
        "spot":         round(spot, 2),
        "is_same_day":  _is_same_day,
        "expiry_label": _expiry_label,
        "source":       "WS@100ms",
        "ts":           time.time(),
    }


async def _ws_loop():
    """
    WebSocket loop tracking a window of +/- strikes around spot.
    """
    global _itm_call_sym, _itm_put_sym, _window_syms, _ws_handle

    import websockets
    _req_id = 1

    def _build_url(syms: List[str]) -> str:
        streams = "/".join([_stream_name(s) for s in syms])
        return f"{_WS_BASE}?streams={streams}"

    while _running and not _all_strikes: await asyncio.sleep(1)
    
    spot = _get_spot()
    while _running and not spot:
        await asyncio.sleep(0.5)
        spot = _get_spot()

    current_syms = _get_window_syms(spot)
    if not current_syms:
        log.error("No symbols found for window — options WS cannot start.")
        return

    _window_syms = current_syms
    _itm_call_sym, _itm_put_sym = _get_itm_syms(spot)

    backoff = 2
    while _running:
        if _expiry_ms and time.time() * 1000 > _expiry_ms:
            log.info("Options expiry passed! Re-fetching target expiry...")
            ok = await _fetch_expiry()
            if ok:
                spot = _get_spot()
                _window_syms = _get_window_syms(spot)
                _itm_call_sym, _itm_put_sym = _get_itm_syms(spot)
            else:
                await asyncio.sleep(5)
                continue

        url = _build_url(_window_syms)
        log.info(f"Options WS: Connecting for {len(_window_syms)} streams")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
                _ws_handle = ws
                backoff = 2
                log.info(f"Options WS: Connected! Tracking window of {len(_window_syms)} strikes")
                await bus.publish(FEED_STATUS, {"feed":"options_feed","status":"ok","source":"WS@100ms"}, source="options_feed")

                async for raw in ws:
                    if not _running: break
                    if _expiry_ms and time.time() * 1000 > _expiry_ms:
                        log.info("Expiry time reached during active connection. Closing WS to refresh.")
                        break # break inner loop to trigger reconnect and re-fetch

                    try: msg = json.loads(raw)
                    except: continue

                    # Check for window shift (spot moves)
                    new_spot = _get_spot()
                    new_syms = _get_window_syms(new_spot)
                    
                    if set(new_syms) != set(_window_syms):
                        old_streams = [_stream_name(s) for s in _window_syms]
                        new_streams = [_stream_name(s) for s in new_syms]
                        
                        to_unsub = [s for s in old_streams if s not in new_streams]
                        to_sub   = [s for s in new_streams if s not in old_streams]
                        
                        if to_unsub:
                            await ws.send(json.dumps({"method":"UNSUBSCRIBE","params":to_unsub,"id":_req_id}))
                            _req_id += 1
                        if to_sub:
                            await ws.send(json.dumps({"method":"SUBSCRIBE","params":to_sub,"id":_req_id}))
                            _req_id += 1
                        
                        _window_syms = new_syms
                        log.info(f"Options WS: Window shifted. Subscribed to {len(to_sub)} new, unsubscribed {len(to_unsub)}")

                    _itm_call_sym, _itm_put_sym = _get_itm_syms(new_spot)

                    stream = msg.get("stream", "")
                    data   = msg.get("data", {})
                    if "@depth10" in stream:
                        sym_upper = stream.split("@")[0].upper()
                        _build_depth_msg(data, sym_upper)

        except Exception as e:
            _ws_handle = None
            if not _running:
                break
            log.error(f"Options WS error: {e} — reconnecting in {backoff}s")
            await bus.publish(FEED_STATUS, {
                "feed": "options_feed", "status": "stale", "source": "WS"
            }, source="options_feed")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ── Broadcaster ───────────────────────────────────────────────────────────────

async def _broadcaster():
    """Broadcast the chain every 200ms (5x/second) over the message bus."""
    while _running:
        await asyncio.sleep(0.2)
        if _chain:
            spot = _get_spot()
            hours_left = max((_expiry_ms - int(time.time()*1000)) / 3_600_000, 0) if _expiry_ms else 0
            
            # Refresh intrinsic/spot for ALL symbols in chain
            for sym, o in _chain.items():
                o["spot"] = round(spot, 2)
                o["intrinsic"] = round(_intrinsic(o["strike"], spot, o["side"]), 2)
                # If we have a mark price, we can also update time_value
                if o.get("mark"):
                    o["time_value"] = round(max(o["mark"] - o["intrinsic"], 0.0), 2)

            # Sort by strike (asc) then side (C then P)
            chain_list = sorted(
                _chain.values(),
                key=lambda x: (x["strike"], x["side"])
            )
            
            await bus.publish(OPTIONS_CHAIN, {
                "expiry":       _expiry_label,
                "expiry_ms":    _expiry_ms,
                "hours_left":   round(hours_left, 2),
                "is_same_day":  _is_same_day,
                "source":       "WS+REST",
                "chain":        chain_list,
                "target_roles": {
                    "itm_call": _itm_call_sym,
                    "itm_put":  _itm_put_sym,
                },
                "ts": time.time(),
            }, source="options_feed")


# ── Start / Stop ──────────────────────────────────────────────────────────────

async def start():
    global _running
    _running = True
    ok = await _fetch_expiry()
    if not ok:
        log.error("Options feed: expiry fetch failed — retrying in background")
    asyncio.create_task(_ws_loop())
    asyncio.create_task(_mark_loop())
    asyncio.create_task(_broadcaster())
    log.info(
        f"Options feed: WS@100ms depth | REST mark every 5s | "
        f"NEAREST ITM CALL + NEAREST ITM PUT | {_expiry_label}"
    )


async def stop():
    global _running
    _running = False
    if _ws_handle:
        try:
            await _ws_handle.close()
        except:
            pass
    _executor.shutdown(wait=False)


# ── Public API ────────────────────────────────────────────────────────────────

def get_chain() -> Dict[str, dict]:
    return dict(_chain)


def get_feed_info() -> dict:
    return {
        "expiry":        _expiry_label,
        "expiry_ms":     _expiry_ms,
        "hours_left":    _expiry_hours_left,
        "is_same_day":   _is_same_day,
        "source":        "WS@100ms",
        "chain_count":   len(_chain),
        "target_roles":  {
            "itm_call": _itm_call_sym,
            "itm_put":  _itm_put_sym,
        },
    }


def _get_eligible_by_side(side: str, min_intrinsic: float, max_premium: float,
                           ask_mark_ratio: float, max_tv: float = 999999.0) -> List[dict]:
    return sorted(
        [
            o for o in _chain.values()
            if o["side"] == side
            and o["intrinsic"] >= min_intrinsic
            and o["ask"] > 0
            and o["ask"] <= max_premium
            and o["time_value"] <= max_tv
            and (not o["mark"] or o["ask"] <= o["mark"] * (1 + ask_mark_ratio))
        ],
        key=lambda x: x["strike"]
    )


def get_calls_for_strike(min_intrinsic: float = 0, max_premium: float = 9999,
                        ask_mark_ratio: float = 0.10, max_tv: float = 9999) -> List[dict]:
    return _get_eligible_by_side("C", min_intrinsic, max_premium, ask_mark_ratio, max_tv)


def get_puts_for_strike(min_intrinsic: float = 0, max_premium: float = 9999,
                       ask_mark_ratio: float = 0.10, max_tv: float = 9999) -> List[dict]:
    return _get_eligible_by_side("P", min_intrinsic, max_premium, ask_mark_ratio, max_tv)


def get_nearest_itm_put(futures_price: float) -> Optional[dict]:
    """
    Nearest ITM put = smallest (strike - futures_price) where strike > futures_price.
    NOTE: does NOT require ask > 0 — returns the option even without depth data so
    the caller's checklist can show prem_ok=False rather than pending indefinitely.
    Intrinsic recomputed here using futures_price (more accurate than spot-based chain).
    """
    itm_puts = [
        o for o in _chain.values()
        if o["side"] == "P" and o["strike"] > futures_price
    ]
    if not itm_puts:
        return None
    nearest = min(itm_puts, key=lambda x: x["strike"])
    nearest = dict(nearest)  # always copy — we mutate below
    # Recompute intrinsic & TV using futures mark price (not spot)
    intr = max(nearest["strike"] - futures_price, 0.0)
    ask  = nearest.get("ask") or 0.0
    mark = nearest.get("mark") or 0.0
    tv   = max((ask if ask > 0 else mark) - intr, 0.0)
    nearest["intrinsic"]   = round(intr, 2)
    nearest["time_value"]  = round(tv, 2)
    if mark > 0:
        nearest["spread_pct"] = round(abs(ask - mark) / mark * 100, 2)
    return nearest


def get_nearest_itm_call(futures_price: float) -> Optional[dict]:
    """
    Nearest ITM call = smallest (futures_price - strike) where strike < futures_price.
    Does NOT require ask > 0 — see get_nearest_itm_put for rationale.
    """
    itm_calls = [
        o for o in _chain.values()
        if o["side"] == "C" and o["strike"] < futures_price
    ]
    if not itm_calls:
        return None
    nearest = max(itm_calls, key=lambda x: x["strike"])
    nearest = dict(nearest)
    intr = max(futures_price - nearest["strike"], 0.0)
    ask  = nearest.get("ask") or 0.0
    mark = nearest.get("mark") or 0.0
    tv   = max((ask if ask > 0 else mark) - intr, 0.0)
    nearest["intrinsic"]   = round(intr, 2)
    nearest["time_value"]  = round(tv, 2)
    if mark > 0:
        nearest["spread_pct"] = round(abs(ask - mark) / mark * 100, 2)
    return nearest
