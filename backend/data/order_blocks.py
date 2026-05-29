"""
Order Block Detector — Fresh A/B Order Blocks

Detects fresh demand (support) and supply (resistance) zones using pivot-based
displacement analysis with ATR scaling and a multi-factor quality score.

Mirrors the Pine Script "Fresh A/B Order Blocks [Custom]" indicator exactly:
  - Pivot detection (strict, ±pivotLength bars)
  - ATR-based displacement filter (minimum move to confirm a zone is "fresh")
  - Origin score: move size 42%, compactness 24%, volume 18%, close position 16%
  - Zone lifecycle: removed when price touches zone OR age exceeds maxZoneAge bars
  - Grade: A+ ≥86, A ≥74, B ≥58 (threshold), C ≥45, D <45

Only zones scoring ≥58 (grade B or better) are returned.
"""

import logging
from typing import Any, Dict, List, Optional

from backend.config import cfg

log = logging.getLogger("order_blocks")

_MIN_SCORE = 58.0


# ── Helpers ────────────────────────────────────────────────────────────────

def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _grade(score: float) -> str:
    if score >= 86: return "A+"
    if score >= 74: return "A"
    if score >= 58: return "B"
    if score >= 45: return "C"
    return "D"


def _calc_true_range(highs: List[float], lows: List[float], closes: List[float]) -> List[float]:
    n = len(highs)
    tr = [0.0] * n
    for i in range(n):
        hl = highs[i] - lows[i]
        if i == 0:
            tr[i] = hl
        else:
            tr[i] = max(hl, abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
    return tr


def _calc_atr(highs: List[float], lows: List[float], closes: List[float], length: int) -> List[float]:
    """Wilder's ATR — matches ta.atr() in Pine Script."""
    tr  = _calc_true_range(highs, lows, closes)
    n   = len(tr)
    atr = [0.0] * n
    if n < length:
        return atr
    atr[length - 1] = sum(tr[:length]) / length
    for i in range(length, n):
        atr[i] = (atr[i-1] * (length - 1) + tr[i]) / length
    return atr


def _calc_sma(series: List[float], length: int) -> List[float]:
    n   = len(series)
    out = [0.0] * n
    for i in range(length - 1, n):
        out[i] = sum(series[i - length + 1: i + 1]) / length
    return out


def _is_pivot_high(highs: List[float], pi: int, length: int) -> bool:
    """True if highs[pi] is strictly greater than all bars within ±length."""
    n = len(highs)
    if pi < length or pi + length >= n:
        return False
    peak = highs[pi]
    for j in range(1, length + 1):
        if highs[pi - j] >= peak or highs[pi + j] >= peak:
            return False
    return True


def _is_pivot_low(lows: List[float], pi: int, length: int) -> bool:
    """True if lows[pi] is strictly less than all bars within ±length."""
    n = len(lows)
    if pi < length or pi + length >= n:
        return False
    trough = lows[pi]
    for j in range(1, length + 1):
        if lows[pi - j] <= trough or lows[pi + j] <= trough:
            return False
    return True


def _origin_score(is_demand: bool, move_atr: float, height_atr: float,
                  vol_ratio: float, close_pos: float,
                  need_displacement: float, max_zone_height: float) -> float:
    """
    Quality score 0-100. Matches Pine Script f_origin_score().
    Weights: move 42%, compactness 24%, volume 18%, close-position 16%.
    """
    move_score    = _clamp((move_atr - need_displacement) / max(need_displacement * 1.6, 1e-8) * 100, 0, 100)
    compact_score = _clamp((1.0 - height_atr / max(max_zone_height, 1e-8)) * 100, 0, 100)
    vol_score     = _clamp((vol_ratio - 0.7) / 1.2 * 100, 0, 100) if vol_ratio > 0 else 60.0
    if is_demand:
        close_score = _clamp((close_pos - 0.3) / 0.5 * 100, 0, 100)
    else:
        close_score = _clamp((0.7 - close_pos) / 0.5 * 100, 0, 100)
    return move_score * 0.42 + compact_score * 0.24 + vol_score * 0.18 + close_score * 0.16


# ── Main detector ──────────────────────────────────────────────────────────

def detect(candles: List[Dict], params: Optional[Dict] = None) -> Dict:
    """
    Detect active demand/supply order block zones from OHLCV candles.

    candles : list of {open, high, low, close, volume, ts}  (ts = epoch ms)
    params  : optional dict to override config keys (ob_pivot_length etc.)
    Returns : {demand, supply, current_price, stats, candle_count}
    """
    if not candles or len(candles) < 10:
        return {"demand": [], "supply": [], "current_price": 0,
                "stats": {}, "candle_count": 0}

    # ── Resolve config ────────────────────────────────────────────────────
    p = params or {}

    def _cfg(key: str, default: Any) -> Any:
        return p.get(key, getattr(cfg, key, default))

    pivot_len      = int(_cfg("ob_pivot_length",     5))
    max_zone_age   = int(_cfg("ob_max_zone_age",      200))
    atr_len        = int(_cfg("ob_atr_length",        14))
    vol_len        = int(_cfg("ob_volume_length",     34))
    disp_atr       = float(_cfg("ob_displacement_atr",  0.62))
    max_thick_atr  = float(_cfg("ob_max_thickness_atr", 1.65))
    sensitivity    = str(_cfg("ob_sensitivity",       "Balanced"))

    sens_mult = {"Conservative": 1.24, "Aggressive": 0.82}.get(sensitivity, 1.0)
    need_displacement = disp_atr * sens_mult

    # ── Raw series ────────────────────────────────────────────────────────
    n       = len(candles)
    opens   = [float(c["open"])          for c in candles]
    highs   = [float(c["high"])          for c in candles]
    lows    = [float(c["low"])           for c in candles]
    closes  = [float(c["close"])         for c in candles]
    volumes = [float(c.get("volume", 0)) for c in candles]
    times   = [int(c["ts"])              for c in candles]  # epoch ms

    atr_series = _calc_atr(highs, lows, closes, atr_len)
    vol_sma    = _calc_sma(volumes, vol_len)

    # ── Bar-by-bar simulation ─────────────────────────────────────────────
    # Replicates Pine Script execution order:
    #   (1) lifecycle check with current bar's high/low  →  remove touched/expired
    #   (2) if pivot at i-pl, try to create a new zone
    pl = pivot_len
    active: List[Dict] = []

    for i in range(n):
        atr            = atr_series[i]
        safe_atr       = max(atr, 1e-8)
        max_zone_height = safe_atr * max_thick_atr
        vol_base       = vol_sma[i] if vol_sma[i] > 0 else 1e-8

        # (1) Lifecycle: remove expired or close-broken zones
        # A zone is consumed only when price CLOSES through it (wick touches don't count).
        # Demand: consumed when close < zone bottom (closed below the low of origin bar)
        # Supply: consumed when close > zone top   (closed above the high of origin bar)
        surviving = []
        for z in active:
            if i - z["_born"] > max_zone_age:
                continue
            if z["is_demand"] and closes[i] < z["bottom"]:
                continue  # demand zone invalidated: close broke below bottom
            if not z["is_demand"] and closes[i] > z["top"]:
                continue  # supply zone invalidated: close broke above top
            surviving.append(z)
        active = surviving

        # (2) Pivot detection — reference bar is pl bars back
        pi = i - pl
        if pi < pl:
            continue

        # ── Demand zone (pivot low) ───────────────────────────────────────
        if _is_pivot_low(lows, pi, pl):
            highest_close = max(closes[pi + 1: i + 1]) if i > pi else closes[i]
            move_atr = (highest_close - lows[pi]) / safe_atr

            if move_atr >= need_displacement:
                raw_bottom = lows[pi]
                raw_top    = min(min(opens[pi], closes[pi]), raw_bottom + max_zone_height)
                final_top  = max(raw_top, raw_bottom + 1e-8)

                src_vol   = volumes[pi] / vol_base
                src_range = max(highs[pi] - lows[pi], 1e-8)
                src_cp    = (closes[pi] - lows[pi]) / src_range
                h_atr     = (final_top - raw_bottom) / safe_atr

                score = _origin_score(True, move_atr, h_atr, src_vol, src_cp,
                                      need_displacement, max_zone_height)
                if score >= _MIN_SCORE:
                    active.append({
                        "is_demand": True,
                        "top":       round(final_top, 1),
                        "bottom":    round(raw_bottom, 1),
                        "_born":     pi,
                        "born_ts":   times[pi],
                        "score":     round(score, 1),
                        "grade":     _grade(score),
                    })

        # ── Supply zone (pivot high) ──────────────────────────────────────
        if _is_pivot_high(highs, pi, pl):
            lowest_close = min(closes[pi + 1: i + 1]) if i > pi else closes[i]
            move_atr = (highs[pi] - lowest_close) / safe_atr

            if move_atr >= need_displacement:
                raw_top      = highs[pi]
                raw_bottom   = max(max(opens[pi], closes[pi]), raw_top - max_zone_height)
                final_bottom = min(raw_bottom, raw_top - 1e-8)

                src_vol   = volumes[pi] / vol_base
                src_range = max(highs[pi] - lows[pi], 1e-8)
                src_cp    = (closes[pi] - lows[pi]) / src_range
                h_atr     = (raw_top - final_bottom) / safe_atr

                score = _origin_score(False, move_atr, h_atr, src_vol, src_cp,
                                      need_displacement, max_zone_height)
                if score >= _MIN_SCORE:
                    active.append({
                        "is_demand": False,
                        "top":       round(raw_top, 1),
                        "bottom":    round(final_bottom, 1),
                        "_born":     pi,
                        "born_ts":   times[pi],
                        "score":     round(score, 1),
                        "grade":     _grade(score),
                    })

    # ── Build output ──────────────────────────────────────────────────────
    current_price = closes[-1] if closes else 0.0

    out_zones = []
    for z in active:
        mid       = (z["top"] + z["bottom"]) / 2
        dist_pct  = round(abs(current_price - mid) / current_price * 100, 2) if current_price else 0
        age_bars  = n - 1 - z["_born"]
        out_zones.append({
            "is_demand":    z["is_demand"],
            "top":          z["top"],
            "bottom":       z["bottom"],
            "mid":          round(mid, 1),
            "born_ts":      z["born_ts"],
            "score":        z["score"],
            "grade":        z["grade"],
            "age_bars":     age_bars,
            "distance_pct": dist_pct,
            "above_price":  mid > current_price,
        })

    demand = sorted([z for z in out_zones if     z["is_demand"]], key=lambda z: z["distance_pct"])
    supply = sorted([z for z in out_zones if not z["is_demand"]], key=lambda z: z["distance_pct"])

    nd = demand[0] if demand else None
    ns = supply[0] if supply else None

    return {
        "demand":        demand,
        "supply":        supply,
        "current_price": round(current_price, 1),
        "candle_count":  n,
        "stats": {
            "demand_count":          len(demand),
            "supply_count":          len(supply),
            "a_plus":                sum(1 for z in out_zones if z["grade"] == "A+"),
            "a_grade":               sum(1 for z in out_zones if z["grade"] == "A"),
            "b_grade":               sum(1 for z in out_zones if z["grade"] == "B"),
            "nearest_demand_top":    nd["top"]    if nd else None,
            "nearest_demand_bottom": nd["bottom"] if nd else None,
            "nearest_supply_top":    ns["top"]    if ns else None,
            "nearest_supply_bottom": ns["bottom"] if ns else None,
        },
    }
