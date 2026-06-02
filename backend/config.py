"""
Dynamic configuration — all trading parameters live here.
All agents import `cfg` directly. Updates via /api/config propagate instantly.
"""

import os
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, Any

log = logging.getLogger("config")


@dataclass
class Config:
    # ── Exchange credentials ───────────────────────────────────────────────
    binance_api_key:    str = field(default_factory=lambda: os.environ.get("BINANCE_API_KEY", ""))
    binance_api_secret: str = field(default_factory=lambda: os.environ.get("BINANCE_API_SECRET", ""))

    # ── BTC Technicals chart (display only, no trading dependency) ─────────
    chart_candle_count:    int   = 500
    chart_tf_minutes:      int   = 5

    session_expiry_h: int = 13
    session_expiry_m: int = 30

    # ── Bullish Trader — full self-contained settings ──────────────────────
    # Trading window (Mon–Fri only; weekends auto-skipped)
    bull_skip_weekends:  bool = True
    bull_blackout_dates: str  = ""
    bull_trade_start_h:          int   = 4
    bull_trade_start_m:          int   = 0
    bull_trade_end_h:            int   = 18
    bull_trade_end_m:            int   = 30
    bull_force_close_h:          int   = 18
    bull_force_close_m:          int   = 30

    # Self-contained S/R analysis (runs at window open each day)
    bull_analysis_candles:       int   = 30       # N candles to analyse
    bull_analysis_tf_minutes:    int   = 15       # timeframe per candle (minutes)
    bull_dominance_threshold:    float = 0.80     # min fraction of touches that must be LOWs
    bull_touch_tolerance:        float = 30.0     # ±pts — touches within this range count
    bull_min_touches:            int   = 3        # minimum touches to qualify as valid level

    # Entry conditions (all must pass simultaneously)
    bull_max_premium:            float = 320.0    # max nearest ITM PUT ask (USDT)
    bull_max_time_value:         float = 220.0    # max (ask − intrinsic)
    bull_price_diff_percent:     float = 5.0      # max |ask−mark|/mark × 100 %
    bull_max_distance_from_line: float = 100.0    # ±pts from locked support line

    # Position sizing
    bull_contract_qty:           float = 1.0      # BTC per trade

    # Profit management
    bull_partial_profit_ratio:   float = 1.10     # trigger 50% futures sell at premium × ratio
    bull_session_pnl_target:     float = 600.0    # total futures PnL target for session (USDT)
    bull_sl_points:              float = 500.0    # stop loss: close if price drops this many pts from entry (0 = disabled)

    # Legacy alias kept so old DB configs restore without KeyError
    bull_n_hours:                float = 2.0
    bull_n_points:               float = 150.0
    bull_full_close_target:      float = 800.0    # fallback if session_pnl_target not set

    # ── Bearish Trader — full self-contained settings ──────────────────────
    bear_skip_weekends:  bool = True
    bear_blackout_dates: str  = ""
    bear_trade_start_h:          int   = 4
    bear_trade_start_m:          int   = 0
    bear_trade_end_h:            int   = 18
    bear_trade_end_m:            int   = 30
    bear_force_close_h:          int   = 18
    bear_force_close_m:          int   = 30

    bear_analysis_candles:       int   = 30
    bear_analysis_tf_minutes:    int   = 15
    bear_dominance_threshold:    float = 0.80
    bear_touch_tolerance:        float = 30.0
    bear_min_touches:            int   = 3

    bear_max_premium:            float = 320.0
    bear_max_time_value:         float = 220.0
    bear_price_diff_percent:     float = 5.0
    bear_max_distance_from_line: float = 100.0

    bear_contract_qty:           float = 1.0

    bear_partial_profit_ratio:   float = 1.10
    bear_session_pnl_target:     float = 600.0
    bear_sl_points:              float = 500.0    # stop loss: close if price rises this many pts from entry (0 = disabled)

    bear_n_hours:                float = 2.0
    bear_n_points:               float = 150.0
    bear_full_close_target:      float = 800.0

    # ── Order Block Detector ──────────────────────────────────────────────
    ob_pivot_length:     int   = 5       # pivot window, 2-20 bars each side
    ob_max_zone_age:     int   = 200     # bars before zone expires (30-500)
    ob_atr_length:       int   = 14      # ATR calculation period
    ob_volume_length:    int   = 34      # volume SMA baseline period
    ob_displacement_atr: float = 0.62   # min move in ATR units to qualify
    ob_max_thickness_atr: float = 1.65  # max zone height in ATR units
    ob_sensitivity:      str   = "Balanced"  # Conservative / Balanced / Aggressive
    ob_candle_count:     int   = 500     # candles to analyse
    ob_tf:               str   = "15m"   # timeframe for analysis (traders + OB tab default)
    bull_ob_tf:          str   = ""      # per-trader override; empty = use global ob_tf
    bear_ob_tf:          str   = ""
    vol_ob_tf:           str   = ""

    # ── Volatile Event Trader ──────────────────────────────────────────────
    vol_skip_weekends:   bool = False
    vol_blackout_dates:  str  = ""
    vol_trade_start_h:           int   = 4
    vol_trade_start_m:           int   = 0
    vol_trade_end_h:             int   = 18
    vol_trade_end_m:             int   = 30
    vol_force_close_h:           int   = 18
    vol_force_close_m:           int   = 30
    vol_contract_qty:            float = 1.0
    vol_session_pnl_target:      float = 800.0
    vol_max_premium:             float = 500.0
    vol_max_time_value:          float = 400.0
    vol_price_diff_percent:      float = 8.0
    vol_max_distance_from_line:  float = 1000.0  # large: event entry is near-market
    vol_partial_profit_ratio:    float = 1.10
    vol_full_close_target:       float = 800.0

    # Event-specific parameters (legacy — kept for backward compat)
    vol_pre_event_minutes:       int   = 30
    vol_direction_confirm_sec:   int   = 60
    vol_min_move_points:         float = 150.0
    vol_post_event_window_min:   int   = 5

    # New straddle strategy parameters
    vol_combined_premium_max:    float = 800.0   # max (put_ask + call_ask) to enter
    vol_tp_multiplier:           float = 1.10    # TP = combined_entry × multiplier
    vol_min_ask_qty:             float = 1.0     # min ask_qty on each leg for instant fill
    vol_sl_pct:                  float = 0.15    # cut losing leg when mark drops to X% of entry (0=disabled)
    vol_close_other_on_tp:       bool  = True    # close the other leg at market when one leg hits TP
    vol_strike_gap:              float = 500.0   # required PUT_strike − CALL_strike distance (pts)
    vol_strike_gap_tolerance:    float = 50.0    # ± tolerance on strike gap (pts)
    vol_window_close_h:          int   = 6       # search window closes at this hour IST (next day)
    vol_window_close_m:          int   = 0       # search window closes at this minute IST

    # ── Event Research API keys (all free, no credit card) ───────────────
    # FRED (St. Louis Fed) — free key at fred.stlouisfed.org/docs/api/api_key.html
    fred_api_key:                str   = ""
    # BLS (Bureau of Labor Statistics) — free key at www.bls.gov/developers/
    bls_api_key:                 str   = ""
    # Groq — free LLM inference (14k req/day) at console.groq.com
    groq_api_key:                str   = ""
    # Ollama — local LLM, zero cost (install: ollama.ai)
    ollama_url:                  str   = "http://localhost:11434"
    ollama_model:                str   = "llama3.2"
    # Statistics of the World API — PBOC (China) + ECB (EU) actual data
    # No auth needed for basic 100 req/day; free signup at statisticsoftheworld.com for 1000/day
    stat_world_api_key:          str   = ""
    # Legacy FMP key (kept for backward compat, no longer used)
    fmp_api_key:                 str   = ""

    # ── Risk / paper ───────────────────────────────────────────────────────
    q_max_btc:             float = 1.0
    max_option_spend:      float = 400.0
    safe_mode_timeout_sec: int   = 5
    ws_reconnect_max_sec:  int   = 30
    latency_warn_ms:       int   = 500
    fill_timeout_sec:      int   = 5

    # ── Hedge verification timeout ─────────────────────────────────────────
    verify_hedge_timeout_sec: int = 120

    # ── Per-trader virtual balances (each trader independent $100k) ────────
    virtual_balance_usdt:  float = 100_000.0
    min_paper_balance:     float = 1_000.0

    # ── Legacy / backward-compat ────────────────────────────────────────────
    candle_count:         int   = 300
    candle_tf_minutes:    int   = 5
    delta_touch:          float = 30.0
    min_touches:          int   = 3
    dominance_ratio_high: float = 0.75
    dominance_ratio_low:  float = 0.75
    analyst_h:            int   = 4
    analyst_m:            int   = 0
    options_hold_until_h: int   = 11
    options_hold_until_m: int   = 0
    squareoff_start_h:    int   = 11
    squareoff_start_m:    int   = 45
    squareoff_end_h:      int   = 12
    squareoff_end_m:      int   = 0
    window_start_h:       int   = 9
    window_start_m:       int   = 30
    window_end_h:         int   = 11
    window_end_m:         int   = 45


# Module-level singleton
cfg = Config()

_listeners: list = []


def register_listener(coro):
    _listeners.append(coro)


async def update(params: Dict[str, Any]) -> Dict[str, str]:
    changes = {}
    for key, new_val in params.items():
        if not hasattr(cfg, key):
            log.warning(f"Unknown config key ignored: {key}")
            continue
        old_val = getattr(cfg, key)
        try:
            typed = type(old_val)(new_val)
        except (TypeError, ValueError):
            typed = new_val
        if typed == old_val:
            continue
        setattr(cfg, key, typed)
        changes[key] = f"{old_val} -> {typed}"
        log.info(f"Config: {key} = {old_val} -> {typed}")

    if changes:
        for listener in _listeners:
            try:
                await listener(changes)
            except Exception as e:
                log.error(f"Config listener error: {e}")

    return changes


def as_dict() -> Dict[str, Any]:
    return asdict(cfg)
