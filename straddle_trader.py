#!/usr/bin/env python3
"""
straddle_trader.py  -  BTC ITM Straddle Bot
==========================================
Strategy:
  1. Find nearest ITM CALL + nearest ITM PUT pair
  2. Buy both simultaneously (FOK) if all conditions pass
  3. Immediately place opposing LONG + SHORT futures LIMIT orders:
       LONG  limit = PUT_strike  âˆ' total_premium_paid
       SHORT limit = CALL_strike + total_premium_paid
  4. When each futures fills -> TP at fill +- (total_prem + TV_at_entry)
     Immediately place liq protection option at intrinsic value at liq price
  5. Protection is placed once on fill — no periodic polling
  6. Squareoff: 15m kline high/low determines if TP was hit during session
     Options closed at intrinsic value; futures exit at TP price or spot

Run:  python straddle_trader.py
Stop: Ctrl+C
"""

import asyncio
import concurrent.futures
import hashlib
import hmac
import json
import re
import signal
import os
import socket
import sqlite3
import sys
import time
from pathlib import Path
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# Force UTF-8 on Windows consoles / log files so emoji in log() never crash
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


# ╔╗
# â•'                        CREDENTIALS  (hardcoded)                           â•'
# ╚
BINANCE_API_KEY    = "sXT49QlPlMLmeijKTAoO8c4GM53PXLTxax2mnE5uAKLw70Zp7PtxhOCyvByPTtF2"
BINANCE_SECRET_KEY = "IOQ4bwElCoRzd8Gux34J1MBq5lPqJ77Zwm8BxqYVAzvySMj3yPRSGXroXXmgtZvE"
TELEGRAM_TOKEN     = "8813334723:AAFjL4JpyLaDmMgmhgiOrKFidNjmFo45CAk"
TELEGRAM_CHAT_ID   = "6110048937"


# ╔╗
# â•'                        TRADING SETTINGS                                   â•'
# â•'  All strategy parameters here  -  change only this block.                   â•'
# ╚

# â"€â"€ Entry Window (IST) â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
WINDOW_START    = ( 9,  0)   # start scanning for fresh setup
WINDOW_END      = (13,  0)   # stop accepting new entries after this time

# â"€â"€ Squareoff Window (IST) â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
SQUAREOFF_START = (13, 10)   # begin watching for clean bid > intrinsic exit
SQUAREOFF_HARD  = (13, 30)   # hard deadline  -  close futures at market regardless
EXPIRY_TIME     = (13, 30)   # used for session/chain logic

# â"€â"€ Trade Size â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
TRADE_QTY       = 0.01       # BTC qty per leg (options AND futures  -  both same qty)

# â"€â"€ Entry Conditions â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
MIN_EXPIRY_HOURS   = 6.0     # both options must expire > this many hours from now
MIN_STRIKE_GAP     = 500     # PUT_strike âˆ' CALL_strike must be >= this (USDT)
#                              guarantees minimum "locked" intrinsic between strikes
MAX_ASK_MARK_PCT   = 4.0     # max allowed (ask âˆ' mark) / mark * 100 at entry (both legs)
MAX_ASK_MARK_RETRY = 5.0     # relaxed spread limit when retrying a failed single leg
MIN_ASK_PER_LEG    = 350     # both options: best ask must be strictly > this (USDT/BTC)
MAX_TOTAL_ASK      = 800     # call_ask + put_ask must be <= this (USDT/BTC)
SCAN_INTERVAL      = 2.0     # seconds between entry condition checks
RETRY_TIMEOUT      = 60      # seconds to keep retrying a failed single leg before giving up

# â"€â"€ Futures Management â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
# TP distance is computed from premium + time value, NOT a fixed constant.
# Formula (in _on_both_filled):  tp_dist = max(prem + (prem - gap), 50)
#   where gap = put_strike - call_strike,  prem = total_premium_paid
#   LONG  TP = long_entry  + tp_dist
#   SHORT TP = short_entry - tp_dist


# â"€â"€ Binance Endpoints â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
#  Paper Trading 
PAPER_TRADE        = True            # True  no real orders, all simulated
_PTAG = "[PAPER] " if PAPER_TRADE else ""  # Telegram prefix
FUTURES_LEVERAGE   = 100             # margin + cross-margin liq price calc
FUTURES_MM_RATE    = 0.004           # 0.4% maintenance margin rate (BTC perp)
PAPER_WALLET_USDT  = 10_000.0       # paper futures wallet balance
DASHBOARD_PASSWORD = "admin1234"     # settings panel password
DB_PATH            = os.environ.get("DB_PATH", str(Path(__file__).parent / "straddle_paper.db"))

FAPI = "https://fapi.binance.com"
EAPI = "https://eapi.binance.com"
IST  = timezone(timedelta(hours=5, minutes=30))


# ╔╗
# â•'                        TERMINAL LOGGING                                   â•'
# ╚
_R   = "\033[0m";  _B   = "\033[1m"
_GRN = "\033[92m"; _YLW = "\033[93m"; _RED = "\033[91m"
_CYN = "\033[96m"; _BLU = "\033[94m"; _MGT = "\033[95m"; _WHT = "\033[97m"
_LEVEL_CLR = {"INFO": _WHT, "WARN": _YLW, "ERROR": _RED,
              "OK":   _GRN, "TRADE": _CYN, "STATE": _BLU, "CRIT": _MGT}
_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')
_IS_TTY  = sys.stdout.isatty()

def log(tag: str, msg: str, level: str = "INFO"):
    ts  = datetime.now(IST).strftime("%H:%M:%S")
    tc  = _CYN if tag == "SYS" else (_GRN if tag == "BOT" else _YLW)
    lc  = _LEVEL_CLR.get(level, _WHT)
    pfx = f"{_WHT}{ts}{_R}  {tc}{_B}[{tag:4s}]{_R}  {lc}{level:5s}{_R}  "
    if not _IS_TTY:
        pfx = _ANSI_RE.sub("", pfx)
        msg = _ANSI_RE.sub("", msg)
    lines = msg.split("\n")
    print(pfx + lines[0], flush=True)
    for line in lines[1:]:
        print(" " * 26 + line, flush=True)

def separator(title: str = ""):
    bar = (_CYN + "" * 72 + _R) if _IS_TTY else ("" * 72)
    print(f"\n{bar}")
    if title:
        if _IS_TTY:
            print(f"{_B}{_CYN}  {title}{_R}")
        else:
            print(f"  {title}")
        print(bar)
    print()


# ╔╗
# â•'                        TELEGRAM                                           â•'
# ╚
async def _tg(text: str):
    if not TELEGRAM_CHAT_ID:
        return
    try:
        url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID,
                           "text": text, "parse_mode": "HTML"}).encode()
        def _send():
            req = urllib.request.Request(url, data=body,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=8)
        await asyncio.get_running_loop().run_in_executor(None, _send)
    except Exception as e:
        log("SYS", f"Telegram failed: {e}", "WARN")


# 
#  DATABASE  (SQLite)
# 
_db: Optional[sqlite3.Connection] = None

def _ts() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

def _db_init():
    global _db
    _db = sqlite3.connect(DB_PATH, check_same_thread=False)
    _db.row_factory = sqlite3.Row
    _db.execute("PRAGMA journal_mode=WAL")
    _db.executescript("""
    CREATE TABLE IF NOT EXISTS config (
        key          TEXT PRIMARY KEY,
        value        TEXT,
        label        TEXT,
        input_type   TEXT DEFAULT 'text',
        is_sensitive INTEGER DEFAULT 0,
        section      TEXT,
        sort_order   INTEGER
    );

    -- One row per trading session (entry window → squareoff)
    CREATE TABLE IF NOT EXISTS sessions (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        date          TEXT,          -- IST date of entry (YYYY-MM-DD)
        expiry_sym    TEXT,          -- Binance expiry code e.g. "260706"
        expiry_dt     TEXT,          -- Full expiry datetime "2026-07-06 13:30"
        state         TEXT DEFAULT 'ACTIVE',   -- ACTIVE | DONE
        -- Options leg
        call_sym      TEXT DEFAULT '',
        put_sym       TEXT DEFAULT '',
        call_fill     REAL DEFAULT 0,  -- CALL option fill price
        put_fill      REAL DEFAULT 0,  -- PUT option fill price
        call_qty      REAL DEFAULT 0,
        put_qty       REAL DEFAULT 0,
        total_premium REAL DEFAULT 0,  -- call_fill*qty + put_fill*qty
        -- Futures leg
        long_entry    REAL DEFAULT 0,  -- LONG futures fill price (0 if not filled)
        short_entry   REAL DEFAULT 0,  -- SHORT futures fill price (0 if not filled)
        long_qty      REAL DEFAULT 0,
        short_qty     REAL DEFAULT 0,
        long_tp_px    REAL DEFAULT 0,  -- pre-placed TP limit price
        short_tp_px   REAL DEFAULT 0,
        long_liq_px   REAL DEFAULT 0,  -- estimated liq price at entry
        short_liq_px  REAL DEFAULT 0,
        margin_used   REAL DEFAULT 0,
        -- Context at entry
        entry_btc     REAL DEFAULT 0,  -- BTC mark price when straddle was entered
        tp_dist       REAL DEFAULT 0,  -- TP distance used (from formula: prem + TV)
        wallet_before REAL DEFAULT 0,  -- wallet balance BEFORE this session's premium deduction
        -- Squareoff results
        sq_call_exit  REAL DEFAULT 0,
        sq_put_exit   REAL DEFAULT 0,
        sq_long_exit  REAL DEFAULT 0,
        sq_short_exit REAL DEFAULT 0,
        options_pnl   REAL DEFAULT 0,
        futures_pnl   REAL DEFAULT 0,
        net_pnl       REAL DEFAULT 0,
        sq_type       TEXT DEFAULT '',  -- TP_HIT | PARTIAL_TP | TIMEOUT
        wallet_after  REAL DEFAULT 0,  -- wallet balance AFTER squareoff settlement
        -- Timestamps
        entry_ts_ms   INTEGER DEFAULT 0,  -- entry unix ms (for SQ time calc)
        start_dt      TEXT,
        end_dt        TEXT
    );

    -- One row per order placed (options FOK + futures LIMIT)
    CREATE TABLE IF NOT EXISTS orders (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id     INTEGER REFERENCES sessions(id),
        paper_order_id INTEGER,       -- internal paper engine order ID
        symbol         TEXT,
        asset_type     TEXT,          -- option | future
        leg_label      TEXT,          -- CALL_ENTRY | PUT_ENTRY | LONG_ENTRY | LONG_TP | SHORT_ENTRY | SHORT_TP
        side           TEXT,          -- BUY | SELL
        order_type     TEXT,          -- FOK | LIMIT
        qty            REAL,
        limit_price    REAL DEFAULT 0,
        fill_price     REAL DEFAULT 0,
        status         TEXT,          -- NEW | FILLED | CANCELLED
        placed_at      TEXT,
        filled_at      TEXT,
        cancel_reason  TEXT,
        updated_at     TEXT
    );

    -- Time-series UPnL snapshots during MANAGING (kept last 120 per session = ~1h at 30s interval)
    CREATE TABLE IF NOT EXISTS pnl_snapshots (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id  INTEGER REFERENCES sessions(id),
        ts          TEXT,
        btc_mark    REAL,
        call_mark   REAL,
        put_mark    REAL,
        call_upnl   REAL,
        put_upnl    REAL,
        long_upnl   REAL,
        short_upnl  REAL,
        total_upnl  REAL,
        long_liq    REAL,
        short_liq   REAL,
        margin_used REAL
    );

    -- Immutable audit log: every state transition, fill, and squareoff
    CREATE TABLE IF NOT EXISTS events (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER REFERENCES sessions(id),
        ts         TEXT,
        event_type TEXT,   -- STATE_SCANNING | STRADDLE_ENTERED | LONG_FILLED | SHORT_FILLED
                           -- LONG_TP_HIT | SHORT_TP_HIT | SESSION_CLOSED | PARTIAL_STRADDLE
        detail     TEXT
    );

    -- Full wallet ledger: every debit/credit is an immutable signed row
    -- Balance always reconstructible: SELECT balance_after FROM wallet_ledger ORDER BY id DESC LIMIT 1
    CREATE TABLE IF NOT EXISTS wallet_ledger (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            TEXT NOT NULL,
        session_id    INTEGER DEFAULT 0,   -- 0 = no session (manual reset)
        type          TEXT NOT NULL,       -- RESET | PREMIUM_DEBIT | OPTIONS_CREDIT | FUTURES_PNL
        amount        REAL NOT NULL,       -- signed: negative=debit, positive=credit
        balance_after REAL NOT NULL,
        note          TEXT
    );

    -- Singleton: live bot state written every tick for dashboard
    CREATE TABLE IF NOT EXISTS bot_status (
        id         INTEGER PRIMARY KEY DEFAULT 1,
        ts         TEXT,
        state      TEXT,
        btc_mark   REAL,
        session_id INTEGER,
        detail     TEXT
    );

    -- Indexes for fast queries
    CREATE INDEX IF NOT EXISTS idx_sessions_state   ON sessions(state);
    CREATE INDEX IF NOT EXISTS idx_sessions_expiry  ON sessions(expiry_dt);
    CREATE INDEX IF NOT EXISTS idx_orders_session   ON orders(session_id);
    CREATE INDEX IF NOT EXISTS idx_orders_status    ON orders(session_id, status);
    CREATE INDEX IF NOT EXISTS idx_snapshots_sess   ON pnl_snapshots(session_id);
    CREATE INDEX IF NOT EXISTS idx_events_session   ON events(session_id);
    CREATE INDEX IF NOT EXISTS idx_ledger_session   ON wallet_ledger(session_id);
    CREATE INDEX IF NOT EXISTS idx_ledger_type      ON wallet_ledger(type);
    """)
    # Safe column migrations for existing DBs — idempotent
    for _sql in [
        "ALTER TABLE sessions ADD COLUMN entry_ts_ms  INTEGER DEFAULT 0",
        "ALTER TABLE sessions ADD COLUMN entry_btc    REAL DEFAULT 0",
        "ALTER TABLE sessions ADD COLUMN tp_dist      REAL DEFAULT 0",
        "ALTER TABLE sessions ADD COLUMN wallet_before REAL DEFAULT 0",
        "ALTER TABLE sessions ADD COLUMN wallet_after  REAL DEFAULT 0",
        "ALTER TABLE orders   ADD COLUMN cancel_reason TEXT",
        "ALTER TABLE orders   ADD COLUMN updated_at    TEXT",
        "ALTER TABLE config   ADD COLUMN section       TEXT",
        "ALTER TABLE config   ADD COLUMN sort_order    INTEGER",
        "ALTER TABLE bot_status ADD COLUMN detail      TEXT",
    ]:
        try:
            _db.execute(_sql)
        except Exception:
            pass  # column already exists
    _db.commit()

    # key, default_value, label, input_type, is_sensitive, section, sort_order
    defaults = [
        ("WINDOW_START",       f"{WINDOW_START[0]:02d}:{WINDOW_START[1]:02d}",
         "Entry Window Open",        "time",     0, "time",       1),
        ("WINDOW_END",         f"{WINDOW_END[0]:02d}:{WINDOW_END[1]:02d}",
         "Entry Window Close",       "time",     0, "time",       2),
        ("SQ_START",           f"{SQUAREOFF_START[0]:02d}:{SQUAREOFF_START[1]:02d}",
         "Squareoff Start",          "time",     0, "time",       3),
        ("SQ_END",             f"{SQUAREOFF_HARD[0]:02d}:{SQUAREOFF_HARD[1]:02d}",
         "Squareoff End",            "time",     0, "time",       4),
        ("MIN_EXPIRY_HOURS",   str(MIN_EXPIRY_HOURS),
         "Min Expiry (hours)",       "number",   0, "entry",      1),
        ("MIN_STRIKE_GAP",     str(MIN_STRIKE_GAP),
         "Min Strike Gap (USDT)",    "number",   0, "entry",      2),
        ("MIN_ASK_PER_LEG",    str(MIN_ASK_PER_LEG),
         "Min Ask Per Leg (USDT)",   "number",   0, "entry",      3),
        ("MAX_TOTAL_ASK",      str(MAX_TOTAL_ASK),
         "Max Total Premium (USDT)", "number",   0, "entry",      4),
        ("MAX_ASK_MARK_PCT",   str(MAX_ASK_MARK_PCT),
         "Max Ask-Mark %",           "number",   0, "entry",      5),
        ("TRADE_QTY",          str(TRADE_QTY),
         "Trade Qty (BTC)",          "number",   0, "sizing",     1),
        ("PAPER_WALLET_USDT",  str(PAPER_WALLET_USDT),
         "Paper Wallet (USDT)",      "number",   0, "sizing",     2),
        ("DASHBOARD_PASSWORD", DASHBOARD_PASSWORD,
         "Dashboard Password",       "password", 1, "security",   1),
        ("_config_dirty",      "0",
         "Config Dirty Flag",        "text",     1, "system",     99),
        ("PAPER_WALLET_BALANCE", str(PAPER_WALLET_USDT),
         "Paper Wallet Balance",     "number",   1, "system",     98),
        ("LAST_TRADED_EXPIRY",  "",
         "Last Traded Expiry",       "text",     1, "system",     97),
    ]
    for key, val, label, inp, sens, section, sort_order in defaults:
        _db.execute(
            "INSERT OR IGNORE INTO config "
            "(key,value,label,input_type,is_sensitive,section,sort_order) "
            "VALUES (?,?,?,?,?,?,?)",
            (key, val, label, inp, sens, section, sort_order)
        )
        # Always refresh metadata (label, section, sort_order) without touching user's saved value
        _db.execute(
            "UPDATE config SET label=?, section=?, sort_order=? WHERE key=?",
            (label, section, sort_order, key)
        )
    _db.commit()


def _db_load_config():
    """Read all trading parameters from config table and override module globals."""
    global TRADE_QTY, MAX_TOTAL_ASK, MIN_STRIKE_GAP
    global MAX_ASK_MARK_PCT, WINDOW_START, WINDOW_END, SQUAREOFF_START, SQUAREOFF_HARD
    global PAPER_WALLET_USDT, DASHBOARD_PASSWORD, MIN_EXPIRY_HOURS, MIN_ASK_PER_LEG
    global _paper_wallet_balance
    if not _db:
        return
    try:
        rows = {r["key"]: r["value"]
                for r in _db.execute("SELECT key, value FROM config").fetchall()}
        def _f(k):
            try: return float(rows[k]) if k in rows and rows[k] not in ("", None) else None
            except (ValueError, TypeError): return None
        def _t(k):
            try:
                if k not in rows or not rows[k]: return None
                h, m = rows[k].split(":")
                return int(h), int(m)
            except Exception: return None
        if _f("TRADE_QTY")        is not None: TRADE_QTY        = _f("TRADE_QTY")
        if _f("MAX_TOTAL_ASK")    is not None: MAX_TOTAL_ASK    = _f("MAX_TOTAL_ASK")
        if _f("MIN_STRIKE_GAP")   is not None: MIN_STRIKE_GAP   = _f("MIN_STRIKE_GAP")
        if _f("MAX_ASK_MARK_PCT") is not None: MAX_ASK_MARK_PCT = _f("MAX_ASK_MARK_PCT")
        if _f("MIN_EXPIRY_HOURS") is not None: MIN_EXPIRY_HOURS = _f("MIN_EXPIRY_HOURS")
        if _f("MIN_ASK_PER_LEG")  is not None: MIN_ASK_PER_LEG  = _f("MIN_ASK_PER_LEG")
        if _f("PAPER_WALLET_USDT") is not None: PAPER_WALLET_USDT = _f("PAPER_WALLET_USDT")
        if "DASHBOARD_PASSWORD" in rows: DASHBOARD_PASSWORD = rows["DASHBOARD_PASSWORD"]
        if _t("WINDOW_START")     is not None: WINDOW_START     = _t("WINDOW_START")
        if _t("WINDOW_END")       is not None: WINDOW_END       = _t("WINDOW_END")
        if _t("SQ_START")         is not None: SQUAREOFF_START  = _t("SQ_START")
        if _t("SQ_END")           is not None: SQUAREOFF_HARD   = _t("SQ_END")
        # Load running paper wallet balance (persisted across sessions)
        _wb = rows.get("PAPER_WALLET_BALANCE")
        if _wb not in (None, ""):
            try:
                _paper_wallet_balance = float(_wb)
            except ValueError:
                pass
        # Seed wallet_ledger with RESET entry if this is a fresh DB (no ledger rows)
        ledger_count = _db.execute("SELECT COUNT(*) FROM wallet_ledger").fetchone()[0]
        if ledger_count == 0:
            _db.execute(
                "INSERT INTO wallet_ledger (ts,session_id,type,amount,balance_after,note) "
                "VALUES (?,0,'RESET',?,?,?)",
                (_ts(), round(_paper_wallet_balance, 4), round(_paper_wallet_balance, 4),
                 "Initial wallet balance")
            )
            _db.commit()
        log("SYS", f"Config loaded from DB  wallet={_paper_wallet_balance:.2f}", "OK")
    except Exception as e:
        log("SYS", f"Config load error (using hardcoded defaults): {e}", "WARN")

def _db_get_config(key: str, default: Optional[str] = None) -> Optional[str]:
    """Read a single value from the config table."""
    if not _db:
        return default
    try:
        row = _db.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        return row[0] if row and row[0] is not None else default
    except Exception:
        return default

def _db_set_config(key: str, value: str):
    """Write a single value to the config table (upsert)."""
    if not _db:
        return
    try:
        _db.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (key, value))
        _db.commit()
    except Exception as e:
        log("SYS", f"_db_set_config({key}): {e}", "WARN")

def _db_session_create(expiry_sym: str, expiry_dt: str) -> int:
    if not _db:
        return 0
    cur = _db.execute(
        "INSERT INTO sessions (date,expiry_sym,expiry_dt,state,start_dt) VALUES (?,?,?,?,?)",
        (datetime.now(IST).strftime("%Y-%m-%d"), expiry_sym, expiry_dt, "ACTIVE", _ts())
    )
    _db.commit()
    return cur.lastrowid

def _db_session_update(session_id: int, **kwargs):
    if not _db or not session_id:
        return
    cols = ", ".join(f"{k}=?" for k in kwargs)
    _db.execute(f"UPDATE sessions SET {cols} WHERE id=?", list(kwargs.values()) + [session_id])
    _db.commit()

def _db_order_insert(session_id: int, paper_order_id: int, symbol: str, asset_type: str,
                     leg_label: str, side: str, order_type: str, qty: float,
                     limit_price: float = 0, fill_price: float = 0,
                     status: str = "NEW", filled_at: str = None,
                     cancel_reason: str = None) -> int:
    if not _db:
        return 0
    cur = _db.execute(
        "INSERT INTO orders (session_id,paper_order_id,symbol,asset_type,leg_label,"
        "side,order_type,qty,limit_price,fill_price,status,placed_at,filled_at,"
        "cancel_reason,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (session_id, paper_order_id, symbol, asset_type, leg_label, side, order_type,
         qty, limit_price, fill_price, status, _ts(), filled_at, cancel_reason, _ts())
    )
    _db.commit()
    return cur.lastrowid


def _db_order_cancel(session_id: int, leg_label: str, cancel_reason: str):
    """Mark all NEW orders for a leg as CANCELLED."""
    if not _db or not session_id:
        return
    _db.execute(
        "UPDATE orders SET status='CANCELLED', cancel_reason=?, updated_at=? "
        "WHERE session_id=? AND leg_label=? AND status='NEW'",
        (cancel_reason, _ts(), session_id, leg_label)
    )
    _db.commit()

def _db_snapshot_insert(session_id: int, btc_mark: float, call_mark: float, put_mark: float,
                        call_upnl: float, put_upnl: float, long_upnl: float, short_upnl: float,
                        long_liq: float, short_liq: float, margin_used: float):
    if not _db or not session_id:
        return
    total = call_upnl + put_upnl + long_upnl + short_upnl
    _db.execute(
        "INSERT INTO pnl_snapshots (session_id,ts,btc_mark,call_mark,put_mark,"
        "call_upnl,put_upnl,long_upnl,short_upnl,total_upnl,long_liq,short_liq,margin_used) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (session_id, _ts(), btc_mark, call_mark, put_mark,
         call_upnl, put_upnl, long_upnl, short_upnl, total,
         long_liq, short_liq, margin_used)
    )
    # Keep last 120 rows per session (~1h at 30s interval) to prevent unbounded growth
    _db.execute(
        "DELETE FROM pnl_snapshots WHERE session_id=? AND id NOT IN "
        "(SELECT id FROM pnl_snapshots WHERE session_id=? ORDER BY id DESC LIMIT 120)",
        (session_id, session_id)
    )
    _db.commit()

def _db_event_insert(session_id: int, event_type: str, detail: str = ""):
    if not _db:
        return
    _db.execute(
        "INSERT INTO events (session_id,ts,event_type,detail) VALUES (?,?,?,?)",
        (session_id, _ts(), event_type, detail)
    )
    _db.commit()


def _db_wallet_ledger(session_id: int, tx_type: str, amount: float,
                      balance_after: float, note: str = ""):
    """Record a signed wallet movement. amount < 0 = debit, amount > 0 = credit."""
    if not _db:
        return
    _db.execute(
        "INSERT INTO wallet_ledger (ts,session_id,type,amount,balance_after,note) "
        "VALUES (?,?,?,?,?,?)",
        (_ts(), session_id, tx_type, round(amount, 4), round(balance_after, 4), note)
    )
    _db.commit()


# ╔╗
# â•'                        BINANCE REST  (HMAC-SHA256)                        â•'
# ╚
def _sign(params: dict) -> str:
    qs  = urllib.parse.urlencode(params)
    sig = hmac.new(BINANCE_SECRET_KEY.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig

_HDR = {"X-MBX-APIKEY": BINANCE_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded"}

def _parse_418_ban(err_str: str) -> float:
    """Return Unix seconds when the Binance EAPI IP ban expires (from the 418 error body)."""
    m = re.search(r'banned until (\d+)', err_str)
    if m:
        ts = int(m.group(1))
        return ts / 1000.0 if ts > 1e12 else float(ts)  # >1e12 = milliseconds -> convert
    return time.time() + 120   # default 2-min backoff if timestamp not parseable

def _is_rate_limited(err: Exception) -> bool:
    s = str(err).lower()
    return "418" in s or "banned" in s

async def _post(base: str, path: str, params: dict) -> dict:
    params["timestamp"] = int(time.time() * 1000)
    params.setdefault("recvWindow", 5000)
    body = _sign(params).encode()
    def _do():
        import urllib.error
        req = urllib.request.Request(f"{base}{path}", data=body,
                                     headers=_HDR, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            try:    eb = e.read().decode()
            except: eb = "(unreadable)"
            raise RuntimeError(f"HTTP {e.code} {e.reason}  -  {eb}") from e
    return await asyncio.get_running_loop().run_in_executor(None, _do)

async def _delete(base: str, path: str, params: dict) -> dict:
    params["timestamp"] = int(time.time() * 1000)
    params.setdefault("recvWindow", 5000)
    qs = _sign(params)
    def _do():
        import urllib.error
        req = urllib.request.Request(f"{base}{path}?{qs}",
                                     headers={"X-MBX-APIKEY": BINANCE_API_KEY},
                                     method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            try:    eb = e.read().decode()
            except: eb = "(unreadable)"
            raise RuntimeError(f"HTTP {e.code}  -  {eb}") from e
    return await asyncio.get_running_loop().run_in_executor(None, _do)

async def _get(base: str, path: str, params: dict = None) -> Any:
    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000)
    p.setdefault("recvWindow", 5000)
    qs = _sign(p)
    def _do():
        import urllib.error
        req = urllib.request.Request(f"{base}{path}?{qs}",
                                     headers={"X-MBX-APIKEY": BINANCE_API_KEY})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            try:    eb = e.read().decode()
            except: eb = "(unreadable)"
            raise RuntimeError(f"HTTP {e.code}  -  {eb}") from e
    return await asyncio.get_running_loop().run_in_executor(None, _do)


#
#  PAPER TRADING STATE
#
_paper_orders:    Dict[int, dict] = {}
_paper_oid_seq:   int             = 1000
_last_valid_mark: float           = 0.0   # last non-zero BTC mark price
_last_valid_opt:  Dict[str, float] = {}   # sym  last non-zero option mark
_paper_wallet_balance: float      = PAPER_WALLET_USDT  # tracks deductions during session

def _paper_next_oid() -> int:
    global _paper_oid_seq
    _paper_oid_seq += 1
    return _paper_oid_seq

def _calc_liq_price_cross(entry: float, qty: float, side: str,
                          wallet: float = None) -> float:
    # Cross-margin 100x liq price. Returns 0 if no meaningful liq risk.
    if entry <= 0 or qty <= 0:
        return 0.0
    wb  = wallet if wallet is not None else _paper_wallet_balance
    mm  = FUTURES_MM_RATE
    if side == "LONG":
        denom = qty * (1.0 - mm)
        liq   = (entry * qty - wb) / denom if denom else 0.0
    else:
        denom = qty * (1.0 + mm)
        liq   = (wb + entry * qty) / denom if denom else 0.0
    return round(max(0.0, liq), 1)

def _opt_upnl(fill: float, mark: float, qty: float) -> float:
    # Options unrealized PnL: (mark - fill) * qty
    if fill <= 0 or mark <= 0 or qty <= 0:
        return 0.0
    return round((mark - fill) * qty, 4)

def _fut_upnl(entry: float, mark: float, qty: float, side: str) -> float:
    # Futures unrealized PnL: LONG = (mark-entry)*qty, SHORT = (entry-mark)*qty
    if entry <= 0 or mark <= 0 or qty <= 0:
        return 0.0
    pnl = (mark - entry) * qty if side == "LONG" else (entry - mark) * qty
    return round(pnl, 4)

# ╔╗
# â•'                        ORDER HELPERS                                      â•'
# ╚
def _opt_tick(price: float) -> int:
    """Round to nearest 5 USDT tick  -  Binance EAPI requirement for ALL premium levels."""
    return max(5, int(round(price / 5)) * 5)

async def option_order(sym: str, side: str, qty: float,
                       price: float = 0, order_type: str = "LIMIT",
                       tif: str = "FOK") -> dict:
    if PAPER_TRADE:
        oid = _paper_next_oid()
        ticked = _opt_tick(price) if price else 0
        status, fill_px = "NEW", 0.0
        if order_type == "LIMIT" and tif == "FOK":
            data   = _chain.get(sym, {})
            ask    = data.get("ask", 0.0)
            aqty   = data.get("ask_qty", 0.0)
            if ask > 0 and ask <= ticked and aqty >= qty:
                status, fill_px = "FILLED", ask
            else:
                status = "EXPIRED"
        _paper_orders[oid] = {
            "orderId": oid, "symbol": sym, "side": side,
            "type": order_type, "tif": tif, "qty": qty,
            "price": ticked, "status": status, "fill_price": fill_px,
            "asset": "option", "pos_side": None, "placed_at": time.time(),
        }
        return {"orderId": oid, "status": status,
                "avgPrice": str(fill_px), "executedQty": str(qty if status == "FILLED" else 0)}
    p: dict = {"symbol": sym, "side": side, "type": order_type,
               "quantity": f"{qty:.2f}"}
    if order_type == "LIMIT":
        ticked = _opt_tick(price)
        p["price"]       = str(ticked) if price >= 10 else f"{ticked:.2f}"
        p["timeInForce"] = tif
    return await _post(EAPI, "/eapi/v1/order", p)

async def option_cancel(sym: str, order_id: int) -> dict:
    if PAPER_TRADE:
        if order_id in _paper_orders:
            _paper_orders[order_id]["status"] = "CANCELLED"
        return {}
    return await _delete(EAPI, "/eapi/v1/order",
                         {"symbol": sym, "orderId": order_id})

async def option_query(sym: str, order_id: int) -> dict:
    if PAPER_TRADE:
        o = _paper_orders.get(order_id, {})
        return {"status": o.get("status", "NEW"),
                "avgPrice": str(o.get("fill_price", 0)),
                "executedQty": str(o.get("qty", 0))}
    return await _get(EAPI, "/eapi/v1/order", {"symbol": sym, "orderId": order_id})

async def futures_limit(side: str, qty: float, price: float,
                        pos_side: str = "LONG") -> dict:
    if PAPER_TRADE:
        oid = _paper_next_oid()
        _paper_orders[oid] = {
            "orderId": oid, "symbol": "BTCUSDT", "side": side,
            "type": "LIMIT", "qty": qty, "price": round(price, 1),
            "status": "NEW", "fill_price": 0.0,
            "asset": "future", "pos_side": pos_side, "placed_at": time.time(),
        }
        return {"orderId": oid, "status": "NEW"}
    p = {"symbol": "BTCUSDT", "side": side, "type": "LIMIT",
         "quantity": round(qty, 3), "price": round(price, 1),
         "timeInForce": "GTC", "positionSide": pos_side}
    return await _post(FAPI, "/fapi/v1/order", p)

async def futures_market(side: str, qty: float, pos_side: str = "LONG") -> dict:
    if PAPER_TRADE:
        mark = _price() or _last_valid_mark or 0
        oid  = _paper_next_oid()
        _paper_orders[oid] = {
            "orderId": oid, "symbol": "BTCUSDT", "side": side,
            "type": "MARKET", "qty": qty, "price": mark,
            "status": "FILLED", "fill_price": mark,
            "asset": "future", "pos_side": pos_side, "placed_at": time.time(),
        }
        return {"orderId": oid, "status": "FILLED",
                "avgPrice": str(mark), "executedQty": str(qty)}
    p = {"symbol": "BTCUSDT", "side": side, "type": "MARKET",
         "quantity": round(qty, 3), "positionSide": pos_side}
    return await _post(FAPI, "/fapi/v1/order", p)

async def futures_cancel(order_id: int) -> dict:
    if PAPER_TRADE:
        if order_id in _paper_orders:
            _paper_orders[order_id]["status"] = "CANCELLED"
        return {}
    return await _delete(FAPI, "/fapi/v1/order",
                         {"symbol": "BTCUSDT", "orderId": order_id})

async def futures_query(order_id: int) -> dict:
    if PAPER_TRADE:
        o = _paper_orders.get(order_id, {})
        return {"status": o.get("status", "NEW"),
                "avgPrice": str(o.get("fill_price", 0)),
                "executedQty": str(o.get("qty", 0))}
    return await _get(FAPI, "/fapi/v1/order", {"symbol": "BTCUSDT", "orderId": order_id})


# ╔╗
# â•'                        SHARED PRICE STATE                                 â•'
# ╚
_fut_px: dict            = {"bid": 0.0, "ask": 0.0, "last": 0.0, "mark": 0.0}
_fut_px_update_ms: float = 0.0          # epoch-ms of last futures price update
_chain:  Dict[str, dict] = {}           # sym -> {bid, ask, bid_qty, ask_qty, mark, intrinsic, *_ms}
_chain_ms: Dict[str, Dict[str, int]] = {}  # sym -> {field -> epoch_ms}  LKG timestamps
_all_strikes: List[float] = []
_sym_meta:    Dict[str, dict] = {}      # sym -> {symbol, strike, side, expiry_ms}
_expiry_6d    = ""
_expiry_ms    = 0
_ws_last_msg: Dict[str, float] = {"fut": time.time(), "opt": time.time()}
_last_chain_refresh = 0.0
_ws_opt_fail_count  = 0   # consecutive options WS failures; resets on success
_rest_banned_until  = 0.0  # Unix time: stop all EAPI REST calls while time() < this
_rest_last_ok:  float = 0.0   # epoch time of last successful REST ticker fetch
_rest_fail_streak: int = 0    # consecutive REST failures (resets on success)
_ws_mark_time: Dict[str, float] = {}  # sym -> last time mark price arrived from WS ticker

# ── LKG helpers ──────────────────────────────────────────────────────────────
_LKG_FIELDS = ("bid", "ask", "bid_qty", "ask_qty", "mark")

def _chain_set(sym: str, field: str, value: float):
    """Update chain field only when value > 0 (Last-Known-Good pattern)."""
    if value <= 0:
        return
    if sym not in _chain:
        _chain[sym] = {}
    _chain[sym][field] = round(value, 2)
    if sym not in _chain_ms:
        _chain_ms[sym] = {}
    _chain_ms[sym][field] = int(time.time() * 1000)

def _chain_get(sym: str, field: str, max_age_ms: int = 15_000) -> Optional[float]:
    """Return chain field value if fresh, None if stale or missing."""
    val = _chain.get(sym, {}).get(field)
    if not val:
        return None
    age = int(time.time() * 1000) - _chain_ms.get(sym, {}).get(field, 0)
    return val if age < max_age_ms else None

def _feed_age_s(feed: str) -> float:
    """Seconds since last message on 'fut' or 'opt' WS feed."""
    return time.time() - _ws_last_msg.get(feed, 0)

def _price() -> float:
    return _fut_px.get("mark") or _fut_px.get("last") or _fut_px.get("bid") or 0.0

def _sym(strike: float, side: str) -> str:
    return f"BTC-{_expiry_6d}-{int(strike)}-{side}"

def _intr(strike: float, spot: float, side: str) -> float:
    return max(spot - strike, 0.0) if side == "C" else max(strike - spot, 0.0)

def _to_min(h: int, m: int) -> int:
    return h * 60 + m

def _in_entry_window(now_min: int) -> bool:
    """True when current time is inside the entry window. Handles overnight windows correctly."""
    start = _to_min(*WINDOW_START)
    end   = _to_min(*WINDOW_END)
    if start <= end:                         # same-day  e.g. 09:00 -> 13:00
        return start <= now_min < end
    else:                                    # overnight e.g. 23:00 -> 05:00
        return now_min >= start or now_min < end

def _is_expiry_day() -> bool:
    """True only on the calendar day the current options expiry falls on."""
    if not _expiry_ms:
        return False
    return datetime.fromtimestamp(_expiry_ms / 1000, tz=IST).date() == datetime.now(IST).date()

def _expiry_iso() -> str:
    """ISO date string (YYYY-MM-DD) of the current expiry  -  used as session key."""
    if not _expiry_ms:
        return ""
    return datetime.fromtimestamp(_expiry_ms / 1000, tz=IST).date().isoformat()


# ╔╗
# â•'                        OPTIONS CHAIN                                      â•'
# ╚
async def _fetch_chain():
    """Load/refresh full options chain from exchangeInfo. Preserves existing price data."""
    global _all_strikes, _sym_meta, _expiry_6d, _expiry_ms, _chain, _last_chain_refresh
    _last_chain_refresh = time.time()
    old_strikes = set(_all_strikes)
    try:
        def _do():
            import urllib.error
            req = urllib.request.Request(f"{EAPI}/eapi/v1/exchangeInfo",
                                         headers={"User-Agent": "StraddleBot/1.0"})
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                try:    eb = e.read().decode()
                except: eb = ""
                raise RuntimeError(f"HTTP {e.code}  -  {eb}") from e
        data    = await asyncio.get_running_loop().run_in_executor(None, _do)
        now_ms  = int(time.time() * 1000)
        now_ist = datetime.now(IST)
        btc     = [s for s in data.get("optionSymbols", [])
                   if s.get("underlying") == "BTCUSDT"
                   and s.get("status") == "TRADING"
                   and s.get("expiryDate", 0) > now_ms]
        if not btc:
            raise ValueError("No active BTC option symbols")
        by_exp = {}
        for s in btc:
            by_exp.setdefault(s["expiryDate"], []).append(s)
        today_ts = next((ts for ts in sorted(by_exp)
                         if datetime.fromtimestamp(ts/1000, tz=IST).date() == now_ist.date()),
                        None)
        ts_use   = today_ts or sorted(by_exp.keys())[0]
        raw      = by_exp[ts_use]
        exp_dt   = datetime.fromtimestamp(ts_use/1000, tz=IST)
        _expiry_6d   = exp_dt.strftime("%y%m%d")
        _expiry_ms   = ts_use
        strikes      = sorted(set(float(s["strikePrice"]) for s in raw))
        _all_strikes = strikes
        meta = {}
        for s in raw:
            sc = "C" if s.get("side", "CALL") == "CALL" else "P"
            meta[s["symbol"]] = {"symbol": s["symbol"],
                                 "strike": float(s["strikePrice"]),
                                 "side":   sc,
                                 "expiry_ms": ts_use,
                                 "min_qty":  float(s.get("minQty", 0.01))}
        # Build new chain  -  preserve existing price data for known symbols
        new_chain: Dict[str, dict] = {}
        for k, v in meta.items():
            ex = _chain.get(k, {})
            new_chain[k] = {
                "symbol":    k,
                "strike":    v["strike"],
                "side":      v["side"],
                "bid":       ex.get("bid", 0.0),
                "ask":       ex.get("ask", 0.0),
                "bid_qty":   ex.get("bid_qty", 0.0),
                "ask_qty":   ex.get("ask_qty", 0.0),
                "mark":      ex.get("mark", 0.0),
                "intrinsic": ex.get("intrinsic", 0.0),
            }
        _sym_meta = meta
        _chain    = new_chain
        new_syms  = sorted(set(strikes) - old_strikes)
        if new_syms and old_strikes:
            log("SYS", f"New strikes: +{len(new_syms)} -> {[int(s) for s in new_syms]}", "WARN")
        log("SYS", f"Chain loaded: {exp_dt.strftime('%d %b %Y %H:%M IST')} | "
                   f"{len(strikes)} strikes", "OK")
    except Exception as e:
        global _rest_banned_until
        err_s = str(e)
        if "429" in err_s or "Too Many Requests" in err_s:
            _rest_banned_until = time.time() + 60
            log("SYS", "exchangeInfo HTTP 429  -  rate limited. Backing off 60s.", "WARN")
        elif _is_rate_limited(e):
            _rest_banned_until = _parse_418_ban(err_s)
            remain = max(0, int(_rest_banned_until - time.time()))
            log("SYS", f"exchangeInfo HTTP 418  -  EAPI REST paused for {remain}s", "WARN")
        else:
            log("SYS", f"exchangeInfo failed: {e}", "WARN")


async def _chain_refresh_loop():
    """Re-fetch chain every 15 minutes to pick up new mid-session strikes."""
    while True:
        await asyncio.sleep(15 * 60)
        await _fetch_chain()


# ╔╗
# â•'                        ITM PAIR LOGIC                                     â•'
# ╚
def _find_itm_pair(spot: float) -> Optional[Tuple[dict, dict]]:
    """
    Returns (call_opt, put_opt) for the nearest ITM pair, or None.
    Nearest ITM CALL = highest strike strictly below spot.
    Nearest ITM PUT  = lowest  strike strictly above spot.
    """
    if not _all_strikes or not _expiry_6d:
        return None
    call_stk = max((s for s in _all_strikes if s < spot), default=None)
    put_stk  = min((s for s in _all_strikes if s > spot), default=None)
    if call_stk is None or put_stk is None:
        return None
    call_sym = _sym(call_stk, "C")
    put_sym  = _sym(put_stk,  "P")
    call_opt = _chain.get(call_sym)
    put_opt  = _chain.get(put_sym)
    if call_opt is None or put_opt is None:
        return None
    # Return shallow copies so we don't mutate shared state
    return dict(call_opt), dict(put_opt)


def _check_conditions(call_opt: dict, put_opt: dict) -> Tuple[bool, str]:
    """
    Returns (True, "") if ALL entry conditions pass simultaneously.
    Returns (False, reason_string) on first failure.
    """
    now_ms = int(time.time() * 1000)

    # 1. Expiry > MIN_EXPIRY_HOURS
    if _expiry_ms:
        hours_left = (_expiry_ms - now_ms) / (3600 * 1000)
        if hours_left < MIN_EXPIRY_HOURS:
            return False, f"expiry too soon ({hours_left:.1f}h left)"

    # 2. Strike gap >= MIN_STRIKE_GAP
    gap = put_opt["strike"] - call_opt["strike"]
    if gap < MIN_STRIKE_GAP:
        return False, f"strike gap {gap:.0f} < {MIN_STRIKE_GAP}"

    # 3. Ask-mark spread — use LKG reads (None = stale > 15s)
    for leg, sym in (("CALL", call_opt["symbol"]), ("PUT", put_opt["symbol"])):
        ask  = _chain_get(sym, "ask",  max_age_ms=15_000)
        mark = _chain_get(sym, "mark", max_age_ms=20_000)
        if ask is None:
            return False, f"{leg} ask stale/unavailable"
        if mark is None:
            return False, f"{leg} mark stale/unavailable"
        if ask <= 0 or mark <= 0:
            return False, f"{leg} ask/mark is zero"
        spread = abs(ask - mark) / mark * 100
        if spread > MAX_ASK_MARK_PCT:
            return False, f"{leg} spread {spread:.1f}% > {MAX_ASK_MARK_PCT}%"

    # 4. Offered qty at best ask >= TRADE_QTY (LKG: None if stale > 15s)
    for leg, sym in (("CALL", call_opt["symbol"]), ("PUT", put_opt["symbol"])):
        aq = _chain_get(sym, "ask_qty", max_age_ms=15_000)
        if aq is None:
            return False, f"{leg} ask_qty stale"
        if aq < TRADE_QTY:
            return False, f"{leg} ask_qty {aq:.4f} < {TRADE_QTY}"

    # 5. Min ask per leg (re-read fresh after spread check passed)
    call_ask = _chain_get(call_opt["symbol"], "ask", max_age_ms=15_000) or 0.0
    put_ask  = _chain_get(put_opt["symbol"],  "ask", max_age_ms=15_000) or 0.0
    if call_ask <= MIN_ASK_PER_LEG:
        return False, f"CALL ask {call_ask:.0f} <= {MIN_ASK_PER_LEG}"
    if put_ask <= MIN_ASK_PER_LEG:
        return False, f"PUT ask {put_ask:.0f} <= {MIN_ASK_PER_LEG}"

    # 6. Total ask <= MAX_TOTAL_ASK
    total = call_ask + put_ask
    if total > MAX_TOTAL_ASK:
        return False, f"total ask {total:.0f} > {MAX_TOTAL_ASK}"

    return True, ""


def _conditions_detail(call_opt: dict, put_opt: dict) -> str:
    """All entry conditions with computed values  -  shown every scan cycle."""
    now_ms    = int(time.time() * 1000)
    c_ask     = call_opt.get("ask")     or 0.0
    c_mark    = call_opt.get("mark")    or 0.0
    c_aq      = call_opt.get("ask_qty") or 0.0
    p_ask     = put_opt.get("ask")      or 0.0
    p_mark    = put_opt.get("mark")     or 0.0
    p_aq      = put_opt.get("ask_qty")  or 0.0
    total     = c_ask + p_ask
    gap       = put_opt["strike"] - call_opt["strike"]
    h_left    = (_expiry_ms - now_ms) / 3_600_000 if _expiry_ms else 0.0
    c_sp      = abs(c_ask - c_mark) / c_mark * 100 if c_mark > 0 and c_ask > 0 else 999.0
    p_sp      = abs(p_ask - p_mark) / p_mark * 100 if p_mark > 0 and p_ask > 0 else 999.0

    def ok(cond): return "OK" if cond else "NO"

    return (
        f"  {call_opt['symbol']}  <->  {put_opt['symbol']}\n"
        f"  expiry    {h_left:.1f}h              need >= {MIN_EXPIRY_HOURS:.0f}h       {ok(h_left >= MIN_EXPIRY_HOURS)}\n"
        f"  gap       {gap:.0f} USDT           need >= {MIN_STRIKE_GAP}      {ok(gap >= MIN_STRIKE_GAP)}\n"
        f"  CALL spd  {c_sp:.1f}%  (ask={c_ask:.0f}  mk={c_mark:.0f})   need <= {MAX_ASK_MARK_PCT:.0f}%    {ok(c_sp <= MAX_ASK_MARK_PCT and c_ask > 0)}\n"
        f"  PUT  spd  {p_sp:.1f}%  (ask={p_ask:.0f}  mk={p_mark:.0f})   need <= {MAX_ASK_MARK_PCT:.0f}%    {ok(p_sp <= MAX_ASK_MARK_PCT and p_ask > 0)}\n"
        f"  CALL qty  {c_aq:.4f}              need >= {TRADE_QTY}    {ok(c_aq >= TRADE_QTY)}\n"
        f"  PUT  qty  {p_aq:.4f}              need >= {TRADE_QTY}    {ok(p_aq >= TRADE_QTY)}\n"
        f"  CALL ask  {c_ask:.0f}               need > {MIN_ASK_PER_LEG}   {ok(c_ask > MIN_ASK_PER_LEG)}\n"
        f"  PUT  ask  {p_ask:.0f}               need > {MIN_ASK_PER_LEG}   {ok(p_ask > MIN_ASK_PER_LEG)}\n"
        f"  total     {total:.0f}               need <= {MAX_TOTAL_ASK}   {ok(total <= MAX_TOTAL_ASK)}"
    )


# ╔╗
# â•'                        WEBSOCKET FEEDS                                    â•'
# ╚
async def _fut_ws():
    """Perpetual futures: mark price + book ticker -> _fut_px."""
    import websockets
    url = ("wss://fstream.binance.com/stream?streams="
           "btcusdt@bookTicker/btcusdt@markPrice@1s")
    _was_down = False
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=15, ssl=True) as ws:
                if _was_down:
                    log("SYS", "Futures WS: reconnected", "OK")
                    asyncio.create_task(_tg("✅ <b>Futures WS reconnected</b>\nPrice feed restored."))
                    _was_down = False
                async for raw in ws:
                    global _fut_px_update_ms
                    _ws_last_msg["fut"] = time.time()
                    _fut_px_update_ms   = time.time() * 1000
                    msg = json.loads(raw)
                    st  = msg.get("stream", "")
                    d   = msg.get("data", msg)
                    if "bookTicker" in st:
                        bid = float(d.get("b") or 0)
                        ask = float(d.get("a") or 0)
                        if bid > 0: _fut_px["bid"] = bid
                        if ask > 0: _fut_px["ask"] = ask
                    elif "markPrice" in st:
                        mp = float(d.get("p") or 0)
                        if mp > 0:
                            _fut_px["mark"] = mp
                            globals()["_last_valid_mark"] = mp
                        else:
                            log("SYS", "BTC mark=0 from WS — keeping last valid", "WARN")
        except Exception as e:
            if not _was_down:
                log("SYS", f"Futures WS: {e}  -  retry 3s", "WARN")
                asyncio.create_task(_tg(
                    f"⚠ <b>Futures WS disconnected</b>\n"
                    f"Spot price feed lost  -  retrying...\n"
                    f"{type(e).__name__}: {e}"
                ))
            _was_down = True
            await asyncio.sleep(3)


async def _opt_ws():
    """
    Options WS via nbstream.binance.com/eoptions  -  per ITM CALL+PUT pair subscribes to:
      {sym}@depth10@100ms   -  best bid/ask price + qty at 100ms
      {sym}@ticker           -  mark price (mp field) at real-time
    Resubscribes live (no reconnect) when spot moves and ITM strikes shift.
    """
    import websockets
    global _chain, _ws_opt_fail_count, _ws_mark_time

    while not _all_strikes or not _expiry_6d:
        await asyncio.sleep(1)

    def _itm_streams() -> Tuple[List[str], List[str]]:
        spot = _price()
        if not spot:
            return [], []
        pair = _find_itm_pair(spot)
        if not pair:
            return [], []
        syms    = [pair[0]["symbol"], pair[1]["symbol"]]
        streams = []
        for s in syms:
            streams.append(f"{s.lower()}@depth10@100ms")
            streams.append(f"{s.lower()}@ticker")
            streams.append(f"{s.lower()}@markPrice")
        return streams, syms

    backoff = 2
    _req_id = 1
    while True:
        streams, syms = _itm_streams()
        while not streams:
            await asyncio.sleep(2)
            streams, syms = _itm_streams()

        url = f"wss://nbstream.binance.com/eoptions/stream?streams={'/'.join(streams)}"
        try:
            async with websockets.connect(url, ping_interval=None, ping_timeout=None, ssl=True) as ws:
                _was_reconnect      = _ws_opt_fail_count > 0
                _ws_opt_fail_count  = 0
                backoff = 2
                current_streams = set(streams)
                log("SYS",
                    f"Options WS connected | {syms[0]}  {syms[1]}  (depth+mark 100ms)", "OK")
                if _was_reconnect:
                    asyncio.create_task(_tg(
                        f"✅ <b>Options WS reconnected</b>\n"
                        f"Bid/ask/mark feed restored.\n"
                        f"{syms[0]}  |  {syms[1]}"
                    ))

                async for raw in ws:
                    _ws_last_msg["opt"] = time.time()
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    # Subscription responses have no "stream" key  -  check for errors
                    if "stream" not in msg:
                        if msg.get("error"):
                            log("SYS", f"Options WS subscription error: {msg['error']}", "WARN")
                        continue

                    # Live resubscribe when ITM strikes shift with spot
                    new_streams, _ = _itm_streams()
                    if new_streams and set(new_streams) != current_streams:
                        to_unsub = list(current_streams - set(new_streams))
                        to_sub   = [s for s in new_streams if s not in current_streams]
                        if to_unsub:
                            await ws.send(json.dumps(
                                {"method": "UNSUBSCRIBE", "params": to_unsub, "id": _req_id}))
                            _req_id += 1
                        if to_sub:
                            await ws.send(json.dumps(
                                {"method": "SUBSCRIBE", "params": to_sub, "id": _req_id}))
                            _req_id += 1
                        current_streams = set(new_streams)
                        log("SYS", f"Options WS: strikes shifted -> {new_streams}", "INFO")

                    # Parse depth10 / ticker / markPrice messages
                    stream = msg.get("stream", "")
                    data   = msg.get("data", {})
                    sym    = stream.split("@")[0].upper()

                    if "@depth10" in stream:
                        asks = data.get("asks", [])
                        bids = data.get("bids", [])
                        if sym not in _chain:
                            _chain[sym] = {}
                        # LKG: only update if value > 0
                        _chain_set(sym, "ask",     float(asks[0][0]) if asks else 0.0)
                        _chain_set(sym, "ask_qty", float(asks[0][1]) if asks else 0.0)
                        _chain_set(sym, "bid",     float(bids[0][0]) if bids else 0.0)
                        ask_val = _chain.get(sym, {}).get("ask", 0)
                        if ask_val > 0:
                            _last_valid_opt[sym] = ask_val
                    elif "@markPrice" in stream or "@ticker" in stream:
                        mp = float(data.get("mp") or data.get("markPrice") or 0)
                        if mp > 0:
                            if sym not in _chain:
                                _chain[sym] = {}
                            _chain_set(sym, "mark", mp)
                            _last_valid_opt[sym] = mp

        except Exception as e:
            _ws_opt_fail_count += 1
            log("SYS", f"Options WS: {e}  -  retry {backoff}s", "WARN")
            if _ws_opt_fail_count == 1:
                asyncio.create_task(_tg(
                    f"⚠ <b>Options WS disconnected</b>\n"
                    f"Bid/ask/mark feed lost. Reconnecting in {backoff}s..."
                ))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def _ticker_loop():
    """
    Primary REST data feed for options bid/ask/mark/qty.
    Options WS (nbstream.binance.com) unavailable in some regions -> REST is sole source.
    Cycle: 5s. Fetches: ticker (all BTC, bid/ask), mark (all BTC), depth (ITM pair, qty).
    Stops on HTTP 418 until ban expires.
    """
    global _last_chain_refresh, _rest_banned_until
    loop  = asyncio.get_running_loop()
    tick  = 0
    first = True
    while True:
        await asyncio.sleep(3 if first else 5)
        first = False
        tick += 1

        if time.time() < _rest_banned_until:
            remain = int(_rest_banned_until - time.time())
            if tick % 12 == 0:
                log("SYS", f"EAPI REST IP banned  -  {remain}s remaining", "WARN")
            continue

        def _eapi_get(path, params=""):
            import urllib.error
            url = f"{EAPI}{path}{'?' + params if params else ''}"
            req = urllib.request.Request(url, headers={"User-Agent": "StraddleBot/1.0"})
            try:
                with urllib.request.urlopen(req, timeout=8) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                try:    eb = e.read().decode()
                except: eb = ""
                raise RuntimeError(f"HTTP {e.code}  -  {eb}") from e

        try:
            global _rest_last_ok, _rest_fail_streak
            spot = _price()

            # 1. Ticker: bid / ask for all active BTC symbols (LKG: only update if > 0)
            ticks     = await loop.run_in_executor(None, lambda: _eapi_get("/eapi/v1/ticker"))
            new_found = False
            if isinstance(ticks, list):
                for t in ticks:
                    sym = t.get("symbol", "")
                    if not sym.startswith("BTC-"):
                        continue
                    if sym not in _sym_meta:
                        new_found = True
                        continue
                    if sym in _chain:
                        try:
                            bid  = float(t.get("bidPrice") or 0)
                            ask  = float(t.get("askPrice") or 0)
                            meta = _sym_meta[sym]
                            intr = _intr(meta["strike"], spot, meta["side"])
                            _chain_set(sym, "bid", bid)
                            _chain_set(sym, "ask", ask)
                            if intr > 0:
                                _chain[sym]["intrinsic"] = round(intr, 2)
                        except (TypeError, ValueError):
                            pass
                if new_found and time.time() - _last_chain_refresh > 120:
                    log("SYS", "New option symbols detected  -  refreshing chain", "WARN")
                    _last_chain_refresh = time.time()
                    asyncio.create_task(_fetch_chain())

            # 2. Mark prices (LKG: only update if > 0)
            marks = await loop.run_in_executor(None, lambda: _eapi_get("/eapi/v1/mark"))
            if isinstance(marks, list):
                for m in marks:
                    sym = m.get("symbol", "")
                    if sym in _chain:
                        _chain_set(sym, "mark", float(m.get("markPrice") or 0))

            # 3. Depth: ask_qty / bid_qty for ITM pair only (LKG: keep old if fetch fails)
            if spot:
                pair = _find_itm_pair(spot)
                if pair:
                    for opt in pair:
                        sym = opt["symbol"]
                        try:
                            depth = await loop.run_in_executor(
                                None, lambda s=sym: _eapi_get("/eapi/v1/depth", f"symbol={s}"))
                            asks = depth.get("asks", [])
                            bids = depth.get("bids", [])
                            _chain_set(sym, "ask_qty", float(asks[0][1]) if asks else 0.0)
                            _chain_set(sym, "bid_qty", float(bids[0][1]) if bids else 0.0)
                        except Exception:
                            # Brief fetch failure: keep last known qty (do NOT zero it)
                            aq_age = int(time.time()*1000) - _chain_ms.get(sym, {}).get("ask_qty", 0)
                            if aq_age > 30_000:
                                # Data is genuinely stale (>30s) — zero out so conditions block
                                if sym in _chain:
                                    _chain[sym]["ask_qty"] = 0.0

            _rest_last_ok     = time.time()
            _rest_fail_streak = 0

            # 4. BTC futures price — REST fallback when WS is stale.
            # Weight=1, runs only when needed, well within FAPI rate limit.
            if _feed_age_s("fut") > 8:
                try:
                    global _fut_px_update_ms
                    def _fapi_premium():
                        req = urllib.request.Request(
                            f"{FAPI}/fapi/v1/premiumIndex?symbol=BTCUSDT",
                            headers={"User-Agent": "StraddleBot/1.0"})
                        with urllib.request.urlopen(req, timeout=5) as r:
                            return json.loads(r.read())
                    _pm = await loop.run_in_executor(None, _fapi_premium)
                    _mp = float(_pm.get("markPrice") or 0)
                    _lp = float(_pm.get("lastPrice")  or 0)
                    if _mp > 0:
                        _fut_px["mark"]              = _mp
                        globals()["_last_valid_mark"] = _mp
                        _fut_px_update_ms             = time.time() * 1000
                        _ws_last_msg["fut"]           = time.time()   # data fresh from REST
                        if _lp > 0:
                            _fut_px["last"] = _lp
                        log("SYS", f"BTC mark via FAPI REST (WS stale): {_mp:.0f}", "WARN")
                except Exception as _fe:
                    log("SYS", f"FAPI mark-price REST fallback failed: {_fe}", "WARN")

        except Exception as e:
            err_s = str(e)
            if "429" in err_s or "Too Many Requests" in err_s:
                _rest_banned_until = time.time() + 60
                log("SYS", "EAPI REST HTTP 429  -  rate limited. Backing off 60s.", "WARN")
                asyncio.create_task(_tg("⚠ <b>EAPI 429 rate limit</b>\nREST feed paused 60s. Bot continuing."))
                continue
            if _is_rate_limited(e):
                _rest_banned_until = _parse_418_ban(err_s)
                remain = max(0, int(_rest_banned_until - time.time()))
                log("SYS", f"EAPI REST HTTP 418  -  IP banned for {remain}s. REST paused.", "WARN")
                continue
            _rest_fail_streak += 1
            log("SYS", f"Ticker REST (fail #{_rest_fail_streak}): {e}", "WARN")
            if _rest_fail_streak in (5, 20, 60):
                asyncio.create_task(_tg(
                    f"⚠ <b>REST feed {_rest_fail_streak} failures in a row</b>\n"
                    f"Options data may be stale. Investigating...\n{e}"
                ))


# -----------------------------------------------------------------------------
#  PAPER FILL MONITOR  (only runs when PAPER_TRADE = True)
# -----------------------------------------------------------------------------
async def _paper_fill_monitor():
    while True:
        await asyncio.sleep(0.2)
        mark = _price()
        if mark <= 0:
            continue
        for oid, o in list(_paper_orders.items()):
            if o.get("status") != "NEW":
                continue
            asset = o.get("asset")
            if asset == "future":
                side  = o.get("side")
                lpx   = o.get("price", 0)
                fills = (side == "BUY"  and mark <= lpx) or \
                        (side == "SELL" and mark >= lpx)
                if fills:
                    _paper_orders[oid]["status"]     = "FILLED"
                    _paper_orders[oid]["fill_price"] = lpx
                    log("BOT",
                        f"[PAPER] Futures LIMIT filled: {side} {o.get('pos_side')} @ {lpx:.0f}",
                        "OK")
            elif asset == "option" and o.get("side") == "SELL":
                sym = o.get("symbol", "")
                bid = _chain.get(sym, {}).get("bid", 0.0)
                lpx = o.get("price", 0)
                if bid > 0 and bid >= lpx:
                    _paper_orders[oid]["status"]     = "FILLED"
                    _paper_orders[oid]["fill_price"] = lpx
                    log("BOT",
                        f"[PAPER] Option SELL LIMIT filled: {sym} @ {lpx:.0f}", "OK")


# ╔╗
# â•'                        TASK SUPERVISOR                                    â•'
# ╚
async def _supervised(fn, name: str, delay: float = 3.0):
    fail_count = 0
    while True:
        try:
            await fn()
            fail_count = 0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            fail_count += 1
            log("SYS", f"[{name}] crashed (#{fail_count}): {e!r}  -  restart in {delay:.0f}s", "ERROR")
            if fail_count in (1, 5, 20):
                asyncio.create_task(_tg(
                    f"⚠ <b>{name} error (#{fail_count})</b>\n"
                    f"{type(e).__name__}: {e}\n"
                    f"Restarting in {delay:.0f}s..."
                ))
            await asyncio.sleep(delay)


def _fire(coro) -> asyncio.Task:
    """Schedule a coroutine as a background task; log any uncaught exception."""
    async def _wrapper():
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log("SYS", f"Background task error: {e!r}", "ERROR")
    return asyncio.create_task(_wrapper())


# ╔╗
# â•'                        STRADDLE BOT  (state machine)                      â•'
# ╚
class StraddleBot:
    """
    State machine for one ITM straddle position.

    States
    "€"€"€"€"€"€
    SLEEP       : outside window or session already used
    SCANNING    : polling for entry conditions every SCAN_INTERVAL seconds
    EXECUTING   : FOK orders in flight (handled by coroutine, tick skips)
    MANAGING    : position open; futures limits placed; monitoring fills + TP
    DONE        : squareoff complete, all positions settled for this session
    """

    def __init__(self):
        self.state     = "SLEEP"
        self.sess_date = ""
        self.entry_ts  = ""

        # â"€â"€ Options â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        self.call_sym   = ""      # e.g. BTC-260703-62000-C
        self.call_fill  = 0.0     # fill price (USDT/BTC)
        self.call_qty   = 0.0
        self.call_open  = False   # still holding (not closed via protection)

        self.put_sym    = ""
        self.put_fill   = 0.0
        self.put_qty    = 0.0
        self.put_open   = False

        self.total_prem = 0.0     # call_fill + put_fill

        # â"€â"€ Futures limit orders â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        self.long_limit_px  = 0.0
        self.short_limit_px = 0.0
        self.long_oid        = 0
        self.short_oid       = 0
        self.long_filled     = False
        self.short_filled    = False
        self.long_entry      = 0.0   # actual fill price
        self.short_entry     = 0.0
        self.long_qty        = 0.0
        self.short_qty       = 0.0

        # â"€â"€ Futures TP orders â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        self.long_tp_oid     = 0
        self.short_tp_oid    = 0
        self.long_tp_px      = 0.0
        self.short_tp_px     = 0.0
        self.long_tp_done    = False
        self.short_tp_done   = False

        # â"€â"€ Squareoff tracking â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        self._sq_done       = False  # soft squareoff fired (SQ_START)
        self._hard_sq_fired = False  # hard squareoff fired (SQ_HARD)

        # â"€â"€ Execution guard â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        self._executing      = False

        #  DB / paper tracking
        self._session_id    = 0     # SQLite sessions.id for this run
        self._call_oid      = 0     # paper order IDs saved from last FOK attempt
        self._put_oid       = 0
        self._last_snap_ts  = 0.0   # last pnl_snapshot write time
        self.call_sq_exit   = 0.0   # squareoff exit prices (for records)
        self.put_sq_exit    = 0.0
        self.long_sq_exit   = 0.0
        self.short_sq_exit  = 0.0

        # â"€â"€ Throttle timestamps â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        self.entry_ts_ms     = 0      # epoch-ms when futures limits were placed

        # â"€â"€ Running session high/low (sub-second accuracy, updated every 0.5s) â"€â"€â"€
        self.session_high    = 0.0
        self.session_low     = float("inf")

        self._last_scan_ts   = 0.0
        self._last_poll_ts   = 0.0
        self._last_log_ts    = 0.0

    # 
    #  MAIN LOOP
    # 
    async def run(self):
        while True:
            await asyncio.sleep(0.5)
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log("BOT", f"Tick error: {e!r}", "ERROR")
            if self.state == "DONE":
                log("SYS", "Session settled — flushing Telegram (2 min), then reset for next window", "OK")
                await asyncio.sleep(120)   # allow in-flight Telegram messages to send
                await self._reset_for_next_session()

    async def _reset_for_next_session(self):
        """
        Called after DONE. Resets all session state so the bot loops back to SLEEP
        and is ready to trade the next day without a process restart.
        """
        global _paper_orders
        prev_sess_date = self.sess_date   # remember which expiry was just traded

        # Full state reset — clears all positions, order IDs, PnL tracking
        self.__init__()

        # Preserve sess_date so already_traded stays True for rest of today
        # (also persisted in LAST_TRADED_EXPIRY config, so survives restart too).
        self.sess_date = prev_sess_date

        # Clear stale paper engine orders from finished session.
        # Wallet is NOT reset — it carries cumulative PnL across sessions.
        _paper_orders.clear()

        log("SYS", f"Bot reset → SLEEP  (traded={prev_sess_date}, waiting for next window)", "OK")
        await _tg(
            f"{_PTAG}🔄 <b>Bot reset → SLEEP</b>\n"
            f"Session settled. Waiting for next trading window."
        )

    async def _tick(self):
        now = datetime.now(IST)
        h, m = now.hour, now.minute

        # Hot-reload config whenever dashboard marks it dirty — runs in ANY state
        if _db:
            try:
                row = _db.execute("SELECT value FROM config WHERE key='_config_dirty'").fetchone()
                if row and str(row[0]) == "1":
                    _db_load_config()
                    _db.execute("UPDATE config SET value='0' WHERE key='_config_dirty'")
                    _db.commit()
                    log("SYS", "Config hot-reloaded from DB (settings saved via dashboard)", "OK")
            except Exception:
                pass

        if self.state == "SLEEP":
            await self._tick_sleep(h, m)
        elif self.state == "SCANNING":
            await self._tick_scan(h, m)
        elif self.state == "EXECUTING":
            pass   # coroutine running; do nothing until it completes
        elif self.state == "MANAGING":
            await self._tick_manage(h, m)
        # DONE: nothing to do

        # Write live bot state + diagnostics every tick (single-row UPSERT, O(1))
        if _db:
            try:
                detail_json = json.dumps(self._build_status_detail())
                _db.execute(
                    "INSERT OR REPLACE INTO bot_status (id,ts,state,btc_mark,session_id,detail) "
                    "VALUES (1,?,?,?,?,?)",
                    (_ts(), self.state, _price() or _last_valid_mark, self._session_id, detail_json)
                )
                _db.commit()
            except Exception:
                pass

        # Periodic status log
        if self.state not in ("SLEEP", "DONE") and time.time() - self._last_log_ts > 30:
            self._last_log_ts = time.time()
            self._log_status()

    # 
    #  SLEEP
    # 
    async def _tick_sleep(self, h: int, m: int):
        if _has_open_positions:
            return   # blocked: existing positions detected at startup
        now_min = _to_min(h, m)
        if not _in_entry_window(now_min):
            return
        _last_traded = self.sess_date or _db_get_config("LAST_TRADED_EXPIRY") or ""
        if _last_traded == _expiry_iso():
            return   # already traded this expiry (persisted across restarts)
        _fut_stale = _feed_age_s("fut") > 10
        _opt_stale = _feed_age_s("opt") > 30 and (time.time() - _rest_last_ok) > 30
        if not _all_strikes or not _price() or _fut_stale or _opt_stale:
            return   # data not ready

        # Activate only when within 24h of the next expiry.
        # Session = 13:31 day N → 13:30 day N+1.  Window can span both dates.
        # "Same-day expiry" means < 24h remaining — NOT a calendar-date comparison.
        if _expiry_ms:
            hours_to_expiry = (_expiry_ms - int(time.time() * 1000)) / 3_600_000
            if hours_to_expiry < 0 or hours_to_expiry >= 24:
                return   # more than 24h to expiry — stay SLEEP
        self.state = "SCANNING"
        log("BOT", "Window open -> SCANNING", "STATE")
        _db_event_insert(0, "STATE_SCANNING", f"expiry={_expiry_iso()} spot={round(_price() or 0,0):.0f}")
        start_m   = _to_min(*WINDOW_START)
        end_m     = _to_min(*WINDOW_END)
        overnight = start_m > end_m
        if not overnight:
            mins_left = end_m - now_min
        else:
            mins_left = ((24*60 - now_min) + end_m) if now_min >= start_m else (end_m - now_min)
        asyncio.create_task(_tg(
            f" <b>Entry Window OPEN</b>\n"
            f"Scanning for ITM straddle setup...\n"
            f"Window closes in {_fmt_hm(mins_left)}\n"
            f"Squareoff starts {SQUAREOFF_START[0]:02d}:{SQUAREOFF_START[1]:02d} IST"
        ))

    # 
    #  SCANNING
    # 
    async def _tick_scan(self, h: int, m: int):
        now_min = _to_min(h, m)

        # Window closed before we entered
        if not _in_entry_window(now_min):
            self.state = "SLEEP"
            log("BOT", "Window closed -> SLEEP", "STATE")
            asyncio.create_task(_tg(
                f" <b>Entry Window CLOSED</b>\n"
                f"No valid setup found during window.\n"
                f"Bot sleeping until next session."
            ))
            return

        # Squareoff time reached without entry  -  only relevant on expiry day
        if _is_expiry_day() and now_min >= _to_min(*SQUAREOFF_START):
            self.state = "SLEEP"
            log("BOT", "Squareoff time reached without entry -> SLEEP", "STATE")
            asyncio.create_task(_tg(
                f" <b>Squareoff time  -  no position taken</b>\n"
                f"Window passed without a valid setup.\n"
                f"Bot sleeping."
            ))
            return

        if time.time() - self._last_scan_ts < SCAN_INTERVAL:
            return
        self._last_scan_ts = time.time()

        spot = _price()
        if not spot:
            return

        pair = _find_itm_pair(spot)
        if pair is None:
            return

        call_opt, put_opt = pair
        ok, reason = _check_conditions(call_opt, put_opt)
        if not ok:
            if time.time() - self._last_log_ts > 10:
                self._last_log_ts = time.time()
                detail = _conditions_detail(call_opt, put_opt)
                log("BOT", f"Waiting: {reason}\n{detail}", "INFO")
            return

        # All conditions pass -> execute
        if self._executing or self.state != "SCANNING":
            return   # safety: never double-execute
        log("BOT",
            f"✅ All conditions met\n"
            f"   CALL {call_opt['symbol']} ask={call_opt['ask']:.0f}  "
            f"mark={call_opt['mark']:.0f}  aqty={call_opt['ask_qty']:.3f}\n"
            f"   PUT  {put_opt['symbol']} ask={put_opt['ask']:.0f}  "
            f"mark={put_opt['mark']:.0f}  aqty={put_opt['ask_qty']:.3f}\n"
            f"   Total ask={call_opt['ask'] + put_opt['ask']:.0f}  "
            f"gap={put_opt['strike'] - call_opt['strike']:.0f}", "OK")
        self.state    = "EXECUTING"
        self._executing = True
        _fire(self._execute_straddle(call_opt, put_opt))

    # 
    #  EXECUTE  -  simultaneous FOK orders
    # 
    async def _execute_straddle(self, call_opt: dict, put_opt: dict):
        self._executing = True
        try:
            call_ask_ticked = _opt_tick(call_opt["ask"])
            put_ask_ticked  = _opt_tick(put_opt["ask"])

            log("BOT", f"Firing FOK: CALL@{call_ask_ticked}  PUT@{put_ask_ticked}", "TRADE")

            # Fire both FOK simultaneously
            results = await asyncio.gather(
                option_order(call_opt["symbol"], "BUY", TRADE_QTY,
                             call_ask_ticked, "LIMIT", "FOK"),
                option_order(put_opt["symbol"],  "BUY", TRADE_QTY,
                             put_ask_ticked,  "LIMIT", "FOK"),
                return_exceptions=True
            )
            call_res, put_res = results

            await asyncio.sleep(1.5)   # brief pause for fills to register

            # â"€â"€ Query fill status â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
            call_filled, call_oid = False, 0
            put_filled,  put_oid  = False, 0

            if not isinstance(call_res, Exception):
                call_oid = int(call_res.get("orderId", 0))
            else:
                log("BOT", f"CALL FOK placement error: {call_res}", "ERROR")

            if not isinstance(put_res, Exception):
                put_oid = int(put_res.get("orderId", 0))
            else:
                log("BOT", f"PUT FOK placement error: {put_res}", "ERROR")

            self._call_oid = call_oid   # save for _on_both_filled / _handle_partial_only
            self._put_oid  = put_oid

            if call_oid:
                try:
                    q = {}
                    for _att in range(3):
                        q = await option_query(call_opt["symbol"], call_oid)
                        if q.get("status") in ("FILLED", "EXPIRED", "CANCELLED"):
                            break
                        await asyncio.sleep(1.0)
                    if q.get("status") == "FILLED":
                        call_filled     = True
                        self.call_sym   = call_opt["symbol"]
                        self.call_fill  = float(q.get("avgPrice") or call_ask_ticked)
                        self.call_qty   = float(q.get("executedQty") or TRADE_QTY)
                        self.call_open  = True
                    else:
                        # FOK not filled — record with session_id=0 (no session yet)
                        _db_order_insert(0, call_oid, call_opt["symbol"], "option",
                                         "CALL_ENTRY", "BUY", "FOK", TRADE_QTY,
                                         call_ask_ticked, 0, "EXPIRED")
                except Exception as e:
                    log("BOT", f"CALL order query: {e}", "WARN")

            if put_oid:
                try:
                    q = {}
                    for _att in range(3):
                        q = await option_query(put_opt["symbol"], put_oid)
                        if q.get("status") in ("FILLED", "EXPIRED", "CANCELLED"):
                            break
                        await asyncio.sleep(1.0)
                    if q.get("status") == "FILLED":
                        put_filled    = True
                        self.put_sym  = put_opt["symbol"]
                        self.put_fill = float(q.get("avgPrice") or put_ask_ticked)
                        self.put_qty  = float(q.get("executedQty") or TRADE_QTY)
                        self.put_open = True
                    else:
                        _db_order_insert(0, put_oid, put_opt["symbol"], "option",
                                         "PUT_ENTRY", "BUY", "FOK", TRADE_QTY,
                                         put_ask_ticked, 0, "EXPIRED")
                except Exception as e:
                    log("BOT", f"PUT order query: {e}", "WARN")

            log("BOT",
                f"FOK result: CALL={'FILLED' if call_filled else 'FAILED'}  "
                f"PUT={'FILLED' if put_filled else 'FAILED'}", "TRADE")

            if call_filled and put_filled:
                self.total_prem = self.call_fill + self.put_fill
                await self._on_both_filled()

            elif not call_filled and not put_filled:
                # Both failed  -  back to scanning immediately
                log("BOT", "Both FOK failed -> back to SCANNING", "WARN")
                self.state = "SCANNING"
                self._last_scan_ts = 0   # allow immediate re-scan

            elif call_filled and not put_filled:
                log("BOT", "CALL filled / PUT failed -> retrying PUT", "WARN")
                put_ok = await self._retry_single(put_opt["symbol"], "PUT")
                if put_ok:
                    self.total_prem = self.call_fill + self.put_fill
                    await self._on_both_filled()
                else:
                    await self._handle_partial_only("CALL")

            else:
                log("BOT", "PUT filled / CALL failed -> retrying CALL", "WARN")
                call_ok = await self._retry_single(call_opt["symbol"], "CALL")
                if call_ok:
                    self.total_prem = self.call_fill + self.put_fill
                    await self._on_both_filled()
                else:
                    await self._handle_partial_only("PUT")

        except Exception as e:
            log("BOT", f"Execute straddle error: {e!r}", "ERROR")
            await _tg(f" <b>Execute error</b>\n{e}\nCheck terminal  -  returning to SCANNING")
            self.state = "SCANNING"
        finally:
            self._executing = False

    async def _retry_single(self, sym: str, leg: str) -> bool:
        """
        Retry buying a single failed leg for up to RETRY_TIMEOUT seconds.
        Returns True if filled, False if timed out.
        """
        # Safety guard: confirm we don't already hold the position before retrying
        try:
            existing = await _get(EAPI, "/eapi/v1/position")
            for p in (existing if isinstance(existing, list) else []):
                if p.get("symbol") == sym and float(p.get("quantity") or 0) >= TRADE_QTY:
                    fill_px = float(p.get("entryPrice") or 0)
                    log("BOT", f"Retry {leg}: already holding {sym} @ {fill_px:.0f}  -  skip retry", "WARN")
                    if leg == "CALL":
                        self.call_sym  = sym; self.call_fill = fill_px
                        self.call_qty  = TRADE_QTY; self.call_open = True
                    else:
                        self.put_sym   = sym; self.put_fill  = fill_px
                        self.put_qty   = TRADE_QTY; self.put_open  = True
                    return True
        except Exception as e:
            log("BOT", f"Retry {leg}: position safety check failed: {e}", "WARN")

        deadline = time.time() + RETRY_TIMEOUT
        attempt  = 0
        while time.time() < deadline:
            opt = _chain.get(sym, {})
            ask  = opt.get("ask") or 0.0
            mark = opt.get("mark") or 0.0
            aqty = opt.get("ask_qty") or 0.0

            if ask > 0 and mark > 0:
                spread = abs(ask - mark) / mark * 100
                if spread <= MAX_ASK_MARK_RETRY and aqty >= TRADE_QTY:
                    ticked = _opt_tick(ask)
                    attempt += 1
                    log("BOT", f"Retry {leg} #{attempt} FOK @ {ticked}  "
                               f"spread={spread:.1f}%  aqty={aqty:.3f}", "TRADE")
                    try:
                        res = await option_order(sym, "BUY", TRADE_QTY,
                                                 ticked, "LIMIT", "FOK")
                        oid = int(res.get("orderId", 0))
                        await asyncio.sleep(1.5)
                        if oid:
                            q = await option_query(sym, oid)
                            if q.get("status") == "FILLED":
                                fill_px = float(q.get("avgPrice") or ticked)
                                if leg == "CALL":
                                    self.call_sym  = sym
                                    self.call_fill = fill_px
                                    self.call_qty  = float(q.get("executedQty") or TRADE_QTY)
                                    self.call_open = True
                                else:
                                    self.put_sym   = sym
                                    self.put_fill  = fill_px
                                    self.put_qty   = float(q.get("executedQty") or TRADE_QTY)
                                    self.put_open  = True
                                log("BOT", f"✅ Retry {leg} FILLED @ {fill_px:.0f}", "OK")
                                return True
                            log("BOT", f"Retry {leg} FOK not filled", "WARN")
                    except Exception as e:
                        log("BOT", f"Retry {leg} order error: {e}", "WARN")
                else:
                    log("BOT", f"Retry {leg}: spread={spread:.1f}% aqty={aqty:.3f}  -  waiting", "INFO")
            await asyncio.sleep(2)

        log("BOT", f"Retry {leg} timed out after {RETRY_TIMEOUT}s", "WARN")
        return False

    async def _handle_partial_only(self, filled_leg: str):
        """One leg filled, other could not be retried. Alert and manage single option."""
        msg = (f"⚠ <b>Partial straddle  -  only {filled_leg} filled</b>\n"
               f"Retry timed out. Managing single option until squareoff.\n"
               f"No futures setup (no valid entry levels without both legs).")
        log("BOT", f"Partial entry: only {filled_leg} open  -  no futures", "CRIT")
        await _tg(msg)

        self.total_prem = (self.call_fill or 0.0) + (self.put_fill or 0.0)
        self.sess_date  = _expiry_iso()
        self.entry_ts   = datetime.now(IST).strftime("%H:%M:%S")

        # Create a session so all downstream DB writes (snapshots, events) have a valid ID
        exp_sym = _expiry_6d
        exp_dt  = (datetime.fromtimestamp(_expiry_ms / 1000, tz=IST).strftime("%Y-%m-%d %H:%M")
                   if _expiry_ms else "")
        self._session_id = _db_session_create(exp_sym, exp_dt)

        if filled_leg == "CALL" and self.call_sym:
            _db_session_update(self._session_id,
                call_sym=self.call_sym, call_fill=self.call_fill,
                call_qty=self.call_qty, total_premium=self.total_prem)
            _db_order_insert(self._session_id, self._call_oid, self.call_sym, "option",
                             "CALL_ENTRY", "BUY", "FOK", self.call_qty,
                             0, self.call_fill, "FILLED", _ts())
        elif filled_leg == "PUT" and self.put_sym:
            _db_session_update(self._session_id,
                put_sym=self.put_sym, put_fill=self.put_fill,
                put_qty=self.put_qty, total_premium=self.total_prem)
            _db_order_insert(self._session_id, self._put_oid, self.put_sym, "option",
                             "PUT_ENTRY", "BUY", "FOK", self.put_qty,
                             0, self.put_fill, "FILLED", _ts())

        _db_event_insert(self._session_id, "PARTIAL_STRADDLE",
                         f"Only {filled_leg} filled. No futures. prem={self.total_prem:.0f}")
        self.state = "MANAGING"

    # 
    #  BOTH FILLED  -  set up futures limits
    # 
    async def _on_both_filled(self):
        global _paper_wallet_balance
        call_strike = float(self.call_sym.split("-")[2])
        put_strike  = float(self.put_sym.split("-")[2])
        tp          = self.total_prem

        long_px  = round(put_strike  - tp, 1)   # LONG limit: PUT_strike âˆ' total_prem
        short_px = round(call_strike + tp, 1)   # SHORT limit: CALL_strike + total_prem

        self.long_limit_px  = long_px
        self.short_limit_px = short_px
        self.long_qty       = TRADE_QTY
        self.short_qty      = TRADE_QTY

        # Deduct options premium from paper wallet (cumulative — not reset per session)
        if PAPER_TRADE:
            prem_cost = tp * self.call_qty
            wallet_before_deduction = _paper_wallet_balance
            _paper_wallet_balance = max(0.0, _paper_wallet_balance - prem_cost)
            _db_set_config("PAPER_WALLET_BALANCE", str(round(_paper_wallet_balance, 2)))
            self._wallet_before = wallet_before_deduction
            log("BOT", f"Paper wallet: -{prem_cost:.2f} (options prem)  bal={_paper_wallet_balance:.2f}", "INFO")

        log("BOT",
            f"Straddle open:\n"
            f"   CALL {self.call_sym} @ {self.call_fill:.0f}\n"
            f"   PUT  {self.put_sym}  @ {self.put_fill:.0f}\n"
            f"   Total premium = {tp:.0f} USDT\n"
            f"   LONG  LIMIT @ {long_px:.0f}  (= {put_strike:.0f} âˆ' {tp:.0f})\n"
            f"   SHORT LIMIT @ {short_px:.0f}  (= {call_strike:.0f} + {tp:.0f})", "TRADE")

        # TP distance = prem + TV; TV = prem - gap; floor at 50 pts to prevent degenerate case
        _cs      = float(self.call_sym.split("-")[2])
        _ps      = float(self.put_sym.split("-")[2])
        _tp_dist = max(self.total_prem + (self.total_prem - (_ps - _cs)), 50.0)
        long_tp_px  = round(long_px  + _tp_dist, 1)
        short_tp_px = round(short_px - _tp_dist, 1)

        # Place LONG entry + TP together
        try:
            r = await futures_limit("BUY", TRADE_QTY, long_px, "LONG")
            self.long_oid = int(r.get("orderId", 0))
            log("BOT", f"LONG entry LIMIT @ {long_px:.0f}  oid={self.long_oid}", "OK")
        except Exception as e:
            log("BOT", f"LONG entry LIMIT failed: {e}", "ERROR")
            await _tg(f"⚠ LONG entry LIMIT failed:\n{e}")

        try:
            r = await futures_limit("SELL", TRADE_QTY, long_tp_px, "LONG")
            self.long_tp_oid = int(r.get("orderId", 0))
            self.long_tp_px  = long_tp_px
            log("BOT", f"LONG TP pre-placed @ {long_tp_px:.0f}  oid={self.long_tp_oid}", "OK")
        except Exception as e:
            log("BOT", f"LONG TP pre-placement failed: {e}", "ERROR")

        # Place SHORT entry + TP together
        try:
            r = await futures_limit("SELL", TRADE_QTY, short_px, "SHORT")
            self.short_oid = int(r.get("orderId", 0))
            log("BOT", f"SHORT entry LIMIT @ {short_px:.0f}  oid={self.short_oid}", "OK")
        except Exception as e:
            log("BOT", f"SHORT entry LIMIT failed: {e}", "ERROR")
            await _tg(f"⚠ SHORT entry LIMIT failed:\n{e}")

        try:
            r = await futures_limit("BUY", TRADE_QTY, short_tp_px, "SHORT")
            self.short_tp_oid = int(r.get("orderId", 0))
            self.short_tp_px  = short_tp_px
            log("BOT", f"SHORT TP pre-placed @ {short_tp_px:.0f}  oid={self.short_tp_oid}", "OK")
        except Exception as e:
            log("BOT", f"SHORT TP pre-placement failed: {e}", "ERROR")

        self.sess_date   = _expiry_iso()
        self.entry_ts    = datetime.now(IST).strftime("%H:%M:%S")
        self.entry_ts_ms = int(time.time() * 1000)
        self.state       = "MANAGING"

        #  DB: atomic write — session + all orders in one BEGIN/COMMIT
        if _db:
            try:
                exp_sym = _expiry_6d
                exp_dt  = (datetime.fromtimestamp(_expiry_ms / 1000, tz=IST).strftime("%Y-%m-%d %H:%M")
                           if _expiry_ms else "")
                now_s = _ts()
                def _oi(oid, sym, at, ll, sd, ot, qty, lx=0, fx=0, st="NEW", fat=None):
                    _db.execute(
                        "INSERT INTO orders "
                        "(session_id,paper_order_id,symbol,asset_type,leg_label,"
                        "side,order_type,qty,limit_price,fill_price,status,"
                        "placed_at,filled_at,cancel_reason,updated_at) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (self._session_id, oid, sym, at, ll, sd, ot, qty,
                         lx, fx, st, now_s, fat, None, now_s)
                    )
                _wb = getattr(self, "_wallet_before", _paper_wallet_balance)
                with _db:
                    cur = _db.execute(
                        "INSERT INTO sessions "
                        "(date,expiry_sym,expiry_dt,state,start_dt,entry_ts_ms,"
                        "call_sym,put_sym,call_fill,put_fill,call_qty,put_qty,total_premium,"
                        "entry_btc,tp_dist,wallet_before) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (datetime.now(IST).strftime("%Y-%m-%d"), exp_sym, exp_dt,
                         "ACTIVE", now_s, self.entry_ts_ms,
                         self.call_sym, self.put_sym,
                         self.call_fill, self.put_fill,
                         self.call_qty, self.put_qty, self.total_prem,
                         round(_price() or 0, 1), round(_tp_dist, 2), round(_wb, 2))
                    )
                    self._session_id = cur.lastrowid
                    _oi(self._call_oid, self.call_sym, "option", "CALL_ENTRY",
                        "BUY", "FOK", self.call_qty, 0, self.call_fill, "FILLED", now_s)
                    _oi(self._put_oid,  self.put_sym,  "option", "PUT_ENTRY",
                        "BUY", "FOK", self.put_qty,  0, self.put_fill,  "FILLED", now_s)
                    if self.long_oid:
                        _oi(self.long_oid, "BTCUSDT", "future", "LONG_ENTRY",
                            "BUY", "LIMIT", TRADE_QTY, long_px)
                    if self.long_tp_oid:
                        _oi(self.long_tp_oid, "BTCUSDT", "future", "LONG_TP",
                            "SELL", "LIMIT", TRADE_QTY, long_tp_px)
                    if self.short_oid:
                        _oi(self.short_oid, "BTCUSDT", "future", "SHORT_ENTRY",
                            "SELL", "LIMIT", TRADE_QTY, short_px)
                    if self.short_tp_oid:
                        _oi(self.short_tp_oid, "BTCUSDT", "future", "SHORT_TP",
                            "BUY", "LIMIT", TRADE_QTY, short_tp_px)
                    _db.execute(
                        "INSERT INTO events (session_id,ts,event_type,detail) "
                        "VALUES (?,?,?,?)",
                        (self._session_id, now_s, "STRADDLE_ENTERED",
                         f"CALL@{self.call_fill:.0f} PUT@{self.put_fill:.0f}"
                         f" prem={self.total_prem:.0f} tp_dist={_tp_dist:.0f} btc={round(_price() or 0,0):.0f}")
                    )
                    # Wallet ledger: PREMIUM_DEBIT
                    _db.execute(
                        "INSERT INTO wallet_ledger (ts,session_id,type,amount,balance_after,note) "
                        "VALUES (?,?,?,?,?,?)",
                        (now_s, self._session_id, "PREMIUM_DEBIT",
                         -round(self.total_prem * self.call_qty, 4),
                         round(_paper_wallet_balance, 4),
                         f"CALL@{self.call_fill:.0f} PUT@{self.put_fill:.0f} qty={self.call_qty}")
                    )
            except Exception as _dbe:
                log("BOT", f"Session DB write CRITICAL — in-memory OK, session_id=0: {_dbe!r}", "CRIT")
                self._session_id = 0

        await _tg(
            f"{_PTAG} <b>Straddle ENTERED</b>  [{self.entry_ts} IST]\n"
            f"─────────────────────────────────────────────────────────────────────\n"
            f" CALL : {self.call_sym}  @ <b>{self.call_fill:.0f}</b>\n"
            f" PUT  : {self.put_sym}  @ <b>{self.put_fill:.0f}</b>\n"
            f" Premium : <b>{tp:.0f}</b> USDT\n\n"
            f"📊 Futures:\n"
            f"  LONG  entry @ <b>{long_px:.0f}</b>  TP @ <b>{long_tp_px:.0f}</b>\n"
            f"  SHORT entry @ <b>{short_px:.0f}</b>  TP @ <b>{short_tp_px:.0f}</b>"
        )

    # 
    #  MANAGING
    # 
    async def _tick_manage(self, h: int, m: int):
        now_min = _to_min(h, m)

        # Determine if squareoff should fire (handles evening window + midnight crossing)
        _sq_h, _sq_m = SQUAREOFF_START
        _past_sq = False
        if self.entry_ts_ms > 0:
            # Primary: squareoff at SQ_TIME on the entry day; if SQ_TIME ≤ entry time, push to next day
            _entry_dt = datetime.fromtimestamp(self.entry_ts_ms / 1000, tz=IST)
            _sq_dt    = _entry_dt.replace(hour=_sq_h, minute=_sq_m, second=0, microsecond=0)
            if _sq_dt <= _entry_dt:
                _sq_dt += timedelta(days=1)
            _past_sq = datetime.now(IST) >= _sq_dt
        elif self._session_id == 0:
            # Orphaned session (DB write failed at entry): squareoff whenever outside entry window
            _past_sq = not _in_entry_window(now_min)
        else:
            # Fallback: same-day sq_time AND expiry approaching within 24h
            _hours_left = (_expiry_ms - int(time.time() * 1000)) / 3_600_000 if _expiry_ms else 25
            _past_sq = now_min >= _to_min(_sq_h, _sq_m) and 0 < _hours_left < 24

        # SQUAREOFF_HARD — absolute deadline: reset _sq_done and re-fire if still MANAGING
        _hq_h, _hq_m = SQUAREOFF_HARD
        if self.entry_ts_ms > 0:
            _hard_dt = _entry_dt.replace(hour=_hq_h, minute=_hq_m, second=0, microsecond=0)
            if _hard_dt <= _entry_dt:
                _hard_dt += timedelta(days=1)
            _past_hard = datetime.now(IST) >= _hard_dt
        elif self._session_id == 0:
            _past_hard = not _in_entry_window(now_min)
        else:
            _past_hard = _to_min(h, m) >= _to_min(_hq_h, _hq_m)

        if _past_hard and not self._hard_sq_fired:
            self._hard_sq_fired = True
            self._sq_done = False   # allow squareoff to run even if soft already fired
            log("BOT", "SQUAREOFF_HARD deadline — forcing squareoff now", "CRIT")
            asyncio.create_task(self._do_squareoff())
            return

        if _past_sq:
            if not self._sq_done:
                log("BOT", "Squareoff time -> running _do_squareoff", "STATE")
                asyncio.create_task(self._do_squareoff())
            return

        # Update running session high/low every tick (0.5s resolution)
        spot = _price() or _last_valid_mark
        if spot:
            if spot > self.session_high: self.session_high = spot
            if spot < self.session_low:  self.session_low  = spot

        # Poll futures order fills every 5s
        if time.time() - self._last_poll_ts >= 5.0:
            self._last_poll_ts = time.time()
            await self._poll_futures_fills()

        # Write PnL snapshot to DB every 30s
        if time.time() - self._last_snap_ts >= 30.0:
            self._last_snap_ts = time.time()
            self._write_pnl_snapshot()

    def _write_pnl_snapshot(self):
        """Compute current MTM PnL for all legs and persist to pnl_snapshots table."""
        if not _db or not self._session_id:
            return
        spot   = _price()
        c_data = _chain.get(self.call_sym, {}) if self.call_sym else {}
        p_data = _chain.get(self.put_sym,  {}) if self.put_sym  else {}
        c_mark = float(c_data.get("mark") or 0)
        p_mark = float(p_data.get("mark") or 0)
        c_upnl = _opt_upnl(self.call_fill,  c_mark, self.call_qty)
        p_upnl = _opt_upnl(self.put_fill,   p_mark, self.put_qty)
        l_upnl = _fut_upnl(self.long_entry,  spot, self.long_qty,  "LONG")  if self.long_filled  else 0.0
        s_upnl = _fut_upnl(self.short_entry, spot, self.short_qty, "SHORT") if self.short_filled else 0.0
        l_liq  = _calc_liq_price_cross(self.long_entry,  self.long_qty,  "LONG")  if self.long_filled  else 0.0
        s_liq  = _calc_liq_price_cross(self.short_entry, self.short_qty, "SHORT") if self.short_filled else 0.0
        margin = ((self.long_entry  * self.long_qty  if self.long_filled  else 0)
                + (self.short_entry * self.short_qty if self.short_filled else 0)) / FUTURES_LEVERAGE
        _db_snapshot_insert(self._session_id, spot, c_mark, p_mark,
                            c_upnl, p_upnl, l_upnl, s_upnl, l_liq, s_liq, margin)

    async def _poll_futures_fills(self):
        """Poll entry fills and TP fills. TPs are pre-placed at entry time."""
        # â"€â"€ LONG entry â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        if self.long_oid and not self.long_filled:
            try:
                q = await futures_query(self.long_oid)
                if q.get("status") == "FILLED":
                    self.long_filled = True
                    self.long_entry  = float(q.get("avgPrice") or self.long_limit_px)
                    liq    = _calc_liq_price_cross(self.long_entry, self.long_qty, "LONG")
                    margin = (self.long_entry * self.long_qty) / FUTURES_LEVERAGE
                    _db_session_update(self._session_id,
                        long_entry=self.long_entry, long_qty=self.long_qty,
                        long_tp_px=self.long_tp_px, long_liq_px=liq, margin_used=margin)
                    _db_event_insert(self._session_id, "LONG_FILLED",
                                     f"entry={self.long_entry:.0f} tp={self.long_tp_px:.0f}")
                    log("BOT", f"LONG FILLED @ {self.long_entry:.0f}  TP already @ {self.long_tp_px:.0f}", "TRADE")
                    asyncio.create_task(_tg(
                        f"⚡ <b>LONG filled</b> @ <b>{self.long_entry:.0f}</b>\n"
                        f"TP already in market @ <b>{self.long_tp_px:.0f}</b>"
                    ))
            except Exception as e:
                log("BOT", f"LONG entry query: {e}", "WARN")

        # â"€â"€ SHORT entry â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        if self.short_oid and not self.short_filled:
            try:
                q = await futures_query(self.short_oid)
                if q.get("status") == "FILLED":
                    self.short_filled = True
                    self.short_entry  = float(q.get("avgPrice") or self.short_limit_px)
                    liq       = _calc_liq_price_cross(self.short_entry, self.short_qty, "SHORT")
                    margin_tot = ((self.long_entry * self.long_qty if self.long_filled else 0)
                                  + self.short_entry * self.short_qty) / FUTURES_LEVERAGE
                    _db_session_update(self._session_id,
                        short_entry=self.short_entry, short_qty=self.short_qty,
                        short_tp_px=self.short_tp_px, short_liq_px=liq, margin_used=margin_tot)
                    _db_event_insert(self._session_id, "SHORT_FILLED",
                                     f"entry={self.short_entry:.0f} tp={self.short_tp_px:.0f}")
                    log("BOT", f"SHORT FILLED @ {self.short_entry:.0f}  TP already @ {self.short_tp_px:.0f}", "TRADE")
                    asyncio.create_task(_tg(
                        f"⚡ <b>SHORT filled</b> @ <b>{self.short_entry:.0f}</b>\n"
                        f"TP already in market @ <b>{self.short_tp_px:.0f}</b>"
                    ))
            except Exception as e:
                log("BOT", f"SHORT entry query: {e}", "WARN")

        # â"€â"€ TP fills â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        if self.long_tp_oid and self.long_filled and not self.long_tp_done:
            try:
                q = await futures_query(self.long_tp_oid)
                if q.get("status") == "FILLED":
                    self.long_tp_done = True
                    pnl = (self.long_tp_px - self.long_entry) * self.long_qty
                    log("BOT", f"LONG TP HIT @ {self.long_tp_px:.0f}  PnL=+{pnl:.2f}", "OK")
                    if _db and self._session_id:
                        _db.execute(
                            "UPDATE orders SET status='FILLED', fill_price=?, filled_at=?"
                            " WHERE session_id=? AND leg_label='LONG_TP' AND status='NEW'",
                            (self.long_tp_px, _ts(), self._session_id))
                        _db.commit()
                    _db_session_update(self._session_id, sq_long_exit=self.long_tp_px)
                    _db_event_insert(self._session_id, "LONG_TP_HIT",
                                     f"fill={self.long_tp_px:.0f} pnl=+{pnl:.2f}")
                    asyncio.create_task(_tg(
                        f"✅ <b>LONG TP hit</b> @ {self.long_tp_px:.0f}\n"
                        f"Futures PnL: +{pnl:.2f} USDT"))
            except Exception as e:
                log("BOT", f"LONG TP query: {e}", "WARN")

        if self.short_tp_oid and self.short_filled and not self.short_tp_done:
            try:
                q = await futures_query(self.short_tp_oid)
                if q.get("status") == "FILLED":
                    self.short_tp_done = True
                    pnl = (self.short_entry - self.short_tp_px) * self.short_qty
                    log("BOT", f"SHORT TP HIT @ {self.short_tp_px:.0f}  PnL=+{pnl:.2f}", "OK")
                    if _db and self._session_id:
                        _db.execute(
                            "UPDATE orders SET status='FILLED', fill_price=?, filled_at=?"
                            " WHERE session_id=? AND leg_label='SHORT_TP' AND status='NEW'",
                            (self.short_tp_px, _ts(), self._session_id))
                        _db.commit()
                    _db_session_update(self._session_id, sq_short_exit=self.short_tp_px)
                    _db_event_insert(self._session_id, "SHORT_TP_HIT",
                                     f"fill={self.short_tp_px:.0f} pnl=+{pnl:.2f}")
                    asyncio.create_task(_tg(
                        f"✅ <b>SHORT TP hit</b> @ {self.short_tp_px:.0f}\n"
                        f"Futures PnL: +{pnl:.2f} USDT"))
            except Exception as e:
                log("BOT", f"SHORT TP query: {e}", "WARN")

    #
    #  CANCEL ALL MANAGING-PHASE PENDING ORDERS
    #
    async def _cancel_all_pending_orders(self):
        """
        Cancel every open order placed during the MANAGING phase before squareoff.
        Covers: unfilled futures limits, futures TP orders, liq protection options.
        Does NOT cancel option SQ orders (those are the close orders we want to fill).
        """
        log("BOT", "Cancelling all pending MANAGING orders before squareoff...", "INFO")

        # â"€â"€ Futures limit orders (if not yet filled) â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        if self.long_oid and not self.long_filled:
            try:
                await futures_cancel(self.long_oid)
                log("BOT", f"Cancelled LONG LIMIT oid={self.long_oid}", "INFO")
                _db_order_cancel(self._session_id, "LONG_ENTRY", "SQ_PREP")
            except Exception as e:
                log("BOT", f"Cancel LONG LIMIT: {e}", "WARN")

        if self.short_oid and not self.short_filled:
            try:
                await futures_cancel(self.short_oid)
                log("BOT", f"Cancelled SHORT LIMIT oid={self.short_oid}", "INFO")
                _db_order_cancel(self._session_id, "SHORT_ENTRY", "SQ_PREP")
            except Exception as e:
                log("BOT", f"Cancel SHORT LIMIT: {e}", "WARN")

        # â"€â"€ Futures TP orders (cancel so market close isn't blocked) â"€â"€â"€â"€â"€â"€â"€â"€â"€
        if self.long_tp_oid and not self.long_tp_done:
            try:
                await futures_cancel(self.long_tp_oid)
                log("BOT", f"Cancelled LONG TP oid={self.long_tp_oid}", "INFO")
                _db_order_cancel(self._session_id, "LONG_TP", "SQ_PREP")
            except Exception as e:
                log("BOT", f"Cancel LONG TP: {e}", "WARN")

        if self.short_tp_oid and not self.short_tp_done:
            try:
                await futures_cancel(self.short_tp_oid)
                log("BOT", f"Cancelled SHORT TP oid={self.short_tp_oid}", "INFO")
                _db_order_cancel(self._session_id, "SHORT_TP", "SQ_PREP")
            except Exception as e:
                log("BOT", f"Cancel SHORT TP: {e}", "WARN")

        log("BOT", "All pending orders cancelled", "OK")

    #
    #  SQUAREOFF  —  15m kline high/low determines TP hit; options at intrinsic
    #
    async def _fetch_spot_at_time(self, ts_ms: int) -> float:
        """
        Fetch BTC perpetual close price from the 1m kline that contains ts_ms.
        Used when squareoff fires late (connection break) to price options at
        the scheduled squareoff time rather than the (delayed) current time.
        Returns 0.0 on failure.
        """
        try:
            rows = await _get(FAPI, "/fapi/v1/klines", {
                "symbol":    "BTCUSDT",
                "interval":  "1m",
                "startTime": ts_ms - 60_000,
                "endTime":   ts_ms + 90_000,
                "limit":     3,
            })
            if not rows:
                return 0.0
            # Last kline whose open time is at or before ts_ms
            valid = [r for r in rows if int(r[0]) <= ts_ms]
            row   = valid[-1] if valid else rows[0]
            return float(row[4])   # close price of that 1m candle
        except Exception as e:
            log("BOT", f"_fetch_spot_at_time failed: {e}", "WARN")
            return 0.0

    async def _fetch_klines_high_low(self, start_ms: int, end_ms: int) -> tuple:
        """
        Fetch 15m BTCUSDT perpetual klines for the session window.
        Returns (session_high, session_low). Falls back to (0, 0) on error.
        """
        try:
            rows = await _get(FAPI, "/fapi/v1/klines", {
                "symbol":    "BTCUSDT",
                "interval":  "15m",
                "startTime": start_ms,
                "endTime":   end_ms,
                "limit":     200,   # 200 × 15 min = 50 h — covers any session
            })
            if not rows:
                return 0.0, 0.0
            highs = [float(r[2]) for r in rows]
            lows  = [float(r[3]) for r in rows]
            return max(highs), min(lows)
        except Exception as e:
            log("BOT", f"klines fetch failed: {e}", "WARN")
            return 0.0, 0.0

    async def _do_squareoff(self):
        if self._sq_done:
            return

        # Spot: live WS first, immediate REST if stale, LKG if fresh (< 120s), else retry
        spot = _price()
        if not spot:
            try:
                def _sq_premium():
                    req = urllib.request.Request(
                        f"{FAPI}/fapi/v1/premiumIndex?symbol=BTCUSDT",
                        headers={"User-Agent": "StraddleBot/1.0"})
                    with urllib.request.urlopen(req, timeout=5) as r:
                        return json.loads(r.read())
                _pm  = await asyncio.get_running_loop().run_in_executor(None, _sq_premium)
                _mp  = float(_pm.get("markPrice") or 0)
                if _mp > 0:
                    _fut_px["mark"]              = _mp
                    globals()["_last_valid_mark"] = _mp
                    globals()["_fut_px_update_ms"] = time.time() * 1000
                    _ws_last_msg["fut"]           = time.time()
                    spot = _mp
                    log("BOT", f"Squareoff: BTC via REST (WS down): {spot:.0f}", "WARN")
            except Exception as _re:
                log("BOT", f"Squareoff: REST fetch failed: {_re}", "WARN")
        if not spot:
            _mk_age_s = (time.time() * 1000 - _fut_px_update_ms) / 1000
            if _mk_age_s < 120 and _last_valid_mark > 0:
                spot = _last_valid_mark
                log("BOT", f"Squareoff: using LKG mark (age={_mk_age_s:.0f}s): {spot:.0f}", "WARN")
        if not spot:
            log("BOT", "Squareoff: no reliable spot — will retry next tick", "WARN")
            return

        self._sq_done = True

        # Compute scheduled squareoff time so we can detect a late/delayed squareoff
        # (e.g. bot was disconnected during the squareoff window and reconnected after).
        # Reference time: SQUAREOFF_HARD if hard-deadline fired, else SQUAREOFF_START.
        _ref_ts_ms:   int   = 0   # epoch-ms of the scheduled squareoff moment
        _sq_delay_s:  float = 0.0
        if self.entry_ts_ms > 0:
            _edt = datetime.fromtimestamp(self.entry_ts_ms / 1000, tz=IST)
            _rh, _rm = SQUAREOFF_HARD if self._hard_sq_fired else SQUAREOFF_START
            _rdt = _edt.replace(hour=_rh, minute=_rm, second=0, microsecond=0)
            if _rdt <= _edt:
                _rdt += timedelta(days=1)
            _ref_ts_ms  = int(_rdt.timestamp() * 1000)
            _sq_delay_s = (datetime.now(IST) - _rdt).total_seconds()

        log("BOT", f"Squareoff started  delay={_sq_delay_s:.0f}s", "STATE")

        try:
            # Step 1: Cancel all pending orders (entry limits + pre-placed TPs)
            await self._cancel_all_pending_orders()

            # Step 2: One final fill poll — catches fills that landed in the
            # gap between last 5s poll and squareoff (edge case, live mode)
            await self._poll_futures_fills()

            # Step 3: Determine session high/low for TP check
            # Primary: in-memory running values (0.5s resolution, built during MANAGING)
            high = self.session_high
            low  = self.session_low if self.session_low < float("inf") else 0.0

            # Fallback: klines (covers restarts where in-memory was reset)
            if not high:
                now_ms   = int(time.time() * 1000)
                start_ms = self.entry_ts_ms if self.entry_ts_ms > 0 else now_ms - 86_400_000
                kline_high, kline_low = await self._fetch_klines_high_low(start_ms, now_ms)
                if kline_high:
                    high = kline_high
                    low  = kline_low
                    log("BOT", f"In-memory high/low empty — klines used: high={high:.0f} low={low:.0f}", "WARN")

            # Last resort: pnl_snapshots (if both in-memory and klines fail)
            if not high and _db and self._session_id:
                try:
                    row = _db.execute(
                        "SELECT MAX(btc_mark), MIN(btc_mark) FROM pnl_snapshots"
                        " WHERE session_id=? AND btc_mark > 0",
                        (self._session_id,)
                    ).fetchone()
                    if row:
                        high = float(row[0] or 0)
                        low  = float(row[1] or 0)
                    log("BOT", f"Klines also failed — snapshots used: high={high:.0f} low={low:.0f}", "WARN")
                except Exception as e:
                    log("BOT", f"All high/low sources failed: {e}", "ERROR")

            log("BOT", f"Session high={high:.0f}  low={low:.0f}  spot={spot:.0f}", "INFO")

            # Step 4: LONG leg exit — was TP hit during session?
            if self.long_filled:
                if self.long_tp_px > 0 and high >= self.long_tp_px:
                    self.long_tp_done = True
                    self.long_sq_exit = self.long_tp_px
                    log("BOT", f"LONG TP confirmed via session high  exit={self.long_tp_px:.0f}", "OK")
                else:
                    self.long_sq_exit = spot
                    log("BOT", f"LONG timeout exit @ spot={spot:.0f}", "INFO")

            # Step 5: SHORT leg exit
            if self.short_filled:
                if self.short_tp_px > 0 and low > 0 and low <= self.short_tp_px:
                    self.short_tp_done = True
                    self.short_sq_exit = self.short_tp_px
                    log("BOT", f"SHORT TP confirmed via session low  exit={self.short_tp_px:.0f}", "OK")
                else:
                    self.short_sq_exit = spot
                    log("BOT", f"SHORT timeout exit @ spot={spot:.0f}", "INFO")

            # Step 6: Options exit pricing.
            # Primary  : live mark price from _chain (if > 0  →  data aa raha hai)
            # Fallback : intrinsic at current spot   (if mark = 0  →  data nahi aa raha)
            call_strike = float(self.call_sym.split("-")[2]) if self.call_sym else 0.0
            put_strike  = float(self.put_sym.split("-")[2])  if self.put_sym  else 0.0

            for sym, strike, side, _open_attr, _exit_attr in [
                (self.call_sym, call_strike, "C", "call_open", "call_sq_exit"),
                (self.put_sym,  put_strike,  "P", "put_open",  "put_sq_exit"),
            ]:
                if not getattr(self, _open_attr) or not strike:
                    continue
                leg = "CALL" if side == "C" else "PUT"

                mark = _chain.get(sym or "", {}).get("mark", 0.0)
                if mark > 0:
                    # Live mark price available — use it
                    setattr(self, _exit_attr, mark)
                    setattr(self, _open_attr, False)
                    log("BOT", f"{leg} closed [mark={mark:.0f}]  delay={_sq_delay_s:.0f}s", "TRADE")
                else:
                    # Mark unavailable — fallback to intrinsic at current spot
                    intr = (max(spot - strike, 0.0) if side == "C"
                            else max(strike - spot, 0.0))
                    setattr(self, _exit_attr, intr)
                    setattr(self, _open_attr, False)
                    log("BOT", f"{leg} closed [intrinsic, mark=0]"
                               f"  spot={spot:.0f}  strike={strike:.0f}  exit={intr:.0f}", "TRADE")

            # Step 7: Final PnL
            opts_pnl = ((self.call_sq_exit - self.call_fill) * self.call_qty
                      + (self.put_sq_exit  - self.put_fill)  * self.put_qty)
            fut_pnl  = 0.0
            if self.long_filled:
                fut_pnl += (self.long_sq_exit   - self.long_entry)   * self.long_qty
            if self.short_filled:
                fut_pnl += (self.short_entry    - self.short_sq_exit) * self.short_qty
            net_pnl = round(opts_pnl + fut_pnl, 4)

            sq_type = ("TP_HIT"     if self.long_tp_done and self.short_tp_done else
                       "PARTIAL_TP" if self.long_tp_done or  self.short_tp_done else
                       "TIMEOUT")

            # Mark remaining NEW orders as cancelled
            if _db and self._session_id:
                try:
                    _db.execute(
                        "UPDATE orders SET status='CANCELLED', cancel_reason='SESSION_END', updated_at=?"
                        " WHERE session_id=? AND status='NEW'",
                        (_ts(), self._session_id))
                    _db.commit()
                except Exception:
                    pass

            # Update paper wallet: add options proceeds + futures PnL (cumulative)
            if PAPER_TRADE:
                global _paper_wallet_balance
                opts_proceeds = (self.call_sq_exit * self.call_qty
                               + self.put_sq_exit  * self.put_qty)
                sq_now = _ts()
                # Ledger: OPTIONS_CREDIT
                _db_wallet_ledger(self._session_id, "OPTIONS_CREDIT",
                                  round(opts_proceeds, 4),
                                  round(_paper_wallet_balance + opts_proceeds, 4),
                                  f"CALL@{self.call_sq_exit:.0f} PUT@{self.put_sq_exit:.0f} qty={self.call_qty}")
                _paper_wallet_balance = round(_paper_wallet_balance + opts_proceeds, 2)
                # Ledger: FUTURES_PNL (can be positive or negative)
                _db_wallet_ledger(self._session_id, "FUTURES_PNL",
                                  round(fut_pnl, 4),
                                  round(_paper_wallet_balance + fut_pnl, 4),
                                  f"sq_type={sq_type} long_sq={self.long_sq_exit:.0f} short_sq={self.short_sq_exit:.0f}")
                _paper_wallet_balance = round(_paper_wallet_balance + fut_pnl, 2)
                _db_set_config("PAPER_WALLET_BALANCE", str(_paper_wallet_balance))
                log("BOT", f"Paper wallet after squareoff: {_paper_wallet_balance:.2f}", "INFO")

            _db_session_update(self._session_id,
                sq_call_exit=self.call_sq_exit, sq_put_exit=self.put_sq_exit,
                sq_long_exit=self.long_sq_exit, sq_short_exit=self.short_sq_exit,
                options_pnl=round(opts_pnl, 4), futures_pnl=round(fut_pnl, 4),
                net_pnl=net_pnl, sq_type=sq_type, end_dt=_ts(), state="DONE",
                wallet_after=round(_paper_wallet_balance, 2))
            _db_event_insert(self._session_id, "SESSION_CLOSED",
                             f"sq={sq_type} opts={opts_pnl:.2f} fut={fut_pnl:.2f} net={net_pnl:.2f} wallet={_paper_wallet_balance:.2f}")

            # Persist: this expiry is done — survives process restart
            _db_set_config("LAST_TRADED_EXPIRY", _expiry_iso())

            log("BOT", f"Squareoff complete  sq_type={sq_type}  net={net_pnl:+.2f}", "STATE")
            await self._send_summary(sq_type, opts_pnl, fut_pnl, net_pnl)

        except Exception as e:
            log("BOT", f"_do_squareoff error: {e}", "ERROR")
            asyncio.create_task(_tg(f"⚠ Squareoff error — session may need manual review:\n{e}"))
        finally:
            self.state = "DONE"

    #
    #  STATUS DETAIL (written to DB every tick for dashboard diagnostics)
    #
    def _build_status_detail(self) -> dict:
        now     = datetime.now(IST)
        now_min = _to_min(now.hour, now.minute)
        spot    = _price()
        win_open    = _in_entry_window(now_min)
        _fut_stale  = _feed_age_s("fut") > 10
        _opt_stale  = _feed_age_s("opt") > 30 and (time.time() - _rest_last_ok) > 30
        data_ready  = bool(_all_strikes and spot and not _fut_stale and not _opt_stale)

        exp_date_str   = ""
        hours_to_expiry = -1.0
        within_24h      = False
        if _expiry_ms:
            exp_date_str    = datetime.fromtimestamp(_expiry_ms / 1000, tz=IST).strftime("%Y-%m-%d %H:%M")
            hours_to_expiry = (_expiry_ms - int(time.time() * 1000)) / 3_600_000
            within_24h      = 0 < hours_to_expiry < 24

        d: dict = {
            "window_open":      win_open,
            "within_24h":       within_24h,
            "hours_to_expiry":  round(hours_to_expiry, 1) if hours_to_expiry >= 0 else None,
            "data_ready":       data_ready,
            "spot":           round(spot, 1) if spot else 0,
            "window_start":   f"{WINDOW_START[0]:02d}:{WINDOW_START[1]:02d}",
            "window_end":     f"{WINDOW_END[0]:02d}:{WINDOW_END[1]:02d}",
            "sq_start":       f"{SQUAREOFF_START[0]:02d}:{SQUAREOFF_START[1]:02d}",
            "sq_hard":        f"{SQUAREOFF_HARD[0]:02d}:{SQUAREOFF_HARD[1]:02d}",
            "expiry_date":    exp_date_str,
            "already_traded": bool(
                (self.sess_date or _db_get_config("LAST_TRADED_EXPIRY") or "") == _expiry_iso()
            ),
            "fut_feed_age_s":   round(_feed_age_s("fut"), 1),
            "opt_feed_age_s":   round(_feed_age_s("opt"), 1),
            "rest_fail_streak": _rest_fail_streak,
            "rest_last_ok_s":   round(time.time() - _rest_last_ok, 1) if _rest_last_ok > 0 else None,
            "min_expiry_h":   MIN_EXPIRY_HOURS,
            "min_strike_gap": MIN_STRIKE_GAP,
            "min_ask_leg":    MIN_ASK_PER_LEG,
            "max_total_ask":  MAX_TOTAL_ASK,
            "max_ask_pct":    MAX_ASK_MARK_PCT,
            "trade_qty":      TRADE_QTY,
            "paper_wallet":   round(_paper_wallet_balance, 2),
            "paper_trade":    PAPER_TRADE,
        }

        if self.state == "SLEEP":
            reasons = []
            if not win_open:        reasons.append("window closed")
            if not within_24h:      reasons.append("expiry > 24h away")
            if _fut_stale:          reasons.append(f"futures feed stale {d['fut_feed_age_s']:.0f}s")
            if _opt_stale:          reasons.append(f"options feed stale {d['opt_feed_age_s']:.0f}s")
            if not _all_strikes:    reasons.append("chain not loaded")
            elif not spot:          reasons.append("no spot price")
            if d["already_traded"]: reasons.append("already traded today")
            d["sleep_reason"] = ", ".join(reasons) if reasons else "waiting"

        if self.state in ("SLEEP", "SCANNING") and data_ready and spot:
            try:
                pair = _find_itm_pair(spot)
                if pair:
                    call_opt, put_opt = pair
                    now_ms = int(time.time() * 1000)
                    c_ask  = call_opt.get("ask")     or 0.0
                    c_mark = call_opt.get("mark")    or 0.0
                    c_aq   = call_opt.get("ask_qty") or 0.0
                    p_ask  = put_opt.get("ask")      or 0.0
                    p_mark = put_opt.get("mark")     or 0.0
                    p_aq   = put_opt.get("ask_qty")  or 0.0
                    gap    = put_opt["strike"] - call_opt["strike"]
                    h_left = (_expiry_ms - now_ms) / 3_600_000 if _expiry_ms else 0.0
                    c_sp   = abs(c_ask - c_mark) / c_mark * 100 if c_mark > 0 and c_ask > 0 else 999.0
                    p_sp   = abs(p_ask - p_mark) / p_mark * 100 if p_mark > 0 and p_ask > 0 else 999.0
                    d["conditions"] = {
                        "call_sym":       call_opt["symbol"],
                        "put_sym":        put_opt["symbol"],
                        "expiry_h":       round(h_left, 1),
                        "expiry_ok":      h_left >= MIN_EXPIRY_HOURS,
                        "gap":            round(gap, 0),
                        "gap_ok":         gap >= MIN_STRIKE_GAP,
                        "call_spread":    round(c_sp, 1),
                        "put_spread":     round(p_sp, 1),
                        "call_spread_ok": c_sp <= MAX_ASK_MARK_PCT and c_ask > 0,
                        "put_spread_ok":  p_sp <= MAX_ASK_MARK_PCT and p_ask > 0,
                        "spread_ok":      c_sp <= MAX_ASK_MARK_PCT and p_sp <= MAX_ASK_MARK_PCT and c_ask > 0 and p_ask > 0,
                        "call_aqty":      round(c_aq, 4),
                        "put_aqty":       round(p_aq, 4),
                        "call_qty_ok":    c_aq >= TRADE_QTY,
                        "put_qty_ok":     p_aq >= TRADE_QTY,
                        "qty_ok":         c_aq >= TRADE_QTY and p_aq >= TRADE_QTY,
                        "call_ask":       round(c_ask, 0),
                        "put_ask":        round(p_ask, 0),
                        "call_mark":      round(c_mark, 0),
                        "put_mark":       round(p_mark, 0),
                        "call_min_ok":    c_ask > MIN_ASK_PER_LEG,
                        "put_min_ok":     p_ask > MIN_ASK_PER_LEG,
                        "min_ask_ok":     c_ask > MIN_ASK_PER_LEG and p_ask > MIN_ASK_PER_LEG,
                        "total_ask":      round(c_ask + p_ask, 0),
                        "total_ask_ok":   (c_ask + p_ask) <= MAX_TOTAL_ASK,
                    }
            except Exception:
                pass

        return d

    #
    #  STATUS LOG
    #
    def _log_status(self):
        spot = _price()
        if self.state == "SCANNING" and not self.call_sym:
            # Show actual market data for the ITM pair being scanned (not position data)
            pair = _find_itm_pair(spot) if spot else None
            if pair:
                cd, pd = pair
                log("BOT",
                    f"[SCANNING]  spot={spot:.0f}  "
                    f"CALL {cd['symbol']} ask={cd.get('ask') or 0:.0f} mark={cd.get('mark') or 0:.0f} aqty={cd.get('ask_qty') or 0:.3f}  "
                    f"PUT {pd['symbol']} ask={pd.get('ask') or 0:.0f} mark={pd.get('mark') or 0:.0f} aqty={pd.get('ask_qty') or 0:.3f}", "INFO")
            else:
                log("BOT", f"[SCANNING]  spot={spot:.0f}  waiting for chain data", "INFO")
            return
        call_data = _chain.get(self.call_sym, {}) if self.call_sym else {}
        put_data  = _chain.get(self.put_sym,  {}) if self.put_sym  else {}
        c_mark = call_data.get("mark") or 0.0
        p_mark = put_data.get("mark") or 0.0
        c_bid  = call_data.get("bid") or 0.0
        p_bid  = put_data.get("bid") or 0.0
        log("BOT",
            f"[{self.state}]  spot={spot:.0f}  "
            f"CALL bid={c_bid:.0f} mark={c_mark:.0f}  "
            f"PUT bid={p_bid:.0f} mark={p_mark:.0f}", "INFO")

    async def _send_summary(self, sq_type: str, opts_pnl: float, fut_pnl: float, net_pnl: float):
        """Send final P&L summary to Telegram."""
        tag  = {"TP_HIT": "✅", "PARTIAL_TP": "⚡", "TIMEOUT": "⏱"}.get(sq_type, "📋")
        sign = "+" if net_pnl >= 0 else ""

        call_line = (f"  CALL  buy={self.call_fill:.0f}  exit={self.call_sq_exit:.0f}"
                     f"  pnl={(self.call_sq_exit - self.call_fill) * self.call_qty:+.2f}")
        put_line  = (f"  PUT   buy={self.put_fill:.0f}  exit={self.put_sq_exit:.0f}"
                     f"  pnl={(self.put_sq_exit - self.put_fill) * self.put_qty:+.2f}")

        long_line  = (f"  LONG  entry={self.long_entry:.0f}  exit={self.long_sq_exit:.0f}"
                      f"  {'TP ✓' if self.long_tp_done else 'TIMEOUT'}"
                      f"  pnl={(self.long_sq_exit - self.long_entry) * self.long_qty:+.2f}"
                      if self.long_filled else "  LONG  not filled")
        short_line = (f"  SHORT entry={self.short_entry:.0f}  exit={self.short_sq_exit:.0f}"
                      f"  {'TP ✓' if self.short_tp_done else 'TIMEOUT'}"
                      f"  pnl={(self.short_entry - self.short_sq_exit) * self.short_qty:+.2f}"
                      if self.short_filled else "  SHORT not filled")

        await _tg(
            f"{tag} <b>Session Closed — {sq_type}</b>\n"
            f"──────────────────────────────────────────\n"
            f"Entry : {self.entry_ts} IST\n\n"
            f"<b>Options</b>  (prem paid = {self.total_prem:.0f})\n"
            f"{call_line}\n"
            f"{put_line}\n"
            f"  Opts PnL : <b>{opts_pnl:+.2f} USDT</b>\n\n"
            f"<b>Futures</b>\n"
            f"{long_line}\n"
            f"{short_line}\n"
            f"  Fut  PnL : <b>{fut_pnl:+.2f} USDT</b>\n\n"
            f"<b>Net PnL  : {sign}{net_pnl:.2f} USDT</b>"
        )

    #
    #  STATE RESTORE  -  called on restart when open positions detected
    #

    async def _restore_from_db(self) -> bool:
        """Paper-mode restore: rebuild bot state from the most recent ACTIVE session."""
        if not _db:
            return False
        sess = _db.execute(
            "SELECT * FROM sessions WHERE state='ACTIVE' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not sess:
            log("BOT", "[PAPER] No active session in DB", "WARN")
            return False

        self._session_id = sess["id"]
        self.call_sym    = sess["call_sym"]   or ""
        self.put_sym     = sess["put_sym"]    or ""
        self.call_fill   = sess["call_fill"]  or 0.0
        self.put_fill    = sess["put_fill"]   or 0.0
        self.call_qty    = sess["call_qty"]   or TRADE_QTY
        self.put_qty     = sess["put_qty"]    or TRADE_QTY
        self.call_open   = True
        self.put_open    = True
        self.total_prem  = (self.call_fill + self.put_fill)
        self.long_entry  = sess["long_entry"]  or 0.0
        self.short_entry = sess["short_entry"] or 0.0
        self.long_qty    = sess["long_qty"]    or TRADE_QTY
        self.short_qty   = sess["short_qty"]   or TRADE_QTY
        self.long_filled  = self.long_entry  > 0
        self.short_filled = self.short_entry > 0
        self.long_tp_px   = sess["long_tp_px"]   or 0.0
        self.short_tp_px  = sess["short_tp_px"]  or 0.0
        self.sess_date    = _expiry_iso()
        self.entry_ts     = "restored"
        self.entry_ts_ms  = int(sess["entry_ts_ms"] or 0)

        # Restore paper order IDs from orders table and re-register in _paper_orders
        # so _paper_fill_monitor() can re-evaluate pending fills after restart.
        orders = _db.execute(
            "SELECT * FROM orders WHERE session_id=?", (self._session_id,)
        ).fetchall()
        for o in orders:
            label = o["leg_label"]
            oid   = o["paper_order_id"]
            px    = float(o["limit_price"] or 0)
            if not oid:
                continue
            st = o["status"]
            if label == "LONG_ENTRY" and st == "NEW":
                self.long_oid       = oid
                self.long_limit_px  = px
                _paper_orders[oid]  = {
                    "orderId": oid, "symbol": "BTCUSDT", "side": "BUY",
                    "type": "LIMIT", "qty": self.long_qty, "price": px,
                    "status": "NEW", "fill_price": 0.0,
                    "asset": "future", "pos_side": "LONG", "placed_at": time.time(),
                }
            elif label == "SHORT_ENTRY" and st == "NEW":
                self.short_oid      = oid
                self.short_limit_px = px
                _paper_orders[oid]  = {
                    "orderId": oid, "symbol": "BTCUSDT", "side": "SELL",
                    "type": "LIMIT", "qty": self.short_qty, "price": px,
                    "status": "NEW", "fill_price": 0.0,
                    "asset": "future", "pos_side": "SHORT", "placed_at": time.time(),
                }
            elif label == "LONG_TP" and st == "NEW":
                self.long_tp_oid    = oid
                self.long_tp_px     = px
                _paper_orders[oid]  = {
                    "orderId": oid, "symbol": "BTCUSDT", "side": "SELL",
                    "type": "LIMIT", "qty": self.long_qty, "price": px,
                    "status": "NEW", "fill_price": 0.0,
                    "asset": "future", "pos_side": "LONG", "placed_at": time.time(),
                }
            elif label == "SHORT_TP" and st == "NEW":
                self.short_tp_oid   = oid
                self.short_tp_px    = px
                _paper_orders[oid]  = {
                    "orderId": oid, "symbol": "BTCUSDT", "side": "BUY",
                    "type": "LIMIT", "qty": self.short_qty, "price": px,
                    "status": "NEW", "fill_price": 0.0,
                    "asset": "future", "pos_side": "SHORT", "placed_at": time.time(),
                }


        # Restore session high/low from pnl_snapshots so in-memory tracking continues correctly
        try:
            hl = _db.execute(
                "SELECT MAX(btc_mark), MIN(btc_mark) FROM pnl_snapshots"
                " WHERE session_id=? AND btc_mark > 0",
                (self._session_id,)
            ).fetchone()
            if hl and hl[0]:
                self.session_high = float(hl[0])
                self.session_low  = float(hl[1])
                log("BOT", f"Session high/low restored: high={self.session_high:.0f} low={self.session_low:.0f}", "INFO")
        except Exception:
            pass

        # Set the correct run state — MANAGING handles polling, PnL snapshots, and squareoff
        self.state = "MANAGING"
        log("BOT", f"[PAPER] Session {self._session_id} restored -> MANAGING  "
                   f"CALL={self.call_sym} PUT={self.put_sym}  "
                   f"long_oid={self.long_oid} short_oid={self.short_oid}  "
                   f"long_filled={self.long_filled} short_filled={self.short_filled}  "
                   f"paper_orders_restored={len(_paper_orders)}", "OK")
        _db_event_insert(self._session_id, "SESSION_RESTORED",
                         f"restarted  paper_orders={len(_paper_orders)}"
                         f" long_filled={self.long_filled} short_filled={self.short_filled}")

        # Write immediate PnL snapshot so dashboard Data timestamp is fresh
        self._write_pnl_snapshot()
        return True

    async def restore_from_exchange(self) -> bool:
        """
        Read open positions and rebuild bot state. Paper mode: from SQLite.
        Live mode: from Binance APIs. Returns True if any position was found.
        """
        if PAPER_TRADE:
            return await self._restore_from_db()

        log("BOT", "Restoring state from Binance...", "INFO")
        found = False

        # â"€â"€ Open option positions â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        try:
            rows = await _get(EAPI, "/eapi/v1/position")
            for p in (rows if isinstance(rows, list) else []):
                sym = p.get("symbol", "")
                qty = float(p.get("quantity") or 0)
                if not sym.startswith("BTC-") or qty <= 0:
                    continue
                fill = float(p.get("entryPrice") or 0)
                if sym.endswith("-C"):
                    self.call_sym  = sym;  self.call_fill = fill
                    self.call_qty  = qty;  self.call_open = True
                    found = True
                    log("BOT", f"  CALL restored: {sym} @ {fill:.0f}", "WARN")
                elif sym.endswith("-P"):
                    self.put_sym   = sym;  self.put_fill  = fill
                    self.put_qty   = qty;  self.put_open  = True
                    found = True
                    log("BOT", f"  PUT  restored: {sym} @ {fill:.0f}", "WARN")
        except Exception as e:
            log("BOT", f"Restore options query: {e}", "WARN")

        # â"€â"€ Open futures positions â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        try:
            rows = await _get(FAPI, "/fapi/v2/positionRisk", {"symbol": "BTCUSDT"})
            for p in rows:
                ps  = p.get("positionSide")
                amt = abs(float(p.get("positionAmt") or 0))
                if amt < 0.001:
                    continue
                entry = float(p.get("entryPrice") or 0)
                if ps == "LONG":
                    self.long_filled = True;  self.long_entry = entry;  self.long_qty = amt
                    found = True
                    log("BOT", f"  LONG futures restored @ {entry:.0f}  qty={amt}", "WARN")
                elif ps == "SHORT":
                    self.short_filled = True; self.short_entry = entry; self.short_qty = amt
                    found = True
                    log("BOT", f"  SHORT futures restored @ {entry:.0f}  qty={amt}", "WARN")
        except Exception as e:
            log("BOT", f"Restore futures query: {e}", "WARN")

        if not found:
            log("BOT", "Restore: no open positions found on Binance", "INFO")
            return False

        # â"€â"€ Classify and restore open futures orders â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        try:
            orders = await _get(FAPI, "/fapi/v1/openOrders", {"symbol": "BTCUSDT"})
            for o in (orders if isinstance(orders, list) else []):
                oid      = int(o.get("orderId", 0))
                side     = o.get("side", "")
                pos_side = o.get("positionSide", "")
                price    = float(o.get("price") or 0)
                if not oid:
                    continue
                if pos_side == "LONG" and self.long_filled:
                    if side == "SELL":
                        # GTC TP order still live  -  restore it
                        self.long_tp_oid = oid
                        self.long_tp_px  = price
                        log("BOT", f"  Restored LONG TP oid={oid} @ {price:.0f}", "INFO")
                    else:
                        # Stale entry limit (position already filled)  -  cancel
                        try:
                            await futures_cancel(oid)
                            log("BOT", f"  Cancelled stale LONG entry limit oid={oid}", "INFO")
                        except Exception:
                            pass
                elif pos_side == "SHORT" and self.short_filled:
                    if side == "BUY":
                        # GTC TP order still live  -  restore it
                        self.short_tp_oid = oid
                        self.short_tp_px  = price
                        log("BOT", f"  Restored SHORT TP oid={oid} @ {price:.0f}", "INFO")
                    else:
                        # Stale entry limit (position already filled)  -  cancel
                        try:
                            await futures_cancel(oid)
                            log("BOT", f"  Cancelled stale SHORT entry limit oid={oid}", "INFO")
                        except Exception:
                            pass
                else:
                    # No corresponding open position  -  orphaned order, cancel it
                    try:
                        await futures_cancel(oid)
                        log("BOT", f"  Cancelled orphaned futures order oid={oid}", "INFO")
                    except Exception:
                        pass
        except Exception as e:
            log("BOT", f"Restore: futures orders: {e}", "WARN")

        # â"€â"€ Cancel any stale open option orders (no protection orders in use) â"€â"€â"€â"€
        for sym in [s for s in [self.call_sym, self.put_sym] if s]:
            try:
                opt_orders = await _get(EAPI, "/eapi/v1/openOrders", {"symbol": sym})
                for o in (opt_orders if isinstance(opt_orders, list) else []):
                    oid  = int(o.get("orderId", 0))
                    side = o.get("side", "")
                    if oid and side == "SELL":
                        try:
                            await option_cancel(sym, oid)
                            log("BOT", f"  Cancelled stale option SELL order {sym} oid={oid}", "INFO")
                        except Exception:
                            pass
            except Exception as e:
                log("BOT", f"Restore: option orders {sym}: {e}", "WARN")

        # â"€â"€ Place TP for any filled futures that have no live TP order â"€â"€â"€â"€â"€â"€â"€â"€
        if self.long_filled and not self.long_tp_oid and not self.long_tp_done:
            try:
                _cs = float(self.call_sym.split("-")[2]) if self.call_sym else 0
                _ps = float(self.put_sym.split("-")[2])  if self.put_sym  else 0
                _gap = _ps - _cs; _tv = self.total_prem - _gap
                tp_px = round(self.long_entry + self.total_prem + _tv, 1)
                self.long_tp_px = tp_px
                r = await futures_limit("SELL", self.long_qty, tp_px, "LONG")
                self.long_tp_oid = int(r.get("orderId", 0))
                log("BOT", f"  Placed missing LONG TP @ {tp_px:.0f}  oid={self.long_tp_oid}", "OK")
                await _tg(f"⚡ <b>Restore: LONG TP placed</b>\nEntry @ {self.long_entry:.0f}\nTP @ {tp_px:.0f}")
            except Exception as e:
                log("BOT", f"  LONG TP placement failed on restore: {e}", "ERROR")
                await _tg(f" Restore: LONG TP failed:\n{e}")

        if self.short_filled and not self.short_tp_oid and not self.short_tp_done:
            try:
                _cs = float(self.call_sym.split("-")[2]) if self.call_sym else 0
                _ps = float(self.put_sym.split("-")[2])  if self.put_sym  else 0
                _gap = _ps - _cs; _tv = self.total_prem - _gap
                tp_px = round(self.short_entry - (self.total_prem + _tv), 1)
                self.short_tp_px = tp_px
                r = await futures_limit("BUY", self.short_qty, tp_px, "SHORT")
                self.short_tp_oid = int(r.get("orderId", 0))
                log("BOT", f"  Placed missing SHORT TP @ {tp_px:.0f}  oid={self.short_tp_oid}", "OK")
                await _tg(f"⚡ <b>Restore: SHORT TP placed</b>\nEntry @ {self.short_entry:.0f}\nTP @ {tp_px:.0f}")
            except Exception as e:
                log("BOT", f"  SHORT TP placement failed on restore: {e}", "ERROR")
                await _tg(f" Restore: SHORT TP failed:\n{e}")

        # â"€â"€ Rebuild derived state â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        self.total_prem = (self.call_fill or 0.0) + (self.put_fill or 0.0)
        self.sess_date  = _expiry_iso()
        self.entry_ts   = "restored"

        # â"€â"€ Set state based on current time â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
        now = datetime.now(IST)
        past_sq = False
        if self.entry_ts_ms and self.entry_ts_ms > 0:
            # Use actual entry time to compute squareoff deadline — handles overnight sessions
            entry_dt = datetime.fromtimestamp(self.entry_ts_ms / 1000, tz=IST)
            sq_h, sq_m = SQUAREOFF_START
            sq_dt = entry_dt.replace(hour=sq_h, minute=sq_m, second=0, microsecond=0)
            if sq_dt <= entry_dt:
                sq_dt += timedelta(days=1)
            past_sq = now >= sq_dt
        else:
            # Fallback: calendar-date check (works for same-day sessions)
            past_sq = _is_expiry_day() and _to_min(now.hour, now.minute) >= _to_min(*SQUAREOFF_START)
        if past_sq:
            # Squareoff window already passed — run squareoff immediately
            asyncio.create_task(self._do_squareoff())
        self.state = "MANAGING"   # _do_squareoff() will flip to DONE when complete

        log("BOT", f"State restored -> {self.state}", "OK")
        return True


# ╔╗
# â•'                        STARTUP POSITION CHECK                             â•'
# ╚

# Set to True if Binance reports open positions at startup.
# _tick_sleep() refuses to transition to SCANNING while this is True,
# preventing a fresh entry on top of an unrecognised existing position.
_has_open_positions: bool = False


async def _check_existing_positions():
    """
    Check for open positions on startup. In paper mode, checks SQLite for an
    active session. In live mode, queries Binance APIs.
    """
    global _has_open_positions

    if PAPER_TRADE:
        if _db:
            row = _db.execute(
                "SELECT id FROM sessions WHERE state='ACTIVE' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                _has_open_positions = True
                log("SYS", f"[PAPER] Active session {row[0]} found in DB  -  will restore state", "WARN")
            else:
                log("SYS", "[PAPER] No active session in DB  -  clean start", "OK")
        return

    found_futures = False
    found_options = False

    try:
        rows = await _get(FAPI, "/fapi/v2/positionRisk", {"symbol": "BTCUSDT"})
        open_fut = [p for p in rows if abs(float(p.get("positionAmt") or 0)) > 0.001]
        if open_fut:
            found_futures = True
            for p in open_fut:
                log("SYS",
                    f"  FUTURES {p.get('positionSide','?')} "
                    f"amt={p.get('positionAmt')}  entry={p.get('entryPrice')}  "
                    f"liq={p.get('liquidationPrice')}", "CRIT")
    except Exception as e:
        log("SYS", f"Futures position check failed: {e}", "WARN")

    try:
        rows = await _get(EAPI, "/eapi/v1/position")
        if isinstance(rows, list):
            open_opt = [p for p in rows
                        if p.get("symbol", "").startswith("BTC-")
                        and float(p.get("quantity") or 0) > 0]
            if open_opt:
                found_options = True
                for p in open_opt:
                    log("SYS",
                        f"  OPTION {p.get('symbol')} "
                        f"qty={p.get('quantity')}  entry={p.get('entryPrice')}", "CRIT")
    except Exception as e:
        log("SYS", f"Option position check failed: {e}", "WARN")

    if found_futures or found_options:
        _has_open_positions = True
        log("SYS", "Open positions detected  -  will restore state after YES", "WARN")
    else:
        log("SYS", "No open positions  -  clean start", "OK")


# ╔╗
# â•'                        SINGLE INSTANCE LOCK                               â•'
# ╚
_lock_sock: Optional[socket.socket] = None

def _acquire_lock():
    global _lock_sock
    _lock_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _lock_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
    try:
        _lock_sock.bind(("127.0.0.1", 19877))
        _lock_sock.listen(1)
    except OSError:
        print("\n⚠  straddle_trader.py is already running (port 19877 busy). Exiting.\n")
        sys.exit(1)


# ╔╗
# â•'                        STARTUP TELEGRAM MESSAGE                           â•'
# ╚
def _fmt_hm(total_mins: int) -> str:
    """Format a minute count as '2h 05m' or '45m'."""
    if total_mins <= 0:
        return "now"
    h, m = divmod(int(total_mins), 60)
    return f"{h}h {m:02d}m" if h > 0 else f"{m}m"

def _build_startup_msg() -> str:
    """
    Build a comprehensive startup Telegram message.
    Called after _fetch_chain() so _expiry_ms is populated.
    Shows session name, window open/close countdown, squareoff countdown.
    """
    now       = datetime.now(IST)
    now_min   = _to_min(now.hour, now.minute)
    start_min = _to_min(*WINDOW_START)
    end_min   = _to_min(*WINDOW_END)
    overnight = start_min > end_min   # e.g. 23:00 -> 05:00

    # â"€â"€ Session / expiry â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    if _expiry_ms:
        exp_dt      = datetime.fromtimestamp(_expiry_ms / 1000, tz=IST)
        session_str = f" Session  : {exp_dt.strftime('%d %b %Y')}  |  Expiry {exp_dt.strftime('%H:%M')} IST"

        sq_dt   = exp_dt.replace(hour=SQUAREOFF_START[0], minute=SQUAREOFF_START[1], second=0, microsecond=0)
        hard_dt = exp_dt.replace(hour=SQUAREOFF_HARD[0],  minute=SQUAREOFF_HARD[1],  second=0, microsecond=0)
        mins_to_sq   = max(0, int((sq_dt   - now).total_seconds() / 60))
        mins_to_hard = max(0, int((hard_dt - now).total_seconds() / 60))
        sq_str   = f"  {SQUAREOFF_START[0]:02d}:{SQUAREOFF_START[1]:02d} IST  -  in {_fmt_hm(mins_to_sq)}"
        hard_str = f"  Hard stop  : {SQUAREOFF_HARD[0]:02d}:{SQUAREOFF_HARD[1]:02d} IST  -  in {_fmt_hm(mins_to_hard)}"
    else:
        session_str = " Session  : loading..."
        sq_str   = f"  {SQUAREOFF_START[0]:02d}:{SQUAREOFF_START[1]:02d} IST"
        hard_str = f"  Hard stop  : {SQUAREOFF_HARD[0]:02d}:{SQUAREOFF_HARD[1]:02d} IST"

    # â"€â"€ Entry window countdown â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€
    if _in_entry_window(now_min):
        if not overnight:
            mins_to_close = end_min - now_min
        else:
            mins_to_close = ((24 * 60 - now_min) + end_min) if now_min >= start_min else (end_min - now_min)
        win_str = (
            f"  {WINDOW_START[0]:02d}:{WINDOW_START[1]:02d} -> {WINDOW_END[0]:02d}:{WINDOW_END[1]:02d} IST\n"
            f"  🟢 OPEN  -  closes in {_fmt_hm(mins_to_close)}"
        )
    else:
        if not overnight:
            mins_to_open = (start_min - now_min) if now_min < start_min else ((24 * 60 - now_min) + start_min)
        else:
            mins_to_open = start_min - now_min   # between end_min and start_min
        win_str = (
            f"  {WINDOW_START[0]:02d}:{WINDOW_START[1]:02d} -> {WINDOW_END[0]:02d}:{WINDOW_END[1]:02d} IST\n"
            f"   Closed  -  opens in {_fmt_hm(mins_to_open)}"
        )

    return (
        f"🚀 <b>Straddle Bot LIVE</b>\n"
        f"─────────────────────────────────────────────────────────────────────\n"
        f"{session_str}\n\n"
        f" <b>Entry Window</b>\n"
        f"{win_str}\n\n"
        f"🎯 <b>Squareoff</b>\n"
        f"{sq_str}\n"
        f"{hard_str}\n\n"
        f" <b>Entry Conditions</b>\n"
        f"  Qty / leg      : {TRADE_QTY} BTC\n"
        f"  Min expiry     : {MIN_EXPIRY_HOURS:.0f}h\n"
        f"  Min strike gap : {MIN_STRIKE_GAP} USDT\n"
        f"  Max ask-mark % : {MAX_ASK_MARK_PCT}%  (retry: {MAX_ASK_MARK_RETRY}%)\n"
        f"  Min ask / leg  : {MIN_ASK_PER_LEG} USDT\n"
        f"  Max total ask  : {MAX_TOTAL_ASK} USDT\n\n"
        f"🔔 <b>Levels</b>\n"
        f"  LONG  = PUT_strike âˆ' total_prem\n"
        f"  SHORT = CALL_strike + total_prem\n"
        f"  Futures TP : entry ± (prem + TV_at_entry)\n"
        f"  Liq prot   : immediate on fill @ intrinsic at liq price\n"
        f"─────────────────────────────────────────────────────────────────────\n"
    )


# ╔╗
# â•'                        MAIN                                               â•'
# ╚
async def main():
    _acquire_lock()
    _db_init()
    _db_load_config()

    # Log uncaught exceptions from any asyncio task so they're never silently dropped
    def _handle_task_exception(loop, context):
        exc = context.get("exception")
        msg = context.get("message", "unknown asyncio error")
        if exc:
            log("SYS", f"Unhandled task exception: {exc!r}\n{msg}", "ERROR")
        else:
            log("SYS", f"Asyncio error: {msg}", "ERROR")
    _loop = asyncio.get_running_loop()
    _loop.set_exception_handler(_handle_task_exception)
    _loop.set_default_executor(
        concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="rest")
    )

    separator("BTC ITM Straddle Bot")

    # Load chain first so expiry date is known before we print or Telegram anything
    log("SYS", "Fetching options chain...", "INFO")
    await _fetch_chain()

    # Check for existing positions
    await _check_existing_positions()

    # Terminal confirm prompt
    print()
    print(f"  Session      : {datetime.fromtimestamp(_expiry_ms/1000, tz=IST).strftime('%d %b %Y  Expiry %H:%M IST') if _expiry_ms else 'unknown'}")
    print(f"  Entry window : {WINDOW_START[0]:02d}:{WINDOW_START[1]:02d} -> "
          f"{WINDOW_END[0]:02d}:{WINDOW_END[1]:02d} IST")
    print(f"  SQ window    : {SQUAREOFF_START[0]:02d}:{SQUAREOFF_START[1]:02d} -> "
          f"{SQUAREOFF_HARD[0]:02d}:{SQUAREOFF_HARD[1]:02d} IST")
    print(f"  Qty          : {TRADE_QTY} BTC")
    print(f"  Total ask <=  : {MAX_TOTAL_ASK} USDT")
    print()
    if _UNATTENDED:
        print("  Unattended mode (--yes)  -  starting automatically.\n")
    else:
        confirm = input("  Type YES to start: ").strip().upper()
        if confirm != "YES":
            print("  Aborted."); sys.exit(0)
        print()

    # Send rich startup message to Telegram (chain loaded -> expiry + countdowns are accurate)
    await _tg(_build_startup_msg())

    bot = StraddleBot()

    # If positions were found at startup, restore state from Binance
    # instead of blocking in SLEEP  -  bot resumes MANAGING (or fires squareoff if past SQ time)
    if _has_open_positions:
        restored = await bot.restore_from_exchange()
        if not restored:
            # Couldn't confirm positions via API  -  stay blocked in SLEEP for safety
            log("SYS",
                "⛔ Position restore failed  -  staying in SLEEP to prevent double entry.",
                "CRIT")
            await _tg(
                "⛔ <b>Position restore FAILED</b>\n"
                "Could not read open positions from Binance.\n"
                "Bot locked in SLEEP. Close all positions manually and restart."
            )

    # Start all background tasks
    tasks = [
        asyncio.create_task(_supervised(_fut_ws,           "fut_ws")),
        asyncio.create_task(_supervised(_opt_ws,           "opt_ws")),
        asyncio.create_task(_supervised(_ticker_loop,      "ticker")),
        asyncio.create_task(_supervised(_chain_refresh_loop, "chain_refresh")),
        asyncio.create_task(bot.run()),
    ]
    if PAPER_TRADE:
        tasks.append(asyncio.create_task(_paper_fill_monitor()))

    # SIGTERM handler  -  systemctl stop sends SIGTERM, not SIGINT
    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(sig, lambda: [t.cancel() for t in tasks])
    except NotImplementedError:
        pass   # Windows: add_signal_handler not supported

    log("SYS", "All tasks running. Ctrl+C to stop.", "OK")
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log("SYS", "Shutting down...", "WARN")
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await _tg(f"{_PTAG}🛑 <b>Straddle Bot stopped</b>\nProcess terminated (manual stop or system shutdown).")
        except Exception:
            pass
        if _lock_sock:
            _lock_sock.close()


_UNATTENDED = True    # paper-mode auto-start; set False to require interactive YES prompt

if __name__ == "__main__":
    import argparse as _ap
    _p = _ap.ArgumentParser(add_help=False)
    _p.add_argument("--yes", action="store_true", help="Unattended mode  -  skip interactive YES prompt")
    if _p.parse_known_args()[0].yes:
        _UNATTENDED = True
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
