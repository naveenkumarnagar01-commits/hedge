#!/usr/bin/env python3
"""
dashboard.py  -  BTC Straddle Paper-Trade Dashboard
Reads straddle_paper.db written by straddle_trader.py.
Run:  python dashboard.py  [--port 8080] [--db straddle_paper.db]
"""
import argparse
import datetime
import json
import sqlite3
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

# ── Config ───────────────────────────────────────────────────────────────────
DEFAULT_DB    = str(Path(__file__).parent / "straddle_paper.db")
DEFAULT_PORT  = 8080
TOTAL_CAPITAL = 10_000.0
_IST          = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

app       = FastAPI(title="Straddle Dashboard", docs_url=None, redoc_url=None)
HTML_FILE = Path(__file__).parent / "straddle_panel.html"

_db_path: str = DEFAULT_DB

# ── Schema (mirrors straddle_trader.py _db_init exactly) ─────────────────────
# straddle_trader.py owns schema creation. This is a read-only mirror used only
# when dashboard starts before the bot (edge case). Always idempotent.
_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS config (
    key          TEXT PRIMARY KEY,
    value        TEXT,
    label        TEXT,
    input_type   TEXT DEFAULT 'text',
    is_sensitive INTEGER DEFAULT 0,
    section      TEXT,
    sort_order   INTEGER
);
CREATE TABLE IF NOT EXISTS sessions (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    date          TEXT,
    expiry_sym    TEXT,
    expiry_dt     TEXT,
    state         TEXT DEFAULT 'ACTIVE',
    call_sym      TEXT DEFAULT '',
    put_sym       TEXT DEFAULT '',
    call_fill     REAL DEFAULT 0,
    put_fill      REAL DEFAULT 0,
    call_qty      REAL DEFAULT 0,
    put_qty       REAL DEFAULT 0,
    total_premium REAL DEFAULT 0,
    long_entry    REAL DEFAULT 0,
    short_entry   REAL DEFAULT 0,
    long_qty      REAL DEFAULT 0,
    short_qty     REAL DEFAULT 0,
    long_tp_px    REAL DEFAULT 0,
    short_tp_px   REAL DEFAULT 0,
    long_liq_px   REAL DEFAULT 0,
    short_liq_px  REAL DEFAULT 0,
    margin_used   REAL DEFAULT 0,
    entry_btc     REAL DEFAULT 0,
    tp_dist       REAL DEFAULT 0,
    wallet_before REAL DEFAULT 0,
    sq_call_exit  REAL DEFAULT 0,
    sq_put_exit   REAL DEFAULT 0,
    sq_long_exit  REAL DEFAULT 0,
    sq_short_exit REAL DEFAULT 0,
    options_pnl   REAL DEFAULT 0,
    futures_pnl   REAL DEFAULT 0,
    net_pnl       REAL DEFAULT 0,
    sq_type       TEXT DEFAULT '',
    wallet_after  REAL DEFAULT 0,
    entry_ts_ms   INTEGER DEFAULT 0,
    start_dt      TEXT,
    end_dt        TEXT
);
CREATE TABLE IF NOT EXISTS orders (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id     INTEGER REFERENCES sessions(id),
    paper_order_id INTEGER,
    symbol         TEXT,
    asset_type     TEXT,
    leg_label      TEXT,
    side           TEXT,
    order_type     TEXT,
    qty            REAL,
    limit_price    REAL DEFAULT 0,
    fill_price     REAL DEFAULT 0,
    status         TEXT,
    placed_at      TEXT,
    filled_at      TEXT,
    cancel_reason  TEXT,
    updated_at     TEXT
);
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
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER REFERENCES sessions(id),
    ts         TEXT,
    event_type TEXT,
    detail     TEXT
);
CREATE TABLE IF NOT EXISTS wallet_ledger (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            TEXT NOT NULL,
    session_id    INTEGER DEFAULT 0,
    type          TEXT NOT NULL,
    amount        REAL NOT NULL,
    balance_after REAL NOT NULL,
    note          TEXT
);
CREATE TABLE IF NOT EXISTS bot_status (
    id         INTEGER PRIMARY KEY DEFAULT 1,
    ts         TEXT,
    state      TEXT,
    btc_mark   REAL,
    session_id INTEGER,
    detail     TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_state   ON sessions(state);
CREATE INDEX IF NOT EXISTS idx_sessions_expiry  ON sessions(expiry_dt);
CREATE INDEX IF NOT EXISTS idx_orders_session   ON orders(session_id);
CREATE INDEX IF NOT EXISTS idx_orders_status    ON orders(session_id, status);
CREATE INDEX IF NOT EXISTS idx_snapshots_sess   ON pnl_snapshots(session_id);
CREATE INDEX IF NOT EXISTS idx_events_session   ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_ledger_session   ON wallet_ledger(session_id);
CREATE INDEX IF NOT EXISTS idx_ledger_type      ON wallet_ledger(type);
"""

def _ensure_schema():
    db = sqlite3.connect(_db_path, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=5000")
    db.executescript(_SCHEMA)
    # Safe column migrations for pre-existing DBs — all idempotent
    for sql in [
        "ALTER TABLE orders    ADD COLUMN cancel_reason TEXT",
        "ALTER TABLE orders    ADD COLUMN updated_at    TEXT",
        "ALTER TABLE config    ADD COLUMN section       TEXT",
        "ALTER TABLE config    ADD COLUMN sort_order    INTEGER",
        "ALTER TABLE bot_status ADD COLUMN detail       TEXT",
        "ALTER TABLE sessions  ADD COLUMN entry_btc     REAL DEFAULT 0",
        "ALTER TABLE sessions  ADD COLUMN tp_dist       REAL DEFAULT 0",
        "ALTER TABLE sessions  ADD COLUMN wallet_before REAL DEFAULT 0",
        "ALTER TABLE sessions  ADD COLUMN wallet_after  REAL DEFAULT 0",
        "ALTER TABLE sessions  ADD COLUMN entry_ts_ms   INTEGER DEFAULT 0",
        "ALTER TABLE sessions  ADD COLUMN expiry_dt     TEXT",
    ]:
        try:
            db.execute(sql)
        except Exception:
            pass
    db.commit()
    db.close()

# ── DB helpers ────────────────────────────────────────────────────────────────
def _get_db() -> sqlite3.Connection:
    db = sqlite3.connect(_db_path, check_same_thread=False, timeout=10)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=5000")   # wait up to 5s if bot is writing
    db.execute("PRAGMA synchronous=NORMAL")  # safe + fast with WAL
    db.row_factory = sqlite3.Row
    return db

def _rows(cur) -> List[Dict]:
    return [dict(r) for r in cur.fetchall()]

def _one(cur) -> Optional[Dict]:
    r = cur.fetchone()
    return dict(r) if r else None

def _scalar(cur, default=0):
    r = cur.fetchone()
    return (r[0] if r[0] is not None else default) if r else default

def _fetch_btc_price() -> float:
    """Futures mark price fallback when bot is offline."""
    try:
        with urllib.request.urlopen(
            "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT", timeout=2
        ) as r:
            return float(json.loads(r.read())["price"])
    except Exception:
        return 0.0

def _fetch_btc_spot() -> float:
    """BTC/USDT spot price. Tries Binance Vision CDN (works in India) then global."""
    for url in [
        "https://data-api.binance.vision/api/v3/ticker/price?symbol=BTCUSDT",
        "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
    ]:
        try:
            with urllib.request.urlopen(url, timeout=2) as r:
                return float(json.loads(r.read())["price"])
        except Exception:
            continue
    return 0.0

def _check_password(pw: str) -> bool:
    try:
        db = _get_db()
        row = db.execute("SELECT value FROM config WHERE key='DASHBOARD_PASSWORD'").fetchone()
        db.close()
        return pw == (row[0] if row else "admin1234")
    except Exception:
        return pw == "admin1234"


# ── HTML ──────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def root():
    if HTML_FILE.exists():
        return HTMLResponse(
            content=HTML_FILE.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-store"},
        )
    return HTMLResponse("<h2>straddle_panel.html not found alongside dashboard.py</h2>")


# ── GET /api/live ─────────────────────────────────────────────────────────────
@app.get("/api/live")
async def api_live():
    try:
        db = _get_db()

        # Bot status (single row, written every ~0.5s by trader)
        bot_st = _one(db.execute("SELECT * FROM bot_status WHERE id=1"))
        bot_state  = "OFFLINE"
        btc_mark   = 0.0
        bot_detail: dict = {}
        if bot_st:
            try:
                st_ts = datetime.datetime.fromisoformat(bot_st["ts"]).replace(tzinfo=_IST)
                age_s = (datetime.datetime.now(_IST) - st_ts).total_seconds()
                if age_s < 30:
                    bot_state = bot_st["state"] or "OFFLINE"
                    btc_mark  = float(bot_st["btc_mark"] or 0)
                    if bot_st.get("detail"):
                        try:
                            bot_detail = json.loads(bot_st["detail"])
                        except Exception:
                            pass
            except Exception:
                pass

        # Fallback BTC price when bot is offline
        if btc_mark == 0:
            btc_mark = _fetch_btc_price()

        # Real BTC/USDT spot price (always fetched, not futures)
        btc_spot = _fetch_btc_spot()

        # Active session + latest snapshot
        sess = _one(db.execute(
            "SELECT * FROM sessions WHERE state='ACTIVE' ORDER BY id DESC LIMIT 1"
        ))
        snap = None
        if sess:
            snap = _one(db.execute(
                "SELECT * FROM pnl_snapshots WHERE session_id=? ORDER BY id DESC LIMIT 1",
                (sess["id"],)
            ))

        # Snapshot age
        snap_age_s = None
        if snap and snap.get("ts"):
            try:
                snap_ts    = datetime.datetime.fromisoformat(snap["ts"]).replace(tzinfo=_IST)
                snap_age_s = round((datetime.datetime.now(_IST) - snap_ts).total_seconds())
            except Exception:
                pass

        # Realised PnL: closed sessions only
        realised_pnl: float = _scalar(db.execute(
            "SELECT COALESCE(SUM(net_pnl),0) FROM sessions WHERE state IN ('DONE','COMPLETE')"
        ))

        # Unrealised: from latest snapshot of active session
        unrealised_pnl: float = float((snap or {}).get("total_upnl") or 0)

        # ROI on total capital
        roi_pct: float = (realised_pnl + unrealised_pnl) / TOTAL_CAPITAL * 100

        # Trading lifespan: now − first ever trade start_dt
        trading_days = None
        first_trade = _one(db.execute(
            "SELECT MIN(start_dt) AS first_dt FROM sessions WHERE start_dt IS NOT NULL"
        ))
        if first_trade and first_trade.get("first_dt"):
            try:
                first_dt     = datetime.datetime.fromisoformat(first_trade["first_dt"]).replace(tzinfo=_IST)
                trading_days = round((datetime.datetime.now(_IST) - first_dt).total_seconds() / 86400, 1)
            except Exception:
                pass

        # All orders for active session (full accountability — not just NEW)
        open_orders: List[Dict] = []
        if sess:
            open_orders = _rows(db.execute(
                """SELECT leg_label, symbol, asset_type, side, qty, order_type,
                          limit_price, fill_price, status, placed_at, filled_at, paper_order_id
                   FROM orders WHERE session_id=? ORDER BY id""",
                (sess["id"],)
            ))

        db.close()

        return {
            "bot_state":      bot_state,
            "btc_mark":       btc_mark,
            "btc_spot":       btc_spot,
            "snap_age_s":     snap_age_s,
            "realised_pnl":   round(realised_pnl, 4),
            "unrealised_pnl": round(unrealised_pnl, 4),
            "roi_pct":        round(roi_pct, 4),
            "trading_days":   trading_days,
            "session":        sess,
            "snapshot":       snap,
            "open_orders":    open_orders,
            "bot_detail":     bot_detail,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/sessions ─────────────────────────────────────────────────────────
@app.get("/api/sessions")
async def api_sessions():
    try:
        db = _get_db()
        rows = _rows(db.execute(
            """SELECT id, date, expiry_sym, expiry_dt, state,
                      call_sym, put_sym, total_premium,
                      call_qty, put_qty, long_qty, short_qty,
                      long_entry, short_entry, long_tp_px, short_tp_px,
                      options_pnl, futures_pnl, net_pnl,
                      sq_type, start_dt, end_dt
               FROM sessions ORDER BY id DESC"""
        ))
        for r in rows:
            net = r.get("net_pnl") or 0
            r["roi_pct"] = round(net / TOTAL_CAPITAL * 100, 2)
            if r["state"] == "ACTIVE":
                snap = _one(db.execute(
                    "SELECT total_upnl, btc_mark FROM pnl_snapshots WHERE session_id=? ORDER BY id DESC LIMIT 1",
                    (r["id"],)
                ))
                r["live_upnl"] = float(snap["total_upnl"]) if snap else 0
                r["live_roi"]  = round((snap["total_upnl"] or 0) / TOTAL_CAPITAL * 100, 2) if snap else 0
            else:
                r["live_upnl"] = None
                r["live_roi"]  = None
        db.close()
        return rows
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/sessions/{id} ────────────────────────────────────────────────────
@app.get("/api/stats")
async def api_stats():
    try:
        db = _get_db()
        agg = _one(db.execute("""
            SELECT
                COUNT(*)                                          AS total,
                SUM(CASE WHEN net_pnl > 0    THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN net_pnl < 0    THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN net_pnl = 0    THEN 1 ELSE 0 END) AS breakeven,
                SUM(CASE WHEN sq_type='TP_HIT'     THEN 1 ELSE 0 END) AS tp_hits,
                SUM(CASE WHEN sq_type='PARTIAL_TP' THEN 1 ELSE 0 END) AS partial_tps,
                SUM(CASE WHEN sq_type='TIMEOUT'    THEN 1 ELSE 0 END) AS timeouts,
                ROUND(AVG(net_pnl),     2) AS avg_pnl,
                ROUND(SUM(net_pnl),     2) AS total_pnl,
                ROUND(AVG(options_pnl), 2) AS avg_opts_pnl,
                ROUND(AVG(futures_pnl), 2) AS avg_fut_pnl,
                ROUND(MAX(net_pnl),     2) AS best_pnl,
                ROUND(MIN(net_pnl),     2) AS worst_pnl,
                ROUND(AVG(total_premium), 2) AS avg_premium
            FROM sessions WHERE state='DONE'
        """))
        recent = _rows(db.execute("""
            SELECT id, date, sq_type, total_premium,
                   options_pnl, futures_pnl, net_pnl,
                   long_entry, short_entry, long_tp_px, short_tp_px,
                   start_dt, end_dt
            FROM sessions WHERE state='DONE'
            ORDER BY id DESC LIMIT 15
        """))
        db.close()
        result = dict(agg) if agg else {}
        result["total"]     = result.get("total") or 0
        result["wins"]      = result.get("wins")  or 0
        result["losses"]    = result.get("losses") or 0
        result["win_rate"]  = (round(result["wins"] / result["total"] * 100, 1)
                               if result["total"] > 0 else 0)
        result["tp_rate"]   = (round((result.get("tp_hits") or 0) / result["total"] * 100, 1)
                               if result["total"] > 0 else 0)
        result["recent"]    = recent
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/sessions/{session_id}")
async def api_session_detail(session_id: int):
    try:
        db = _get_db()
        sess = _one(db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)))
        if not sess:
            raise HTTPException(status_code=404, detail="Session not found")

        options_trades = _rows(db.execute(
            "SELECT * FROM orders WHERE session_id=? AND asset_type='option' ORDER BY id",
            (session_id,)
        ))
        futures_trades = _rows(db.execute(
            "SELECT * FROM orders WHERE session_id=? AND asset_type='future' ORDER BY id",
            (session_id,)
        ))
        events = _rows(db.execute(
            "SELECT ts, event_type, detail FROM events WHERE session_id=? ORDER BY id",
            (session_id,)
        ))
        db.close()

        sess_out = dict(sess)
        sess_out["roi_pct"] = round((sess_out.get("net_pnl") or 0) / TOTAL_CAPITAL * 100, 2)

        return {
            "session":        sess_out,
            "options_trades": options_trades,
            "futures_trades": futures_trades,
            "events":         events,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/orders ───────────────────────────────────────────────────────────
@app.get("/api/orders")
async def api_orders(status: str = "ALL", limit: int = 200, offset: int = 0):
    try:
        db = _get_db()
        valid_statuses = {"ALL", "NEW", "FILLED", "CANCELLED", "EXPIRED"}
        if status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid status filter: {status}")
        if status == "ALL":
            rows = _rows(db.execute(
                """SELECT o.id, o.session_id, s.date AS session_date, s.expiry_dt,
                          o.leg_label, o.symbol, o.asset_type, o.side,
                          o.order_type, o.qty, o.limit_price, o.fill_price,
                          o.status, o.cancel_reason, o.placed_at, o.filled_at, o.updated_at
                   FROM orders o
                   LEFT JOIN sessions s ON o.session_id = s.id
                   ORDER BY o.id DESC LIMIT ? OFFSET ?""",
                (limit, offset)
            ))
        else:
            rows = _rows(db.execute(
                """SELECT o.id, o.session_id, s.date AS session_date, s.expiry_dt,
                          o.leg_label, o.symbol, o.asset_type, o.side,
                          o.order_type, o.qty, o.limit_price, o.fill_price,
                          o.status, o.cancel_reason, o.placed_at, o.filled_at, o.updated_at
                   FROM orders o
                   LEFT JOIN sessions s ON o.session_id = s.id
                   WHERE o.status=?
                   ORDER BY o.id DESC LIMIT ? OFFSET ?""",
                (status, limit, offset)
            ))
        db.close()
        return rows
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/settings ─────────────────────────────────────────────────────────
@app.get("/api/settings")
async def api_settings_get(request: Request, password: str = ""):
    pw = request.headers.get("X-Dashboard-Password", password)
    if not _check_password(pw):
        raise HTTPException(status_code=401, detail="Wrong password")
    try:
        db = _get_db()
        rows = _rows(db.execute(
            """SELECT key, value, label, input_type, section, sort_order
               FROM config WHERE is_sensitive=0
               ORDER BY section, sort_order, key"""
        ))
        db.close()
        return rows
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── POST /api/settings ────────────────────────────────────────────────────────
@app.post("/api/settings")
async def api_settings_update(request: Request):
    body     = await request.json()
    password = body.get("password", "")
    if not _check_password(password):
        raise HTTPException(status_code=401, detail="Wrong password")
    settings: List[Dict] = body.get("settings", [])
    if not settings:
        raise HTTPException(status_code=400, detail="No settings provided")
    db = None
    try:
        db = _get_db()
        for item in settings:
            key = item.get("key", "")
            if key and key != "DASHBOARD_PASSWORD" and not key.startswith("_"):
                db.execute("UPDATE config SET value=? WHERE key=?", (str(item.get("value", "")), key))
        # Signal the bot to hot-reload config on its next tick
        db.execute("INSERT OR IGNORE INTO config (key,value) VALUES ('_config_dirty','0')")
        db.execute("UPDATE config SET value='1' WHERE key='_config_dirty'")
        db.commit()
        return {"ok": True, "note": "Settings saved. Bot will reload on next tick — no restart needed."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if db: db.close()


# ── POST /api/settings/password ──────────────────────────────────────────────
@app.post("/api/settings/password")
async def api_change_password(request: Request):
    body   = await request.json()
    old_pw = body.get("old_password", "")
    new_pw = body.get("new_password", "")
    if not _check_password(old_pw):
        raise HTTPException(status_code=401, detail="Wrong current password")
    if len(new_pw) < 6:
        raise HTTPException(status_code=400, detail="Minimum 6 characters")
    try:
        db = _get_db()
        db.execute("UPDATE config SET value=? WHERE key='DASHBOARD_PASSWORD'", (new_pw,))
        db.commit()
        db.close()
        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/journal ─────────────────────────────────────────────────────────
@app.get("/api/journal")
async def api_journal(limit: int = 60):
    try:
        db = _get_db()
        rows = _rows(db.execute("""
            SELECT e.id, e.session_id, e.ts, e.event_type, e.detail,
                   s.date AS session_date, s.expiry_dt
            FROM events e
            LEFT JOIN sessions s ON e.session_id = s.id
            ORDER BY e.id DESC LIMIT ?
        """, (min(limit, 200),)))
        db.close()
        return rows
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/export/sessions ─────────────────────────────────────────────────
@app.get("/api/export/sessions")
async def api_export_sessions():
    import io
    import csv
    from fastapi.responses import StreamingResponse
    try:
        db = _get_db()
        rows = _rows(db.execute("""
            SELECT id, date, expiry_sym, state,
                   call_sym, put_sym, call_fill, put_fill, call_qty, put_qty,
                   total_premium, long_entry, short_entry, long_tp_px, short_tp_px,
                   sq_call_exit, sq_put_exit, sq_long_exit, sq_short_exit,
                   options_pnl, futures_pnl, net_pnl, sq_type, start_dt, end_dt
            FROM sessions ORDER BY id
        """))
        db.close()
        output = io.StringIO()
        if rows:
            writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=straddle_sessions.csv"}
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── GET /api/wallet_ledger ────────────────────────────────────────────────────
@app.get("/api/wallet_ledger")
async def api_wallet_ledger(limit: int = 100):
    try:
        db = _get_db()
        rows = _rows(db.execute(
            """SELECT wl.id, wl.ts, wl.session_id, wl.type, wl.amount, wl.balance_after, wl.note,
                      s.expiry_dt
               FROM wallet_ledger wl
               LEFT JOIN sessions s ON wl.session_id = s.id
               ORDER BY wl.id DESC LIMIT ?""",
            (min(limit, 500),)
        ))
        # Also return current balance (last row) and running totals
        totals = {}
        if rows:
            totals = {
                "current_balance": rows[0]["balance_after"],
                "total_credits": sum(r["amount"] for r in rows if r["amount"] > 0),
                "total_debits":  sum(r["amount"] for r in rows if r["amount"] < 0),
            }
        db.close()
        return {"ledger": rows, "summary": totals}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--db",   type=str, default=DEFAULT_DB)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    args    = parser.parse_args()
    _db_path = args.db
    _ensure_schema()
    print(f"\n  Straddle Dashboard  http://localhost:{args.port}\n  DB: {_db_path}\n")
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
