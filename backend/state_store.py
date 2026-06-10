"""
State persistence — SQLite (primary) with PostgreSQL fallback.

Tables:
  kv_state             — arbitrary key-value state (executor snapshots, config, etc.)
  paper_trades         — persistent trade history per trader (survives restarts)
  trading_sessions     — per-trader session records (window open → squareoff)
  trader_config        — per-trader user-saved config defaults
  system_events        — log of important system events (connections, errors, etc.)
"""

import asyncio
import json
import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

log  = logging.getLogger("state_store")
_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="state_io")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS kv_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at REAL
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    trader_name   TEXT    NOT NULL,
    session_id    TEXT,
    ts_ist        TEXT    NOT NULL,
    ts_utc        REAL    NOT NULL,
    action        TEXT    NOT NULL,
    symbol        TEXT,
    side          TEXT,
    qty           REAL    DEFAULT 0,
    fill_price    REAL    DEFAULT 0,
    pnl           REAL    DEFAULT 0,
    notes         TEXT,
    created_at    REAL    DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_pt_trader ON paper_trades(trader_name, ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_pt_session ON paper_trades(session_id);

CREATE TABLE IF NOT EXISTS trading_sessions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      TEXT    UNIQUE NOT NULL,
    trader_name     TEXT    NOT NULL,
    session_date    TEXT    NOT NULL,
    target_line     REAL,
    entry_zone      TEXT,
    entry_price     REAL,
    entry_ts_ist    TEXT,
    close_reason    TEXT,
    close_ts_ist    TEXT,
    futures_pnl     REAL    DEFAULT 0,
    hedge_pnl       REAL    DEFAULT 0,
    total_pnl       REAL    DEFAULT 0,
    status          TEXT    DEFAULT 'open',
    created_at      REAL    DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_ts_trader ON trading_sessions(trader_name, created_at DESC);

CREATE TABLE IF NOT EXISTS trader_config (
    trader_name  TEXT    PRIMARY KEY,
    config_json  TEXT    NOT NULL,
    updated_at   REAL    DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS system_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT,
    source     TEXT,
    message    TEXT,
    ts_ist     TEXT,
    ts_utc     REAL    DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS trade_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc     REAL,
    ts_ist     TEXT,
    executor   TEXT,
    action     TEXT,
    instrument TEXT,
    side       TEXT,
    qty        REAL,
    price      REAL,
    pnl        REAL,
    status     TEXT,
    detail     TEXT,
    is_paper   INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS session_event_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    trader_name  TEXT    NOT NULL,
    session_date TEXT    NOT NULL,
    event_ts_ist TEXT    NOT NULL,
    event_type   TEXT    NOT NULL,
    state        TEXT,
    price        REAL,
    locked_line  REAL,
    message      TEXT,
    created_at   REAL    DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_sel_trader_date ON session_event_log(trader_name, session_date DESC);

CREATE TABLE IF NOT EXISTS ob_history (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    trader_name      TEXT    NOT NULL,
    session_date     TEXT    NOT NULL,
    tf               TEXT    NOT NULL,
    zone_type        TEXT    NOT NULL,
    status           TEXT    NOT NULL,
    born_ts          INTEGER,
    zone_top         REAL,
    zone_bottom      REAL,
    zone_mid         REAL,
    zone_score       REAL,
    zone_grade       TEXT,
    wait_started_ts  TEXT,
    found_at_ts      TEXT,
    locked_at_ts     TEXT,
    notes            TEXT,
    created_at       REAL    DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_ob_trader_date ON ob_history(trader_name, session_date DESC);

CREATE TABLE IF NOT EXISTS ob_zone_lifecycle (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    tf               TEXT    NOT NULL,
    zone_type        TEXT    NOT NULL,
    born_ts          INTEGER NOT NULL,
    top              REAL    NOT NULL,
    bottom           REAL    NOT NULL,
    mid              REAL    NOT NULL,
    grade            TEXT,
    score            REAL,
    created_ist      TEXT    NOT NULL,
    last_seen_ist    TEXT,
    consumed_ist     TEXT,
    consumed_price   REAL,
    is_active        INTEGER DEFAULT 1,
    created_at       REAL    DEFAULT (strftime('%s','now')),
    UNIQUE(tf, zone_type, born_ts)
);
CREATE INDEX IF NOT EXISTS idx_obzl_tf_active ON ob_zone_lifecycle(tf, zone_type, is_active);
"""


class StateStore:
    def __init__(self):
        self._pg_pool  = None
        self._db_path: Optional[str] = None
        self._cache: Dict[str, Any]  = {}

    # ── Connection ─────────────────────────────────────────────────────────

    async def connect(self, dsn: str = ""):
        loop = asyncio.get_running_loop()
        if dsn and dsn.startswith("postgresql"):
            try:
                import asyncpg
                self._pg_pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
                await self._pg_ensure_table()
                await self._pg_load_all()
                log.info("PostgreSQL connected.")
                return
            except Exception as e:
                log.warning(f"PostgreSQL unavailable ({e}) — using SQLite.")

        # Use env var DB_PATH if set, otherwise absolute path next to this file
        # so the DB is found regardless of which directory the server starts from.
        import os as _os
        default_db = _os.path.join(
            _os.path.dirname(_os.path.abspath(__file__)), "..", "hedge_state.db"
        )
        self._db_path = _os.path.abspath(
            _os.environ.get("DB_PATH", default_db)
        )
        await loop.run_in_executor(_pool, self._sqlite_init)
        await loop.run_in_executor(_pool, self._sqlite_load_all)
        log.info(f"SQLite state store ready: {self._db_path}")

    # ── SQLite init ────────────────────────────────────────────────────────

    def _sqlite_init(self):
        with sqlite3.connect(self._db_path) as c:
            c.execute("PRAGMA journal_mode=WAL")   # concurrent-safe writes
            c.execute("PRAGMA synchronous=NORMAL")
            c.executescript(_SCHEMA)
        # Migrations — each wrapped in try/except since the column may already exist
        with sqlite3.connect(self._db_path) as c:
            for ddl in [
                "ALTER TABLE trading_sessions ADD COLUMN is_force_closed INTEGER DEFAULT 0",
                "ALTER TABLE trading_sessions ADD COLUMN balance_before REAL",
                "ALTER TABLE trading_sessions ADD COLUMN balance_after  REAL",
            ]:
                try: c.execute(ddl)
                except Exception: pass
        # session_event_log: schema handles creation; no column migrations needed yet
        # ob_history migrations — run in order before index creation
        with sqlite3.connect(self._db_path) as c:
            for ddl in [
                "ALTER TABLE ob_history ADD COLUMN born_ts INTEGER",
            ]:
                try: c.execute(ddl)
                except Exception: pass
            # Partial unique index for snapshot dedup — must be after born_ts column exists
            try:
                c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_ob_snapshot_dedup
                             ON ob_history(trader_name, tf, born_ts)
                             WHERE trader_name='__snapshot__'""")
            except Exception: pass

    def _sqlite_load_all(self):
        with sqlite3.connect(self._db_path) as c:
            for key, val in c.execute("SELECT key, value FROM kv_state").fetchall():
                try:
                    self._cache[key] = json.loads(val)
                except Exception:
                    pass

    def _sqlite_write(self, key: str, value: Any):
        with sqlite3.connect(self._db_path) as c:
            c.execute(
                "INSERT OR REPLACE INTO kv_state (key, value, updated_at) VALUES (?,?,?)",
                (key, json.dumps(value), time.time()),
            )

    # ── Public KV API ──────────────────────────────────────────────────────

    async def set(self, key: str, value: Any):
        self._cache[key] = value
        loop = asyncio.get_running_loop()
        if self._pg_pool:
            try:
                await self._pg_write(key, value)
            except Exception as e:
                log.error(f"PostgreSQL save [{key}]: {e}")
        elif self._db_path:
            try:
                await loop.run_in_executor(_pool, self._sqlite_write, key, value)
            except Exception as e:
                log.error(f"SQLite save [{key}]: {e}")

    def get(self, key: str, default=None) -> Any:
        return self._cache.get(key, default)

    # ── Paper Trades (structured, per-trader) ─────────────────────────────

    def _sqlite_insert_paper_trade(self, row: dict):
        with sqlite3.connect(self._db_path) as c:
            c.execute(
                """INSERT INTO paper_trades
                   (trader_name, session_id, ts_ist, ts_utc, action, symbol,
                    side, qty, fill_price, pnl, notes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["trader_name"], row.get("session_id"),
                    row["ts_ist"], row["ts_utc"],
                    row["action"], row.get("symbol", ""),
                    row.get("side", ""), row.get("qty", 0),
                    row.get("fill_price", 0), row.get("pnl", 0),
                    row.get("notes", ""),
                ),
            )

    async def save_paper_trade(self, trader_name: str, action: str, symbol: str,
                                side: str, qty: float, fill_price: float, pnl: float,
                                session_id: str = "", notes: str = "",
                                ts_ist: str = "", ts_utc: float = 0.0):
        from backend.utils import utc_now, ist_now_str
        # Use caller-supplied execution timestamp when provided so DB records
        # match the exact moment the order was placed, not when it was persisted.
        row = {
            "trader_name": trader_name, "session_id": session_id,
            "ts_ist": ts_ist or ist_now_str(),
            "ts_utc": ts_utc or utc_now(),
            "action": action, "symbol": symbol,
            "side": side, "qty": qty, "fill_price": fill_price,
            "pnl": pnl, "notes": notes,
        }
        if self._db_path:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(_pool, self._sqlite_insert_paper_trade, row)
            except Exception as e:
                log.error(f"save_paper_trade error: {e}")
        return row

    def _sqlite_get_paper_trades(self, trader_name: str, limit: int) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                "SELECT * FROM paper_trades WHERE trader_name=? ORDER BY ts_utc DESC LIMIT ?",
                (trader_name, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    async def get_paper_trades(self, trader_name: str, limit: int = 200) -> list:
        if not self._db_path:
            return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(_pool, self._sqlite_get_paper_trades,
                                              trader_name, limit)
        except Exception as e:
            log.error(f"get_paper_trades error: {e}")
            return []

    # ── Trading Sessions ──────────────────────────────────────────────────

    def _sqlite_upsert_session(self, row: dict):
        with sqlite3.connect(self._db_path) as c:
            c.execute(
                """INSERT INTO trading_sessions
                   (session_id, trader_name, session_date, target_line, entry_zone,
                    entry_price, entry_ts_ist, close_reason, close_ts_ist,
                    futures_pnl, hedge_pnl, total_pnl, status,
                    balance_before, balance_after)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(session_id) DO UPDATE SET
                     close_reason=excluded.close_reason,
                     close_ts_ist=excluded.close_ts_ist,
                     futures_pnl=excluded.futures_pnl,
                     hedge_pnl=excluded.hedge_pnl,
                     total_pnl=excluded.total_pnl,
                     status=excluded.status,
                     balance_after=COALESCE(excluded.balance_after, trading_sessions.balance_after)""",
                (
                    row["session_id"], row["trader_name"], row["session_date"],
                    row.get("target_line"), row.get("entry_zone", ""),
                    row.get("entry_price"), row.get("entry_ts_ist", ""),
                    row.get("close_reason", ""), row.get("close_ts_ist", ""),
                    row.get("futures_pnl", 0), row.get("hedge_pnl", 0),
                    row.get("total_pnl", 0), row.get("status", "open"),
                    row.get("balance_before"), row.get("balance_after"),
                ),
            )

    async def save_session(self, session_id: str, trader_name: str, **kwargs):
        from backend.utils import ist_now, get_session_day
        from backend.config import cfg as _cfg
        n = ist_now()
        try:
            exp_h = int(getattr(_cfg, "session_expiry_h", 13))
            exp_m = int(getattr(_cfg, "session_expiry_m", 30))
            session_date = get_session_day(n, exp_h, exp_m)
        except Exception:
            session_date = n.strftime("%Y-%m-%d")
        row = {
            "session_id": session_id,
            "trader_name": trader_name,
            "session_date": kwargs.pop("session_date", session_date),
            **kwargs,
        }
        if self._db_path:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(_pool, self._sqlite_upsert_session, row)
            except Exception as e:
                log.error(f"save_session error: {e}")

    def _sqlite_get_sessions(self, trader_name: str, limit: int) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                "SELECT * FROM trading_sessions WHERE trader_name=? ORDER BY created_at DESC LIMIT ?",
                (trader_name, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    async def get_sessions(self, trader_name: str, limit: int = 50) -> list:
        if not self._db_path:
            return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(_pool, self._sqlite_get_sessions,
                                              trader_name, limit)
        except Exception as e:
            log.error(f"get_sessions error: {e}")
            return []

    def _sqlite_get_sessions_filtered(self, trader_name: str, from_date: str, to_date: str, limit: int) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row

            # ── Part A: real sessions from trading_sessions ──────────────────
            conds_a, params_a = [], []
            if trader_name and trader_name.lower() != "all":
                conds_a.append("trader_name=?"); params_a.append(trader_name)
            if from_date:
                conds_a.append("session_date >= ?"); params_a.append(from_date)
            if to_date:
                conds_a.append("session_date <= ?"); params_a.append(to_date)
            where_a = ("WHERE " + " AND ".join(conds_a)) if conds_a else ""
            q_a = f"""
                SELECT session_id, trader_name, session_date,
                       target_line, entry_zone, entry_price,
                       entry_ts_ist, close_reason, close_ts_ist,
                       futures_pnl, hedge_pnl, total_pnl, status,
                       created_at, is_force_closed
                FROM trading_sessions {where_a}"""

            # ── Part B: no-trade activity days (events exist, no session row) ──
            # A "no-trade day" = session_event_log has rows for that trader+date
            # but trading_sessions does NOT have a row for that same trader+date.
            conds_b, params_b = [], []
            if trader_name and trader_name.lower() != "all":
                conds_b.append("e.trader_name=?"); params_b.append(trader_name)
            if from_date:
                conds_b.append("e.session_date >= ?"); params_b.append(from_date)
            if to_date:
                conds_b.append("e.session_date <= ?"); params_b.append(to_date)
            where_b = ("WHERE " + " AND ".join(conds_b) + " AND ") if conds_b else "WHERE "
            q_b = f"""
                SELECT
                    'notrade_' || e.trader_name || '_' || e.session_date AS session_id,
                    e.trader_name,
                    e.session_date,
                    NULL AS target_line,
                    NULL AS entry_zone,
                    NULL AS entry_price,
                    MIN(e.event_ts_ist) AS entry_ts_ist,
                    NULL AS close_reason,
                    MAX(e.event_ts_ist) AS close_ts_ist,
                    0.0 AS futures_pnl,
                    0.0 AS hedge_pnl,
                    0.0 AS total_pnl,
                    'no_trade' AS status,
                    MIN(e.created_at) AS created_at,
                    0 AS is_force_closed
                FROM session_event_log e
                {where_b}NOT EXISTS (
                    SELECT 1 FROM trading_sessions t
                    WHERE t.trader_name = e.trader_name
                      AND t.session_date = e.session_date
                )
                GROUP BY e.trader_name, e.session_date"""

            union_q = f"{q_a} UNION ALL {q_b} ORDER BY created_at DESC LIMIT ?"
            rows = c.execute(union_q, tuple(params_a + params_b + [limit])).fetchall()
        return [dict(r) for r in rows]

    async def get_sessions_filtered(self, trader_name: str, from_date: str = "", to_date: str = "", limit: int = 100) -> list:
        if not self._db_path: return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(_pool, self._sqlite_get_sessions_filtered, trader_name, from_date, to_date, limit)
        except Exception:
            return []

    def _sqlite_get_trades_by_session(self, session_id: str) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute("SELECT * FROM paper_trades WHERE session_id=? ORDER BY ts_utc ASC", (session_id,)).fetchall()
        return [dict(r) for r in rows]

    async def get_trades_by_session(self, session_id: str) -> list:
        if not self._db_path: return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(_pool, self._sqlite_get_trades_by_session, session_id)
        except Exception:
            return []

    # ── Delete Trader History ─────────────────────────────────────────────

    def _sqlite_delete_trader_history(self, trader_name: str):
        with sqlite3.connect(self._db_path) as c:
            c.execute("DELETE FROM paper_trades WHERE trader_name=?", (trader_name,))
            c.execute("DELETE FROM trading_sessions WHERE trader_name=?", (trader_name,))
            # Also wipe persisted executor state so it doesn't reload stale PnL on restart
            c.execute("DELETE FROM kv_state WHERE key=?", (f"{trader_name}_state",))

    async def delete_trader_history(self, trader_name: str):
        if not self._db_path: return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(_pool, self._sqlite_delete_trader_history, trader_name)
            # Also remove from kv_state cache for this trader
            self._cache.pop(f"{trader_name}_state", None)
        except Exception as e:
            log.error(f"delete_trader_history error: {e}")

    # ── Mark Session Force-Closed ─────────────────────────────────────────

    def _sqlite_mark_force_closed(self, session_id: str):
        with sqlite3.connect(self._db_path) as c:
            c.execute(
                "UPDATE trading_sessions SET is_force_closed=1, status='force_closed' WHERE session_id=?",
                (session_id,)
            )

    async def mark_session_force_closed(self, session_id: str):
        if not self._db_path or not session_id: return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(_pool, self._sqlite_mark_force_closed, session_id)
        except Exception as e:
            log.error(f"mark_session_force_closed error: {e}")

    # ── Per-trader Config Defaults ─────────────────────────────────────────

    def _sqlite_save_trader_cfg(self, trader_name: str, cfg: dict):
        with sqlite3.connect(self._db_path) as c:
            c.execute(
                """INSERT INTO trader_config (trader_name, config_json, updated_at)
                   VALUES (?,?,?)
                   ON CONFLICT(trader_name) DO UPDATE SET
                     config_json=excluded.config_json,
                     updated_at=excluded.updated_at""",
                (trader_name, json.dumps(cfg), time.time()),
            )

    async def save_trader_config(self, trader_name: str, config: dict):
        if self._db_path:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(_pool, self._sqlite_save_trader_cfg,
                                           trader_name, config)
            except Exception as e:
                log.error(f"save_trader_config error: {e}")

    def _sqlite_get_trader_cfg(self, trader_name: str) -> Optional[dict]:
        with sqlite3.connect(self._db_path) as c:
            row = c.execute(
                "SELECT config_json FROM trader_config WHERE trader_name=?",
                (trader_name,),
            ).fetchone()
        return json.loads(row[0]) if row else None

    async def get_trader_config(self, trader_name: str) -> Optional[dict]:
        if not self._db_path:
            return None
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(_pool, self._sqlite_get_trader_cfg, trader_name)
        except Exception as e:
            log.error(f"get_trader_config error: {e}")
            return None

    # ── Legacy trade_log (kept for backward-compat) ────────────────────────

    def _sqlite_log_trade_legacy(self, row: dict):
        with sqlite3.connect(self._db_path) as c:
            c.execute(
                """INSERT INTO trade_log
                   (ts_utc,ts_ist,executor,action,instrument,side,qty,price,pnl,status,detail,is_paper)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["ts_utc"], row["ts_ist"], row["executor"],
                    row["action"], row["instrument"], row["side"],
                    row["qty"], row["price"], row["pnl"],
                    row["status"], json.dumps(row.get("detail") or {}),
                    1 if row.get("is_paper") else 0,
                ),
            )

    async def log_trade(self, executor: str, action: str, instrument: str,
                        side: str, qty: float, price: float, pnl: float,
                        status: str, detail: dict = None, is_paper: bool = True):
        from backend.utils import utc_now, ist_now_str
        row = {
            "ts_utc": utc_now(), "ts_ist": ist_now_str(),
            "executor": executor, "action": action, "instrument": instrument,
            "side": side, "qty": qty, "price": price, "pnl": pnl,
            "status": status, "detail": detail or {}, "is_paper": is_paper,
        }
        loop = asyncio.get_running_loop()
        if self._db_path:
            try:
                await loop.run_in_executor(_pool, self._sqlite_log_trade_legacy, row)
            except Exception as e:
                log.error(f"log_trade error: {e}")
        return row

    async def get_recent_trades(self, limit: int = 200, is_paper: bool = True) -> list:
        if not self._db_path:
            return []
        loop = asyncio.get_running_loop()
        try:
            def _q():
                with sqlite3.connect(self._db_path) as c:
                    c.row_factory = sqlite3.Row
                    rows = c.execute(
                        "SELECT * FROM trade_log WHERE is_paper=? ORDER BY ts_utc DESC LIMIT ?",
                        (1 if is_paper else 0, limit),
                    ).fetchall()
                return [dict(r) for r in rows]
            return await loop.run_in_executor(_pool, _q)
        except Exception:
            return []

    async def get_manager_logs(self, _limit: int = 200) -> list:
        return []

    async def log_analyst(self, high_line: float, low_line: float,
                          candidates: list, touches: list):
        await self.set("analyst_lines", {
            "high_line": high_line, "low_line": low_line,
            "candidates": candidates, "touches": touches,
        })

    # ── Session Event Log ──────────────────────────────────────────────────

    def _sqlite_insert_session_event(self, row: dict):
        with sqlite3.connect(self._db_path) as c:
            c.execute(
                """INSERT INTO session_event_log
                   (trader_name, session_date, event_ts_ist, event_type,
                    state, price, locked_line, message)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (row["trader_name"], row["session_date"], row["event_ts_ist"],
                 row["event_type"], row.get("state"), row.get("price"),
                 row.get("locked_line"), row.get("message", "")),
            )

    def _sqlite_get_session_events(self, trader_name: str,
                                    session_date: str, limit: int) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row
            clauses, params = ["trader_name=?"], [trader_name]
            if session_date:
                clauses.append("session_date=?"); params.append(session_date)
            params.append(limit)
            rows = c.execute(
                f"SELECT * FROM session_event_log WHERE {' AND '.join(clauses)} "
                f"ORDER BY created_at ASC LIMIT ?",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    async def save_session_event(self, trader_name: str, session_date: str,
                                  event_ts_ist: str, event_type: str,
                                  state: str = "", price: float = 0.0,
                                  locked_line: float = 0.0, message: str = ""):
        row = {
            "trader_name": trader_name, "session_date": session_date,
            "event_ts_ist": event_ts_ist, "event_type": event_type,
            "state": state, "price": price or None,
            "locked_line": locked_line or None, "message": message,
        }
        if self._db_path:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(_pool, self._sqlite_insert_session_event, row)
            except Exception as e:
                log.error(f"save_session_event error: {e}")

    def _sqlite_get_session_events_by_id(self, session_id: str) -> list:
        """
        Get session_event_log rows for a session.
        Handles two session_id formats:
          - Real trade:  '2026-06-08_00-05-04_IST'  → lookup trader from trading_sessions, ±1 day
          - No-trade:    'notrade_BullishExecutor_Paper_2026-06-07' → parse trader+date directly
        """
        from datetime import datetime, timedelta
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row

            if session_id.startswith("notrade_"):
                # Format: notrade_{trader_name}_{YYYY-MM-DD}
                # Last 10 chars = date, everything between prefix and date = trader_name
                date_str    = session_id[-10:]
                trader_name = session_id[len("notrade_"):-11]  # strip prefix + '_' + date
                candidates  = [date_str]
            else:
                # Real session: first 10 chars = date portion
                date_str = session_id[:10]
                try:
                    d = datetime.strptime(date_str, "%Y-%m-%d")
                    candidates = [date_str, (d - timedelta(days=1)).strftime("%Y-%m-%d")]
                except Exception:
                    candidates = [date_str]
                row = c.execute(
                    "SELECT trader_name FROM trading_sessions WHERE session_id=?",
                    (session_id,)
                ).fetchone()
                if not row:
                    return []
                trader_name = row["trader_name"]

            placeholders = ",".join("?" for _ in candidates)
            rows = c.execute(
                f"SELECT * FROM session_event_log WHERE trader_name=? "
                f"AND session_date IN ({placeholders}) ORDER BY created_at ASC",
                [trader_name] + candidates,
            ).fetchall()
        return [dict(r) for r in rows]

    async def get_session_events_by_id(self, session_id: str) -> list:
        if not self._db_path or not session_id:
            return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                _pool, self._sqlite_get_session_events_by_id, session_id
            )
        except Exception as e:
            log.error(f"get_session_events_by_id error: {e}")
            return []

    async def get_session_events(self, trader_name: str, session_date: str = "",
                                  limit: int = 500) -> list:
        if not self._db_path:
            return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                _pool, self._sqlite_get_session_events,
                trader_name, session_date, limit
            )
        except Exception as e:
            log.error(f"get_session_events error: {e}")
            return []

    def _sqlite_get_session_dates(self, trader_name: str, limit: int) -> list:
        with sqlite3.connect(self._db_path) as c:
            rows = c.execute(
                """SELECT DISTINCT session_date FROM session_event_log
                   WHERE trader_name=? ORDER BY session_date DESC LIMIT ?""",
                (trader_name, limit),
            ).fetchall()
        return [r[0] for r in rows]

    async def get_session_dates(self, trader_name: str, limit: int = 30) -> list:
        if not self._db_path:
            return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                _pool, self._sqlite_get_session_dates, trader_name, limit
            )
        except Exception as e:
            return []

    # ── OB History ─────────────────────────────────────────────────────────

    def _sqlite_upsert_ob_record(self, row: dict):
        with sqlite3.connect(self._db_path) as c:
            # INSERT OR IGNORE: snapshot records deduplicated by (trader_name, tf, born_ts)
            # via the partial unique index; trader records always insert fresh rows.
            c.execute(
                """INSERT OR IGNORE INTO ob_history
                   (trader_name, session_date, tf, zone_type, status, born_ts,
                    zone_top, zone_bottom, zone_mid, zone_score, zone_grade,
                    wait_started_ts, found_at_ts, locked_at_ts, notes)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["trader_name"], row["session_date"], row["tf"],
                    row["zone_type"], row["status"], row.get("born_ts"),
                    row.get("zone_top"), row.get("zone_bottom"), row.get("zone_mid"),
                    row.get("zone_score"), row.get("zone_grade"),
                    row.get("wait_started_ts"), row.get("found_at_ts"),
                    row.get("locked_at_ts"), row.get("notes", ""),
                ),
            )

    def _sqlite_update_ob_record(self, trader_name: str, session_date: str, tf: str,
                                  updates: dict):
        if not updates:
            return
        cols = ", ".join(f"{k}=?" for k in updates)
        # Update the most recent row for this trader+date+tf so "waiting" becomes "formed"
        vals = list(updates.values()) + [trader_name, session_date, tf]
        with sqlite3.connect(self._db_path) as c:
            c.execute(
                f"""UPDATE ob_history SET {cols}
                    WHERE id = (
                        SELECT id FROM ob_history
                        WHERE trader_name=? AND session_date=? AND tf=?
                        ORDER BY id DESC LIMIT 1
                    )""",
                vals,
            )

    def _sqlite_get_ob_history(self, trader_name: Optional[str],
                                session_date: Optional[str], limit: int) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row
            clauses, params = [], []
            if trader_name:
                clauses.append("trader_name=?"); params.append(trader_name)
            if session_date:
                clauses.append("session_date=?"); params.append(session_date)
            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            params.append(limit)
            rows = c.execute(
                f"SELECT * FROM ob_history {where} ORDER BY created_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    async def save_ob_record(self, trader_name: str, session_date: str, tf: str,
                              zone_type: str, status: str,
                              born_ts: int = 0,
                              zone_top=None, zone_bottom=None, zone_mid=None,
                              zone_score=None, zone_grade=None,
                              wait_started_ts: str = "", found_at_ts: str = "",
                              locked_at_ts: str = "", notes: str = ""):
        row = {
            "trader_name": trader_name, "session_date": session_date,
            "tf": tf, "zone_type": zone_type, "status": status,
            "born_ts": born_ts or None,
            "zone_top": zone_top, "zone_bottom": zone_bottom, "zone_mid": zone_mid,
            "zone_score": zone_score, "zone_grade": zone_grade,
            "wait_started_ts": wait_started_ts, "found_at_ts": found_at_ts,
            "locked_at_ts": locked_at_ts, "notes": notes,
        }
        if self._db_path:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(_pool, self._sqlite_upsert_ob_record, row)
            except Exception as e:
                log.error(f"save_ob_record error: {e}")

    async def update_ob_record(self, trader_name: str, session_date: str, tf: str,
                                **updates):
        if self._db_path:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(
                    _pool, self._sqlite_update_ob_record,
                    trader_name, session_date, tf, updates
                )
            except Exception as e:
                log.error(f"update_ob_record error: {e}")

    async def get_ob_history(self, trader_name: str = "", session_date: str = "",
                              limit: int = 50) -> list:
        if not self._db_path:
            return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                _pool, self._sqlite_get_ob_history,
                trader_name or None, session_date or None, limit
            )
        except Exception as e:
            log.error(f"get_ob_history error: {e}")
            return []

    # ── PostgreSQL stubs ────────────────────────────────────────────────────

    async def _pg_ensure_table(self):
        async with self._pg_pool.acquire() as con:
            await con.execute("""
                CREATE TABLE IF NOT EXISTS agent_state (
                    key TEXT PRIMARY KEY,
                    value JSONB NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL
                );
            """)

    async def _pg_load_all(self):
        async with self._pg_pool.acquire() as con:
            rows = await con.fetch("SELECT key, value FROM agent_state")
            for row in rows:
                self._cache[row["key"]] = json.loads(row["value"])

    async def _pg_write(self, key: str, value: Any):
        async with self._pg_pool.acquire() as con:
            await con.execute(
                """INSERT INTO agent_state (key, value, updated_at)
                   VALUES ($1, $2::jsonb, $3)
                   ON CONFLICT (key) DO UPDATE
                   SET value = EXCLUDED.value,
                       updated_at = EXCLUDED.updated_at""",
                key, json.dumps(value), time.time(),
            )


    # ── OB Zone Tracking (all 4 TF, creation + consumption) ──────────────

    # ── OB Zone Lifecycle (per-TF, nearest zone creation + consumption) ────

    def _sqlite_upsert_ob_lifecycle(self, zones: list, tf: str, zone_type: str,
                                     now_ist: str, consumed_born_ts: list,
                                     consumed_price: Optional[float]):
        with sqlite3.connect(self._db_path) as c:
            # Insert new zones (IGNORE duplicate born_ts)
            for z in zones:
                c.execute(
                    """INSERT OR IGNORE INTO ob_zone_lifecycle
                       (tf, zone_type, born_ts, top, bottom, mid, grade, score,
                        created_ist, last_seen_ist, is_active)
                       VALUES (?,?,?,?,?,?,?,?,?,?,1)""",
                    (tf, zone_type, z["born_ts"], z["top"], z["bottom"], z["mid"],
                     z.get("grade"), z.get("score"), now_ist, now_ist),
                )
                c.execute(
                    """UPDATE ob_zone_lifecycle SET last_seen_ist=?
                       WHERE tf=? AND zone_type=? AND born_ts=? AND is_active=1""",
                    (now_ist, tf, zone_type, z["born_ts"]),
                )
            # Mark consumed zones
            for bts in consumed_born_ts:
                c.execute(
                    """UPDATE ob_zone_lifecycle
                       SET is_active=0, consumed_ist=?, consumed_price=?
                       WHERE tf=? AND zone_type=? AND born_ts=? AND is_active=1""",
                    (now_ist, consumed_price, tf, zone_type, bts),
                )

    def _sqlite_get_ob_lifecycle(self, tf: str, limit: int) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute(
                """SELECT * FROM ob_zone_lifecycle
                   WHERE tf=? ORDER BY born_ts DESC LIMIT ?""",
                (tf, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    async def upsert_ob_lifecycle(self, zones: list, tf: str, zone_type: str,
                                   now_ist: str, consumed_born_ts: list,
                                   consumed_price: Optional[float] = None):
        if not self._db_path:
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                _pool, self._sqlite_upsert_ob_lifecycle,
                zones, tf, zone_type, now_ist, consumed_born_ts, consumed_price
            )
        except Exception as e:
            log.error(f"upsert_ob_lifecycle error: {e}")

    async def get_ob_lifecycle(self, tf: str, limit: int = 60) -> list:
        if not self._db_path:
            return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                _pool, self._sqlite_get_ob_lifecycle, tf, limit
            )
        except Exception as e:
            log.error(f"get_ob_lifecycle error: {e}")
            return []


store = StateStore()
