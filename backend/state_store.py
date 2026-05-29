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
"""


class StateStore:
    def __init__(self):
        self._pg_pool  = None
        self._db_path: Optional[str] = None
        self._cache: Dict[str, Any]  = {}

    # ── Connection ─────────────────────────────────────────────────────────

    async def connect(self, dsn: str = ""):
        loop = asyncio.get_event_loop()
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

        self._db_path = "hedge_state.db"
        await loop.run_in_executor(_pool, self._sqlite_init)
        await loop.run_in_executor(_pool, self._sqlite_load_all)
        log.info(f"SQLite state store ready: {self._db_path}")

    # ── SQLite init ────────────────────────────────────────────────────────

    def _sqlite_init(self):
        with sqlite3.connect(self._db_path) as c:
            c.executescript(_SCHEMA)
        # Migration: add is_force_closed if not present
        with sqlite3.connect(self._db_path) as c:
            try:
                c.execute("ALTER TABLE trading_sessions ADD COLUMN is_force_closed INTEGER DEFAULT 0")
            except Exception:
                pass  # column already exists

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
        loop = asyncio.get_event_loop()
        if self._db_path:
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
            loop = asyncio.get_event_loop()
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
        loop = asyncio.get_event_loop()
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
                    futures_pnl, hedge_pnl, total_pnl, status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(session_id) DO UPDATE SET
                     close_reason=excluded.close_reason,
                     close_ts_ist=excluded.close_ts_ist,
                     futures_pnl=excluded.futures_pnl,
                     hedge_pnl=excluded.hedge_pnl,
                     total_pnl=excluded.total_pnl,
                     status=excluded.status""",
                (
                    row["session_id"], row["trader_name"], row["session_date"],
                    row.get("target_line"), row.get("entry_zone", ""),
                    row.get("entry_price"), row.get("entry_ts_ist", ""),
                    row.get("close_reason", ""), row.get("close_ts_ist", ""),
                    row.get("futures_pnl", 0), row.get("hedge_pnl", 0),
                    row.get("total_pnl", 0), row.get("status", "open"),
                ),
            )

    async def save_session(self, session_id: str, trader_name: str, **kwargs):
        from backend.utils import ist_now
        row = {
            "session_id": session_id,
            "trader_name": trader_name,
            "session_date": ist_now().strftime("%Y-%m-%d"),
            **kwargs,
        }
        if self._db_path:
            loop = asyncio.get_event_loop()
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
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(_pool, self._sqlite_get_sessions,
                                              trader_name, limit)
        except Exception as e:
            log.error(f"get_sessions error: {e}")
            return []

    def _sqlite_get_sessions_filtered(self, trader_name: str, from_date: str, to_date: str, limit: int, exclude_force_closed: bool = True) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row
            query = "SELECT * FROM trading_sessions"
            params = []
            conditions = []
            if trader_name and trader_name.lower() != "all":
                conditions.append("trader_name=?")
                params.append(trader_name)
            if from_date:
                conditions.append("session_date >= ?")
                params.append(from_date)
            if to_date:
                conditions.append("session_date <= ?")
                params.append(to_date)
            if exclude_force_closed:
                conditions.append("(is_force_closed IS NULL OR is_force_closed=0)")
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            rows = c.execute(query, tuple(params)).fetchall()
        return [dict(r) for r in rows]

    async def get_sessions_filtered(self, trader_name: str, from_date: str = "", to_date: str = "", limit: int = 100, exclude_force_closed: bool = True) -> list:
        if not self._db_path: return []
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(_pool, self._sqlite_get_sessions_filtered, trader_name, from_date, to_date, limit, exclude_force_closed)
        except Exception:
            return []

    def _sqlite_get_trades_by_session(self, session_id: str) -> list:
        with sqlite3.connect(self._db_path) as c:
            c.row_factory = sqlite3.Row
            rows = c.execute("SELECT * FROM paper_trades WHERE session_id=? ORDER BY ts_utc ASC", (session_id,)).fetchall()
        return [dict(r) for r in rows]

    async def get_trades_by_session(self, session_id: str) -> list:
        if not self._db_path: return []
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(_pool, self._sqlite_get_trades_by_session, session_id)
        except Exception:
            return []

    # ── Delete Trader History ─────────────────────────────────────────────

    def _sqlite_delete_trader_history(self, trader_name: str):
        with sqlite3.connect(self._db_path) as c:
            c.execute("DELETE FROM paper_trades WHERE trader_name=?", (trader_name,))
            c.execute("DELETE FROM trading_sessions WHERE trader_name=?", (trader_name,))

    async def delete_trader_history(self, trader_name: str):
        if not self._db_path: return
        loop = asyncio.get_event_loop()
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
        loop = asyncio.get_event_loop()
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
            loop = asyncio.get_event_loop()
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
        loop = asyncio.get_event_loop()
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
        loop = asyncio.get_event_loop()
        if self._db_path:
            try:
                await loop.run_in_executor(_pool, self._sqlite_log_trade_legacy, row)
            except Exception as e:
                log.error(f"log_trade error: {e}")
        return row

    async def get_recent_trades(self, limit: int = 200, is_paper: bool = True) -> list:
        if not self._db_path:
            return []
        loop = asyncio.get_event_loop()
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


store = StateStore()
