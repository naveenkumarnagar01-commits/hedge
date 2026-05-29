"""
Paper Trading Engine.

Balance model (futures):
  - Balance changes ONLY when a position is CLOSED, by the realized PnL.
  - Opening a futures position does not debit balance (futures use margin).
  - Equity = balance (realized) + unrealized mark-to-market.

Balance model (options):
  - Buying an option debits the premium from balance immediately.
  - Selling an option credits the proceeds to balance immediately.

Executor tagging:
  - Set paper.executor = self.name before any call so every trade record
    carries the executor that placed it.
"""

import logging
import time
from typing import Dict, List

from backend.config import cfg
from backend.state_store import store
from backend.utils import ist_now_str

log = logging.getLogger("paper_engine")


class PaperEngine:
    def __init__(self):
        self.balance:                float      = cfg.virtual_balance_usdt
        self.session_start_balance:  float      = cfg.virtual_balance_usdt
        self.equity_curve:           List[dict] = []
        self.trade_history:          List[dict] = []
        # Positions keyed as "SYMBOL::executor" so bull/bear don't cancel each other
        self._positions:        Dict[str, dict] = {}
        self._option_positions: Dict[str, dict] = {}
        self.executor:          str        = ""   # set by caller before each trade
        # Tracks ONLY closed-position PnL (option sells + closed futures)
        # Option buys do NOT count as realized — they are capital deployment
        self._realized_pnl:     float      = 0.0

    # ── Persistence ────────────────────────────────────────────────────────

    def load_state(self, state_key: str = "paper_engine_state"):
        """Restore paper engine state from SQLite — called once at startup.
        trade_history and equity_curve are NOT restored — they are session-only
        and should start fresh on each server run."""
        saved = store.get(state_key)
        if not saved:
            return
        self.balance               = float(saved.get("balance", self.balance))
        self.session_start_balance = self.balance
        self._positions            = saved.get("positions", {})
        self._option_positions     = saved.get("option_positions", {})
        self._realized_pnl         = float(saved.get("realized_pnl", 0.0))
        # trade_history / equity_curve intentionally NOT restored (session-only)
        log.info(
            f"Paper engine state restored [{state_key}]: balance={self.balance:,.2f}  "
            f"positions={len(self._positions)}"
        )

    async def save_state(self):
        """Persist paper engine state to SQLite after every trade."""
        key = getattr(self, "_state_key", "paper_engine_state")
        await store.set(key, {
            "balance":          self.balance,
            "positions":        dict(self._positions),
            "option_positions": dict(self._option_positions),
            "realized_pnl":     self._realized_pnl,
        })

    async def reset(self):
        self.balance                = cfg.virtual_balance_usdt
        self.session_start_balance  = cfg.virtual_balance_usdt
        self.equity_curve           = []
        self.trade_history          = []
        self._positions             = {}
        self._option_positions      = {}
        self._realized_pnl          = 0.0
        key = getattr(self, "_state_key", "paper_engine_state")
        await store.set(key, {
            "balance": self.balance, "positions": {}, "option_positions": {}, "realized_pnl": 0.0,
        })
        log.info(f"Paper engine reset. Balance: {self.balance:,.2f} USDT")

    def clear_session_trades(self):
        """Clear trade history and equity curve at the start of a new trading session.
        Balance and open positions are NOT affected."""
        self.trade_history = []
        self.equity_curve  = []
        self.session_start_balance = self.balance
        log.info(f"Session trades cleared. Balance carried forward: {self.balance:,.2f} USDT")

    # ── Price lookups ──────────────────────────────────────────────────────

    def _mark_price(self) -> float:
        from backend.data.futures_feed import get_state
        return float(get_state().get("mark_price", 0) or 0)

    def _option_ask(self, symbol: str) -> float:
        from backend.data.options_feed import get_chain
        return float(get_chain().get(symbol, {}).get("ask", 0) or 0)

    def _option_bid(self, symbol: str) -> float:
        from backend.data.options_feed import get_chain
        return float(get_chain().get(symbol, {}).get("bid", 0) or 0)

    # ── Core fill ─────────────────────────────────────────────────────────

    async def _fill_futures(self, symbol: str, side: str,
                             qty: float, fill_price: float,
                             action: str = "FUTURES") -> dict:
        # Key positions per executor so bull LONG and bear SHORT don't cancel each other
        exec_tag = self.executor or "default"
        pos_key  = f"{symbol}::{exec_tag}"
        pos = self._positions.get(pos_key)
        pnl = 0.0

        if side == "BUY":
            if pos and pos["side"] == "SELL":
                close_qty = min(qty, pos["qty"])
                pnl = (pos["avg_price"] - fill_price) * close_qty
                self.balance     += pnl
                self._realized_pnl += pnl          # closed position → realized
                pos["qty"] -= close_qty
                if pos["qty"] <= 1e-8:
                    self._positions.pop(pos_key, None)
            else:
                if pos:
                    total = pos["qty"] + qty
                    pos["avg_price"] = (pos["avg_price"]*pos["qty"] + fill_price*qty) / total
                    pos["qty"] = total
                else:
                    self._positions[pos_key] = {
                        "side": "BUY", "qty": qty, "avg_price": fill_price,
                        "executor": exec_tag, "symbol": symbol,
                    }
        else:
            if pos and pos["side"] == "BUY":
                close_qty = min(qty, pos["qty"])
                pnl = (fill_price - pos["avg_price"]) * close_qty
                self.balance     += pnl
                self._realized_pnl += pnl          # closed position → realized
                pos["qty"] -= close_qty
                if pos["qty"] <= 1e-8:
                    self._positions.pop(pos_key, None)
            else:
                if pos:
                    total = pos["qty"] + qty
                    pos["avg_price"] = (pos["avg_price"]*pos["qty"] + fill_price*qty) / total
                    pos["qty"] = total
                else:
                    self._positions[pos_key] = {
                        "side": "SELL", "qty": qty, "avg_price": fill_price,
                        "executor": exec_tag, "symbol": symbol,
                    }

        row = {
            "ts_ist":    ist_now_str(),
            "action":    action,
            "symbol":    symbol,
            "side":      side,
            "qty":       qty,
            "fill_price":fill_price,
            "pnl":       round(pnl, 2),
            "is_paper":  True,
            "executor":  self.executor,
        }
        self.trade_history.append(row)
        self._snapshot_equity()
        return {"order_id": f"paper_{int(time.time()*1000)}",
                "avg_price": fill_price, "qty": qty, "filled": True}

    # ── Public futures API ────────────────────────────────────────────────

    async def place_futures_order(self, symbol: str, side: str,
                                   qty: float, price: float = 0,
                                   action: str = "FUTURES") -> dict:
        fill_price = self._mark_price() or price
        if not fill_price:
            return {}
        return await self._fill_futures(symbol, side, qty, fill_price, action)

    async def place_futures_limit(self, symbol: str, side: str,
                                   qty: float, limit_price: float,
                                   action: str = "FUTURES") -> dict:
        if not limit_price:
            return {}
        return await self._fill_futures(symbol, side, qty, limit_price, action)

    # ── Options ────────────────────────────────────────────────────────────

    async def buy_option(self, symbol: str, qty: float, ask_price: float,
                         action: str = "OPTION_BUY") -> dict:
        fill_price = self._option_ask(symbol) or ask_price
        if not fill_price:
            return {}
        cost = fill_price * qty
        if self.balance - cost < cfg.min_paper_balance:
            log.warning(
                f"Paper balance floor: balance={self.balance:.2f} - cost={cost:.2f} "
                f"would go below min={cfg.min_paper_balance:.2f}. Order rejected."
            )
            from backend import telegram_alert as tg
            tg.send(
                f"⚠️ <b>Paper Balance Floor Hit</b>\n"
                f"Balance: ${self.balance:.2f}  Cost: ${cost:.2f}\n"
                f"Trading paused — reset account or lower min_paper_balance."
            )
            return {}
        self.balance -= cost
        exec_tag = self.executor or "default"
        opt_key  = f"{symbol}::{exec_tag}"
        self._option_positions[opt_key] = {
            "side": "BUY", "qty": qty, "avg_price": fill_price,
            "ts": time.time(), "symbol": symbol, "executor": exec_tag,
        }
        row = {
            "ts_ist":    ist_now_str(),
            "action":    action,
            "symbol":    symbol,
            "side":      "BUY",
            "qty":       qty,
            "fill_price":fill_price,
            "pnl":       0.0,
            "is_paper":  True,
            "executor":  self.executor,
        }
        self.trade_history.append(row)
        self._snapshot_equity()
        return {"order_id": f"paper_opt_{int(time.time()*1000)}",
                "avg_price": fill_price, "qty": qty, "filled": True}

    async def sell_option(self, symbol: str, qty: float, bid_price: float,
                          action: str = "OPTION_SELL") -> dict:
        fill_price = self._option_bid(symbol) or bid_price or 0
        proceeds   = fill_price * qty
        self.balance += proceeds
        exec_tag = self.executor or "default"
        opt_key  = f"{symbol}::{exec_tag}"
        pos = self._option_positions.pop(opt_key, {})
        pnl = (fill_price - pos.get("avg_price", 0)) * qty
        self._realized_pnl += pnl          # option sold → realized
        row = {
            "ts_ist":    ist_now_str(),
            "action":    action,
            "symbol":    symbol,
            "side":      "SELL",
            "qty":       qty,
            "fill_price":fill_price,
            "pnl":       round(pnl, 2),
            "is_paper":  True,
            "executor":  self.executor,
        }
        self.trade_history.append(row)
        self._snapshot_equity()
        return {"order_id": f"paper_opt_{int(time.time()*1000)}",
                "avg_price": fill_price, "qty": qty, "filled": True}

    # ── Equity snapshot ───────────────────────────────────────────────────

    def _option_current_value(self) -> float:
        """Current market value of all open option positions (absolute, NOT MTM change).
        Used for equity calculation: equity = cash + option_current_value + futures_mtm."""
        from backend.data.options_feed import get_chain
        chain = get_chain()
        total = 0.0
        for composite_key, pos in self._option_positions.items():
            sym  = pos.get("symbol") or composite_key.split("::")[0]
            opt  = chain.get(sym, {})
            mark = float(opt.get("mark", 0) or 0)
            bid  = float(opt.get("bid",  0) or 0)
            intr = float(opt.get("intrinsic", 0) or 0)
            cur  = mark or bid or intr
            if cur > 0:
                total += cur * pos["qty"]
            else:
                # Chain not yet loaded — use entry price so equity stays flat until chain arrives
                total += pos["avg_price"] * pos["qty"]
        return total

    def _option_cost_deployed(self) -> float:
        """Total premium paid for all currently open option positions."""
        return sum(pos["avg_price"] * pos["qty"] for pos in self._option_positions.values())

    def _snapshot_equity(self):
        mark = self._mark_price()
        futures_mtm = 0.0
        if mark:
            for pos in self._positions.values():
                if pos["side"] == "BUY":
                    futures_mtm += (mark - pos["avg_price"]) * pos["qty"]
                else:
                    futures_mtm += (pos["avg_price"] - mark) * pos["qty"]
        # equity = cash + current_option_value + futures_mtm
        equity = self.balance + self._option_current_value() + futures_mtm
        self.equity_curve.append({
            "ts":      time.time(),
            "ts_ist":  ist_now_str(),
            "equity":  round(equity, 2),
            "balance": round(self.balance, 2),
        })
        if len(self.equity_curve) > 10_000:
            self.equity_curve = self.equity_curve[-10_000:]
        # Persist after every trade so state survives restarts
        import asyncio
        asyncio.create_task(self.save_state())

    # ── Summary ───────────────────────────────────────────────────────────

    def get_summary(self) -> dict:
        start = cfg.virtual_balance_usdt   # always 100k

        # ── Futures unrealized (MTM change on open futures positions) ───────
        mark = self._mark_price()
        futures_mtm = 0.0
        if mark:
            for pos in self._positions.values():
                if pos["side"] == "BUY":
                    futures_mtm += (mark - pos["avg_price"]) * pos["qty"]
                else:
                    futures_mtm += (pos["avg_price"] - mark) * pos["qty"]

        # ── Options ─────────────────────────────────────────────────────────
        opt_current_value = self._option_current_value()   # abs market value
        opt_cost          = self._option_cost_deployed()   # what was paid
        opt_mtm           = opt_current_value - opt_cost   # change from cost

        # ── Core accounting ─────────────────────────────────────────────────
        # equity = cash + current option value + futures MTM
        # (balance already has option cost deducted, so we add back current value)
        equity       = round(self.balance + opt_current_value + futures_mtm, 2)

        # unrealized = how much open positions have moved from entry (shown to user)
        unrealized   = round(opt_mtm + futures_mtm, 2)

        # realized = only from CLOSED positions (option sells + closed futures)
        realized_pnl = round(self._realized_pnl, 2)

        # total = full picture = realized + unrealized = equity - start
        total_pnl    = round(equity - start, 2)

        # session_pnl = intraday reference (equity vs window-open balance)
        session_pnl  = round(equity - self.session_start_balance, 2)

        return {
            "balance":          round(self.balance, 2),
            "unrealized_pnl":   unrealized,
            "equity":           equity,
            "total_pnl":        total_pnl,
            "realized_pnl":     realized_pnl,
            "session_pnl":      session_pnl,
            "starting_balance": round(start, 2),
            "open_positions":   dict(self._positions),
            "option_positions": dict(self._option_positions),
            "trade_count":      len(self.trade_history),
            "equity_curve":     self.equity_curve[-200:],
            "recent_trades":    list(reversed(self.trade_history[-100:])),
        }

    def get_trades_by_executor(self, executor: str) -> List[dict]:
        return list(reversed([t for t in self.trade_history if t.get("executor") == executor]))

    def clear_executor_positions(self, executor_tag: str):
        """
        Remove all open futures + option positions for one executor from in-memory tracking.
        Used when the user manually clears a position. Does NOT affect balance.
        """
        fut_keys = [k for k in list(self._positions)      if k.endswith(f"::{executor_tag}")]
        opt_keys = [k for k in list(self._option_positions) if k.endswith(f"::{executor_tag}")]
        for k in fut_keys:
            self._positions.pop(k, None)
        for k in opt_keys:
            self._option_positions.pop(k, None)
        log.info(
            f"[clear_executor_positions] {executor_tag}: "
            f"removed {len(fut_keys)} futures + {len(opt_keys)} option position(s)."
        )


paper = PaperEngine()
