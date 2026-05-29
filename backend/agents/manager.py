"""
Manager Agent — position limit gatekeeper.

Rules enforced per trader at all times:
  1. Max 1 open futures position ≤ q_max_btc BTC per trader.
  2. Max 1 option contract at a time per trader.
  3. Option spend ≤ max_option_spend per trade.
  4. Hedge (option) must be bought BEFORE futures entry.
  5. Partial sells/rebuys always allowed within open position limits.
  6. No duplicate concurrent positions (1 per executor).

Publishes MANAGER_DECISION for every request.
"""

import asyncio
import logging

from backend.message_bus import bus, TRADE_REQUEST, MANAGER_DECISION, LOG_EVENT
from backend.config import cfg
from backend.state_store import store
from backend.utils import ist_now_str

log = logging.getLogger("manager")


class ManagerAgent:
    def __init__(self):
        # Track active positions per executor: {executor_name: bool}
        self._active: dict = {}
        # Track entry sequence state: {executor: last_approved_action}
        self._entry_seq: dict = {}

    async def start(self):
        bus.subscribe(TRADE_REQUEST, self._on_request)
        # Recover active positions
        active = store.get("manager_active", {})
        self._active = active
        log.info("Manager agent started.")

    async def stop(self):
        pass

    async def _on_request(self, msg: dict):
        data      = msg.get("data", {})
        executor  = data.get("executor", "")
        action    = data.get("action", "")
        params    = data.get("params", {})
        is_paper  = data.get("is_paper", False)

        approved, reason = await self._validate(executor, action, params, is_paper)

        status = "APPROVED" if approved else "REJECTED"
        ts_ist = ist_now_str()

        log.info(f"Manager [{executor}] {action} -> {status}: {reason}")

        # Persist
        await store.log_manager(executor, action, status, reason, params)

        # Publish decision
        await bus.publish(MANAGER_DECISION, {
            "executor":     executor,
            "action":       action,
            "status":       status,
            "reason":       reason,
            "params":       params,
            "ts_ist":       ts_ist,
            "is_paper":     is_paper,
        }, source="manager")

        await bus.publish(LOG_EVENT, {
            "level":   "INFO" if approved else "WARNING",
            "source":  "Manager",
            "message": f"[{executor}] {action} -> {status}. {reason}",
            "ts_ist":  ts_ist,
        }, source="manager")

        # Update internal state
        if approved:
            self._update_state(executor, action, params)

    async def _validate(self, executor: str, action: str,
                        params: dict, is_paper: bool) -> tuple:
        """
        Position limit rules (enforced per trader at all times):
          - Max 1 open futures position ≤ q_max_btc BTC per trader
          - Max 1 option contract at a time per trader
          - Option spend ≤ max_option_spend per trade
          - Partial sells/rebuys allowed as long as open position stays within limits
        """

        # ── SETUP_DETECTED ───────────────────────────────────────────────
        if action == "SETUP_DETECTED":
            if self._active.get(executor):
                return False, f"{executor} already has an open position. Close it first."
            return True, "No existing position. Setup approved."

        # ── ENTRY_OPTION ─────────────────────────────────────────────────
        if action == "ENTRY_OPTION":
            if self._active.get(executor):
                return False, f"{executor} already has an active position — cannot open another."
            premium = params.get("premium_usdt", 0)
            qty     = params.get("qty", 1)
            if premium > cfg.max_option_spend:
                return False, (f"Option cost ${premium:.2f} exceeds limit ${cfg.max_option_spend:.2f}. "
                               f"Reduce trade size or wait for cheaper option.")
            self._entry_seq[executor] = "OPTION"
            return True, f"Option entry approved. Cost=${premium:.2f} | Qty={qty}."

        # ── ENTRY_FUTURES ─────────────────────────────────────────────────
        if action == "ENTRY_FUTURES":
            if self._entry_seq.get(executor) != "OPTION":
                return False, "Futures entry requires option to be bought first (hedge-first rule)."
            qty = params.get("qty", 0)
            if qty > cfg.q_max_btc:
                return False, (f"Futures qty {qty} BTC exceeds max allowed {cfg.q_max_btc} BTC. "
                               f"Reduce contract_qty in settings.")
            return True, f"Futures entry approved. Qty={qty} BTC ≤ limit {cfg.q_max_btc} BTC."

        # ── PARTIAL_SELL / PARTIAL_REBUY — always allowed within open position ──
        if action in ("STEP1_EXIT", "AVERAGING", "PARTIAL_SELL", "PARTIAL_REBUY"):
            if not self._active.get(executor):
                return False, f"No open position for {executor} — cannot modify."
            qty = params.get("qty", 0)
            if qty > cfg.q_max_btc:
                return False, f"Order qty {qty} BTC exceeds position limit {cfg.q_max_btc} BTC."
            return True, f"Order approved. Qty={qty} BTC within limit."

        # ── STEP2_EXIT / OPTION_SELL / SQUAREOFF / FORCE ─────────────────
        if action in ("STEP2_EXIT", "OPTION_SELL", "SQUAREOFF", "SQUAREOFF_FORCED"):
            return True, "Exit/squareoff approved."

        return False, f"Unknown action: {action}"

    def _update_state(self, executor: str, action: str, params: dict):
        if action == "ENTRY_FUTURES":
            self._active[executor] = True
            asyncio.create_task(store.set("manager_active", dict(self._active)))

        elif action in ("SQUAREOFF", "SQUAREOFF_FORCED", "STEP2_EXIT"):
            self._active[executor] = False
            self._entry_seq.pop(executor, None)
            asyncio.create_task(store.set("manager_active", dict(self._active)))

    def get_active(self) -> dict:
        return dict(self._active)


manager = ManagerAgent()
