"""
Bullish Hedge Executor.
Strategy: LONG futures + nearest ITM PUT as hedge.

At window open: snapshots nearest demand OB zone on all 4 TFs (5m/15m/1h/4h).
Entry when price enters any zone's tolerance window; smallest TF takes priority.
"""

from backend.agents.base_executor import BaseExecutor


class BullishExecutor(BaseExecutor):
    def __init__(self, is_paper: bool = False, force_window: bool = False,
                 paper_engine=None):
        super().__init__(
            name="BullishExecutor" + ("_Paper" if is_paper else ""),
            direction="BULLISH",
            is_paper=is_paper,
            force_window=force_window,
            paper_engine=paper_engine,
        )

    @property
    def _cfg_prefix(self) -> str:
        return "bull_"

    @property
    def _futures_side(self) -> str:
        return "BUY"

    @property
    def _option_side(self) -> str:
        return "P"

    def _is_eligible(self, price: float, _target_line: float) -> bool:
        """Eligible when price is within tolerance of any demand OB zone (smallest TF wins)."""
        return self._find_active_ob_tf(price) is not None
