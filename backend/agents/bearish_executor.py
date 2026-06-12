"""
Bearish Hedge Executor.
Strategy: SHORT futures + nearest ITM CALL as hedge.

At window open: snapshots nearest supply OB zone on all 4 TFs (5m/15m/1h/4h).
Entry when price enters any zone's tolerance window; smallest TF takes priority.
"""

from backend.agents.base_executor import BaseExecutor


class BearishExecutor(BaseExecutor):
    def __init__(self, is_paper: bool = False, force_window: bool = False,
                 paper_engine=None):
        super().__init__(
            name="BearishExecutor" + ("_Paper" if is_paper else ""),
            direction="BEARISH",
            is_paper=is_paper,
            force_window=force_window,
            paper_engine=paper_engine,
        )

    @property
    def _cfg_prefix(self) -> str:
        return "bear_"

    @property
    def _futures_side(self) -> str:
        return "SELL"

    @property
    def _option_side(self) -> str:
        return "C"

    def _is_eligible(self, price: float, _target_line: float) -> bool:
        """Eligible when price is within tolerance of any supply OB zone (smallest TF wins)."""
        return self._find_active_ob_tf(price) is not None
