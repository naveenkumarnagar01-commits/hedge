"""
Bullish Hedge Executor.
Strategy: LONG futures + nearest ITM PUT as hedge.

At window open: fetches nearest demand Order Block zone.
Entry when price within ±max_distance_from_line of zone mid AND option conditions pass.
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

    def _is_eligible(self, price: float, target_line: float) -> bool:
        """Eligible when price is within max_distance_from_line of the demand OB mid."""
        max_dist = self._cfg("max_distance_from_line") or 100.0
        return abs(price - target_line) <= max_dist


