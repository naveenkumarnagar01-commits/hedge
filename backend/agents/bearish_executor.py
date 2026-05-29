"""
Bearish Hedge Executor.
Strategy: SHORT futures + nearest ITM CALL as hedge.

At window open: fetches nearest supply Order Block zone.
Entry when price within ±max_distance_from_line of zone mid AND option conditions pass.
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

    def _is_eligible(self, price: float, target_line: float) -> bool:
        """Eligible when price is within max_distance_from_line of the supply OB mid."""
        max_dist = self._cfg("max_distance_from_line") or 100.0
        return abs(price - target_line) <= max_dist


