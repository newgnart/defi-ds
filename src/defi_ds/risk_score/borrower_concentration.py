import math
from typing import Optional, Dict, Union
import numpy as np
import pandas as pd
from .calculator import garman_klass_volatility, score_with_limits, hhi
from .utils import round_value, ffill_df


class BorrowerConcentration:
    """
    Class to calculate borrower concentration.

    Args:
        df: DataFrame with columns ['address', 'date', 'debt']

    """

    def __init__(
        self,
        df: pd.DataFrame,
    ):
        self.df = df
        self.daily_hhi = self._calculate_daily_hhi()

    def _calculate_daily_hhi(self) -> pd.DataFrame:
        """
        Calculate daily HHI ratio.
        """
        # Apply hhi function and split the tuple into separate columns
        hhi_results = self.df.groupby("date").debt.apply(lambda x: hhi(x.tolist()))

        # Convert to DataFrame with separate columns
        hhi_df = pd.DataFrame(
            hhi_results.tolist(), index=hhi_results.index, columns=["hhi", "hhi_ideal"]
        ).reset_index()

        return ffill_df(hhi_df)

    def relative_hhi(
        self,
        recent_days: int = 7,
        history_days: int = 30,
    ) -> Dict[str, float]:
        """
        Calculate relative distribution score.

        Args:
            recent_days: Number of recent days to consider
            history_days: Number of days to consider in the history
        """

        recent_hhi = self.daily_hhi.tail(recent_days)["hhi"].mean()
        history_hhi = self.daily_hhi.tail(history_days)["hhi"].mean()
        ratio = recent_hhi / history_hhi
        score = score_with_limits(ratio, 1.1, 0.9, 1.06)
        return {
            f"{recent_days} day HHI": f"{recent_hhi:.2e}",
            f"{history_days} day HHI": f"{history_hhi:.2e}",
            "Ratio": round_value(ratio, 4),
            "Relative score": round_value(score),
        }

    def benchamark_hhi(
        self,
    ) -> Dict[str, float]:
        """
        Calculate benchmark HHI ratio.
        """
        current_hhi = self.daily_hhi.iloc[-1]["hhi"]
        current_hhi_ideal = self.daily_hhi.iloc[-1]["hhi_ideal"]
        score = score_with_limits(current_hhi / current_hhi_ideal, 1.1, 0.9, 1.06)
        return {
            "Current HHI": f"{current_hhi:.2e}",
            "HHI ideal": f"{current_hhi_ideal:.2e}",
            "Ratio": round_value(current_hhi / current_hhi_ideal, 4),
            "Benchmark score": round_value(score),
        }
