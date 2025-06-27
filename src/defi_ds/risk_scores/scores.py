import math
from typing import Optional
import pandas as pd
import numpy as np


def garman_klass_volatility(price_data, window=30, trading_periods=252, clean=True):

    log_hl = (price_data["high"] / price_data["low"]).apply(np.log)
    log_co = (price_data["close"] / price_data["open"]).apply(np.log)

    rs = 0.5 * log_hl**2 - (2 * math.log(2) - 1) * log_co**2

    def f(v):
        return (trading_periods * v.mean()) ** 0.5

    result = rs.rolling(window=window, center=False).apply(func=f)

    if clean:
        return result.dropna()
    else:
        return result


def score_with_limits(
    value: float,
    upper_limit: float,
    lower_limit: float,
    reverse: bool = False,
    target: Optional[float] = None,
) -> float:
    """
    Score a value based on limits, with optional target value for peak scoring.

    Args:
        value: Value to score
        upper_limit: Upper boundary
        lower_limit: Lower boundary
        reverse: If True, higher values get better scores
        target: Target value for peak scoring (optional)

    Returns:
        Score between 0 and 1
    """
    if pd.isna(value):
        return 0.0

    if target is not None:
        # Peak scoring around target value
        if reverse:
            # For VaR: closer to target (less negative) is better
            distance = abs(value - target)
            max_distance = max(abs(upper_limit - target), abs(lower_limit - target))
            score = 1 - (distance / max_distance)
        else:
            # For other metrics: closer to target is better
            distance = abs(value - target)
            max_distance = max(abs(upper_limit - target), abs(lower_limit - target))
            score = 1 - (distance / max_distance)
    else:
        # Linear scoring between limits
        if reverse:
            # Higher values get better scores (for VaR, less negative is better)
            if value >= upper_limit:
                score = 1.0
            elif value <= lower_limit:
                score = 0.0
            else:
                score = (value - lower_limit) / (upper_limit - lower_limit)
        else:
            # Lower values get better scores
            if value <= lower_limit:
                score = 1.0
            elif value >= upper_limit:
                score = 0.0
            else:
                score = (upper_limit - value) / (upper_limit - lower_limit)

    return np.clip(score, 0.0, 1.0)
