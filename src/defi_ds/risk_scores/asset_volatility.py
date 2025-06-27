import math
from typing import Optional, Dict, Union
import numpy as np
import pandas as pd
from .scores import garman_klass_volatility, score_with_limits


def _round_value(value: Union[float, np.number]) -> float:
    """Helper function to round numeric values consistently."""
    return value.round(2).item() if hasattr(value, "round") else round(value, 2)


class AssetVolatility:
    """
    Class to calculate asset volatility using the Garman-Klass method.

    Args:
        df: DataFrame with columns ['open', 'high', 'low', 'close']

    """

    def __init__(
        self,
        df: pd.DataFrame,
        reference_df: pd.DataFrame,
        trading_periods: int = 252,
    ):
        self.df = df
        self.reference_df = reference_df
        self.trading_periods = trading_periods

    def volatility_ratio_score(
        self,
        window1: int = 45,
        window2: int = 180,
    ) -> Dict[str, float]:
        """
        Calculate volatility ratio score.

        Args:
            window: Rolling window size for volatility calculation
            trading_periods: Number of trading periods in a year
            clean: If True, drop missing values
        """
        volatility1 = garman_klass_volatility(
            self.df, window=window1, trading_periods=self.trading_periods
        )
        volatility2 = garman_klass_volatility(
            self.df, window=window2, trading_periods=self.trading_periods
        )
        ratio = volatility1.iloc[-1] / volatility2.iloc[-1]
        score = score_with_limits(ratio, 1.5, 0.75)
        return {
            f"{window1} day volatility": _round_value(volatility1.iloc[-1]),
            f"{window2} day volatility": _round_value(volatility2.iloc[-1]),
            "Volatility ratio": _round_value(ratio),
            "Volatility score": _round_value(score),
        }

    def beta_score(self) -> Dict[str, float]:
        """
        Calculate beta score measuring correlation with BTC price movements.

        Args:
            df: DataFrame with asset OHLC data
            reference_df: DataFrame with reference OHLC data, usually BTC

        Returns:
            Dictionary with beta analysis values and score
        """
        # Calculate returns
        asset_returns = self.df["close"].pct_change().dropna()
        reference_returns = self.reference_df["close"].pct_change().dropna()

        # Align data
        common_index = asset_returns.index.intersection(reference_returns.index)
        asset_returns = asset_returns.loc[common_index]
        reference_returns = reference_returns.loc[common_index]

        if len(common_index) < 30:  # Need minimum data points
            return {
                "Asset volatility": 0.0,
                "Reference volatility": 0.0,
                "Correlation": 0.0,
                "Beta": 0.0,
                "Beta score": 0.0,
            }

        # Calculate correlation
        correlation = asset_returns.corr(reference_returns)

        if pd.isna(correlation):
            return {
                "Asset volatility": 0.0,
                "Reference volatility": 0.0,
                "Correlation": 0.0,
                "Beta": 0.0,
                "Beta score": 0.0,
            }

        # Calculate volatilities
        asset_volatility = garman_klass_volatility(
            self.df, window=45, trading_periods=self.trading_periods
        ).iloc[-1]
        reference_volatility = garman_klass_volatility(
            self.reference_df, window=45, trading_periods=self.trading_periods
        ).iloc[-1]

        if (
            pd.isna(asset_volatility)
            or pd.isna(reference_volatility)
            or reference_volatility == 0
        ):
            return {
                "Asset volatility": 0.0,
                "Reference volatility": 0.0,
                "Correlation": 0.0,
                "Beta": 0.0,
                "Beta score": 0.0,
            }

        # Calculate beta
        beta = correlation * (asset_volatility / reference_volatility)

        # Score with limits: 2.5 (bad), 0.5 (good), target=1.75
        score = score_with_limits(beta, 2.5, 0.5, reverse=False, target=1.75)

        return {
            "Asset volatility": _round_value(asset_volatility),
            "Reference volatility": _round_value(reference_volatility),
            "Correlation": _round_value(correlation),
            "Beta": _round_value(beta),
            "Beta score": _round_value(score),
        }

    def var_score(
        self,
    ) -> Dict[str, float]:
        """
        Calculate VaR score using 1st percentile of daily returns.

        Args:
            df: DataFrame with OHLC data

        Returns:
            Dictionary with VaR value and score
        """
        # Calculate daily percentage changes
        daily_returns = self.df["close"].pct_change().dropna()

        if len(daily_returns) < 30:  # Need minimum data points
            return {"99% VaR": 0.0, "VaR score": 0.0}

        # Calculate VaR (1st percentile)
        var_99 = np.percentile(daily_returns, 1)

        # Score with limits: -0.01 (good), -0.12 (bad), target=-0.085
        score = score_with_limits(var_99, -0.01, -0.12, reverse=True, target=-0.085)

        return {
            "99% VaR": _round_value(var_99),
            "VaR score": _round_value(score),
        }

    def final_score(self) -> float:
        """
        Calculate final score.
        """
        return _round_value(
            self.volatility_ratio_score()["Volatility score"] * 0.3
            + self.beta_score()["Beta score"] * 0.3
            + self.var_score()["VaR score"] * 0.4
        )

    def final_score_dict(self) -> Dict[str, float]:
        """
        Calculate final score.
        """
        return {
            "Volatility ratio score": self.volatility_ratio_score(),
            "Beta score": self.beta_score(),
            "VaR score": self.var_score(),
            "Final score": self.final_score(),
        }
