from typing import Union
import numpy as np
import pandas as pd


def round_value(value: Union[float, np.number], decimals: int = 2) -> float:
    """Helper function to round numeric values consistently."""
    return (
        value.round(decimals).item()
        if hasattr(value, "round")
        else round(value, decimals)
    )


def ffill_df(
    df: pd.DataFrame, date_col: str = "date", sort: bool = True
) -> pd.DataFrame:
    df[date_col] = pd.to_datetime(df[date_col])
    if sort:
        df.sort_values(by=date_col, inplace=True)
    date_range = pd.date_range(
        start=df[date_col].iloc[0], end=df[date_col].iloc[-1], freq="D"
    )
    return (
        pd.DataFrame({date_col: date_range})
        .merge(df, on=date_col, how="left")
        .fillna(method="ffill")
    )
