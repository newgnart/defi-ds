import math
import numpy as np


def standard_deviation(price_data, window=30, trading_periods=252, clean=True):

    log_return = (price_data["close"] / price_data["close"].shift(1)).apply(np.log)

    result = log_return.rolling(window=window, center=False).std() * math.sqrt(
        trading_periods
    )

    if clean:
        return result.dropna()
    else:
        return result


def garman_klass(price_data, window=30, trading_periods=252, clean=True):

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
