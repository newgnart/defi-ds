import os
import requests
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv
import dlt
from dlt.sources.helpers.rest_client import RESTClient, paginators
from dlt.sources.rest_api import rest_api_source
from defi_ds.config import *


@dlt.resource(
    columns=COINGECKO_PRICES_COLUMNS,
)
def coingecko_prices(
    coin_id: str,
    vs_currency: str = "usd",
    days: Optional[int] = 180,
):
    """Resource for CoinGecko price data only, volume, market cap is available but not used"""

    url = f"{COINGECKO_API_BASE_URL}/coins/{coin_id}/market_chart"
    headers = {
        "accept": "application/json",
    }
    params = {
        "vs_currency": vs_currency,
        "days": days,
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()

    if "prices" in data:
        for timestamp_ms, price in data["prices"]:
            yield {
                "timestamp": datetime.fromtimestamp(int(timestamp_ms) / 1000),
                "price": float(price),
            }


@dlt.resource(
    columns=COINGECKO_OHLC_COLUMNS,
)
def coingecko_ohlc(
    coin_id: str,
    vs_currency: str = "usd",
    days: int = 180,
):

    url = f"{COINGECKO_API_BASE_URL}/coins/{coin_id}/ohlc"
    headers = {
        "accept": "application/json",
    }
    params = {
        "vs_currency": vs_currency,
        "days": days,
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    for row in data:
        yield {
            "timestamp": datetime.fromtimestamp(int(row[0]) / 1000),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
        }
