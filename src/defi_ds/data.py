from pathlib import Path
import os
from dotenv import load_dotenv
import dlt
from dlt.sources.rest_api import rest_api_source
from typing import Optional, Union
import requests
from datetime import datetime, timezone
from decimal import Decimal

load_dotenv()

ETHERSCAN_API_BASE_URL = "https://api.etherscan.io"
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

ETHERSCAN_TRANSACTION_COLUMNS = {
    "block_number": {"data_type": "bigint"},
    "time_stamp": {"data_type": "timestamp"},
}

COINGECKO_API_BASE_URL = "https://api.coingecko.com/api/v3"
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")


@dlt.resource(columns=ETHERSCAN_TRANSACTION_COLUMNS)
def etherscan_transaction(
    chainid,
    module,
    action,
    address,
    startblock,
    endblock="latest",
    page=1,
    offset=1000,
    sort="asc",
):
    return rest_api_source(
        {
            "client": {"base_url": ETHERSCAN_API_BASE_URL},
            "resource_defaults": {
                "endpoint": {
                    "params": {
                        "chainid": chainid,
                        "module": module,
                        "action": action,
                        "address": address,
                        "startblock": startblock,
                        "endblock": endblock,
                        "page": page,
                        "offset": offset,
                        "sort": sort,
                        "apikey": ETHERSCAN_API_KEY,
                    },
                }
            },
            "resources": ["api"],
        }
    )


@dlt.resource(
    columns={
        "timestamp": {"data_type": "timestamp", "timezone": False, "precision": 3},
        "price": {"data_type": "decimal"},
    },
)
def coingecko_prices(
    coin_id: str,
    vs_currency: str = "usd",
    days: Optional[int] = 90,
):
    """Resource for CoinGecko price data only."""

    url = f"{COINGECKO_API_BASE_URL}/coins/{coin_id}/market_chart"
    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": COINGECKO_API_KEY,
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
