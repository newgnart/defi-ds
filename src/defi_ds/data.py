import os
import requests
from typing import Optional
from datetime import datetime
from dotenv import load_dotenv
import dlt
from dlt.sources.rest_api import rest_api_source

load_dotenv()

ETHERSCAN_API_BASE_URL = "https://api.etherscan.io"
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

ETHERSCAN_TRANSACTION_COLUMNS = {
    "block_number": {"data_type": "bigint"},
    "time_stamp": {"data_type": "timestamp"},
}

COINGECKO_API_BASE_URL = "https://api.coingecko.com/api/v3"
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_PRICES_COLUMNS = {
    "timestamp": {"data_type": "timestamp", "timezone": False, "precision": 3},
    "price": {"data_type": "decimal"},
}

COINGECKO_OHLC_COLUMNS = {
    "timestamp": {"data_type": "timestamp", "timezone": False, "precision": 3},
    "open": {"data_type": "decimal"},
    "high": {"data_type": "decimal"},
    "low": {"data_type": "decimal"},
    "close": {"data_type": "decimal"},
}


@dlt.resource(columns=ETHERSCAN_TRANSACTION_COLUMNS)
def etherscan_transaction(
    chainid,
    address,
    module="account",
    action="txlist",
    startblock=0,
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


@dlt.resource()
def etherscan_log(
    chainid,
    address,
    module="logs",
    action="getLogs",
    fromBlock=0,
    toBlock=22778640,
    page=1,
    offset=1000,
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
                        "fromBlock": fromBlock,
                        "toBlock": toBlock,
                        "page": page,
                        "offset": offset,
                        "apikey": ETHERSCAN_API_KEY,
                    },
                }
            },
            "resources": ["api"],
        }
    )
