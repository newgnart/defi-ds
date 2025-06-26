import requests
from datetime import datetime
import dlt
from dlt.sources.helpers.rest_client import paginators
from dlt.sources.rest_api import rest_api_source
from defi_ds.config import *


@dlt.resource(columns=ETHERSCAN_TRANSACTION_COLUMNS)
def etherscan_transaction(
    chainid,
    address,
    module="account",
    action="txlist",
    startblock=0,
    endblock="latest",
    offset=1000,
    sort="asc",
):
    params = {
        "chainid": chainid,
        "module": module,
        "action": action,
        "address": address,
        "startblock": startblock,
        "endblock": endblock,
        "offset": offset,
        "sort": sort,
        "apikey": ETHERSCAN_API_KEY,
    }
    source = rest_api_source(
        {
            "client": {
                "base_url": ETHERSCAN_API_BASE_URL,
                "paginator": paginators.PageNumberPaginator(
                    base_page=1, total_path=None, page_param="page"
                ),
            },
            "resources": [
                {
                    "name": "",
                    "endpoint": {"params": params},
                },
            ],
        }
    )
    return source


@dlt.resource(columns=ETHERSCAN_LOG_COLUMNS)
def etherscan_log(
    chainid,
    address,
    module="logs",
    action="getLogs",
    fromBlock=0,
    offset=1000,
):
    latest_block = get_latest_block(chainid)
    params = {
        "chainid": chainid,
        "module": module,
        "action": action,
        "address": address,
        "fromBlock": fromBlock,
        "toBlock": latest_block,
        "offset": offset,
        "apikey": ETHERSCAN_API_KEY,
    }

    source = rest_api_source(
        {
            "client": {
                "base_url": ETHERSCAN_API_BASE_URL,
                "paginator": paginators.PageNumberPaginator(
                    base_page=1,
                    total_path=None,
                    page_param="page",
                ),
            },
            "resources": [
                {
                    "name": "",
                    "endpoint": {"params": params},
                }
            ],
        }
    )
    return source


def get_latest_block(
    chainid, timestamp=int(datetime.now().timestamp()), closest="before"
):
    url = f"https://api.etherscan.io/v2/api?chainid={chainid}&module=block&action=getblocknobytime&timestamp={timestamp}&closest={closest}&apikey={ETHERSCAN_API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return int(data["result"])
