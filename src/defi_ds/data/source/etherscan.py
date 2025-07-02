import requests
from datetime import datetime
import dlt
from dlt.sources.helpers.rest_client import paginators
from dlt.sources.rest_api import rest_api_source
from defi_ds.config import *
import json


@dlt.resource(columns=ETHERSCAN_TRANSACTION_COLUMNS)
def etherscan_transactions(
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
def etherscan_logs(
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


def get_contract_abi(chainid, address, save=True, save_dir: str = "data/abi"):
    url = f"https://api.etherscan.io/v2/api?chainid={chainid}&module=contract&action=getabi&address={address}&apikey={ETHERSCAN_API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Parse the ABI string to get proper JSON
    abi_json = json.loads(data["result"])

    if save:
        with open(f"{save_dir}/{address}.json", "w") as f:
            json.dump(abi_json, f, indent=2)

    return abi_json
