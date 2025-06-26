from defi_ds.data import (
    coingecko_prices,
    coingecko_ohlc,
    etherscan_log,
    etherscan_transaction,
)
import dlt
import duckdb
import time

duckdb_destination = "temp/defi_risk.duckdb"


def prices(coin_id):
    pipeline = dlt.pipeline(
        pipeline_name="defi_risk_pipeline",
        destination=dlt.destinations.duckdb(str(duckdb_destination)),
        dataset_name=coin_id,
    )

    pipeline.run(
        coingecko_prices(coin_id, days=180),
        table_name="prices",
    )


def ohlc(coin_id):
    pipeline = dlt.pipeline(
        pipeline_name="defi_risk_pipeline",
        destination=dlt.destinations.duckdb(str(duckdb_destination)),
        dataset_name=coin_id,
    )

    pipeline.run(
        coingecko_ohlc(coin_id, days=30),
        table_name="ohlc",
        write_disposition="replace",
    )


def log(contract_name, address):
    pipeline = dlt.pipeline(
        pipeline_name="defi_risk_pipeline",
        destination=dlt.destinations.duckdb(str(duckdb_destination)),
        dataset_name=contract_name,
    )
    page = 1

    while True:
        print(f"Fetching page {page}")
        try:
            conn = duckdb.connect(duckdb_destination)
            n_logs_old = conn.sql(
                f"SELECT COUNT(*) FROM {contract_name}.log"
            ).fetchone()[0]
        except:
            n_logs_old = 0

        pipeline.run(
            etherscan_log(chainid=1, address=address, page=page),
            table_name="log",
            write_disposition="append",
        )

        conn = duckdb.connect(duckdb_destination)
        n_logs_new = conn.sql(f"SELECT COUNT(*) FROM {contract_name}.log").fetchone()[0]
        n_logs_current = n_logs_new - n_logs_old
        if n_logs_current < 1000:
            break
        else:
            page += 1
            time.sleep(0.2)


def transaction(contract_name, address):
    pipeline = dlt.pipeline(
        pipeline_name="defi_risk_pipeline",
        destination=dlt.destinations.duckdb(str(duckdb_destination)),
        dataset_name=contract_name,
    )
    startblock = 0
    while True:
        print(f"Fetching transactions from block {startblock}")
        try:
            conn = duckdb.connect(duckdb_destination)
            n_transactions_old = conn.sql(
                f"SELECT COUNT(*) FROM {contract_name}.transaction"
            ).fetchone()[0]
        except:
            n_transactions_old = 0

        pipeline.run(
            etherscan_transaction(chainid=1, address=address, startblock=startblock),
            table_name="transaction",
            write_disposition="append",
        )

        conn = duckdb.connect(duckdb_destination)
        n_transactions_new = conn.sql(
            f"SELECT COUNT(*) FROM {contract_name}.transaction"
        ).fetchone()[0]
        max_block = conn.sql(
            f"SELECT MAX(block_number) FROM {contract_name}.transaction"
        ).fetchone()[0]
        n_transactions_current = n_transactions_new - n_transactions_old
        if n_transactions_current < 1000:
            break
        else:
            startblock = max_block + 1
            time.sleep(0.2)


if __name__ == "__main__":
    # transaction("sfrxeth", "0x8472a9a7632b173c8cf3a86d3afec50c35548e76")
    log("sfrxeth_controller_log", "0x8472a9a7632b173c8cf3a86d3afec50c35548e76")
