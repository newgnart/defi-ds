from defi_ds.data import coingecko_prices
import dlt

duckdb_destination = "data/prices.duckdb"


def prices(dataset_name, coin_id):
    pipeline = dlt.pipeline(
        pipeline_name="prices",
        destination=dlt.destinations.duckdb(duckdb_destination),
        dataset_name=dataset_name,
    )

    pipeline.run(
        coingecko_prices(coin_id=coin_id),
        write_disposition="append",
    )


if __name__ == "__main__":
    prices(
        dataset_name="weth_coingecko",
        coin_id="weth",
    )
