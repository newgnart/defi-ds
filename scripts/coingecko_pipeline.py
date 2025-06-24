from defi_ds.data import coingecko_token_price
import dlt
from dlt.destinations import filesystem


def coingecko_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="coingecko_token_price",
        destination=dlt.destinations.duckdb("data/btc"),
        dataset_name="prices",
    )
    pipeline.run(
        coingecko_token_price(coin_id="bitcoin"),
        # loader_file_format="csv",
        # loader_file_name="bitcoin.csv",
    )


if __name__ == "__main__":
    coingecko_pipeline()
#
