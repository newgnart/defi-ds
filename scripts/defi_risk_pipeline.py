from defi_ds.data import coingecko_prices
import dlt


duckdb_destination = "data/defi_risk_test.duckdb"


def main():
    pipeline = dlt.pipeline(
        pipeline_name="defi_risk_pipeline",
        destination=dlt.destinations.duckdb(str(duckdb_destination)),
        dataset_name="weth",
    )

    pipeline.run(
        coingecko_prices("weth"),
        table_name="prices",
    )


if __name__ == "__main__":
    main()
#
