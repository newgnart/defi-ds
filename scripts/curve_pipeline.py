from defi_ds.data import etherscan_logs
import dlt

duckdb_destination = "temp/curve.duckdb"


def logs(dataset_name, address, table_name):
    pipeline = dlt.pipeline(
        pipeline_name="curve",
        destination=dlt.destinations.duckdb(duckdb_destination),
        dataset_name=dataset_name,
    )

    pipeline.run(
        etherscan_logs(chainid=1, address=address),
        table_name=table_name,
        write_disposition="append",
    )


if __name__ == "__main__":
    logs(
        dataset_name="controllers",
        address="0x8472a9a7632b173c8cf3a86d3afec50c35548e76",
        table_name="logs",
    )
