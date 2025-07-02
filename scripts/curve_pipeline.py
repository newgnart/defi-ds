from defi_ds.data.source import etherscan_logs
import dlt

duckdb_destination = "temp/curve.duckdb"
chainid = 1
address = "0x8472A9A7632b173c8Cf3a86D3afec50c35548e76"  # https://etherscan.io/address/0x8472A9A7632b173c8Cf3a86D3afec50c35548e76

# Directory to hold the (temporary) dbt project that will be executed via the dlt dbt runner
DBT_PROJECT_DIR = "dbt_projects/curve"


def logs():
    """Run full pipeline including data loading and transformation."""
    pipeline = dlt.pipeline(
        pipeline_name="defi",
        destination=dlt.destinations.duckdb(str(duckdb_destination)),
        dataset_name="curve",
    )

    # Run the pipeline and load data into DuckDB
    load_info = pipeline.run(
        etherscan_logs(chainid=1, address=address),
        table_name="log_raw",
        write_disposition="replace",
    )
    print(load_info)

    # Package the (dummy) dbt project together with pipeline configs
    dbt_runner = dlt.dbt.package(pipeline, DBT_PROJECT_DIR)

    # Execute all models. Replace with dbt_runner.run(["model_name"]) for fine-grained control
    models = dbt_runner.run_all()

    # Emit a concise run summary
    for m in models:
        print(
            f"Model {m.model_name} completed in {m.time}s | status={m.status} | message={m.message}"
        )


if __name__ == "__main__":
    logs(
        dataset_name="controllers",
        address="0x8472a9a7632b173c8cf3a86d3afec50c35548e76",
        table_name="logs",
    )
