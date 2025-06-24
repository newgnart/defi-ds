from pathlib import Path
import duckdb
import dlt
from defi_ds.data import coingecko_prices

rootpath = Path.cwd()
from datetime import datetime
import json
from decimal import Decimal

# Remove existing database to start fresh
duckdb_destination = rootpath / "data/test.duckdb"
if duckdb_destination.exists():
    duckdb_destination.unlink()


pipeline = dlt.pipeline(
    pipeline_name="coingecko_pipeline",
    destination=dlt.destinations.duckdb(str(duckdb_destination)),
    dataset_name="crypto_data",
)

# Use any of the resources
load_info = pipeline.run(
    coingecko_prices("bitcoin"),
)
