# DeFi Data Science

A Python toolkit for DeFi data engineering and data science.

## Features

- **Data Pipelines**: 
  - Extract data from various sources, including but not limited to: CoinGecko, Etherscan, The Graph, etc, preferably free sources
  - DLT pipelines to ingest data. 
  - ... for data transformation. 
  - Storage can be in a local database (DuckDB) or in a remote database (e.g. BigQuery, Snowflake, etc.).
  
- **Risk Scoring**: Risk scoring modules for CDP protocols

## Curve Market Health Scores
The work tries to replicate the Curve Market Health Scores methodology research by [LlamaRisk](https://www.llamarisk.com/research/curve-market-health-methodology), and still in progress 🚧

### Data:
- Requirements:
  - Asset (collateral) OHLC prices
  - Daily debt state of every borrower in each [market](https://docs.curve.finance/deployments/crvusd/#markets) of crvUSD
  - to be added
- Data sources:
  - CoinGecko for asset prices
  - Etherscan logs for daily debt state
  - ...

### Data Pipeline:
- DuckDB as a local database
- DLT pipelines to ingest data.
- python scripts to transform raw data to the required format for now, but will be replaced by a more robust data pipeline.

```bash
# Install dependencies
uv sync
uv pip install -e .

# Price pipeline, this will result in a DuckDB with ohlc table for the asset
uv run scripts/prices_pipeline.py

# Curve logs, this will result in a DuckDB the log table that contains all the logs for the all Controller contracts
uv run scripts/curve_pipeline.py
```

### Risk Scoring:
Research notebook: [notebooks/llamarisk_curve.ipynb](notebooks/llamarisk_curve.ipynb)
- 

