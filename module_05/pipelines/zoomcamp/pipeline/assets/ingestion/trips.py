"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: pickup_datetime
    type: timestamp
    description: "Date and time when the trip began"
  - name: dropoff_datetime
    type: timestamp
    description: "Date and time when the trip ended"

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import io
import os
import json
from datetime import datetime, timezone
import pandas as pd
import requests

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """

    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types",["yellow"])

    start_month = pd.to_datetime(start_date).to_period("M").to_timestamp()
    end_month = pd.to_datetime(end_date).to_period("M").to_timestamp()
    months = pd.date_range(start=start_month, end=end_month, freq="MS")

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    extracted_at = datetime.now(timezone.utc)

    dataframes = []
    for taxi_type in taxi_types:
        for month in months:
            url = f"{base_url}/{taxi_type}_tripdata_{month.year}-{month.month:02d}.parquet"
            response = requests.get(url)
            df = pd.read_parquet(io.BytesIO(response.content))

            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            if "tpep_pickup_datetime" in df.columns:
              df = df.rename(columns={"tpep_pickup_datetime": "pickup_datetime"})

            if "tpep_dropoff_datetime" in df.columns:
              df = df.rename(columns={"tpep_dropoff_datetime": "dropoff_datetime"})

            df = df.rename(columns={
              "PULocationID": "pickup_location_id",
              "DOLocationID": "dropoff_location_id",
            })

            dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)


