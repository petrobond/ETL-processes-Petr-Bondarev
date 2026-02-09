from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
HW2_OUTPUT = Path("/opt/airflow/project/HW2-basic-data-transformation/output")
TABLE_NAME = "temperature_readings"
INCREMENTAL_DAYS = 3  # Number of recent days to reload


def delete_recent_data() -> None:
    """Delete records for the last N days from the target table."""
    # Read CSV to determine the max date in the dataset
    df = pd.read_csv(HW2_OUTPUT / "dated_data.csv")
    max_date = pd.to_datetime(df["date"]).max()
    cutoff_date = max_date - timedelta(days=INCREMENTAL_DAYS)

    print(f"Max date in dataset: {max_date.date()}")
    print(f"Cutoff date (last {INCREMENTAL_DAYS} days): {cutoff_date.date()}")

    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {TABLE_NAME} WHERE date >= :cutoff"),
            {"cutoff": cutoff_date.date()},
        )
        print(f"Deleted {result.rowcount} records from '{TABLE_NAME}' (date >= {cutoff_date.date()}).")
    engine.dispose()


def load_recent_data() -> None:
    """Load only the last N days of data from CSV into PostgreSQL."""
    df = pd.read_csv(HW2_OUTPUT / "dated_data.csv")

    # Rename columns to match DB schema
    df = df.rename(columns={
        "room_id/id": "room_id",
        "out/in": "out_in",
    })

    # Parse dates
    df["noted_date"] = pd.to_datetime(df["noted_date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Filter only the last N days
    max_date = max(df["date"])
    cutoff_date = max_date - timedelta(days=INCREMENTAL_DAYS)
    df_recent = df[df["date"] >= cutoff_date]

    print(f"Incremental load: {len(df_recent)} records (date >= {cutoff_date})")

    engine = create_engine(DB_URL)
    df_recent.to_sql(TABLE_NAME, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    engine.dispose()

    print(f"Incremental load complete: {len(df_recent)} records inserted.")


with DAG(
    dag_id="hw3_incremental_load",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["hw3"],
    description="Incremental data load (last N days) into PostgreSQL",
) as dag:

    task_delete_recent = PythonOperator(
        task_id="delete_recent_data",
        python_callable=delete_recent_data,
    )

    task_load_recent = PythonOperator(
        task_id="load_recent_data",
        python_callable=load_recent_data,
    )

    task_delete_recent >> task_load_recent
