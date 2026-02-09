from __future__ import annotations

from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

DB_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
HW2_OUTPUT = Path("/opt/airflow/project/HW2-basic-data-transformation/output")
TABLE_NAME = "temperature_readings"


def create_table() -> None:
    """Create the target table and truncate it for a full reload."""
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id TEXT,
                room_id TEXT,
                noted_date TIMESTAMP,
                temp INTEGER,
                out_in TEXT,
                date DATE
            );
        """))
        conn.execute(text(f"TRUNCATE TABLE {TABLE_NAME};"))
    engine.dispose()
    print(f"Table '{TABLE_NAME}' created/truncated.")


def load_full_data() -> None:
    """Load all historical data from HW2 output into PostgreSQL."""
    df = pd.read_csv(HW2_OUTPUT / "dated_data.csv")

    # Rename columns to match DB schema
    df = df.rename(columns={
        "room_id/id": "room_id",
        "out/in": "out_in",
    })

    # Parse dates
    df["noted_date"] = pd.to_datetime(df["noted_date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date

    engine = create_engine(DB_URL)
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    engine.dispose()

    print(f"Full load complete: {len(df)} records inserted into '{TABLE_NAME}'.")


with DAG(
    dag_id="hw3_full_load",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["hw3"],
    description="Full historical data load into PostgreSQL",
) as dag:

    task_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    task_load_data = PythonOperator(
        task_id="load_full_data",
        python_callable=load_full_data,
    )

    task_create_table >> task_load_data
