from __future__ import annotations

from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = Path("/opt/airflow/project/HW2-basic-data-transformation")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"


def load_and_filter_data() -> None:
    """Load data and filter only 'In' records."""
    df = pd.read_csv(INPUT_DIR / "IOT-temp.csv")
    
    # Filter out/in = In (inside readings only)
    df = df[df["out/in"].str.strip().str.lower() == "in"]
    
    # Save intermediate result
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_DIR / "filtered_data.csv", index=False)
    print(f"Filtered data: {len(df)} records (In only)")


def clean_by_percentile() -> None:
    """Clean temperature by 5th and 95th percentile."""
    df = pd.read_csv(OUTPUT_DIR / "filtered_data.csv")
    
    # Calculate percentiles
    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    
    print(f"Temperature percentiles: 5th={p5}, 95th={p95}")
    
    # Filter outliers
    original_count = len(df)
    df = df[(df["temp"] >= p5) & (df["temp"] <= p95)]
    
    print(f"Removed {original_count - len(df)} outliers, {len(df)} records remaining")
    
    # Save cleaned data
    df.to_csv(OUTPUT_DIR / "cleaned_data.csv", index=False)


def convert_date_format() -> None:
    """Convert noted_date to 'yyyy-MM-dd' format with date type."""
    df = pd.read_csv(OUTPUT_DIR / "cleaned_data.csv")
    
    # Parse date from 'dd-mm-yyyy hh:mm' format
    df["noted_date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M")
    
    # Create date column in 'yyyy-MM-dd' format
    df["date"] = df["noted_date"].dt.strftime("%Y-%m-%d")
    
    # Save with converted dates
    df.to_csv(OUTPUT_DIR / "dated_data.csv", index=False)
    print(f"Date conversion complete. Date range: {df['date'].min()} to {df['date'].max()}")


def find_hottest_coldest_days() -> None:
    """Find 5 hottest and 5 coldest days based on average temperature."""
    df = pd.read_csv(OUTPUT_DIR / "dated_data.csv")
    
    # Group by date and calculate average temperature
    daily_avg = df.groupby("date")["temp"].mean().reset_index()
    daily_avg.columns = ["date", "avg_temp"]
    daily_avg["avg_temp"] = daily_avg["avg_temp"].round(2)
    
    # Sort and get top 5 hottest days
    hottest = daily_avg.nlargest(5, "avg_temp")
    hottest.to_csv(OUTPUT_DIR / "hottest_days.csv", index=False)
    print("5 Hottest days:")
    print(hottest.to_string(index=False))
    
    # Sort and get top 5 coldest days
    coldest = daily_avg.nsmallest(5, "avg_temp")
    coldest.to_csv(OUTPUT_DIR / "coldest_days.csv", index=False)
    print("\n5 Coldest days:")
    print(coldest.to_string(index=False))


with DAG(
    dag_id="hw2_temperature_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["hw2"],
    description="ETL pipeline for IoT temperature data transformation",
) as dag:
    
    task_load_filter = PythonOperator(
        task_id="load_and_filter_data",
        python_callable=load_and_filter_data,
    )
    
    task_clean_percentile = PythonOperator(
        task_id="clean_by_percentile",
        python_callable=clean_by_percentile,
    )
    
    task_convert_date = PythonOperator(
        task_id="convert_date_format",
        python_callable=convert_date_format,
    )
    
    task_hottest_coldest = PythonOperator(
        task_id="find_hottest_coldest_days",
        python_callable=find_hottest_coldest_days,
    )
    
    # Define task dependencies
    task_load_filter >> task_clean_percentile >> task_convert_date >> task_hottest_coldest
