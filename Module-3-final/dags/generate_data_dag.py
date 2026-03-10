"""
DAG для генерации тестовых данных в MongoDB.

Вызывает скрипт generate_data.py, который заполняет базу source_db
пятью коллекциями: UserSessions, EventLogs, SupportTickets,
UserRecommendations, ModerationQueue.
"""

from __future__ import annotations

import sys

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/scripts")


def run_generate() -> None:
    """Импортируем и запускаем генерацию данных."""
    from generate_data import populate_mongo
    populate_mongo()


with DAG(
    dag_id="m3_generate_data",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["module3"],
    description="Генерация тестовых данных в MongoDB",
) as dag:

    generate_task = PythonOperator(
        task_id="populate_mongo",
        python_callable=run_generate,
    )
