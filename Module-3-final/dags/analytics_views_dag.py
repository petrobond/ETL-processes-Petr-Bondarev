"""
DAG для построения аналитических витрин в PostgreSQL.

Витрина 1 — dm_user_activity:
    Поведенческий анализ пользователей (сессии, время на сайте,
    популярные страницы и действия).

Витрина 2 — dm_support_efficiency:
    Эффективность работы поддержки (количество тикетов по статусам
    и типам проблем, среднее время решения).
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

PG_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"


# ---------------------------------------------------------------------------
# Витрина 1: активность пользователей
# ---------------------------------------------------------------------------

_SQL_USER_ACTIVITY = """
DROP TABLE IF EXISTS dm_user_activity;

CREATE TABLE dm_user_activity AS
SELECT
    user_id,
    COUNT(*)                                             AS total_sessions,
    ROUND(SUM(session_duration_minutes)::numeric, 2)     AS total_session_duration_min,
    ROUND(AVG(session_duration_minutes)::numeric, 2)     AS avg_session_duration_min,
    SUM(pages_visited_count)                             AS total_pages_visited,
    MIN(start_time)                                      AS first_session,
    MAX(start_time)                                      AS last_session,
    MODE() WITHIN GROUP (ORDER BY device)                AS most_common_device
FROM user_sessions
GROUP BY user_id
ORDER BY total_sessions DESC;
"""


def build_user_activity() -> None:
    """Построить витрину dm_user_activity."""
    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for statement in _SQL_USER_ACTIVITY.strip().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))
    engine.dispose()
    print("[Витрина] dm_user_activity создана.")


# ---------------------------------------------------------------------------
# Витрина 2: эффективность поддержки
# ---------------------------------------------------------------------------

_SQL_SUPPORT_EFFICIENCY = """
DROP TABLE IF EXISTS dm_support_efficiency;

CREATE TABLE dm_support_efficiency AS
SELECT
    status,
    issue_type,
    COUNT(*)                                          AS ticket_count,
    ROUND(AVG(resolution_time_hours)::numeric, 2)     AS avg_resolution_time_hours,
    ROUND(MIN(resolution_time_hours)::numeric, 2)     AS min_resolution_time_hours,
    ROUND(MAX(resolution_time_hours)::numeric, 2)     AS max_resolution_time_hours,
    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_tickets_count
FROM support_tickets
GROUP BY status, issue_type
ORDER BY status, issue_type;
"""


def build_support_efficiency() -> None:
    """Построить витрину dm_support_efficiency."""
    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for statement in _SQL_SUPPORT_EFFICIENCY.strip().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))
    engine.dispose()
    print("[Витрина] dm_support_efficiency создана.")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="m3_analytics_views",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["module3"],
    description="Построение аналитических витрин в PostgreSQL",
) as dag:

    task_user_activity = PythonOperator(
        task_id="build_user_activity",
        python_callable=build_user_activity,
    )

    task_support_efficiency = PythonOperator(
        task_id="build_support_efficiency",
        python_callable=build_support_efficiency,
    )

    task_user_activity >> task_support_efficiency
