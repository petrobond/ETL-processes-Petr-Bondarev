"""
DAG репликации данных из MongoDB в PostgreSQL.

Читает 5 коллекций из MongoDB (source_db), выполняет трансформации
(развёртывание вложенных структур, вычисление метрик, дедупликация)
и загружает результат в PostgreSQL (БД airflow).
"""

from __future__ import annotations

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from sqlalchemy import create_engine, text

MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "source_db"
PG_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"


# ---------------------------------------------------------------------------
# Создание таблиц в PostgreSQL
# ---------------------------------------------------------------------------

_DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS user_sessions (
        session_id   TEXT PRIMARY KEY,
        user_id      TEXT,
        start_time   TIMESTAMP,
        end_time     TIMESTAMP,
        session_duration_minutes DOUBLE PRECISION,
        pages_visited_count      INTEGER,
        pages_visited_list       TEXT,
        device       TEXT,
        actions_list TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS event_logs (
        event_id   TEXT PRIMARY KEY,
        timestamp  TIMESTAMP,
        event_type TEXT,
        page       TEXT,
        element    TEXT,
        product_id TEXT,
        amount     DOUBLE PRECISION,
        query      TEXT,
        error_code INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS support_tickets (
        ticket_id            TEXT PRIMARY KEY,
        user_id              TEXT,
        status               TEXT,
        issue_type           TEXT,
        message_count        INTEGER,
        created_at           TIMESTAMP,
        updated_at           TIMESTAMP,
        resolution_time_hours DOUBLE PRECISION
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_recommendations (
        user_id                  TEXT PRIMARY KEY,
        recommended_count        INTEGER,
        recommended_products_list TEXT,
        last_updated             TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS moderation_queue (
        review_id         TEXT PRIMARY KEY,
        user_id           TEXT,
        product_id        TEXT,
        review_text       TEXT,
        rating            INTEGER,
        moderation_status TEXT,
        flags_list        TEXT,
        submitted_at      TIMESTAMP
    );
    """,
]


def create_pg_tables() -> None:
    """Создать целевые таблицы в PostgreSQL (если не существуют)."""
    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        for ddl in _DDL_STATEMENTS:
            conn.execute(text(ddl))
    engine.dispose()
    print("[PostgreSQL] Таблицы созданы / проверены.")


# ---------------------------------------------------------------------------
# Вспомогательная функция загрузки в PG
# ---------------------------------------------------------------------------

def _load_to_pg(df: pd.DataFrame, table: str) -> None:
    """TRUNCATE + INSERT DataFrame в указанную таблицу PostgreSQL."""
    engine = create_engine(PG_URL)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table};"))
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=500)
    engine.dispose()
    print(f"[PostgreSQL] Таблица '{table}': загружено {len(df)} строк.")


def _read_mongo(collection_name: str) -> list[dict]:
    """Прочитать все документы из коллекции MongoDB."""
    client = MongoClient(MONGO_URI)
    docs = list(client[MONGO_DB][collection_name].find())
    client.close()
    return docs


# ---------------------------------------------------------------------------
# Задачи репликации с трансформациями
# ---------------------------------------------------------------------------

def replicate_user_sessions() -> None:
    """
    UserSessions: развернуть массивы pages_visited и actions в текстовые поля,
    вычислить длительность сессии в минутах, дедуплицировать по session_id.
    """
    docs = _read_mongo("UserSessions")
    df = pd.DataFrame(docs)

    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])
    df["session_duration_minutes"] = (
        (df["end_time"] - df["start_time"]).dt.total_seconds() / 60
    ).round(2)
    df["pages_visited_count"] = df["pages_visited"].apply(len)
    df["pages_visited_list"] = df["pages_visited"].apply(lambda x: ", ".join(x))
    df["actions_list"] = df["actions"].apply(lambda x: ", ".join(x))

    df = df.drop(columns=["_id", "pages_visited", "actions"])
    df = df.drop_duplicates(subset=["session_id"], keep="first")

    _load_to_pg(df, "user_sessions")


def replicate_event_logs() -> None:
    """
    EventLogs: извлечь вложенные поля из details (page, element, product_id,
    amount, query, error code), дедуплицировать по event_id.
    """
    docs = _read_mongo("EventLogs")
    df = pd.DataFrame(docs)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    details = pd.json_normalize(df["details"])
    details.columns = [c if c != "code" else "error_code" for c in details.columns]
    df = pd.concat([df.drop(columns=["_id", "details"]), details], axis=1)

    expected_cols = ["page", "element", "product_id", "amount", "query", "error_code"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df = df[["event_id", "timestamp", "event_type"] + expected_cols]
    df = df.drop_duplicates(subset=["event_id"], keep="first")

    _load_to_pg(df, "event_logs")


def replicate_support_tickets() -> None:
    """
    SupportTickets: подсчитать количество сообщений, вычислить время
    решения (часы между created_at и updated_at), дедуплицировать по ticket_id.
    """
    docs = _read_mongo("SupportTickets")
    df = pd.DataFrame(docs)

    df["created_at"] = pd.to_datetime(df["created_at"])
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    df["message_count"] = df["messages"].apply(len)
    df["resolution_time_hours"] = (
        (df["updated_at"] - df["created_at"]).dt.total_seconds() / 3600
    ).round(2)

    df = df.drop(columns=["_id", "messages"])
    df = df.drop_duplicates(subset=["ticket_id"], keep="first")

    _load_to_pg(df, "support_tickets")


def replicate_user_recommendations() -> None:
    """
    UserRecommendations: развернуть массив recommended_products в текстовое
    поле и подсчитать количество рекомендаций, дедуплицировать по user_id.
    """
    docs = _read_mongo("UserRecommendations")
    df = pd.DataFrame(docs)

    df["recommended_count"] = df["recommended_products"].apply(len)
    df["recommended_products_list"] = df["recommended_products"].apply(
        lambda x: ", ".join(x)
    )
    df["last_updated"] = pd.to_datetime(df["last_updated"])

    df = df.drop(columns=["_id", "recommended_products"])
    df = df.drop_duplicates(subset=["user_id"], keep="first")

    _load_to_pg(df, "user_recommendations")


def replicate_moderation_queue() -> None:
    """
    ModerationQueue: развернуть массив flags в текстовое поле,
    дедуплицировать по review_id.
    """
    docs = _read_mongo("ModerationQueue")
    df = pd.DataFrame(docs)

    df["flags_list"] = df["flags"].apply(lambda x: ", ".join(x) if x else "")
    df["submitted_at"] = pd.to_datetime(df["submitted_at"])

    df = df.drop(columns=["_id", "flags"])
    df = df.drop_duplicates(subset=["review_id"], keep="first")

    _load_to_pg(df, "moderation_queue")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="m3_replicate_mongo_to_pg",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["module3"],
    description="Репликация данных из MongoDB в PostgreSQL с трансформациями",
) as dag:

    task_create_tables = PythonOperator(
        task_id="create_pg_tables",
        python_callable=create_pg_tables,
    )

    task_sessions = PythonOperator(
        task_id="replicate_user_sessions",
        python_callable=replicate_user_sessions,
    )

    task_events = PythonOperator(
        task_id="replicate_event_logs",
        python_callable=replicate_event_logs,
    )

    task_tickets = PythonOperator(
        task_id="replicate_support_tickets",
        python_callable=replicate_support_tickets,
    )

    task_recommendations = PythonOperator(
        task_id="replicate_user_recommendations",
        python_callable=replicate_user_recommendations,
    )

    task_moderation = PythonOperator(
        task_id="replicate_moderation_queue",
        python_callable=replicate_moderation_queue,
    )

    task_create_tables >> [
        task_sessions,
        task_events,
        task_tickets,
        task_recommendations,
        task_moderation,
    ]
