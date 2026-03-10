"""
Скрипт генерации тестовых данных и загрузки в MongoDB.

Создаёт 5 коллекций в базе source_db:
  - UserSessions   (~500 записей)
  - EventLogs      (~1000 записей)
  - SupportTickets (~300 записей)
  - UserRecommendations (123 записи, по одной на пользователя)
  - ModerationQueue (~400 записей)

Можно запускать как самостоятельный скрипт или через Airflow DAG.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from pymongo import MongoClient

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------
MONGO_URI = "mongodb://mongo:27017"
DB_NAME = "source_db"

USERS = [f"user_{i:03d}" for i in range(1, 124)]
PRODUCTS = [f"prod_{i:03d}" for i in range(1, 334)]

STATIC_PAGES = ["/login", "/home", "/products", "/cart", "/checkout", "/order_confirm"]
PRODUCT_PAGES = [f"/products/{i}" for i in range(1, 334)]
ALL_PAGES = STATIC_PAGES + PRODUCT_PAGES

SESSION_START = datetime(2024, 1, 1)
SESSION_END = datetime(2024, 1, 31, 23, 59, 59)

DEVICES = ["mobile", "desktop", "tablet"]
SESSION_ACTIONS = [
    "login", "view_product", "add_to_cart", "remove_from_cart",
    "search", "checkout", "logout",
]

EVENT_TYPES = ["click", "scroll", "purchase", "search", "page_view", "form_submit", "error"]

TICKET_STATUSES = ["open", "in_progress", "resolved", "closed"]
ISSUE_TYPES = ["payment", "delivery", "product_defect", "account", "refund"]

MODERATION_STATUSES = ["pending", "approved", "rejected"]
FLAGS = ["spam", "offensive", "contains_images", "verified_purchase"]

# Шаблоны сообщений для тикетов поддержки
USER_MESSAGES = [
    "Не могу оплатить заказ.",
    "Товар пришёл повреждённый.",
    "Не приходит подтверждение на почту.",
    "Хочу вернуть товар.",
    "Заказ не доставлен в срок.",
    "Не работает промокод.",
    "Списали деньги дважды.",
    "Не могу войти в аккаунт.",
    "Товар не соответствует описанию.",
    "Как отменить подписку?",
]

SUPPORT_MESSAGES = [
    "Пожалуйста, уточните номер заказа.",
    "Мы передали вашу заявку в отдел доставки.",
    "Проверьте папку «Спам».",
    "Оформите возврат через личный кабинет.",
    "Приносим извинения за задержку, уточняем статус.",
    "Промокод активирован, попробуйте снова.",
    "Средства будут возвращены в течение 3 рабочих дней.",
    "Мы отправили ссылку для сброса пароля.",
    "Спасибо, заявка принята. Ожидайте ответа.",
    "Пожалуйста, пришлите фото повреждения.",
]

# Шаблоны текстов отзывов
REVIEW_TEXTS = [
    "Отличный товар, рекомендую!",
    "Качество оставляет желать лучшего.",
    "Всё пришло вовремя, доволен покупкой.",
    "Не соответствует описанию на сайте.",
    "За такую цену — отличный вариант.",
    "Пользуюсь уже месяц, всё работает.",
    "Упаковка была повреждена при доставке.",
    "Хороший товар, но долгая доставка.",
    "Второй раз заказываю, всё отлично.",
    "Не стоит своих денег.",
    "Подарил другу, он в восторге!",
    "Средненько, ожидал большего.",
    "Быстрая доставка, товар как на фото.",
    "Сломался через неделю использования.",
    "Лучшая покупка за последнее время!",
]


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _random_dt(start: datetime, end: datetime) -> datetime:
    """Случайная дата-время в заданном диапазоне."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def _random_dt_pair(start: datetime, end: datetime, max_gap_hours: int = 2):
    """Пара (начало, конец) — конец не раньше начала и не позже end."""
    dt_start = _random_dt(start, end - timedelta(hours=1))
    gap = timedelta(minutes=random.randint(1, max_gap_hours * 60))
    dt_end = min(dt_start + gap, end)
    return dt_start, dt_end


# ---------------------------------------------------------------------------
# Генераторы коллекций
# ---------------------------------------------------------------------------

def generate_user_sessions(n: int = 500) -> list[dict]:
    """Генерация записей UserSessions."""
    records = []
    for i in range(1, n + 1):
        start, end = _random_dt_pair(SESSION_START, SESSION_END)
        pages = random.sample(ALL_PAGES, k=random.randint(1, 8))
        actions = random.sample(SESSION_ACTIONS, k=random.randint(1, len(SESSION_ACTIONS)))
        records.append({
            "session_id": f"sess_{i:04d}",
            "user_id": random.choice(USERS),
            "start_time": start.isoformat() + "Z",
            "end_time": end.isoformat() + "Z",
            "pages_visited": pages,
            "device": random.choice(DEVICES),
            "actions": actions,
        })
    return records


def generate_event_logs(n: int = 1000) -> list[dict]:
    """Генерация записей EventLogs."""
    records = []
    for i in range(1, n + 1):
        evt_type = random.choice(EVENT_TYPES)
        page = random.choice(ALL_PAGES)
        details: dict = {"page": page}
        if evt_type == "click":
            details["element"] = random.choice(["button", "link", "image", "menu"])
        elif evt_type == "purchase":
            details["product_id"] = random.choice(PRODUCTS)
            details["amount"] = round(random.uniform(100, 50000), 2)
        elif evt_type == "search":
            details["query"] = random.choice(["ноутбук", "телефон", "наушники", "кабель", "чехол"])
        elif evt_type == "error":
            details["code"] = random.choice([400, 403, 404, 500])
        records.append({
            "event_id": f"evt_{i:05d}",
            "timestamp": _random_dt(SESSION_START, SESSION_END).isoformat() + "Z",
            "event_type": evt_type,
            "details": details,
        })
    return records


def generate_support_tickets(n: int = 300) -> list[dict]:
    """Генерация записей SupportTickets."""
    records = []
    for i in range(1, n + 1):
        created = _random_dt(SESSION_START, SESSION_END - timedelta(days=1))
        msg_count = random.randint(1, 5)
        messages = []
        ts = created
        for j in range(msg_count):
            sender = "user" if j % 2 == 0 else "support"
            pool = USER_MESSAGES if sender == "user" else SUPPORT_MESSAGES
            ts = ts + timedelta(minutes=random.randint(5, 120))
            messages.append({
                "sender": sender,
                "message": random.choice(pool),
                "timestamp": ts.isoformat() + "Z",
            })
        updated = ts
        records.append({
            "ticket_id": f"ticket_{i:04d}",
            "user_id": random.choice(USERS),
            "status": random.choice(TICKET_STATUSES),
            "issue_type": random.choice(ISSUE_TYPES),
            "messages": messages,
            "created_at": created.isoformat() + "Z",
            "updated_at": updated.isoformat() + "Z",
        })
    return records


def generate_user_recommendations() -> list[dict]:
    """Генерация записей UserRecommendations (по одной на каждого пользователя)."""
    records = []
    for user_id in USERS:
        rec_count = random.randint(3, 10)
        records.append({
            "user_id": user_id,
            "recommended_products": random.sample(PRODUCTS, k=rec_count),
            "last_updated": _random_dt(SESSION_START, SESSION_END).isoformat() + "Z",
        })
    return records


def generate_moderation_queue(n: int = 400) -> list[dict]:
    """Генерация записей ModerationQueue."""
    records = []
    for i in range(1, n + 1):
        flag_count = random.randint(0, 3)
        records.append({
            "review_id": f"rev_{i:04d}",
            "user_id": random.choice(USERS),
            "product_id": random.choice(PRODUCTS),
            "review_text": random.choice(REVIEW_TEXTS),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(MODERATION_STATUSES),
            "flags": random.sample(FLAGS, k=flag_count),
            "submitted_at": _random_dt(SESSION_START, SESSION_END).isoformat() + "Z",
        })
    return records


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------

def populate_mongo(mongo_uri: str = MONGO_URI) -> None:
    """Заполнить MongoDB тестовыми данными. Идемпотентно: пересоздаёт коллекции."""
    client = MongoClient(mongo_uri)
    db = client[DB_NAME]

    collections = {
        "UserSessions": generate_user_sessions,
        "EventLogs": generate_event_logs,
        "SupportTickets": generate_support_tickets,
        "UserRecommendations": generate_user_recommendations,
        "ModerationQueue": generate_moderation_queue,
    }

    for name, generator in collections.items():
        db.drop_collection(name)
        data = generator()
        db[name].insert_many(data)
        print(f"[MongoDB] Коллекция '{name}': вставлено {len(data)} документов")

    client.close()
    print("[MongoDB] Генерация данных завершена.")


if __name__ == "__main__":
    populate_mongo()
