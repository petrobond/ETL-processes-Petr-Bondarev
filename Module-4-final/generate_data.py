#!/usr/bin/env python3
"""
Скрипт генерации синтетических данных для итогового задания модуля 4.
Генерирует три набора данных с общими client_id / customer_id и region_code
для сквозной аналитики в DataLens.
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)

# Константы
REGIONS = [
    "DE-HE", "DE-BE", "DE-BY", "DE-NW", "DE-BW",
    "RU-MOW", "RU-SPE", "RU-TA", "RU-KR", "RU-NGR"
]
CALL_STATUSES = ["answered", "no_answer", "busy", "failed"]
CLIENT_RESPONSES = ["interested", "not_interested", "callback_requested", "opted_out"]
CAMPAIGN_TYPES = ["credit_card_offer", "loan_offer", "deposit_offer", "insurance_offer"]
CHANNELS = ["mobile", "web", "branch", "partner", "atm"]
PRODUCT_TYPES = ["cash_loan", "mortgage", "car_loan", "credit_card", "refinancing"]
RISK_LEVELS = ["low", "medium", "high"]
DECISION_STATUSES = ["approved", "rejected", "manual_review"]

# Генерируем пул клиентов (50K уникальных)
NUM_CLIENTS = 50_000
CLIENT_IDS = [f"client_{i:06d}" for i in range(1, NUM_CLIENTS + 1)]
CUSTOMER_IDS = [f"cust_{i:06d}" for i in range(1, NUM_CLIENTS + 1)]

# Маппинг client_id -> region (для связности данных)
client_region_map = {cid: random.choice(REGIONS) for cid in CLIENT_IDS}
customer_region_map = {cid: region for cid, region in zip(CUSTOMER_IDS, REGIONS * (NUM_CLIENTS // len(REGIONS) + 1))}
customer_region_map.update({cid: random.choice(REGIONS) for cid in CUSTOMER_IDS if cid not in customer_region_map})


def generate_transactions_v2(num_rows=200_000):
    """Генерирует transactions_v2.csv (≥30 MB)"""
    filename = "Module-4-final/transactions_v2.csv"
    start_date = datetime(2026, 5, 1)
    fields = [
        "call_id", "call_time", "client_id", "region_code",
        "campaign_type", "call_status", "client_response",
        "duration_sec", "follow_up_required"
    ]
    print(f"Генерация {filename} ({num_rows} строк)...")
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fields)
        for i in range(1, num_rows + 1):
            client_id = random.choice(CLIENT_IDS)
            call_time = start_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            writer.writerow([
                f"call_{call_time.strftime('%Y%m%d_%H%M%S')}_{i:06d}",
                call_time.strftime("%Y-%m-%d %H:%M:%S"),
                client_id,
                client_region_map[client_id],
                random.choice(CAMPAIGN_TYPES),
                random.choice(CALL_STATUSES),
                random.choice(CLIENT_RESPONSES),
                random.randint(10, 600),
                random.choice(["true", "false"]),
            ])
    print(f"  Создан: {filename}")


def generate_credit_applications(num_rows=350_000):
    """Генерирует credit_applications.csv (≥50 MB)"""
    filename = "Module-4-final/credit_applications.csv"
    start_date = datetime(2026, 5, 1)
    fields = [
        "application_id", "event_time", "customer_id", "region_code",
        "product_type", "requested_amount", "term_months", "credit_score",
        "risk_level", "decision_status", "approved_amount", "channel",
        "employee_review_flag", "processing_time_sec"
    ]
    print(f"Генерация {filename} ({num_rows} строк)...")
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fields)
        for i in range(1, num_rows + 1):
            customer_id = random.choice(CUSTOMER_IDS)
            event_time = start_date + timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            credit_score = random.randint(300, 950)
            if credit_score >= 700:
                risk_level = "low"
            elif credit_score >= 500:
                risk_level = "medium"
            else:
                risk_level = "high"

            requested_amount = random.randint(5000, 100000)
            if risk_level == "low" and requested_amount <= 50000:
                decision = "approved"
                approved_amount = requested_amount
            elif risk_level == "high":
                decision = random.choices(
                    ["rejected", "manual_review"], weights=[0.7, 0.3]
                )[0]
                approved_amount = 0
            else:
                decision = random.choices(
                    ["approved", "rejected", "manual_review"], weights=[0.4, 0.3, 0.3]
                )[0]
                approved_amount = requested_amount if decision == "approved" else 0

            writer.writerow([
                f"app_{event_time.strftime('%Y%m%d_%H%M%S')}_{i:06d}",
                event_time.strftime("%Y-%m-%d %H:%M:%S"),
                customer_id,
                customer_region_map.get(customer_id, "DE-HE"),
                random.choice(PRODUCT_TYPES),
                requested_amount,
                random.choice([6, 12, 24, 36, 48, 60]),
                credit_score,
                risk_level,
                decision,
                approved_amount,
                random.choice(CHANNELS),
                random.choice(["true", "false"]),
                random.randint(5, 300),
            ])
    print(f"  Создан: {filename}")


def generate_loan_applications_json(num_records=50_000):
    """Генерирует loan_applications_structured.json (≥20 MB)"""
    filename = "Module-4-final/loan_applications_structured.json"
    doc_types = ["passport", "driver_license", "income_statement", "employment_certificate"]
    doc_statuses = ["verified", "pending", "failed"]
    start_date = datetime(2026, 5, 1)

    print(f"Генерация {filename} ({num_records} записей)...")
    records = []
    for i in range(1, num_records + 1):
        customer_id = random.choice(CUSTOMER_IDS)
        submitted_at = start_date + timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        score = random.randint(300, 950)
        if score >= 700:
            risk = "low"
        elif score >= 500:
            risk = "medium"
        else:
            risk = "high"

        record = {
            "application_id": f"loan_{submitted_at.strftime('%Y%m%d_%H%M%S')}_{i:06d}",
            "customer": {
                "customer_id": customer_id,
                "region": customer_region_map.get(customer_id, "DE-HE")
            },
            "loan": {
                "amount": random.randint(5000, 100000),
                "term_months": random.choice([6, 12, 24, 36, 48, 60])
            },
            "scoring": {
                "score": score,
                "risk_level": risk
            },
            "documents": [
                {
                    "type": dt,
                    "status": random.choice(doc_statuses)
                }
                for dt in random.sample(doc_types, random.randint(1, 3))
            ],
            "decision_status": random.choice(DECISION_STATUSES),
            "submitted_at": submitted_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        records.append(record)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"  Создан: {filename}")

    # Дополнительно создаём NDJSON (построчный JSON) для потоковой загрузки
    filename_ndjson = "Module-4-final/loan_applications.ndjson"
    print(f"Генерация {filename_ndjson} (построчный JSON)...")
    with open(filename_ndjson, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"  Создан: {filename_ndjson}")


def check_file_size(filename, min_mb):
    import os
    size_mb = os.path.getsize(filename) / (1024 * 1024)
    status = "✅" if size_mb >= min_mb else "❌"
    print(f"  {status} {filename}: {size_mb:.1f} MB (требуется ≥{min_mb} MB)")
    return size_mb >= min_mb


if __name__ == "__main__":
    print("=" * 60)
    print("Генерация синтетических данных для итогового задания")
    print("=" * 60)

    generate_transactions_v2(290_000)   # ~35 MB
    generate_credit_applications(440_000)  # ~55 MB
    generate_loan_applications_json(60_000)  # ~25 MB

    print("\n" + "=" * 60)
    print("Проверка размеров файлов:")
    print("=" * 60)
    import os
    check_file_size("Module-4-final/transactions_v2.csv", 30)
    check_file_size("Module-4-final/credit_applications.csv", 50)
    check_file_size("Module-4-final/loan_applications_structured.json", 20)
    check_file_size("Module-4-final/loan_applications.ndjson", 20)

    print("\nГенерация завершена!")