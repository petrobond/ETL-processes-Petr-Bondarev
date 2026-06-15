#!/usr/bin/env python3
"""
Конвертирует Parquet и NDJSON → CSV для загрузки в DataLens
"""

import pandas as pd
import os

BASE = "/Users/Lenovo/Projects/ETL-processes-Petr-Bondarev/Module-4-final/object-storage-download"

# 1. Parquet → CSV (credit_applications)
print("=== 1. credit_applications.parquet → CSV ===")
parquet_dir = os.path.join(BASE, "credit_applications.parquet")
df_credit = pd.read_parquet(parquet_dir)
print(f"  Строк: {len(df_credit)}, колонок: {len(df_credit.columns)}")
print(f"  Колонки: {list(df_credit.columns)}")
csv_path = os.path.join(BASE, "credit_applications.csv")
df_credit.to_csv(csv_path, index=False)
size_mb = os.path.getsize(csv_path) / (1024 * 1024)
print(f"  Сохранён: {csv_path} ({size_mb:.1f} MB)")

# 2. JSON → CSV (transactions_v2)
print("\n=== 2. transactions_v2.json → CSV ===")
json_dir = os.path.join(BASE, "transactions_v2")
json_files = [f for f in os.listdir(json_dir) if f.endswith(".json")]
print(f"  Найден файл: {json_files[0] if json_files else 'нет'}")
df_trans = pd.read_json(os.path.join(json_dir, json_files[0]), lines=True)
print(f"  Строк: {len(df_trans)}, колонок: {len(df_trans.columns)}")
print(f"  Колонки: {list(df_trans.columns)}")
csv_path2 = os.path.join(BASE, "transactions_v2.csv")
df_trans.to_csv(csv_path2, index=False)
size_mb2 = os.path.getsize(csv_path2) / (1024 * 1024)
print(f"  Сохранён: {csv_path2} ({size_mb2:.1f} MB)")

print("\n=== Готово! ===")
print(f"Итого для загрузки в DataLens:")
print(f"  1. credit_applications.csv ({size_mb:.1f} MB)")
print(f"  2. transactions_v2.csv ({size_mb2:.1f} MB)")
print(f"  3. daily_stats/part-*.csv (уже CSV)")
print(f"  4. risk_analytics/part-*.csv (уже CSV)")
print(f"  5. channel_stats/part-*.csv (уже CSV)")