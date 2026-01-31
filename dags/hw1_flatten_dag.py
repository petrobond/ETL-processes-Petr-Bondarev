from __future__ import annotations

import csv
import json
from pathlib import Path
import xml.etree.ElementTree as ET

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = Path("/opt/airflow/project/HW1-data-sources")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"


def _text(node: ET.Element | None, tag: str) -> str:
    elem = node.find(tag) if node is not None else None
    return elem.text.strip() if elem is not None and elem.text else ""


def flatten_nutrition() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tree = ET.parse(INPUT_DIR / "nutrition.xml")
    root = tree.getroot()

    rows = []
    for food in root.findall("food"):
        serving = food.find("serving")
        calories = food.find("calories")
        vitamins = food.find("vitamins")
        minerals = food.find("minerals")
        rows.append(
            {
                "name": _text(food, "name"),
                "mfr": _text(food, "mfr"),
                "serving": _text(food, "serving"),
                "serving_units": serving.attrib.get("units", "").strip()
                if serving is not None
                else "",
                "calories_total": calories.attrib.get("total", "").strip()
                if calories is not None
                else "",
                "calories_fat": calories.attrib.get("fat", "").strip()
                if calories is not None
                else "",
                "total_fat": _text(food, "total-fat"),
                "saturated_fat": _text(food, "saturated-fat"),
                "cholesterol": _text(food, "cholesterol"),
                "sodium": _text(food, "sodium"),
                "carb": _text(food, "carb"),
                "fiber": _text(food, "fiber"),
                "protein": _text(food, "protein"),
                "vitamin_a": _text(vitamins, "a"),
                "vitamin_c": _text(vitamins, "c"),
                "mineral_ca": _text(minerals, "ca"),
                "mineral_fe": _text(minerals, "fe"),
            }
        )

    fieldnames = list(rows[0].keys()) if rows else []
    with (OUTPUT_DIR / "nutrition_flat.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def flatten_pets() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with (INPUT_DIR / "pets-data.json").open(encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for pet in data.get("pets", []):
        foods = pet.get("favFoods") or []
        fav_foods = ";".join(foods) if isinstance(foods, list) else ""
        rows.append(
            {
                "name": pet.get("name", ""),
                "species": pet.get("species", ""),
                "birthYear": pet.get("birthYear", ""),
                "photo": pet.get("photo", ""),
                "favFoods": fav_foods,
            }
        )

    fieldnames = list(rows[0].keys()) if rows else []
    with (OUTPUT_DIR / "pets_flat.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


with DAG(
    dag_id="hw1_flatten_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["hw1"],
) as dag:
    flatten_nutrition_task = PythonOperator(
        task_id="flatten_nutrition",
        python_callable=flatten_nutrition,
    )
    flatten_pets_task = PythonOperator(
        task_id="flatten_pets",
        python_callable=flatten_pets,
    )

    [flatten_nutrition_task, flatten_pets_task]
