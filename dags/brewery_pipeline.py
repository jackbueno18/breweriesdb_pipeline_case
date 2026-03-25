from airflow import DAG
from airflow.operators.python import PythonOperator
from include.scripts.bronze import run_bronze
from include.scripts.silver import run_silver
from include.scripts.gold import run_gold

from datetime import datetime, timedelta

default_args = {
    "owner": "Jackson Bueno",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="brewery_pipeline",
    default_args=default_args,
    description="Open Brewery DB ingestion pipeline — Medallion Architecture",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["breweries", "medallion", "etl"],
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
    )

    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold,
    )

    bronze_task >> silver_task >> gold_task

    