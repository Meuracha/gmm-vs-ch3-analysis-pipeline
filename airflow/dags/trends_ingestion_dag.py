"""Airflow DAG — เรียก scripts/poll_trends.py (เขียน BigQuery แบบ MERGE, idempotent)"""
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")
from scripts.poll_trends import main as poll_trends_main  # noqa: E402

default_args = {"owner": "gmm_ch3_project", "retries": 2}

with DAG(
    dag_id="google_trends_daily_poll",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 10),
    catchup=False,
    tags=["gmm-vs-ch3"],
) as dag:
    poll_task = PythonOperator(
        task_id="poll_trends_and_merge_bigquery",
        python_callable=lambda **ctx: poll_trends_main(ctx["ds"]),
    )
