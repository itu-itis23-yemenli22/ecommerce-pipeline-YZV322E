"""
dag_etl_pipeline.py
One-shot DAG: runs loader → transform → sync in sequence.
Triggered manually for the initial data load.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_settings import DEFAULT_ARGS, SRC_PATH

# Make src/ importable inside the Airflow container
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)


def _run_loader(**context):
    import loader
    rows = loader.run(dag_id="dag_etl_pipeline")
    context["ti"].xcom_push(key="loader_rows", value=rows)


def _run_transform(**context):
    import transform
    rows = transform.run(dag_id="dag_etl_pipeline")
    context["ti"].xcom_push(key="transform_rows", value=rows)


def _run_sync(**context):
    import sync
    docs = sync.run(dag_id="dag_etl_pipeline")
    context["ti"].xcom_push(key="sync_docs", value=docs)


with DAG(
    dag_id="dag_etl_pipeline",
    description="Full ETL: CSV load → transform → ES sync",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger only
    catchup=False,
    tags=["etl", "olist", "initial-load"],
) as dag:

    loader_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=_run_loader,
    )

    transform_task = PythonOperator(
        task_id="transform_raw_to_staging",
        python_callable=_run_transform,
    )

    sync_task = PythonOperator(
        task_id="sync_staging_to_elasticsearch",
        python_callable=_run_sync,
    )

    loader_task >> transform_task >> sync_task
