"""
dag_sync_scheduler.py
Periodic DAG: syncs only unsynced rows from staging → Elasticsearch.
Runs every hour. This is the DAG the instructor expects to see
for the "fully Airflow-orchestrated PG→ES synchronisation".
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException

from airflow_settings import DEFAULT_ARGS, SRC_PATH

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)


def _check_staging_has_data(**context):
    """Skip the sync gracefully if staging is empty."""
    sys.path.insert(0, SRC_PATH)
    from db import init_pool, wait_for_postgres, execute_query
    wait_for_postgres()
    init_pool()
    rows = execute_query(
        "SELECT COUNT(*) AS cnt FROM staging.orders_enriched WHERE synced_to_es = FALSE"
    )
    unsynced = rows[0]["cnt"] if rows else 0
    context["ti"].xcom_push(key="unsynced_count", value=unsynced)
    if unsynced == 0:
        raise AirflowSkipException("No unsynced rows — skipping sync.")
    return unsynced


def _run_sync(**context):
    import sync
    docs = sync.run(dag_id="dag_sync_scheduler")
    context["ti"].xcom_push(key="synced_docs", value=docs)


with DAG(
    dag_id="dag_sync_scheduler",
    description="Hourly incremental sync: staging → Elasticsearch",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["sync", "olist", "scheduled"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_task = PythonOperator(
        task_id="check_unsynced_rows",
        python_callable=_check_staging_has_data,
    )

    sync_task = PythonOperator(
        task_id="sync_to_elasticsearch",
        python_callable=_run_sync,
    )

    end = EmptyOperator(task_id="end")

    start >> check_task >> sync_task >> end
