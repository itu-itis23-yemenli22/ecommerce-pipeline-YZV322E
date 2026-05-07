"""
dag_health_check.py
Runs every 30 minutes and verifies that PostgreSQL and
Elasticsearch are reachable and contain expected data.
Results are logged and pushed to XCom for monitoring.
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_settings import DEFAULT_ARGS, SRC_PATH

if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)


def _check_postgres(**context):
    from db import init_pool, wait_for_postgres, execute_query
    wait_for_postgres(retries=3, delay=3)
    init_pool()
    result = execute_query("SELECT COUNT(*) AS cnt FROM raw.orders")
    count = result[0]["cnt"] if result else 0
    context["ti"].xcom_push(key="pg_orders_count", value=count)
    if count == 0:
        raise ValueError("PostgreSQL raw.orders is empty — pipeline may not have run.")
    return count


def _check_elasticsearch(**context):
    from es_client import wait_for_es, get_index_count, INDEX_ORDERS
    wait_for_es(retries=3, delay=3)
    count = get_index_count(INDEX_ORDERS)
    context["ti"].xcom_push(key="es_orders_count", value=count)
    return count


def _check_pipeline_runs(**context):
    from db import init_pool, execute_query
    init_pool()
    rows = execute_query("""
        SELECT run_type, status, rows_processed, started_at
        FROM staging.pipeline_runs
        ORDER BY started_at DESC
        LIMIT 5
    """)
    context["ti"].xcom_push(key="recent_runs", value=rows)
    failed = [r for r in rows if r["status"] == "failed"]
    if failed:
        raise ValueError(f"Recent failed pipeline runs detected: {failed}")
    return rows


with DAG(
    dag_id="dag_health_check",
    description="Periodic health check for PostgreSQL and Elasticsearch",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/30 * * * *",   # every 30 minutes
    catchup=False,
    tags=["monitoring", "olist"],
) as dag:

    pg_check = PythonOperator(
        task_id="check_postgres",
        python_callable=_check_postgres,
    )

    es_check = PythonOperator(
        task_id="check_elasticsearch",
        python_callable=_check_elasticsearch,
    )

    run_check = PythonOperator(
        task_id="check_recent_pipeline_runs",
        python_callable=_check_pipeline_runs,
    )

    [pg_check, es_check] >> run_check
