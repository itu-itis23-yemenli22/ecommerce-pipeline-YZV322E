"""
airflow_settings.py
Shared constants used across all DAGs.
Import from here — never hardcode these values in individual DAGs.
"""

import os

# Airflow connection IDs (defined in Airflow UI or via env)
POSTGRES_CONN_ID = "olist_postgres"
ES_CONN_ID       = "olist_elasticsearch"

# Owner tag shown in Airflow UI
DAG_OWNER = "olist_team"

# Default DAG arguments applied to every task
DEFAULT_ARGS = {
    "owner":            DAG_OWNER,
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay_seconds": 30,
    "email_on_failure": False,
    "email_on_retry":   False,
}

# Source module path inside the Airflow container
SRC_PATH = "/opt/airflow/src"
