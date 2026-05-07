"""
loader.py
Reads Olist CSV files from DATA_PATH and bulk-loads them into
the raw schema in PostgreSQL.  Idempotent: truncates raw tables
before each load so re-runs are safe.
"""

import csv
import logging
import os
from pathlib import Path

from db import get_connection, init_pool, wait_for_postgres, \
    start_pipeline_run, finish_pipeline_run

logger = logging.getLogger(__name__)

DATA_PATH = Path(os.environ.get("DATA_PATH", "/opt/airflow/data"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 1000))

# Maps CSV filename → (raw table name, ordered column list)
CSV_TABLE_MAP = {
    "olist_customers_dataset.csv": (
        "raw.customers",
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"],
    ),
    "olist_sellers_dataset.csv": (
        "raw.sellers",
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    ),
    "olist_products_dataset.csv": (
        "raw.products",
        ["product_id", "product_category_name", "product_name_length",
         "product_description_length", "product_photos_qty",
         "product_weight_g", "product_length_cm",
         "product_height_cm", "product_width_cm"],
    ),
    "product_category_name_translation.csv": (
        "raw.category_translation",
        ["product_category_name", "product_category_name_english"],
    ),
    "olist_orders_dataset.csv": (
        "raw.orders",
        ["order_id", "customer_id", "order_status",
         "order_purchase_timestamp", "order_approved_at",
         "order_delivered_carrier_date", "order_delivered_customer_date",
         "order_estimated_delivery_date"],
    ),
    "olist_order_items_dataset.csv": (
        "raw.order_items",
        ["order_id", "order_item_id", "product_id", "seller_id",
         "shipping_limit_date", "price", "freight_value"],
    ),
    "olist_order_payments_dataset.csv": (
        "raw.order_payments",
        ["order_id", "payment_sequential", "payment_type",
         "payment_installments", "payment_value"],
    ),
    "olist_order_reviews_dataset.csv": (
        "raw.order_reviews",
        ["review_id", "order_id", "review_score",
         "review_comment_title", "review_comment_message",
         "review_creation_date", "review_answer_timestamp"],
    ),
    "olist_geolocation_dataset.csv": (
        "raw.geolocation",
        ["geolocation_zip_code_prefix", "geolocation_lat",
         "geolocation_lng", "geolocation_city", "geolocation_state"],
    ),
}


def _truncate_raw_tables() -> None:
    """Truncate all raw tables to allow idempotent reloads."""
    tables = [v[0] for v in CSV_TABLE_MAP.values()]
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"TRUNCATE TABLE {table} CASCADE")
    logger.info("Truncated %d raw tables.", len(tables))


def _load_csv(filepath: Path, table: str, columns: list) -> int:
    """Load a single CSV file into a raw table. Returns row count."""
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
        f"ON CONFLICT DO NOTHING"
    )
    # For category_translation, skip rows where first column (PK) is null
    skip_null_first_col = (table == "raw.category_translation")

    total = 0
    batch = []

    with open(filepath, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            record = tuple(row.get(col) or None for col in columns)
            if skip_null_first_col and record[0] is None:
                continue
            batch.append(record)
            if len(batch) >= BATCH_SIZE:
                _flush_batch(insert_sql, batch)
                total += len(batch)
                batch = []

    if batch:
        _flush_batch(insert_sql, batch)
        total += len(batch)

    logger.info("Loaded %d rows into %s", total, table)
    return total


def _flush_batch(sql: str, batch: list) -> None:
    import psycopg2.extras
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, batch, page_size=500)


def run(dag_id: str = "manual") -> int:
    """
    Entry point called by Airflow (or directly).
    Returns total rows loaded across all CSV files.
    """
    wait_for_postgres()
    init_pool()
    run_id = start_pipeline_run(dag_id, "load")
    total_rows = 0

    try:
        _truncate_raw_tables()

        for filename, (table, columns) in CSV_TABLE_MAP.items():
            filepath = DATA_PATH / filename
            if not filepath.exists():
                logger.warning("CSV not found, skipping: %s", filepath)
                continue
            rows = _load_csv(filepath, table, columns)
            total_rows += rows

        finish_pipeline_run(run_id, "success", total_rows)
        logger.info("Loader finished. Total rows loaded: %d", total_rows)

    except Exception as exc:
        finish_pipeline_run(run_id, "failed", total_rows, str(exc))
        logger.error("Loader failed: %s", exc, exc_info=True)
        raise

    return total_rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s – %(message)s")
    run()
