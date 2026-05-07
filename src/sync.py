"""
sync.py
Syncs staging tables to Elasticsearch.
Only rows where synced_to_es = FALSE are pushed (incremental sync).
After a successful bulk index, marks those rows as synced.
"""

import logging
import os

from db import (get_connection, init_pool, wait_for_postgres,
                start_pipeline_run, finish_pipeline_run, execute_query)
from es_client import (get_es, wait_for_es, ensure_indices, bulk_index,
                       INDEX_ORDERS, INDEX_PRODUCTS, INDEX_SELLERS)
from models import OrderEnriched, ProductRevenue, SellerPerformance

logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 1000))


# ── Orders ─────────────────────────────────────────────────────────────────

UNSYNCED_ORDERS_SQL = """
    SELECT order_id, customer_id, customer_city, customer_state,
           order_status, order_purchase_timestamp, order_approved_at,
           order_delivered_customer_date, order_estimated_delivery_date,
           delivery_days, is_delayed, delay_days,
           total_payment, item_count, avg_review_score
    FROM staging.orders_enriched
    WHERE synced_to_es = FALSE
    LIMIT %s
"""

MARK_ORDERS_SYNCED_SQL = """
    UPDATE staging.orders_enriched
    SET synced_to_es = TRUE
    WHERE order_id = ANY(%s)
"""


def _sync_orders() -> int:
    total_indexed = 0
    while True:
        rows = execute_query(UNSYNCED_ORDERS_SQL, (BATCH_SIZE,))
        if not rows:
            break

        docs = [OrderEnriched(**r).to_es_doc() for r in rows]
        result = bulk_index(INDEX_ORDERS, docs, id_field="order_id")

        if result["indexed"] > 0:
            order_ids = [r["order_id"] for r in rows]
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(MARK_ORDERS_SYNCED_SQL, (order_ids,))
            total_indexed += result["indexed"]

        if len(rows) < BATCH_SIZE:
            break

    logger.info("Orders synced total: %d indexed.", total_indexed)
    return total_indexed


# ── Products ───────────────────────────────────────────────────────────────

PRODUCTS_SQL = """
    SELECT product_id, product_category_name_english,
           total_revenue, total_orders, avg_price
    FROM staging.product_revenue
"""


def _sync_products() -> int:
    rows = execute_query(PRODUCTS_SQL)
    if not rows:
        logger.info("No product rows to sync.")
        return 0

    docs = [ProductRevenue(**r).to_es_doc() for r in rows]
    result = bulk_index(INDEX_PRODUCTS, docs, id_field="product_id")
    logger.info("Products synced: %d indexed.", result["indexed"])
    return result["indexed"]


# ── Sellers ────────────────────────────────────────────────────────────────

SELLERS_SQL = """
    SELECT seller_id, seller_city, seller_state,
           total_orders, total_revenue, avg_review_score, on_time_rate
    FROM staging.seller_performance
"""


def _sync_sellers() -> int:
    rows = execute_query(SELLERS_SQL)
    if not rows:
        logger.info("No seller rows to sync.")
        return 0

    docs = [SellerPerformance(**r).to_es_doc() for r in rows]
    result = bulk_index(INDEX_SELLERS, docs, id_field="seller_id")
    logger.info("Sellers synced: %d indexed.", result["indexed"])
    return result["indexed"]


# ── Entry point ────────────────────────────────────────────────────────────

def run(dag_id: str = "manual") -> int:
    """
    Entry point called by Airflow (or directly).
    Returns total documents indexed.
    """
    wait_for_postgres()
    wait_for_es()
    init_pool()
    ensure_indices()

    run_id = start_pipeline_run(dag_id, "sync")
    total = 0

    try:
        total += _sync_orders()
        total += _sync_products()
        total += _sync_sellers()

        finish_pipeline_run(run_id, "success", total)
        logger.info("Sync finished. Total docs indexed: %d", total)

    except Exception as exc:
        finish_pipeline_run(run_id, "failed", total, str(exc))
        logger.error("Sync failed: %s", exc, exc_info=True)
        raise

    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s – %(message)s")
    run()
