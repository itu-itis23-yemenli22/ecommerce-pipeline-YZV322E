"""
transform.py
Reads from raw schema, applies cleaning and enrichment logic,
writes results to the staging schema.
Must be run after loader.py completes successfully.
"""

import logging
import os

from db import (get_connection, init_pool, wait_for_postgres,
                start_pipeline_run, finish_pipeline_run, execute_query)

logger = logging.getLogger(__name__)


# ── SQL: build staging.orders_enriched ─────────────────────────────────────
ORDERS_ENRICHED_SQL = """
INSERT INTO staging.orders_enriched (
    order_id, customer_id, customer_city, customer_state,
    order_status, order_purchase_timestamp, order_approved_at,
    order_delivered_customer_date, order_estimated_delivery_date,
    delivery_days, is_delayed, delay_days,
    total_payment, item_count, avg_review_score,
    synced_to_es, updated_at
)
SELECT
    o.order_id,
    o.customer_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- delivery_days: NULL when not delivered yet
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_purchase_timestamp IS NOT NULL
        THEN DATE_PART('day',
             o.order_delivered_customer_date - o.order_purchase_timestamp)::INTEGER
        ELSE NULL
    END AS delivery_days,

    -- is_delayed: delivered after estimated date
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
        THEN o.order_delivered_customer_date > o.order_estimated_delivery_date
        ELSE FALSE
    END AS is_delayed,

    -- delay_days: positive = late, negative = early
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
        THEN DATE_PART('day',
             o.order_delivered_customer_date - o.order_estimated_delivery_date)::INTEGER
        ELSE NULL
    END AS delay_days,

    -- total_payment per order
    COALESCE(pay.total_payment, 0),

    -- item_count
    COALESCE(items.item_count, 0),

    -- avg_review_score (may be NULL if no review)
    rev.avg_score,

    FALSE,   -- synced_to_es
    NOW()

FROM raw.orders o
LEFT JOIN raw.customers c
       ON o.customer_id = c.customer_id
LEFT JOIN (
    SELECT order_id,
           SUM(payment_value) AS total_payment
    FROM raw.order_payments
    GROUP BY order_id
) pay ON o.order_id = pay.order_id
LEFT JOIN (
    SELECT order_id,
           COUNT(*) AS item_count
    FROM raw.order_items
    GROUP BY order_id
) items ON o.order_id = items.order_id
LEFT JOIN (
    SELECT order_id,
           ROUND(AVG(review_score)::NUMERIC, 2) AS avg_score
    FROM raw.order_reviews
    WHERE review_score BETWEEN 1 AND 5
    GROUP BY order_id
) rev ON o.order_id = rev.order_id

-- Skip rows with no order_id or customer_id
WHERE o.order_id IS NOT NULL
  AND o.customer_id IS NOT NULL

ON CONFLICT (order_id) DO UPDATE SET
    order_status                   = EXCLUDED.order_status,
    order_delivered_customer_date  = EXCLUDED.order_delivered_customer_date,
    delivery_days                  = EXCLUDED.delivery_days,
    is_delayed                     = EXCLUDED.is_delayed,
    delay_days                     = EXCLUDED.delay_days,
    total_payment                  = EXCLUDED.total_payment,
    item_count                     = EXCLUDED.item_count,
    avg_review_score               = EXCLUDED.avg_review_score,
    synced_to_es                   = FALSE,
    updated_at                     = NOW()
"""

# ── SQL: build staging.product_revenue ─────────────────────────────────────
PRODUCT_REVENUE_SQL = """
INSERT INTO staging.product_revenue (
    product_id, product_category_name_english,
    total_revenue, total_orders, avg_price, updated_at
)
SELECT
    oi.product_id,
    COALESCE(ct.product_category_name_english, 'unknown') AS category,
    ROUND(SUM(oi.price)::NUMERIC, 2)               AS total_revenue,
    COUNT(DISTINCT oi.order_id)                    AS total_orders,
    ROUND(AVG(oi.price)::NUMERIC, 2)               AS avg_price,
    NOW()
FROM raw.order_items oi
LEFT JOIN raw.products p
       ON oi.product_id = p.product_id
LEFT JOIN raw.category_translation ct
       ON p.product_category_name = ct.product_category_name
WHERE oi.product_id IS NOT NULL
GROUP BY oi.product_id, ct.product_category_name_english
ON CONFLICT (product_id) DO UPDATE SET
    product_category_name_english = EXCLUDED.product_category_name_english,
    total_revenue                 = EXCLUDED.total_revenue,
    total_orders                  = EXCLUDED.total_orders,
    avg_price                     = EXCLUDED.avg_price,
    updated_at                    = NOW()
"""

# ── SQL: build staging.seller_performance ──────────────────────────────────
SELLER_PERFORMANCE_SQL = """
INSERT INTO staging.seller_performance (
    seller_id, seller_city, seller_state,
    total_orders, total_revenue, avg_review_score, on_time_rate, updated_at
)
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT oi.order_id)              AS total_orders,
    ROUND(SUM(oi.price)::NUMERIC, 2)         AS total_revenue,
    ROUND(AVG(r.review_score)::NUMERIC, 2)   AS avg_review_score,
    ROUND(
        100.0 * SUM(
            CASE WHEN o.order_delivered_customer_date IS NOT NULL
                      AND o.order_estimated_delivery_date IS NOT NULL
                      AND o.order_delivered_customer_date
                          <= o.order_estimated_delivery_date
                 THEN 1 ELSE 0 END
        ) / NULLIF(COUNT(DISTINCT oi.order_id), 0)
    , 2) AS on_time_rate,
    NOW()
FROM raw.sellers s
JOIN raw.order_items oi  ON s.seller_id = oi.seller_id
JOIN raw.orders o        ON oi.order_id  = o.order_id
LEFT JOIN raw.order_reviews r ON o.order_id = r.order_id
GROUP BY s.seller_id, s.seller_city, s.seller_state
ON CONFLICT (seller_id) DO UPDATE SET
    total_orders      = EXCLUDED.total_orders,
    total_revenue     = EXCLUDED.total_revenue,
    avg_review_score  = EXCLUDED.avg_review_score,
    on_time_rate      = EXCLUDED.on_time_rate,
    updated_at        = NOW()
"""


def _run_sql(label: str, sql: str) -> int:
    """Execute a single INSERT…SELECT and return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            count = cur.rowcount
    logger.info("%s: %d rows upserted.", label, count)
    return count


def run(dag_id: str = "manual") -> int:
    """
    Entry point called by Airflow (or directly).
    Returns total rows written to staging.
    """
    wait_for_postgres()
    init_pool()
    run_id = start_pipeline_run(dag_id, "transform")
    total_rows = 0

    try:
        total_rows += _run_sql("orders_enriched",    ORDERS_ENRICHED_SQL)
        total_rows += _run_sql("product_revenue",    PRODUCT_REVENUE_SQL)
        total_rows += _run_sql("seller_performance", SELLER_PERFORMANCE_SQL)

        finish_pipeline_run(run_id, "success", total_rows)
        logger.info("Transform finished. Total rows: %d", total_rows)

    except Exception as exc:
        finish_pipeline_run(run_id, "failed", total_rows, str(exc))
        logger.error("Transform failed: %s", exc, exc_info=True)
        raise

    return total_rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s – %(message)s")
    run()
