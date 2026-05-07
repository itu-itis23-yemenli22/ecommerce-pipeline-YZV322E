-- =============================================================
-- queries.sql  |  Reference analytics queries
-- These mirror the Kibana dashboards and document the logic.
-- =============================================================

-- 1. Monthly order volume and revenue trend
SELECT
    DATE_TRUNC('month', order_purchase_timestamp) AS month,
    COUNT(*)                                       AS order_count,
    ROUND(SUM(total_payment)::NUMERIC, 2)          AS total_revenue
FROM staging.orders_enriched
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 2. Order status distribution
SELECT
    order_status,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM staging.orders_enriched
GROUP BY order_status
ORDER BY count DESC;

-- 3. Top 10 product categories by revenue
SELECT
    product_category_name_english AS category,
    SUM(total_revenue)             AS revenue,
    SUM(total_orders)              AS orders
FROM staging.product_revenue
GROUP BY category
ORDER BY revenue DESC
LIMIT 10;

-- 4. Average delivery time by state
SELECT
    customer_state,
    ROUND(AVG(delivery_days)::NUMERIC, 1) AS avg_delivery_days,
    COUNT(*)                              AS delivered_orders
FROM staging.orders_enriched
WHERE delivery_days IS NOT NULL
GROUP BY customer_state
ORDER BY avg_delivery_days;

-- 5. Delayed shipment rate overall and by state
SELECT
    customer_state,
    COUNT(*)                                           AS total_orders,
    SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)       AS delayed_orders,
    ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                   AS delay_rate_pct
FROM staging.orders_enriched
WHERE order_delivered_customer_date IS NOT NULL
GROUP BY customer_state
ORDER BY delay_rate_pct DESC;

-- 6. Review score distribution
SELECT
    ROUND(avg_review_score) AS score,
    COUNT(*)                AS order_count
FROM staging.orders_enriched
WHERE avg_review_score IS NOT NULL
GROUP BY ROUND(avg_review_score)
ORDER BY score;

-- 7. Top 10 sellers by revenue
SELECT
    seller_id,
    seller_state,
    total_revenue,
    total_orders,
    on_time_rate
FROM staging.seller_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- 8. Pipeline run history
SELECT
    run_id, dag_id, run_type, status,
    rows_processed,
    EXTRACT(EPOCH FROM (finished_at - started_at))::INT AS duration_sec,
    started_at
FROM staging.pipeline_runs
ORDER BY started_at DESC
LIMIT 20;
