-- =============================================================
-- YZV322E Applied Data Engineering – Olist E-Commerce Pipeline
-- init.sql  |  PostgreSQL schema initialisation
-- =============================================================

-- ── Extensions ───────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Raw schema (loaded directly from CSV) ────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
-- ── Cleaned / transformed schema ─────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;

-- =============================================================
-- RAW TABLES
-- =============================================================

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id               VARCHAR(50) PRIMARY KEY,
    customer_unique_id        VARCHAR(50),
    customer_zip_code_prefix  VARCHAR(10),
    customer_city             VARCHAR(100),
    customer_state            CHAR(2)
);

CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id                VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix   VARCHAR(10),
    seller_city              VARCHAR(100),
    seller_state             CHAR(2)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id                    VARCHAR(50) PRIMARY KEY,
    product_category_name         VARCHAR(100),
    product_name_length           INTEGER,
    product_description_length    INTEGER,
    product_photos_qty            INTEGER,
    product_weight_g              NUMERIC(10,2),
    product_length_cm             NUMERIC(10,2),
    product_height_cm             NUMERIC(10,2),
    product_width_cm              NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS raw.category_translation (
    product_category_name            VARCHAR(100) PRIMARY KEY,
    product_category_name_english    VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                       VARCHAR(50) PRIMARY KEY,
    customer_id                    VARCHAR(50),
    order_status                   VARCHAR(30),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id             VARCHAR(50),
    order_item_id        INTEGER,
    product_id           VARCHAR(50),
    seller_id            VARCHAR(50),
    shipping_limit_date  TIMESTAMP,
    price                NUMERIC(10,2),
    freight_value        NUMERIC(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id                VARCHAR(50),
    payment_sequential      INTEGER,
    payment_type            VARCHAR(30),
    payment_installments    INTEGER,
    payment_value           NUMERIC(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id                  VARCHAR(50),
    order_id                   VARCHAR(50),
    review_score               SMALLINT,
    review_comment_title       TEXT,
    review_comment_message     TEXT,
    review_creation_date       TIMESTAMP,
    review_answer_timestamp    TIMESTAMP,
    PRIMARY KEY (review_id, order_id)
);

CREATE TABLE IF NOT EXISTS raw.geolocation (
    geolocation_zip_code_prefix  VARCHAR(10),
    geolocation_lat              NUMERIC(10,6),
    geolocation_lng              NUMERIC(10,6),
    geolocation_city             VARCHAR(100),
    geolocation_state            CHAR(2)
);

-- =============================================================
-- STAGING TABLES  (transformed, enriched)
-- =============================================================

CREATE TABLE IF NOT EXISTS staging.orders_enriched (
    order_id                       VARCHAR(50) PRIMARY KEY,
    customer_id                    VARCHAR(50),
    customer_city                  VARCHAR(100),
    customer_state                 CHAR(2),
    order_status                   VARCHAR(30),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    -- derived fields
    delivery_days                  INTEGER,
    is_delayed                     BOOLEAN,
    delay_days                     INTEGER,
    total_payment                  NUMERIC(10,2),
    item_count                     INTEGER,
    avg_review_score               NUMERIC(4,2),
    -- sync tracking
    synced_to_es                   BOOLEAN DEFAULT FALSE,
    updated_at                     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.product_revenue (
    product_id                    VARCHAR(50) PRIMARY KEY,
    product_category_name_english VARCHAR(100),
    total_revenue                 NUMERIC(12,2),
    total_orders                  INTEGER,
    avg_price                     NUMERIC(10,2),
    updated_at                    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.seller_performance (
    seller_id          VARCHAR(50) PRIMARY KEY,
    seller_city        VARCHAR(100),
    seller_state       CHAR(2),
    total_orders       INTEGER,
    total_revenue      NUMERIC(12,2),
    avg_review_score   NUMERIC(4,2),
    on_time_rate       NUMERIC(6,2),
    updated_at         TIMESTAMP DEFAULT NOW()
);

-- =============================================================
-- PIPELINE TRACKING
-- =============================================================

CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100),
    run_type        VARCHAR(30),   -- 'load', 'transform', 'sync'
    status          VARCHAR(20),   -- 'running', 'success', 'failed'
    rows_processed  INTEGER,
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    error_message   TEXT
);

-- =============================================================
-- INDEXES
-- =============================================================

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON raw.orders (order_status);
CREATE INDEX IF NOT EXISTS idx_orders_purchase_ts
    ON raw.orders (order_purchase_timestamp);
CREATE INDEX IF NOT EXISTS idx_order_items_product
    ON raw.order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_enriched_synced
    ON staging.orders_enriched (synced_to_es)
    WHERE synced_to_es = FALSE;
CREATE INDEX IF NOT EXISTS idx_enriched_state
    ON staging.orders_enriched (customer_state);
CREATE INDEX IF NOT EXISTS idx_enriched_status
    ON staging.orders_enriched (order_status);
