# End-to-End Containerized Data Engineering Pipeline for E-Commerce Order Analytics

> **YZV 322E — Applied Data Engineering · Spring 2026 · Istanbul Technical University**

[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://docs.docker.com/compose/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)](https://www.postgresql.org/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.4-green)](https://airflow.apache.org/)
[![Elasticsearch](https://img.shields.io/badge/Elasticsearch-7.17.18-yellow)](https://www.elastic.co/)
[![Kibana](https://img.shields.io/badge/Kibana-7.17.18-orange)](https://www.elastic.co/kibana)

---

## Table of Contents

- [Project Summary](#project-summary)
- [Architecture](#architecture)
- [Dataset](#dataset)
- [Services](#services)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Dashboards](#dashboards)
- [Project Structure](#project-structure)
- [Known Limitations](#known-limitations)
- [Team Members](#team-members)

---

## Project Summary

This project implements a fully containerized, end-to-end data engineering pipeline for analyzing e-commerce order data from the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The entire system — including all Python ETL code, databases, orchestration, and visualization tools — runs inside Docker containers and can be launched with a single command.

The pipeline ingests over 100,000 real-world orders, applies schema normalization and data quality transformations, synchronizes processed data into Elasticsearch for high-performance indexing, and exposes interactive dashboards through Kibana covering order trends, category-level revenue, delivery performance, and shipment delay analysis.

---

## Architecture

```
CSV Files (Olist Dataset)
        │
        ▼
┌───────────────┐
│  etl-loader   │  Reads CSVs → loads into PostgreSQL raw schema
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ etl-transform │  Cleans, enriches, computes derived fields → staging schema
└───────┬───────┘
        │
        ▼
┌───────────────┐     ┌─────────────────────┐
│   etl-sync    │────▶│   Elasticsearch     │
└───────────────┘     └──────────┬──────────┘
                                 │
                                 ▼
                          ┌────────────┐
                          │   Kibana   │  Interactive Dashboards
                          └────────────┘

        Orchestrated by Apache Airflow (DAGs)
        All services managed by docker-compose.yml
```

**Data Flow:**
1. Raw CSVs are loaded into PostgreSQL (`raw` schema) by `etl-loader`
2. `etl-transform` joins and enriches data into the `staging` schema
3. `etl-sync` performs incremental sync from `staging` → Elasticsearch
4. Apache Airflow orchestrates and schedules the entire pipeline
5. Kibana visualizes the indexed data through 4 dashboards

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist**
- Source: [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- 100,000+ real orders from 2016 to 2018
- 9 CSV files covering orders, customers, sellers, products, payments, reviews, and geolocation

Download the dataset and place all CSV files inside the `data/` directory before running the pipeline.

---

## Services

| Service | Image | Port | Description |
|---|---|---|---|
| `postgres` | postgres:15-alpine | 5432 | Relational storage (raw + staging schemas) |
| `pgadmin` | dpage/pgadmin4:8 | 5050 | PostgreSQL administration UI |
| `elasticsearch` | elasticsearch:7.17.18 | 9200 | Document indexing and search |
| `kibana` | kibana:7.17.18 | 5601 | Dashboards and visualization |
| `airflow-webserver` | apache/airflow:2.8.4 | 8080 | Airflow UI |
| `airflow-scheduler` | apache/airflow:2.8.4 | — | DAG scheduling |
| `etl-loader` | custom (Dockerfile.etl) | — | CSV → PostgreSQL |
| `etl-transform` | custom (Dockerfile.etl) | — | raw → staging transformation |
| `etl-sync` | custom (Dockerfile.etl) | — | staging → Elasticsearch sync |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v24+)
- 8 GB RAM minimum, 16 GB recommended
- ~20 GB free disk space
- No Python, Java, or any other runtime required on the host machine

---

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/itu-itis23-yemenli22/ecommerce-pipeline-YZV322E.git
cd ecommerce-pipeline
```

### 2. Download the dataset

Download the Olist dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all CSV files inside the `data/` directory:

```
data/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_geolocation_dataset.csv
└── product_category_name_translation.csv
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in the required values:

```bash
POSTGRES_PASSWORD=your_password_here
AIRFLOW_ADMIN_PASSWORD=your_password_here
AIRFLOW__CORE__FERNET_KEY=   # generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW__WEBSERVER__SECRET_KEY=   # generate same way, use a different value
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://olist_user:your_password_here@postgres:5432/olist_db
```

### 4. Launch the pipeline

```bash
docker compose up --build
```

The system will:
1. Pull all required images
2. Build the custom ETL image
3. Initialize PostgreSQL schema (`sql/init.sql`)
4. Run `etl-loader` → `etl-transform` → `etl-sync` automatically
5. Start Airflow, Kibana, and pgAdmin

**The system is fully up within ~10 minutes on first run.**

---

## Usage

### Access the services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / your `AIRFLOW_ADMIN_PASSWORD` |
| Kibana | http://localhost:5601 | No auth required |
| pgAdmin | http://localhost:5050 | `admin@olist.com` / your `POSTGRES_PASSWORD` |

### Trigger the ETL pipeline manually (Airflow)

1. Open http://localhost:8080
2. Click on `dag_etl_pipeline`
3. Click the **▶ Trigger DAG** button
4. Monitor task execution: `load_csv_to_postgres` → `transform_raw_to_staging` → `sync_staging_to_elasticsearch`

### Scheduled sync

`dag_sync_scheduler` runs **hourly** and incrementally syncs any unsynced rows from PostgreSQL staging to Elasticsearch. No manual intervention needed.

### Health monitoring

`dag_health_check` runs every **30 minutes** and verifies that PostgreSQL and Elasticsearch are reachable and contain expected data.

---

## Dashboards

### Step 1: Run the ETL pipeline

1. Open Airflow at http://localhost:8080 (user: `admin`, password: your `AIRFLOW_ADMIN_PASSWORD`)
2. Click on `dag_etl_pipeline`
3. Click **▶ Trigger DAG** and wait for all 3 tasks to turn green (~5 minutes)

### Step 2: Create Kibana index patterns

1. Open Kibana at http://localhost:5601
2. Go to **Stack Management → Index Patterns → Create index pattern**
3. Create the following 3 patterns:

| Pattern | Time field |
|---|---|
| `olist_orders` | `order_purchase_timestamp` |
| `olist_products*` | (no time field) |
| `olist_sellers` | (no time field) |

### Step 3: Open the dashboard

1. Go to **Dashboard** in the left menu
2. Open **Olist E-Commerce Analytics**
3. Set the time range to `Jan 1, 2016 → Dec 31, 2018`

| Visualization | Description |
|---|---|
| Aylık Sipariş Hacmi | Monthly order volume trend (2016–2018) |
| Kategori Bazlı Gelir Dağılımı | Revenue distribution by product category (pie chart) |
| Eyalet Bazlı Ort. Teslimat Süresi | Average delivery days per Brazilian state |
| Eyalet Bazlı Gecikme Oranı | Delayed shipment rate per state (treemap) |

---

## Project Structure

```
ecommerce-pipeline/
├── docker-compose.yml          # All services defined here
├── Dockerfile.etl              # Custom image for ETL services
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
├── .gitignore
├── LICENSE
├── README.md
│
├── src/                        # ETL source modules
│   ├── db.py                   # PostgreSQL connection pool
│   ├── es_client.py            # Elasticsearch client
│   ├── models.py               # Dataclass definitions
│   ├── loader.py               # CSV → PostgreSQL (raw schema)
│   ├── transform.py            # raw → staging transformation
│   └── sync.py                 # staging → Elasticsearch sync
│
├── dags/                       # Apache Airflow DAGs
│   ├── airflow_settings.py     # Shared constants
│   ├── dag_etl_pipeline.py     # Full ETL: load → transform → sync
│   ├── dag_sync_scheduler.py   # Hourly incremental sync
│   └── dag_health_check.py     # Service health monitoring
│
├── sql/
│   ├── init.sql                # PostgreSQL schema initialization
│   └── queries.sql             # Reference analytics queries
│
├── kibana/
│   └── kibana.yml              # Kibana configuration
│
├── docker/                     # (legacy, Dockerfile.etl moved to root)
│
├── docs/                       # Technical report (.tex + PDF)
│
└── data/                       # Dataset CSVs (not committed to git)
```

---

## Known Limitations

- `product_category_name_translation.csv` contains a UTF-8 BOM header which required special handling (`utf-8-sig` encoding).
- Elasticsearch runs in single-node mode with security disabled — not suitable for production.
- The `olist_geolocation` table (~1M rows) significantly increases initial load time.
- Kibana index patterns must be recreated manually after `docker compose down -v` since Kibana's volume is cleared.

---

## Team Members

| Name | Student ID | Department |
|---|---|---|
| Enes Yüksel | 1150230722 | AI & Data Engineering, ITU |
| Emre Günel | — | AI & Data Engineering, ITU |
| Enes Yemenli | 150220311 | AI & Data Engineering, ITU |
