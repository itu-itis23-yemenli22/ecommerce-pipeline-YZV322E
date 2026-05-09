"""
db.py
PostgreSQL connection pool and helper utilities.
All other modules import get_connection() from here — never create
their own connections.
"""

import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from psycopg2 import pool, sql

logger = logging.getLogger(__name__)

# ── Connection pool (initialised once at module level) ─────────────────────
_pool: Optional[pool.ThreadedConnectionPool] = None

DB_CONFIG = {
    "host":     os.environ["POSTGRES_HOST"],
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname":   os.environ["POSTGRES_DB"],
    "user":     os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}


def init_pool(minconn: int = 1, maxconn: int = 5) -> None:
    """Initialise the thread-safe connection pool. Call once at startup."""
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(minconn, maxconn, **DB_CONFIG)
        logger.info("PostgreSQL connection pool initialised (min=%d, max=%d)",
                    minconn, maxconn)


def _get_pool() -> pool.ThreadedConnectionPool:
    if _pool is None:
        init_pool()
    return _pool


@contextmanager
def get_connection():
    """Context manager that borrows a connection from the pool."""
    conn = _get_pool().getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)


def wait_for_postgres(retries: int = 10, delay: int = 5) -> None:
    """Block until PostgreSQL is reachable. Used in entrypoint scripts."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            logger.info("PostgreSQL is ready.")
            return
        except psycopg2.OperationalError as exc:
            logger.warning("Postgres not ready yet (attempt %d/%d): %s",
                           attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError("PostgreSQL did not become ready in time.")


# ── Pipeline run tracking ──────────────────────────────────────────────────

def start_pipeline_run(dag_id: str, run_type: str) -> int:
    """Insert a pipeline_run record and return its run_id."""
    query = """
        INSERT INTO staging.pipeline_runs (dag_id, run_type, status, started_at)
        VALUES (%s, %s, 'running', NOW())
        RETURNING run_id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (dag_id, run_type))
            run_id = cur.fetchone()[0]
    logger.info("Pipeline run started: run_id=%d dag=%s type=%s",
                run_id, dag_id, run_type)
    return run_id


def finish_pipeline_run(run_id: int,
                        status: str,
                        rows_processed: int = 0,
                        error_message: Optional[str] = None) -> None:
    """Mark a pipeline_run as finished."""
    query = """
        UPDATE staging.pipeline_runs
        SET status = %s,
            rows_processed = %s,
            finished_at = NOW(),
            error_message = %s
        WHERE run_id = %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (status, rows_processed, error_message, run_id))
    logger.info("Pipeline run finished: run_id=%d status=%s rows=%d",
                run_id, status, rows_processed)


def execute_query(query: str, params: tuple = ()) -> list:
    """Run a SELECT and return all rows as a list of dicts."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

