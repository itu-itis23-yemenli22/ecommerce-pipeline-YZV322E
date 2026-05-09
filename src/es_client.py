"""
es_client.py
Elasticsearch client singleton and index management utilities.
All sync operations import get_es() from here.
"""

import logging
import os
import time
from typing import Optional

from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger(__name__)

_es: Optional[Elasticsearch] = None

ES_HOST = os.environ.get("ES_HOST", "elasticsearch")
ES_PORT = int(os.environ.get("ES_PORT", 9200))

# ── Index names ────────────────────────────────────────────────────────────
INDEX_ORDERS   = os.environ.get("ES_INDEX_ORDERS",   "olist_orders")
INDEX_PRODUCTS = os.environ.get("ES_INDEX_PRODUCTS",  "olist_products")
INDEX_SELLERS  = os.environ.get("ES_INDEX_SELLERS",   "olist_sellers")

# ── Index mappings ─────────────────────────────────────────────────────────
INDEX_MAPPINGS = {
    INDEX_ORDERS: {
        "mappings": {
            "properties": {
                "order_id":                      {"type": "keyword"},
                "customer_id":                   {"type": "keyword"},
                "customer_city":                 {"type": "keyword"},
                "customer_state":                {"type": "keyword"},
                "order_status":                  {"type": "keyword"},
                "order_purchase_timestamp":      {"type": "date"},
                "order_delivered_customer_date": {"type": "date"},
                "order_estimated_delivery_date": {"type": "date"},
                "delivery_days":                 {"type": "integer"},
                "is_delayed":                    {"type": "boolean"},
                "delay_days":                    {"type": "integer"},
                "total_payment":                 {"type": "float"},
                "item_count":                    {"type": "integer"},
                "avg_review_score":              {"type": "float"},
            }
        }
    },
    INDEX_PRODUCTS: {
        "mappings": {
            "properties": {
                "product_id":    {"type": "keyword"},
                "category":      {"type": "keyword"},
                "total_revenue": {"type": "float"},
                "total_orders":  {"type": "integer"},
                "avg_price":     {"type": "float"},
            }
        }
    },
    INDEX_SELLERS: {
        "mappings": {
            "properties": {
                "seller_id":        {"type": "keyword"},
                "seller_city":      {"type": "keyword"},
                "seller_state":     {"type": "keyword"},
                "total_orders":     {"type": "integer"},
                "total_revenue":    {"type": "float"},
                "avg_review_score": {"type": "float"},
                "on_time_rate":     {"type": "float"},
            }
        }
    },
}


def get_es() -> Elasticsearch:
    """Return the singleton Elasticsearch client."""
    global _es
    if _es is None:
        _es = Elasticsearch(
            [{"host": ES_HOST, "port": ES_PORT, "scheme": "http"}],
            retry_on_timeout=True,
            max_retries=3,
        )
    return _es


def wait_for_es(retries: int = 12, delay: int = 10) -> None:
    """Block until Elasticsearch is reachable."""
    for attempt in range(1, retries + 1):
        try:
            if get_es().ping():
                logger.info("Elasticsearch is ready.")
                return
        except Exception as exc:
            logger.warning("ES not ready yet (attempt %d/%d): %s",
                           attempt, retries, exc)
        time.sleep(delay)
    raise RuntimeError("Elasticsearch did not become ready in time.")


def ensure_indices() -> None:
    """Create indices with mappings if they do not exist."""
    es = get_es()
    for index_name, body in INDEX_MAPPINGS.items():
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=body)
            logger.info("Created Elasticsearch index: %s", index_name)
        else:
            logger.debug("Index already exists: %s", index_name)


def bulk_index(index_name: str, docs: list, id_field: str = None) -> dict:
    """
    Bulk-index a list of dicts into the given index.
    If id_field is provided, uses its value as the document _id.
    Returns a summary with counts and the list of failed document IDs.
    """
    if not docs:
        logger.info("No documents to index for %s", index_name)
        return {"indexed": 0, "errors": 0, "failed_ids": []}

    def _actions():
        for doc in docs:
            action = {"_index": index_name, "_source": doc}
            if id_field and id_field in doc:
                action["_id"] = doc[id_field]
            yield action

    success, errors = helpers.bulk(get_es(), _actions(), raise_on_error=False)

    failed_ids = []
    for err in errors:
        for details in err.values():
            if "_id" in details:
                failed_ids.append(details["_id"])

    if errors:
        logger.warning("Bulk index into '%s': %d ok, %d failed",
                       index_name, success, len(errors))
    else:
        logger.info("Bulk indexed %d docs into '%s'", success, index_name)

    return {"indexed": success, "errors": len(errors), "failed_ids": failed_ids}


def get_index_count(index_name: str) -> int:
    """Return the number of documents in an index."""
    try:
        result = get_es().count(index=index_name)
        return result["count"]
    except Exception:
        return 0
