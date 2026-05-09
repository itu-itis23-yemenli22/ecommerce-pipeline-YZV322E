"""
models.py
Dataclass definitions that mirror the staging schema.
Used by transform.py and sync.py for type-safe data handling.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class OrderEnriched:
    order_id: str
    customer_id: str
    customer_city: str
    customer_state: str
    order_status: str
    order_purchase_timestamp: Optional[datetime]
    order_approved_at: Optional[datetime]
    order_delivered_customer_date: Optional[datetime]
    order_estimated_delivery_date: Optional[datetime]
    delivery_days: Optional[int]
    is_delayed: bool
    delay_days: Optional[int]
    total_payment: float
    item_count: int
    avg_review_score: Optional[float]
    synced_to_es: bool = False
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_es_doc(self) -> dict:
        """Serialize to Elasticsearch document format."""
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "customer_city": self.customer_city,
            "customer_state": self.customer_state,
            "order_status": self.order_status,
            "order_purchase_timestamp": (
                self.order_purchase_timestamp.isoformat()
                if self.order_purchase_timestamp else None
            ),
            "order_delivered_customer_date": (
                self.order_delivered_customer_date.isoformat()
                if self.order_delivered_customer_date else None
            ),
            "order_estimated_delivery_date": (
                self.order_estimated_delivery_date.isoformat()
                if self.order_estimated_delivery_date else None
            ),
            "delivery_days": self.delivery_days,
            "is_delayed": self.is_delayed,
            "delay_days": self.delay_days,
            "total_payment": self.total_payment,
            "item_count": self.item_count,
            "avg_review_score": self.avg_review_score,
        }


@dataclass
class ProductRevenue:
    product_id: str
    product_category_name_english: Optional[str]
    total_revenue: float
    total_orders: int
    avg_price: float
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_es_doc(self) -> dict:
        return {
            "product_id": self.product_id,
            "category": self.product_category_name_english or "unknown",
            "total_revenue": self.total_revenue,
            "total_orders": self.total_orders,
            "avg_price": self.avg_price,
        }


@dataclass
class SellerPerformance:
    seller_id: str
    seller_city: str
    seller_state: str
    total_orders: int
    total_revenue: float
    avg_review_score: Optional[float]
    on_time_rate: Optional[float]
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_es_doc(self) -> dict:
        return {
            "seller_id": self.seller_id,
            "seller_city": self.seller_city,
            "seller_state": self.seller_state,
            "total_orders": self.total_orders,
            "total_revenue": self.total_revenue,
            "avg_review_score": self.avg_review_score,
            "on_time_rate": self.on_time_rate,
        }


@dataclass
class PipelineRun:
    dag_id: str
    run_type: str   # 'load' | 'transform' | 'sync'
    status: str     # 'running' | 'success' | 'failed'
    rows_processed: int = 0
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
