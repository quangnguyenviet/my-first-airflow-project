from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any
from decimal import Decimal

@dataclass
class SCD2Change:
    business_key: int
    old_sk: int | None
    effective_at:datetime
    values: dict[str, Any]

def _values_differ(left:Any, right:Any) -> bool:
    return left!=right

def customer_changed(source:dict, current:dict) -> bool:
    tracked_fields = ("full_name", "city")
    return any(
        _values_differ(source[field], current[field])
        for field in tracked_fields
    )

def product_changed(source:dict, current:dict) -> bool:
    tracked_fields = ("product_name", "category", "price")
    return any(
        _values_differ(source[field], current[field])
        for field in tracked_fields
    )

def detect_customer_changes(
        source_customers: list[dict],
        current_customers: list[dict]
) -> dict[str, list[dict]]:
    current_by_id = {
        customer["customer_id"]: customer
        for customer in current_customers
    }
    changes: list[dict] = []
    unchanged: list[dict] = []

    for source in source_customers:
        customer_id = source["customer_id"]
        current = current_by_id.get(customer_id)

        if current is None:
            changes.append(
                asdict(
                    SCD2Change(
                        business_key=customer_id,
                        old_sk=None,
                        effective_at=source["created_at"],
                        values={
                            "customer_id": customer_id,
                            "full_name": source["full_name"],
                            "email": source["email"],
                            "city": source["city"],
                            "updated_at": source["updated_at"]
                        }
                    )
                )
            )
            continue
        if not customer_changed(source, current):
            unchanged.append(source)
            continue

        effective_at = source["updated_at"]

        if effective_at <= current["valid_from"]:
            raise ValueError(
                f"Customer {customer_id} has invalid updated_at "
                f"{effective_at!r}; current valid_from is "
                f"{current['valid_from']!r}"
            )

        changes.append(
            asdict(
                SCD2Change(
                    business_key=customer_id,
                    old_sk=current["customer_sk"],
                    effective_at=effective_at,
                    values={
                        "customer_id": customer_id,
                        "full_name": source["full_name"],
                        "email": source["email"],
                        "city": source["city"],
                        "updated_at": source["updated_at"],
                    }
                )
            )
        )
    return {
        "changes": changes,
        "unchanged": unchanged,
    }

def detect_product_changes(
    source_products: list[dict],
    current_products: list[dict]
) -> dict[str, list[dict]]:
    current_by_id = {
        product["product_id"]: product
        for product in current_products
    }
    changes: list[dict]= []
    unchanged: list[dict]=[]

    for source in source_products:
        product_id = source["product_id"]
        current = current_by_id.get(product_id)

        if current is None:
            changes.append(
                asdict(
                    SCD2Change(
                        business_key=product_id,
                        old_sk=None,
                        effective_at=source["created_at"],
                        values={
                            "product_id": product_id,
                            "product_name": source["product_name"],
                            "category": source["category"],
                            "price": source["price"],
                            "updated_at": source["updated_at"],
                        },
                    )
                )
            )
            continue

        if not product_changed(source, current):
            unchanged.append(source)
            continue

        effective_at = source["updated_at"]

        if effective_at <= current["valid_from"]:
            raise ValueError(
                f"Product {product_id} has invalid updated_at "
                f"{effective_at!r}; current valid_from is "
                f"{current['valid_from']!r}"
            )

        changes.append(
            asdict(
                SCD2Change(
                    business_key=product_id,
                    old_sk=current["product_sk"],
                    effective_at=effective_at,
                    values={
                        "product_id": product_id,
                        "product_name": source["product_name"],
                        "category": source["category"],
                        "price": source["price"],
                        "updated_at": source["updated_at"],
                    },
                )
            )
        )

    return {
        "changes": changes,
        "unchanged": unchanged,
    }

def resolve_dimension_version(
    versions: list[dict],
    business_key_name: str,
    business_key: int,
    event_time: datetime,
    surrogate_key_name: str
) -> int:
    matches = [
        version
        for version in versions
        if version[business_key_name] == business_key
        and version["valid_from"] <= event_time
        and (
            version["valid_to"] is None
            or event_time < version["valid_to"]
        )
    ]

    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one dimension version for "
            f"{business_key_name}={business_key!r}, "
            f"event_time={event_time!r}; got {len(matches)}"
        )

    return matches[0][surrogate_key_name]

def build_fact_rows(
    orders: list[dict],
    customer_versions: list[dict],
    product_versions: list[dict]
) -> list[tuple]:
    result: list[tuple] = []

    for order in orders:
        customer_sk = resolve_dimension_version(
            versions=customer_versions,
            business_key_name="customer_id",
            business_key=order["customer_id"],
            event_time=order["order_date"],
            surrogate_key_name="customer_sk"
        )

        product_sk = resolve_dimension_version(
            versions=product_versions,
            business_key_name="product_id",
            business_key=order["product_id"],
            event_time=order["order_date"],
            surrogate_key_name="product_sk",
        )
        date_sk = int(order["order_date"].strftime("%Y%m%d"))
        total_amount = (
            Decimal(order["quantity"]) * order["unit_price"]
        )

        result.append(
            (
                order["order_id"],
                order["order_item_id"],
                customer_sk,
                product_sk,
                date_sk,
                order["quantity"],
                order["unit_price"],
                total_amount,
                order["status"],
                order["source_updated_at"]
            )
        )
    return result
