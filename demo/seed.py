from __future__ import annotations

import csv
import io
import math
import random
from datetime import date, time as clock_time, timedelta
from decimal import Decimal
from typing import Iterable

import holidays

from demo import blob
from demo.config import settings


STATE_PLAN = [
    ("NSW", 90, "Australia/Sydney"),
    ("VIC", 60, "Australia/Melbourne"),
    ("QLD", 55, "Australia/Brisbane"),
    ("WA", 35, "Australia/Perth"),
    ("SA", 30, "Australia/Adelaide"),
    ("TAS", 15, "Australia/Hobart"),
    ("ACT", 20, "Australia/Sydney"),
    ("NT", 20, "Australia/Darwin"),
]


def landing_prefix(trading_date: date) -> str:
    return f"landing/trading_date={trading_date.isoformat()}"


def landing_name(trading_date: date, table: str) -> str:
    return f"{landing_prefix(trading_date)}/{table}.csv"


def _store_plan(count: int) -> list[tuple[int, str, str]]:
    if count == 325:
        state_counts = [(state, n, tz) for state, n, tz in STATE_PLAN]
    else:
        raw = [(state, count * n / 325.0, tz) for state, n, tz in STATE_PLAN]
        allocated = [(state, math.floor(value), tz) for state, value, tz in raw]
        remainder = count - sum(n for _, n, _ in allocated)
        order = sorted(
            range(len(raw)), key=lambda i: raw[i][1] - math.floor(raw[i][1]), reverse=True
        )
        state_counts = allocated[:]
        for index in order[:remainder]:
            state, n, tz = state_counts[index]
            state_counts[index] = (state, n + 1, tz)
    stores: list[tuple[int, str, str]] = []
    store_id = 1
    for state, state_count, timezone in state_counts:
        for _ in range(state_count):
            stores.append((store_id, state, timezone))
            store_id += 1
    return stores


def trading_stores(trading_date: date) -> list[dict[str, object]]:
    holiday_sets = {
        state: holidays.country_holidays("AU", subdiv=state, years=[trading_date.year])
        for state, _, _ in STATE_PLAN
    }
    return [
        {
            "store_id": store_id,
            "state_code": state,
            "timezone": timezone,
            "close_time_local": clock_time(21, 0),
        }
        for store_id, state, timezone in _store_plan(settings.store_count)
        if trading_date not in holiday_sets[state]
    ]


def _csv_bytes(header: list[str], rows: Iterable[dict[str, object]]) -> tuple[bytes, int]:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=header)
    writer.writeheader()
    count = 0
    for row in rows:
        writer.writerow(row)
        count += 1
    return stream.getvalue().encode(), count


def _upload_table(
    trading_date: date,
    table: str,
    header: list[str],
    rows: Iterable[dict[str, object]],
) -> int:
    content, count = _csv_bytes(header, rows)
    blob.upload_bytes(landing_name(trading_date, table), content, "text/csv")
    return count


def seed_reference(trading_date: date | None = None) -> dict[str, int]:
    trading_date = trading_date or settings.trading_date
    stores = _store_plan(settings.store_count)
    categories = ["Home", "Apparel", "Kids", "Outdoor", "Electronics", "Consumables"]
    rng = random.Random(settings.demo_seed)
    products = []
    for index in range(1, settings.sku_count + 1):
        cost = Decimal(str(round(rng.uniform(1.5, 120.0), 2)))
        price = (cost * Decimal(str(rng.uniform(1.25, 2.1)))).quantize(Decimal("0.01"))
        products.append(
            {
                "product_sku": f"SKU{index:06d}",
                "product_name": f"Demo Product {index:04d}",
                "category": categories[(index - 1) % len(categories)],
                "unit_cost": str(cost),
                "retail_price": str(price),
                "lead_time_days": 1 + index % 5,
                "review_period_days": 1 + index % 3,
                "safety_stock_units": 4 + index % 12,
                "is_active_line": "true",
            }
        )

    stock_rows = []
    ranged_per_store = min(settings.sku_count, 80)
    for store_id, _, _ in stores:
        for offset in range(ranged_per_store):
            product_index = ((store_id * 37 + offset * 13) % settings.sku_count) + 1
            stock_rows.append(
                {
                    "store_id": store_id,
                    "product_sku": f"SKU{product_index:06d}",
                    "on_hand_units": 5 + (store_id * 11 + offset * 7) % 90,
                    "on_order_units": (store_id + offset) % 12,
                    "snapshot_date": trading_date.isoformat(),
                }
            )

    product_count = _upload_table(
        trading_date,
        "product_master",
        [
            "product_sku",
            "product_name",
            "category",
            "unit_cost",
            "retail_price",
            "lead_time_days",
            "review_period_days",
            "safety_stock_units",
            "is_active_line",
        ],
        products,
    )
    stock_count = _upload_table(
        trading_date,
        "stock_on_hand",
        [
            "store_id",
            "product_sku",
            "on_hand_units",
            "on_order_units",
            "snapshot_date",
        ],
        stock_rows,
    )
    return {
        "stores": len(stores),
        "trading_stores": len(trading_stores(trading_date)),
        "products": product_count,
        "stock_positions": stock_count,
    }


def seed_history(days: int = 28, trading_date: date | None = None) -> int:
    trading_date = trading_date or settings.trading_date
    rng = random.Random(settings.demo_seed + 28)
    stores = _store_plan(settings.store_count)
    rows = []
    skus_per_store = min(settings.sku_count, 40)
    for day_offset in range(days, 0, -1):
        sale_date = trading_date - timedelta(days=day_offset)
        for store_id, _, _ in stores:
            for offset in range(skus_per_store):
                product_index = ((store_id * 37 + offset * 13) % settings.sku_count) + 1
                units = 1 + rng.randrange(18)
                price = Decimal(str(4 + product_index % 75))
                rows.append(
                    {
                        "sale_date": sale_date.isoformat(),
                        "store_id": store_id,
                        "product_sku": f"SKU{product_index:06d}",
                        "units_sold": units,
                        "sales_ex_gst": str(units * price),
                    }
                )
    return _upload_table(
        trading_date,
        "sales_history",
        ["sale_date", "store_id", "product_sku", "units_sold", "sales_ex_gst"],
        rows,
    )
