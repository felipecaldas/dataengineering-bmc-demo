from __future__ import annotations

import math
import random
from datetime import date, timedelta
from decimal import Decimal

import holidays

from demo.config import settings
from demo.db import connect


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


def seed_reference() -> dict[str, int]:
    stores = _store_plan(settings.store_count)
    calendar_rows = []
    start = date(2026, 1, 1)
    end = date(2026, 12, 31)
    for state, _, _ in STATE_PLAN:
        state_holidays = holidays.country_holidays("AU", subdiv=state, years=[2026])
        current = start
        while current <= end:
            holiday_name = state_holidays.get(current)
            calendar_rows.append(
                (
                    current,
                    state,
                    holiday_name is None,
                    str(holiday_name) if holiday_name else None,
                    "CLOSED_FOR_DEMO" if holiday_name else "NORMAL",
                )
            )
            current += timedelta(days=1)

    categories = ["Home", "Apparel", "Kids", "Outdoor", "Electronics", "Consumables"]
    products = []
    rng = random.Random(settings.demo_seed)
    for index in range(1, settings.sku_count + 1):
        sku = f"SKU{index:06d}"
        cost = Decimal(str(round(rng.uniform(1.5, 120.0), 2)))
        price = (cost * Decimal(str(rng.uniform(1.25, 2.1)))).quantize(Decimal("0.01"))
        products.append(
            (
                sku,
                f"Demo Product {index:04d}",
                categories[(index - 1) % len(categories)],
                cost,
                price,
                1 + index % 5,
                1 + index % 3,
                4 + index % 12,
                True,
            )
        )

    snapshot_date = settings.trading_date
    stock_rows = []
    ranged_per_store = min(settings.sku_count, 80)
    for store_id, _, _ in stores:
        for offset in range(ranged_per_store):
            product_index = ((store_id * 37 + offset * 13) % settings.sku_count) + 1
            stock_rows.append(
                (
                    store_id,
                    f"SKU{product_index:06d}",
                    5 + (store_id * 11 + offset * 7) % 90,
                    (store_id + offset) % 12,
                    snapshot_date,
                )
            )

    with connect() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO silver.dim_store
              (store_id, state_code, status, open_date, close_date, timezone, close_time_local)
            VALUES (%s, %s, 'TRADING', '2000-01-01', NULL, %s, '21:00')
            ON CONFLICT (store_id) DO UPDATE SET
              state_code=excluded.state_code, status=excluded.status,
              timezone=excluded.timezone, close_time_local=excluded.close_time_local
            """,
            stores,
        )
        cur.executemany(
            """
            INSERT INTO silver.trading_calendar
              (calendar_date, state_code, is_trading_day, holiday_name, trading_restriction)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (calendar_date, state_code) DO UPDATE SET
              is_trading_day=excluded.is_trading_day,
              holiday_name=excluded.holiday_name,
              trading_restriction=excluded.trading_restriction
            """,
            calendar_rows,
        )
        cur.executemany(
            """
            INSERT INTO silver.product_master
              (product_sku, product_name, category, unit_cost, retail_price,
               lead_time_days, review_period_days, safety_stock_units, is_active_line)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (product_sku) DO UPDATE SET
              product_name=excluded.product_name, category=excluded.category,
              unit_cost=excluded.unit_cost, retail_price=excluded.retail_price,
              lead_time_days=excluded.lead_time_days,
              review_period_days=excluded.review_period_days,
              safety_stock_units=excluded.safety_stock_units,
              is_active_line=excluded.is_active_line
            """,
            products,
        )
        cur.execute("DELETE FROM silver.stock_on_hand WHERE snapshot_date = %s", (snapshot_date,))
        cur.executemany(
            """
            INSERT INTO silver.stock_on_hand
              (store_id, product_sku, on_hand_units, on_order_units, snapshot_date)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (store_id, product_sku, snapshot_date) DO UPDATE SET
              on_hand_units=excluded.on_hand_units, on_order_units=excluded.on_order_units
            """,
            stock_rows,
        )
    return {
        "stores": len(stores),
        "calendar_rows": len(calendar_rows),
        "products": len(products),
        "stock_positions": len(stock_rows),
    }


def seed_history(days: int = 28) -> int:
    rng = random.Random(settings.demo_seed + 28)
    stores = _store_plan(settings.store_count)
    rows = []
    skus_per_store = min(settings.sku_count, 40)
    for day_offset in range(days, 0, -1):
        sale_date = settings.trading_date - timedelta(days=day_offset)
        for store_id, _, _ in stores:
            for offset in range(skus_per_store):
                product_index = ((store_id * 37 + offset * 13) % settings.sku_count) + 1
                units = 1 + rng.randrange(18)
                price = Decimal(str(4 + product_index % 75))
                rows.append(
                    (sale_date, store_id, f"SKU{product_index:06d}", units, units * price)
                )
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM silver.sales_history WHERE sale_date >= %s AND sale_date < %s",
            (settings.trading_date - timedelta(days=days), settings.trading_date),
        )
        cur.executemany(
            """
            INSERT INTO silver.sales_history
              (sale_date, store_id, product_sku, units_sold, sales_ex_gst)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (sale_date, store_id, product_sku) DO UPDATE SET
              units_sold=excluded.units_sold, sales_ex_gst=excluded.sales_ex_gst
            """,
            rows,
        )
    return len(rows)

