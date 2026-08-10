from __future__ import annotations

import csv
import io
import json
import random
import time
import uuid
from datetime import date, datetime, time as clock_time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

from demo import blob
from demo.config import settings
from demo.db import connect, get_config
from demo.gates import asn_name


def _producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": "retail-store-simulator",
            "enable.idempotence": True,
            "linger.ms": 20,
            "batch.num.messages": 10000,
        }
    )


def _trading_stores(trading_date: date) -> list[dict]:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.store_id, s.state_code, s.timezone, s.close_time_local
            FROM silver.dim_store s
            JOIN silver.trading_calendar c
              ON c.state_code=s.state_code AND c.calendar_date=%s
            WHERE s.status='TRADING' AND c.is_trading_day=true
            ORDER BY s.store_id
            """,
            (trading_date,),
        )
        return list(cur.fetchall())


def _publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    while True:
        try:
            producer.produce(topic, key=key, value=json.dumps(payload, separators=(",", ":")))
            return
        except BufferError:
            producer.poll(0.1)


def simulate_day(trading_date: date, withhold_count: int | None = None) -> dict:
    stores = _trading_stores(trading_date)
    if not stores:
        raise RuntimeError(f"No trading stores found for {trading_date}; run 'make seed' first")
    if withhold_count is None:
        withhold_count = int(get_config("withhold_eod_count", "0") or 0)
    withheld_ids = {row["store_id"] for row in stores[-withhold_count:]} if withhold_count else set()
    producer = _producer()
    transaction_messages = 0
    eod_messages = 0
    for store in stores:
        store_id = store["store_id"]
        rng = random.Random(settings.demo_seed + trading_date.toordinal() * 1000 + store_id)
        timezone_name = store["timezone"]
        tz = ZoneInfo(timezone_name)
        total = Decimal("0")
        for txn_no in range(1, settings.txn_per_store + 1):
            minutes = rng.randrange(9 * 60, 21 * 60)
            local_naive = datetime.combine(trading_date, clock_time.min) + timedelta(minutes=minutes)
            local_dt = local_naive.replace(tzinfo=tz)
            utc_dt = local_dt.astimezone(timezone.utc)
            product_index = ((store_id * 37 + txn_no * 13) % settings.sku_count) + 1
            qty = 1 + rng.randrange(3)
            price = Decimal(str(4 + product_index % 75)) + Decimal("0.95")
            transaction_id = str(
                uuid.uuid5(uuid.NAMESPACE_URL, f"kmart-demo:{trading_date}:{store_id}:{txn_no}")
            )
            payload = {
                "transaction_id": transaction_id,
                "trading_date": trading_date.isoformat(),
                "store_id": store_id,
                "till_id": 1 + txn_no % 12,
                "product_sku": f"SKU{product_index:06d}",
                "qty": qty,
                "unit_price_ex_gst": str(price),
                "transaction_ts_local": local_dt.isoformat(),
                "transaction_ts_utc": utc_dt.isoformat(),
            }
            _publish(producer, "pos.transactions.v1", str(store_id), payload)
            transaction_messages += 1
            total += price * qty
        if store_id not in withheld_ids:
            close_value = store["close_time_local"]
            close_dt = datetime.combine(trading_date, close_value).replace(tzinfo=tz)
            eod = {
                "store_id": store_id,
                "trading_date": trading_date.isoformat(),
                "transaction_count": settings.txn_per_store,
                "total_ex_gst": str(total.quantize(Decimal("0.01"))),
                "eod_ts_local": close_dt.isoformat(),
                "eod_ts_utc": close_dt.astimezone(timezone.utc).isoformat(),
            }
            _publish(producer, "pos.store-eod.v1", str(store_id), eod)
            eod_messages += 1
        producer.poll(0)
    remaining = producer.flush(60)
    if remaining:
        raise RuntimeError(f"Kafka producer still has {remaining} undelivered messages")
    return {
        "trading_date": trading_date.isoformat(),
        "transactions": transaction_messages,
        "eod_markers": eod_messages,
        "withheld_store_ids": sorted(withheld_ids),
    }


def release_eod(trading_date: date) -> dict:
    stores = _trading_stores(trading_date)
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT (payload->>'store_id')::int AS store_id
            FROM ingress.kafka_events
            WHERE topic='pos.store-eod.v1' AND payload->>'trading_date'=%s
            """,
            (trading_date.isoformat(),),
        )
        existing = {row["store_id"] for row in cur.fetchall()}
    producer = _producer()
    released = []
    for store in stores:
        store_id = store["store_id"]
        if store_id in existing:
            continue
        tz = ZoneInfo(store["timezone"])
        close_dt = datetime.combine(trading_date, store["close_time_local"]).replace(tzinfo=tz)
        payload = {
            "store_id": store_id,
            "trading_date": trading_date.isoformat(),
            "transaction_count": settings.txn_per_store,
            "total_ex_gst": "0.00",
            "eod_ts_local": close_dt.isoformat(),
            "eod_ts_utc": close_dt.astimezone(timezone.utc).isoformat(),
        }
        _publish(producer, "pos.store-eod.v1", str(store_id), payload)
        released.append(store_id)
    producer.flush(30)
    return {"released_store_ids": released}


ASN_HEADER = [
    "asn_id",
    "trading_date",
    "product_sku",
    "expected_units",
    "expected_arrival_date",
    "supplier_id",
]


def generate_asn(trading_date: date, line_count: int = 5000, force: bool = False) -> dict:
    enabled = (get_config("asn_enabled", "true") or "true").lower() == "true"
    if not enabled and not force:
        return {"status": "SKIPPED", "reason": "asn_enabled=false"}
    variant = get_config("asn_schema_variant", "standard") or "standard"
    header = ASN_HEADER + (["carton_id"] if variant == "drift" else [])
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=header)
    writer.writeheader()
    for index in range(1, line_count + 1):
        row = {
            "asn_id": f"ASN-{trading_date:%Y%m%d}-{index:05d}",
            "trading_date": trading_date.isoformat(),
            "product_sku": f"SKU{((index - 1) % settings.sku_count) + 1:06d}",
            "expected_units": 10 + index % 90,
            "expected_arrival_date": (trading_date + timedelta(days=1)).isoformat(),
            "supplier_id": f"SUP{1 + index % 12:03d}",
        }
        if variant == "drift":
            row["carton_id"] = f"CTN-{index:07d}"
        writer.writerow(row)
    content = stream.getvalue().encode()
    name = asn_name(trading_date)
    blob.upload_bytes(name, content, "text/csv")
    host_visible = Path("/workspace/runtime/asn") / name.rsplit("/", 1)[-1]
    host_visible.parent.mkdir(parents=True, exist_ok=True)
    host_visible.write_bytes(content)
    return {"status": "UPLOADED", "blob": name, "rows": line_count, "schema": variant}


def wait_for_ingest(trading_date: date, expected_transactions: int, timeout: int = 120) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) AS count FROM ingress.kafka_events
                WHERE topic='pos.transactions.v1' AND payload->>'trading_date'=%s
                """,
                (trading_date.isoformat(),),
            )
            count = cur.fetchone()["count"]
        if count >= expected_transactions:
            return count
        time.sleep(1)
    raise TimeoutError(f"Only {count}/{expected_transactions} transactions reached ingress")
