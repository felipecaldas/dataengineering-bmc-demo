from __future__ import annotations

import csv
import io
import json
import random
import uuid
from datetime import date, datetime, time as clock_time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

from demo import blob
from demo.config import settings
from demo.seed import landing_name, trading_stores
from demo.state import get_config, read_json, simulation_path, write_json


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


def _publish(producer: Producer, topic: str, key: str, payload: dict) -> None:
    while True:
        try:
            producer.produce(topic, key=key, value=json.dumps(payload, separators=(",", ":")))
            return
        except BufferError:
            producer.poll(0.1)


def simulate_day(trading_date: date, withhold_count: int | None = None) -> dict:
    stores = trading_stores(trading_date)
    if not stores:
        raise RuntimeError(f"No demo stores are trading on {trading_date}")
    if withhold_count is None:
        withhold_count = int(get_config("withhold_eod_count", "0") or 0)
    if withhold_count < 0 or withhold_count >= len(stores):
        raise ValueError("withhold_count must be between zero and the trading-store count minus one")

    simulation_id = str(uuid.uuid4())
    withheld_ids = (
        {int(row["store_id"]) for row in stores[-withhold_count:]}
        if withhold_count
        else set()
    )
    simulation = {
        "schema_version": 1,
        "simulation_id": simulation_id,
        "trading_date": trading_date.isoformat(),
        "expected_store_ids": [int(row["store_id"]) for row in stores],
        "withheld_store_ids": sorted(withheld_ids),
        "released_store_ids": [],
    }
    write_json(simulation_path(trading_date), simulation)

    producer = _producer()
    transaction_messages = 0
    eod_messages = 0
    for store in stores:
        store_id = int(store["store_id"])
        rng = random.Random(settings.demo_seed + trading_date.toordinal() * 1000 + store_id)
        timezone_name = str(store["timezone"])
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
                "simulation_id": simulation_id,
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
            if not isinstance(close_value, clock_time):
                raise TypeError("close_time_local must be a datetime.time")
            close_dt = datetime.combine(trading_date, close_value).replace(tzinfo=tz)
            eod = {
                "simulation_id": simulation_id,
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
        "simulation_id": simulation_id,
        "trading_date": trading_date.isoformat(),
        "transactions": transaction_messages,
        "eod_markers": eod_messages,
        "withheld_store_ids": sorted(withheld_ids),
    }


def release_eod(trading_date: date) -> dict:
    path = simulation_path(trading_date)
    simulation = read_json(path)
    if not simulation:
        raise RuntimeError(f"No active simulation exists for {trading_date}")
    simulation_id = str(simulation["simulation_id"])
    withheld = set(int(value) for value in simulation.get("withheld_store_ids", []))
    released_already = set(int(value) for value in simulation.get("released_store_ids", []))
    to_release = sorted(withheld - released_already)
    stores = {int(row["store_id"]): row for row in trading_stores(trading_date)}
    producer = _producer()
    for store_id in to_release:
        store = stores[store_id]
        tz = ZoneInfo(str(store["timezone"]))
        close_value = store["close_time_local"]
        if not isinstance(close_value, clock_time):
            raise TypeError("close_time_local must be a datetime.time")
        close_dt = datetime.combine(trading_date, close_value).replace(tzinfo=tz)
        payload = {
            "simulation_id": simulation_id,
            "store_id": store_id,
            "trading_date": trading_date.isoformat(),
            "transaction_count": settings.txn_per_store,
            "total_ex_gst": "0.00",
            "eod_ts_local": close_dt.isoformat(),
            "eod_ts_utc": close_dt.astimezone(timezone.utc).isoformat(),
        }
        _publish(producer, "pos.store-eod.v1", str(store_id), payload)
    if producer.flush(30):
        raise RuntimeError("One or more released EOD markers were not delivered")
    simulation["released_store_ids"] = sorted(released_already | set(to_release))
    write_json(path, simulation)
    return {"simulation_id": simulation_id, "released_store_ids": to_release}


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
    name = landing_name(trading_date, "asn_inbound")
    blob.upload_bytes(name, content, "text/csv")
    host_visible = settings.runtime_root / "asn" / f"ASN_{trading_date:%Y%m%d}.csv"
    host_visible.parent.mkdir(parents=True, exist_ok=True)
    host_visible.write_bytes(content)
    return {"status": "UPLOADED", "blob": name, "rows": line_count, "schema": variant}
