from __future__ import annotations

import csv
import hashlib
import io
import json
import time
import uuid
from datetime import date
from typing import Any, Iterable

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from demo import blob
from demo.config import settings
from demo.gates import eod_status
from demo.seed import landing_name, landing_prefix
from demo.state import get_config, read_json, simulation_path


POS_HEADER = [
    "transaction_id",
    "trading_date",
    "store_id",
    "till_id",
    "product_sku",
    "qty",
    "unit_price_ex_gst",
    "transaction_ts_local",
    "transaction_ts_utc",
]
EOD_HEADER = [
    "store_id",
    "trading_date",
    "transaction_count",
    "total_ex_gst",
    "eod_ts_local",
    "eod_ts_utc",
]
TABLES = {
    "product_master",
    "pos_transactions",
    "store_eod",
    "asn_inbound",
    "stock_on_hand",
    "sales_history",
}


def databricks_landing_path(trading_date: date) -> str:
    if not settings.databricks_storage_base_path:
        raise RuntimeError("DATABRICKS_STORAGE_BASE_PATH is not configured")
    return (
        f"{settings.databricks_storage_base_path}/"
        f"landing/trading_date={trading_date.isoformat()}"
    )


def _bounded_topic(topic: str, simulation_id: str, trading_date: date) -> list[dict[str, Any]]:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"retail-azure-stage-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=15)
        if topic not in metadata.topics or metadata.topics[topic].error is not None:
            raise RuntimeError(f"Kafka topic is unavailable: {topic}")
        partitions = sorted(metadata.topics[topic].partitions)
        bounds: dict[int, tuple[int, int]] = {}
        assignments = []
        for partition_id in partitions:
            partition = TopicPartition(topic, partition_id)
            low, high = consumer.get_watermark_offsets(partition, timeout=15)
            bounds[partition_id] = (low, high)
            assignments.append(TopicPartition(topic, partition_id, low))
        consumer.assign(assignments)
        remaining = {
            partition_id
            for partition_id, (low, high) in bounds.items()
            if high > low
        }
        rows: list[dict[str, Any]] = []
        deadline = time.monotonic() + 120
        while remaining and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    remaining.discard(message.partition())
                    continue
                raise KafkaException(message.error())
            payload = json.loads(message.value())
            if (
                payload.get("simulation_id") == simulation_id
                and payload.get("trading_date") == trading_date.isoformat()
            ):
                rows.append(payload)
            if message.offset() + 1 >= bounds[message.partition()][1]:
                remaining.discard(message.partition())
        if remaining:
            raise TimeoutError(f"Timed out reading bounded Kafka snapshot for {topic}")
        return rows
    finally:
        consumer.close()


def _csv_content(header: list[str], rows: Iterable[dict[str, Any]]) -> tuple[bytes, int]:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=header, extrasaction="ignore")
    writer.writeheader()
    count = 0
    for row in rows:
        writer.writerow(row)
        count += 1
    return stream.getvalue().encode(), count


def _csv_metadata(content: bytes) -> tuple[list[str], int]:
    reader = csv.reader(io.StringIO(content.decode("utf-8-sig"), newline=""))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise ValueError("Landing CSV is empty") from exc
    return header, sum(1 for _ in reader)


def _table_manifest(table: str, content: bytes, rows: int, header: list[str]) -> dict[str, Any]:
    return {
        "file": f"{table}.csv",
        "header": header,
        "rows": rows,
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def stage_inputs(trading_date: date) -> dict[str, Any]:
    simulation = read_json(simulation_path(trading_date))
    if not simulation:
        raise RuntimeError(f"No active simulation exists for {trading_date}")
    simulation_id = str(simulation["simulation_id"])
    gate = eod_status(trading_date)
    if not gate.ready:
        raise RuntimeError(
            f"EOD policy is {gate.decision}: {gate.actual}/{gate.expected} stores"
        )

    pos_messages = _bounded_topic("pos.transactions.v1", simulation_id, trading_date)
    eod_messages = _bounded_topic("pos.store-eod.v1", simulation_id, trading_date)
    pos_by_id = {str(row["transaction_id"]): row for row in pos_messages}
    eod_by_store = {int(row["store_id"]): row for row in eod_messages}
    expected_store_ids = {int(value) for value in simulation["expected_store_ids"]}
    expected_transactions = len(expected_store_ids) * settings.txn_per_store
    if len(pos_by_id) != expected_transactions:
        raise RuntimeError(
            f"POS snapshot is incomplete: expected={expected_transactions}, "
            f"actual={len(pos_by_id)}"
        )
    actual_pos_stores = {int(row["store_id"]) for row in pos_by_id.values()}
    if actual_pos_stores != expected_store_ids:
        raise RuntimeError("POS snapshot store set does not match the active simulation")
    counts_by_store: dict[int, int] = {}
    for row in pos_by_id.values():
        store_id = int(row["store_id"])
        counts_by_store[store_id] = counts_by_store.get(store_id, 0) + 1
    invalid_counts = {
        store_id: count
        for store_id, count in counts_by_store.items()
        if count != settings.txn_per_store
    }
    if invalid_counts:
        raise RuntimeError(f"POS per-store transaction counts are invalid: {invalid_counts}")
    if not set(eod_by_store).issubset(expected_store_ids):
        raise RuntimeError("EOD snapshot contains stores outside the active simulation")
    if len(eod_by_store) != gate.actual:
        raise RuntimeError(
            f"EOD snapshot does not match readiness state: gate={gate.actual}, "
            f"snapshot={len(eod_by_store)}"
        )

    generated: dict[str, bytes] = {}
    pos_content, _ = _csv_content(
        POS_HEADER,
        (pos_by_id[key] for key in sorted(pos_by_id)),
    )
    generated["pos_transactions"] = pos_content
    eod_content, _ = _csv_content(
        EOD_HEADER,
        (eod_by_store[key] for key in sorted(eod_by_store)),
    )
    generated["store_eod"] = eod_content
    for table, content in generated.items():
        blob.upload_bytes(landing_name(trading_date, table), content, "text/csv")

    manifest_tables: dict[str, dict[str, Any]] = {}
    for table in sorted(TABLES):
        name = landing_name(trading_date, table)
        if not blob.exists(name):
            raise FileNotFoundError(f"Required Azure landing object is missing: {name}")
        content = generated.get(table) or blob.download_bytes(name)
        header, rows = _csv_metadata(content)
        manifest_tables[table] = _table_manifest(table, content, rows, header)

    manifest = {
        "format_version": 2,
        "trading_date": trading_date.isoformat(),
        "simulation_id": simulation_id,
        "eod_decision": gate.decision,
        "eod_actual": gate.actual,
        "eod_expected": gate.expected,
        "ingest_delay_seconds": int(get_config("ingest_delay_seconds", "0") or 0),
        "tables": manifest_tables,
    }
    manifest_content = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    blob.upload_bytes(
        f"{landing_prefix(trading_date)}/manifest.json",
        manifest_content,
        "application/json",
    )
    return {
        "trading_date": trading_date.isoformat(),
        "simulation_id": simulation_id,
        "azure_prefix": landing_prefix(trading_date),
        "databricks_path": databricks_landing_path(trading_date),
        "tables": {name: value["rows"] for name, value in manifest_tables.items()},
    }
