from __future__ import annotations

import json
import signal
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg.types.json import Jsonb

from demo.config import settings
from demo.db import connect


running = True


def _stop(*_: object) -> None:
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": "retail-ingress-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["pos.transactions.v1", "pos.store-eod.v1"])
    try:
        while running:
            messages = consumer.consume(num_messages=1000, timeout=1.0)
            if not messages:
                continue
            rows = []
            for message in messages:
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())
                payload = json.loads(message.value())
                if message.topic() == "pos.transactions.v1":
                    event_id = payload["transaction_id"]
                    source_ts = payload["transaction_ts_utc"]
                else:
                    event_id = f"eod:{payload['trading_date']}:{payload['store_id']}"
                    source_ts = payload["eod_ts_utc"]
                rows.append(
                    (
                        event_id,
                        message.topic(),
                        message.key().decode() if message.key() else "",
                        Jsonb(payload),
                        source_ts,
                    )
                )
            if rows:
                with connect() as conn, conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO ingress.kafka_events
                          (event_id, topic, event_key, payload, source_timestamp)
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (event_id) DO UPDATE SET
                          payload=excluded.payload, source_timestamp=excluded.source_timestamp,
                          ingested_at=now()
                        """,
                        rows,
                    )
                consumer.commit(asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

