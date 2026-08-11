from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, Iterable

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)

from demo.config import settings
from demo.gates import classify_percentage


SOURCE_TOPIC = os.getenv("EOD_SOURCE_TOPIC", "pos.store-eod.v1")
COMMAND_TOPIC = os.getenv(
    "EOD_READINESS_COMMAND_TOPIC", "retail.store-eod-readiness-command.v1"
)
STATE_TOPIC = os.getenv(
    "EOD_READINESS_STATE_TOPIC", "retail.store-eod-readiness-state.v1"
)
OUTPUT_TOPIC = os.getenv(
    "EOD_READINESS_OUTPUT_TOPIC", "retail.store-eod-readiness.v1"
)
CONSUMER_GROUP = os.getenv("EOD_READINESS_CONSUMER_GROUP", "retail-eod-readiness-v1")
TRANSACTIONAL_ID = os.getenv(
    "EOD_READINESS_TRANSACTIONAL_ID", "retail-eod-readiness-v1"
)
SETTLE_SECONDS = float(os.getenv("EOD_READINESS_SETTLE_SECONDS", "3"))

LOGGER = logging.getLogger("retail.eod-readiness")
RUNNING = True


@dataclass(frozen=True)
class ReadinessState:
    schema_version: int
    trading_date: str
    generation: int
    arm_id: str
    expected: int
    observed_store_ids: tuple[int, ...]
    emitted: bool
    decision: str
    last_marker_at: str | None

    @classmethod
    def armed(
        cls, trading_date: date, generation: int, arm_id: str, expected: int
    ) -> ReadinessState:
        return cls(
            schema_version=1,
            trading_date=trading_date.isoformat(),
            generation=generation,
            arm_id=arm_id,
            expected=expected,
            observed_store_ids=(),
            emitted=False,
            decision="HOLD",
            last_marker_at=None,
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ReadinessState:
        if value.get("schema_version") != 1:
            raise ValueError("Unsupported EOD readiness state schema version")
        return cls(
            schema_version=1,
            trading_date=date.fromisoformat(str(value["trading_date"])).isoformat(),
            generation=int(value["generation"]),
            arm_id=str(value["arm_id"]),
            expected=int(value["expected"]),
            observed_store_ids=tuple(
                sorted({int(store_id) for store_id in value["observed_store_ids"]})
            ),
            emitted=bool(value["emitted"]),
            decision=str(value["decision"]),
            last_marker_at=(
                str(value["last_marker_at"]) if value.get("last_marker_at") else None
            ),
        )

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["observed_store_ids"] = list(self.observed_store_ids)
        return result


class EodReadinessProjector:
    """Pure threshold projector; Kafka owns persistence and delivery atomicity."""

    def __init__(self, expected_store_ids: Iterable[int]) -> None:
        self.expected_store_ids = frozenset(int(value) for value in expected_store_ids)
        if not self.expected_store_ids:
            raise ValueError("At least one expected store is required")
        self.states: dict[str, ReadinessState] = {}

    def restore(self, state: ReadinessState) -> None:
        if state.expected != len(self.expected_store_ids):
            raise ValueError(
                "Stored readiness state expects "
                f"{state.expected} stores, configured estate has "
                f"{len(self.expected_store_ids)}"
            )
        self.states[state.trading_date] = state

    def arm(self, trading_date: date, arm_id: str) -> ReadinessState:
        current = self.states.get(trading_date.isoformat())
        if current and current.arm_id == arm_id:
            return current
        generation = current.generation + 1 if current else 1
        return ReadinessState.armed(
            trading_date, generation, arm_id, len(self.expected_store_ids)
        )

    def observe(
        self, payload: dict[str, Any], observed_at: datetime | None = None
    ) -> tuple[ReadinessState | None, dict[str, Any] | None]:
        trading_date = date.fromisoformat(str(payload["trading_date"]))
        store_id = int(payload["store_id"])
        current = self.states.get(trading_date.isoformat())
        if current is None:
            return None, None
        if store_id not in self.expected_store_ids:
            LOGGER.warning(
                "Ignoring EOD marker for unknown store_id=%s trading_date=%s",
                store_id,
                trading_date,
            )
            return current, None
        observed = set(current.observed_store_ids)
        if store_id in observed:
            return current, None
        observed.add(store_id)
        actual = len(observed)
        expected = len(self.expected_store_ids)
        missing = sorted(self.expected_store_ids - observed)
        percentage = 100.0 * actual / expected
        decision = classify_percentage(percentage, len(missing))
        observed_at = observed_at or datetime.now(timezone.utc)
        should_emit = actual == expected and not current.emitted
        next_state = replace(
            current,
            observed_store_ids=tuple(sorted(observed)),
            emitted=current.emitted or should_emit,
            decision=decision,
            last_marker_at=observed_at.isoformat(),
        )
        if not should_emit:
            return next_state, None
        return next_state, self._event(next_state, observed_at)

    def settled_events(
        self, now: datetime, settle_seconds: float
    ) -> list[tuple[ReadinessState, dict[str, Any]]]:
        due: list[tuple[ReadinessState, dict[str, Any]]] = []
        for current in self.states.values():
            if current.emitted or not current.decision.startswith("PROCEED"):
                continue
            if current.last_marker_at is None:
                continue
            last_marker_at = datetime.fromisoformat(current.last_marker_at)
            if (now - last_marker_at).total_seconds() < settle_seconds:
                continue
            next_state = replace(current, emitted=True)
            due.append((next_state, self._event(next_state, now)))
        return due

    def _event(
        self, state: ReadinessState, occurred_at: datetime
    ) -> dict[str, Any]:
        trading_date = date.fromisoformat(state.trading_date)
        observed = set(state.observed_store_ids)
        missing = sorted(self.expected_store_ids - observed)
        actual = len(observed)
        percentage = 100.0 * actual / state.expected
        date_key = trading_date.strftime("%Y%m%d")
        event = {
            "schema_version": 1,
            "event_id": (
                f"retail.store-eod-ready:{trading_date.isoformat()}:"
                f"g{state.generation}"
            ),
            "event_type": "retail.store-eod-readiness.v1",
            "event_name": f"RETAIL_EOD_READY_{date_key}",
            "trading_date": trading_date.isoformat(),
            "generation": state.generation,
            "expected_stores": state.expected,
            "actual_stores": actual,
            "percentage": round(percentage, 3),
            "decision": state.decision,
            "missing_store_ids": missing,
            "occurred_at": occurred_at.isoformat(),
        }
        return event

    def commit(self, state: ReadinessState | None) -> None:
        if state is not None:
            self.states[state.trading_date] = state


def _json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def _restore_states(bootstrap: str) -> dict[str, ReadinessState]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"retail-eod-state-restore-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
            "enable.partition.eof": True,
        }
    )
    states: dict[str, ReadinessState] = {}
    partition = TopicPartition(STATE_TOPIC, 0)
    try:
        low, high = consumer.get_watermark_offsets(partition, timeout=15)
        if high == low:
            return states
        consumer.assign([TopicPartition(STATE_TOPIC, 0, low)])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is not None:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaException(message.error())
                elif message.value() is None:
                    states.pop(message.key().decode(), None)
                else:
                    state = ReadinessState.from_dict(json.loads(message.value()))
                    states[state.trading_date] = state
            position = consumer.position([partition])[0].offset
            if position >= high:
                return states
        raise TimeoutError(f"Timed out restoring compacted state from {STATE_TOPIC}")
    finally:
        consumer.close()


def _stop(*_: object) -> None:
    global RUNNING
    RUNNING = False


def _transactional_producer(bootstrap: str) -> Producer:
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "retail-eod-readiness",
            "transactional.id": TRANSACTIONAL_ID,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.init_transactions(30)
    return producer


def _process_message(
    message: Any,
    projector: EodReadinessProjector,
    producer: Producer,
    consumer: Consumer,
) -> None:
    raw_value = message.value()
    payload = json.loads(raw_value) if raw_value is not None else None
    next_state: ReadinessState | None = None
    event: dict[str, Any] | None = None
    if payload is None:
        LOGGER.info(
            "Consumed compacted tombstone topic=%s key=%s",
            message.topic(),
            message.key().decode() if message.key() else "",
        )
    elif message.topic() == COMMAND_TOPIC:
        if payload.get("command") != "ARM":
            raise ValueError(f"Unsupported readiness command: {payload.get('command')!r}")
        trading_date = date.fromisoformat(str(payload["trading_date"]))
        next_state = projector.arm(trading_date, str(payload["command_id"]))
    elif message.topic() == SOURCE_TOPIC:
        next_state, event = projector.observe(payload)
    else:
        raise ValueError(f"Unexpected topic: {message.topic()}")

    producer.begin_transaction()
    try:
        if next_state is not None:
            producer.produce(
                STATE_TOPIC,
                key=next_state.trading_date,
                value=_json_bytes(next_state.as_dict()),
            )
        if event is not None:
            producer.produce(
                OUTPUT_TOPIC,
                key=str(event["event_id"]),
                value=_json_bytes(event),
            )
        producer.send_offsets_to_transaction(
            [TopicPartition(message.topic(), message.partition(), message.offset() + 1)],
            consumer.consumer_group_metadata(),
            30,
        )
        producer.commit_transaction(30)
    except Exception:
        producer.abort_transaction(30)
        raise
    projector.commit(next_state)
    if event is not None:
        LOGGER.info("Published readiness event %s", json.dumps(event, sort_keys=True))
    elif message.topic() == COMMAND_TOPIC and payload is not None:
        LOGGER.info(
            "Armed trading_date=%s generation=%s",
            next_state.trading_date if next_state else payload["trading_date"],
            next_state.generation if next_state else "unchanged",
        )


def _publish_settled_events(
    projector: EodReadinessProjector, producer: Producer
) -> None:
    now = datetime.now(timezone.utc)
    for next_state, event in projector.settled_events(now, SETTLE_SECONDS):
        producer.begin_transaction()
        try:
            producer.produce(
                STATE_TOPIC,
                key=next_state.trading_date,
                value=_json_bytes(next_state.as_dict()),
            )
            producer.produce(
                OUTPUT_TOPIC,
                key=str(event["event_id"]),
                value=_json_bytes(event),
            )
            producer.commit_transaction(30)
        except Exception:
            producer.abort_transaction(30)
            raise
        projector.commit(next_state)
        LOGGER.info(
            "Published settled readiness event %s", json.dumps(event, sort_keys=True)
        )


def run_service() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    bootstrap = settings.kafka_bootstrap
    projector = EodReadinessProjector(range(1, settings.store_count + 1))
    for state in _restore_states(bootstrap).values():
        projector.restore(state)
    producer = _transactional_producer(bootstrap)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "isolation.level": "read_committed",
            "client.id": "retail-eod-readiness",
        }
    )
    consumer.subscribe([COMMAND_TOPIC, SOURCE_TOPIC])
    LOGGER.info(
        "EOD readiness service started expected_stores=%s restored_dates=%s",
        settings.store_count,
        len(projector.states),
    )
    try:
        while RUNNING:
            message = consumer.poll(1)
            if message is None:
                _publish_settled_events(projector, producer)
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            _process_message(message, projector, producer, consumer)
    finally:
        consumer.close()


def _command_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "retail-eod-readiness-admin",
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def arm(trading_date: date, timeout: int) -> dict[str, Any]:
    arm_id = str(uuid.uuid4())
    command = {
        "schema_version": 1,
        "command": "ARM",
        "command_id": arm_id,
        "trading_date": trading_date.isoformat(),
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    producer = _command_producer(settings.kafka_bootstrap)
    producer.produce(
        COMMAND_TOPIC,
        key=trading_date.isoformat(),
        value=_json_bytes(command),
    )
    if producer.flush(15):
        raise RuntimeError("The EOD readiness arm command was not delivered")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = _restore_states(settings.kafka_bootstrap).get(trading_date.isoformat())
        if state and state.arm_id == arm_id:
            return state.as_dict()
        time.sleep(1)
    raise TimeoutError(f"EOD readiness service did not arm {trading_date} within {timeout}s")


def status(trading_date: date) -> dict[str, Any]:
    state = _restore_states(settings.kafka_bootstrap).get(trading_date.isoformat())
    if state is None:
        return {"trading_date": trading_date.isoformat(), "armed": False}
    actual = len(state.observed_store_ids)
    missing = sorted(set(range(1, state.expected + 1)) - set(state.observed_store_ids))
    return {
        **state.as_dict(),
        "armed": True,
        "actual": actual,
        "percentage": round(100.0 * actual / state.expected, 3),
        "missing_store_ids": missing,
    }


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Project store EOD markers into readiness events")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("run")
    arm_parser = subparsers.add_parser("arm")
    arm_parser.add_argument("--date", required=True, type=date.fromisoformat)
    arm_parser.add_argument("--timeout", type=int, default=30)
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    if args.command == "run":
        run_service()
    elif args.command == "arm":
        print(json.dumps(arm(args.date, args.timeout), indent=2, sort_keys=True))
    else:
        print(json.dumps(status(args.date), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
