from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date

from demo import blob
from demo.db import connect


@dataclass(frozen=True)
class EodGate:
    trading_date: str
    expected: int
    actual: int
    percentage: float
    decision: str
    missing_store_ids: list[int]

    @property
    def ready(self) -> bool:
        return self.decision.startswith("PROCEED")

    def as_dict(self) -> dict:
        result = asdict(self)
        result["ready"] = self.ready
        return result


def classify_percentage(percentage: float, missing: int) -> str:
    if percentage >= 99.5:
        return "PROCEED" if missing == 0 else "PROCEED_WITH_EXCEPTIONS"
    if percentage >= 98.0:
        return "PROCEED_WITH_TRADE_OPS_ALERT"
    return "HOLD"


def eod_status(trading_date: date) -> EodGate:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.store_id
            FROM silver.dim_store s
            JOIN silver.trading_calendar c
              ON c.state_code = s.state_code
             AND c.calendar_date = %s
            WHERE s.status = 'TRADING'
              AND s.open_date <= %s
              AND (s.close_date IS NULL OR s.close_date > %s)
              AND c.is_trading_day = true
            ORDER BY s.store_id
            """,
            (trading_date, trading_date, trading_date),
        )
        expected_ids = [row["store_id"] for row in cur.fetchall()]
        cur.execute(
            """
            SELECT DISTINCT (payload->>'store_id')::int AS store_id
            FROM ingress.kafka_events
            WHERE topic = 'pos.store-eod.v1'
              AND payload->>'trading_date' = %s
            """,
            (trading_date.isoformat(),),
        )
        actual_ids = {row["store_id"] for row in cur.fetchall()}
    missing = [store_id for store_id in expected_ids if store_id not in actual_ids]
    expected = len(expected_ids)
    actual = expected - len(missing)
    percentage = (100.0 * actual / expected) if expected else 100.0
    decision = classify_percentage(percentage, len(missing))
    return EodGate(
        trading_date.isoformat(), expected, actual, round(percentage, 3), decision, missing
    )


def asn_name(trading_date: date) -> str:
    return f"inbound/ASN_{trading_date:%Y%m%d}.csv"


def order_name(trading_date: date) -> str:
    return f"outbound/REPLEN_ORDER_{trading_date:%Y%m%d}.csv"


def ack_name(trading_date: date) -> str:
    return f"REPLEN_ACK_{trading_date:%Y%m%d}.txt"


def asn_ready(trading_date: date) -> bool:
    return blob.exists(asn_name(trading_date))
