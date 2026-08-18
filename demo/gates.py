from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date

from demo import blob
from demo.seed import landing_name, trading_stores


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
    # Import lazily because the Kafka readiness projector imports the shared
    # percentage classifier from this module.
    from demo.eod_readiness import status

    readiness = status(trading_date)
    expected_ids = [int(row["store_id"]) for row in trading_stores(trading_date)]
    expected = int(readiness.get("expected", len(expected_ids)))
    actual = int(readiness.get("actual", 0))
    missing = [int(value) for value in readiness.get("missing_store_ids", expected_ids)]
    percentage = float(readiness.get("percentage", 0.0 if expected else 100.0))
    decision = str(readiness.get("decision", classify_percentage(percentage, len(missing))))
    return EodGate(
        trading_date.isoformat(), expected, actual, round(percentage, 3), decision, missing
    )


def asn_name(trading_date: date) -> str:
    return landing_name(trading_date, "asn_inbound")


def order_name(trading_date: date) -> str:
    return f"outbound/REPLEN_ORDER_{trading_date:%Y%m%d}.csv"


def ack_name(trading_date: date) -> str:
    return f"REPLEN_ACK_{trading_date:%Y%m%d}.txt"


def asn_ready(trading_date: date) -> bool:
    return blob.exists(asn_name(trading_date))
