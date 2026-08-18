from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from demo.config import settings


DEFAULT_CONFIG = {
    "asn_enabled": "true",
    "asn_schema_variant": "standard",
    "ingest_delay_seconds": "0",
    "withhold_eod_count": "0",
    "wms_ack_delay_seconds": "2",
    "wms_mode": "ack",
}


def state_root() -> Path:
    root = settings.runtime_root / "state"
    root.mkdir(parents=True, exist_ok=True)
    return root


@contextmanager
def _lock(name: str) -> Iterator[None]:
    path = state_root() / f".{name}.lock"
    with path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    return json.loads(path.read_text())


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def config_path() -> Path:
    return state_root() / "demo-config.json"


def get_config(key: str, default: str | None = None) -> str | None:
    values = read_json(config_path(), {})
    return str(values.get(key, DEFAULT_CONFIG.get(key, default)))


def set_config(key: str, value: str) -> None:
    with _lock("demo-config"):
        values = read_json(config_path(), {})
        values[key] = str(value)
        write_json(config_path(), values)


def reset_config() -> dict[str, str]:
    with _lock("demo-config"):
        write_json(config_path(), DEFAULT_CONFIG)
    return dict(DEFAULT_CONFIG)


def simulation_path(trading_date: object) -> Path:
    date_key = str(trading_date).replace("-", "")
    return state_root() / "simulations" / f"{date_key}.json"


def stock_snapshot_path(trading_date: object) -> Path:
    date_key = str(trading_date).replace("-", "")
    return state_root() / "failures" / f"stock-on-hand-{date_key}.csv"
