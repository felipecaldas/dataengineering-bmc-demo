#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import provision_cluster as base


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = REPOSITORY_ROOT / "runtime" / "databricks" / "azure.json"


def _dotenv() -> dict[str, str]:
    values: dict[str, str] = {}
    path = REPOSITORY_ROOT / ".env"
    if not path.is_file():
        return values
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


def _state() -> dict[str, Any]:
    if not STATE_PATH.is_file():
        raise RuntimeError("Databricks jobs are not provisioned; run make databricks-provision")
    value = json.loads(STATE_PATH.read_text())
    if not isinstance(value, dict):
        raise ValueError("Databricks state must be a JSON object")
    return value


def _storage_base() -> str:
    value = os.environ.get("DATABRICKS_STORAGE_BASE_PATH") or _dotenv().get(
        "DATABRICKS_STORAGE_BASE_PATH", ""
    )
    if not value:
        raise RuntimeError("DATABRICKS_STORAGE_BASE_PATH is not configured")
    return value.rstrip("/")


def run(job: str, trading_date: date) -> dict[str, Any]:
    state = _state()
    job_id = state.get("job_ids", {}).get(job)
    if not job_id:
        raise RuntimeError(f"Databricks job {job!r} is not present in generated state")
    date_prefix = f"trading_date={trading_date.isoformat()}"
    parameters = {"trading_date": trading_date.isoformat()}
    if job == "ingest":
        parameters["landing_path"] = f"{_storage_base()}/landing/{date_prefix}"
    else:
        parameters["outbound_path"] = f"{_storage_base()}/outbound"
    payload = base._run_cli(
        "jobs",
        "run-now",
        "--json",
        json.dumps({"job_id": int(job_id), "job_parameters": parameters}),
        "--timeout",
        "60m",
    )
    return {"job": job, "job_id": int(job_id), "parameters": parameters, "run": payload}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one shared Azure Databricks demo job")
    parser.add_argument("job", choices=("ingest", "export"))
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    print(json.dumps(run(args.job, args.date), indent=2))


if __name__ == "__main__":
    main()
