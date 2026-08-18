#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path
from typing import Any

import provision_cloud_databricks as base
from provision_controlm_jobs import JOB_DEFINITIONS, _job_command


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = REPOSITORY_ROOT / "runtime" / "dbt_cloud" / "azure.json"
SUCCESS = 10
TERMINAL = {10, 20, 30}


def _state() -> dict[str, Any]:
    if not STATE_PATH.is_file():
        raise RuntimeError("dbt Cloud jobs are not provisioned; run make dbt-cloud-provision")
    value = json.loads(STATE_PATH.read_text())
    if not isinstance(value, dict):
        raise ValueError("dbt Cloud state must be a JSON object")
    return value


def run(layer: str, trading_date: date) -> dict[str, Any]:
    state = _state()
    job_id = state.get("job_ids", {}).get(layer)
    if not job_id:
        raise RuntimeError(f"dbt Cloud job {layer!r} is not present in generated state")
    cloud = base._cloud_configuration()
    client = base.CloudClient(
        str(cloud["account-host"]), int(cloud["account-id"]), str(cloud["token-value"])
    )
    path = f"/api/v2/accounts/{client.account_id}/jobs/{int(job_id)}/run/"
    payload = {
        "cause": f"Retail demo {layer} for {trading_date.isoformat()}",
        "steps_override": [
            _job_command(str(JOB_DEFINITIONS[layer]["selector"]), trading_date.isoformat())
        ],
    }
    started = base._data(client.request("POST", path, payload))
    run_id = int(started["id"])
    deadline = time.monotonic() + 3600
    while time.monotonic() < deadline:
        detail = base._data(
            client.request("GET", f"/api/v2/accounts/{client.account_id}/runs/{run_id}/")
        )
        status = int(detail.get("status", 0))
        if status in TERMINAL:
            if status != SUCCESS:
                raise RuntimeError(
                    f"dbt Cloud {layer} run {run_id} ended with status {status}"
                )
            return {"layer": layer, "job_id": int(job_id), "run_id": run_id, "status": status}
        time.sleep(10)
    raise TimeoutError(f"dbt Cloud {layer} run {run_id} did not finish within one hour")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one shared dbt Cloud job")
    parser.add_argument("layer", choices=tuple(JOB_DEFINITIONS))
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    print(json.dumps(run(args.layer, args.date), indent=2))


if __name__ == "__main__":
    main()
