#!/usr/bin/env python3
"""Render environment-specific Control-M JSON without changing the source workflow."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLOUD_STATE = REPOSITORY_ROOT / "runtime" / "dbt_cloud" / "azure.json"


def _replace(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: _replace(item, replacements) for key, item in value.items()}
    if isinstance(value, list):
        return [_replace(item, replacements) for item in value]
    if isinstance(value, str):
        for placeholder, replacement in replacements.items():
            value = value.replace(placeholder, replacement)
    return value


def render(
    source: Path,
    server: str,
    host: str,
    run_as: str,
    cloud_state: Path = DEFAULT_CLOUD_STATE,
) -> dict:
    document = json.loads(source.read_text())
    state = json.loads(cloud_state.read_text())
    job_ids = state.get("job_ids", {})
    missing = [layer for layer in ("bronze", "silver", "gold") if not job_ids.get(layer)]
    if missing:
        raise RuntimeError(
            f"dbt Cloud state is missing Control-M job IDs for: {', '.join(missing)}"
        )
    replacements = {
        "${DBT_BRONZE_JOB_ID}": str(job_ids["bronze"]),
        "${DBT_SILVER_JOB_ID}": str(job_ids["silver"]),
        "${DBT_GOLD_JOB_ID}": str(job_ids["gold"]),
        "${DBT_CONNECTION_PROFILE}": str(
            state.get("controlm_connection_profile", "FMO_AZURE_DBT")
        ),
    }
    document = _replace(document, replacements)
    folder = document["TradeCloseToReplenishment"]
    folder["ControlmServer"] = server
    for value in folder.values():
        if not isinstance(value, dict):
            continue
        if "Host" in value:
            value["Host"] = host
        if "RunAs" in value:
            value["RunAs"] = run_as
    return document


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=Path("controlm/workflows/trade_close_to_replenishment.json"))
    parser.add_argument("--server", default="IN01")
    parser.add_argument("--host", default="fmo-azureuser")
    parser.add_argument("--run-as", default="azureuser")
    parser.add_argument("--cloud-state", type=Path, default=DEFAULT_CLOUD_STATE)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rendered = json.dumps(
        render(args.source, args.server, args.host, args.run_as, args.cloud_state),
        indent=2,
    ) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    else:
        print(rendered, end="")
