#!/usr/bin/env python3
"""Render environment-specific Control-M JSON without changing the source workflow."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def render(source: Path, server: str, host: str, run_as: str) -> dict:
    document = json.loads(source.read_text())
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
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    rendered = json.dumps(render(args.source, args.server, args.host, args.run_as), indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    else:
        print(rendered, end="")

