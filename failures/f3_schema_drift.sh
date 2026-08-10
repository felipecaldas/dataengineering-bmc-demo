#!/usr/bin/env bash
set -euo pipefail
trading_date=${1:-2026-08-14}
cd /home/azureuser/retail-data-demo
docker compose run --rm toolbox python -m demo.cli failure schema-drift --date "$trading_date"

