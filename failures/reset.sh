#!/usr/bin/env bash
set -euo pipefail
trading_date=${1:-2026-08-14}
cd /home/azureuser/retail-data-demo
docker compose run --rm toolbox python -m demo.cli reset --date "$trading_date"

