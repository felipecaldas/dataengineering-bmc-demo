#!/usr/bin/env bash
set -euo pipefail
rows=${1:-400}
trading_date=${2:-2026-08-14}
cd /home/azureuser/retail-data-demo
docker compose run --rm toolbox python -m demo.cli failure phantom-stock --rows "$rows" --date "$trading_date"

