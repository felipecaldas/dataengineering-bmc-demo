#!/usr/bin/env bash
set -euo pipefail
stores=${1:-1}
trading_date=${2:-2026-08-14}
cd /home/azureuser/retail-data-demo
docker compose run --rm toolbox python -m demo.cli failure late-store --stores "$stores" --date "$trading_date"
