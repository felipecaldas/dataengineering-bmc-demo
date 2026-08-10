#!/usr/bin/env bash
set -euo pipefail

count=${1:-15}
trading_date=${2:-2026-08-14}
cd /home/azureuser/retail-data-demo

for ((run=1; run<=count; run++)); do
  echo "Ordering SLA history run $run/$count"
  ./controlm/scripts/order_workflow.sh "$trading_date" true
done

echo "Ordered $count runs. Confirm completion and forecast history in the Services view."
