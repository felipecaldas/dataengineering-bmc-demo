#!/usr/bin/env bash
set -euo pipefail

trading_date=${1:-2026-08-14}
allow_duplicate=${2:-false}

if [[ ! "$trading_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Trading date must be a valid YYYY-MM-DD date; received: $trading_date" >&2
  exit 2
fi

order_date=${trading_date//-/}
if [[ "$(date -d "$trading_date" +%Y%m%d 2>/dev/null || true)" != "$order_date" ]]; then
  echo "Trading date must be a valid YYYY-MM-DD date; received: $trading_date" >&2
  exit 2
fi

config_file=$(mktemp)
trap 'rm -f -- "$config_file"' EXIT
asn_path="/home/azureuser/retail-data-demo/runtime/asn/ASN_${order_date}.csv"
ack_path="/home/azureuser/retail-data-demo/runtime/wms/ack/REPLEN_ACK_${order_date}.txt"
if [[ "$allow_duplicate" == "true" ]]; then
  printf '{"waitForOrderDate":false,"createDuplicate":true,"independentFlow":true,"variables":[{"DEMO_DATE":"%s"},{"DEMO_ISO_DATE":"%s"},{"ASN_PATH":"%s"},{"ACK_PATH":"%s"}]}\n' \
    "$order_date" "$trading_date" "$asn_path" "$ack_path" >"$config_file"
else
  printf '{"waitForOrderDate":false,"variables":[{"DEMO_DATE":"%s"},{"DEMO_ISO_DATE":"%s"},{"ASN_PATH":"%s"},{"ACK_PATH":"%s"}]}\n' \
    "$order_date" "$trading_date" "$asn_path" "$ack_path" >"$config_file"
fi

ctm run order IN01 TradeCloseToReplenishment \
  -f "$config_file" \
  -e "${CTM_ENV:-se-dev}"
