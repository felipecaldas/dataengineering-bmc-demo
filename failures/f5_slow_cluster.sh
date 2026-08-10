#!/usr/bin/env bash
set -euo pipefail
seconds=${1:-45}
cd /home/azureuser/retail-data-demo
docker compose run --rm toolbox python -m demo.cli failure slow-cluster --seconds "$seconds"

