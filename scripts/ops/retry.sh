#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
run_id="${1:?usage: bash scripts/ops/retry.sh RUN_ID}"
traffic-pipeline verify --run-id "$run_id" --repair || [[ $? == 2 ]]
traffic-pipeline retry --run-id "$run_id" --failed-only
traffic-pipeline work --run-id "$run_id"
traffic-pipeline verify --run-id "$run_id"
traffic-pipeline export --run-id "$run_id"
