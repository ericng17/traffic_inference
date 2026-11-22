#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
if [[ ! -f .env ]]; then cp .env.example .env; fi
docker compose --env-file .env -f infra/compose.yaml --profile fake run --build --rm smoke
