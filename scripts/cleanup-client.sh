#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose -p batchedfaas-client -f "$ROOT_DIR/deploy/client-compose.yml" down --remove-orphans
