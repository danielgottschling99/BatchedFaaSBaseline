#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KAFKA_BOOTSTRAP_SERVERS=unused:9094 \
KAFKA_GROUP_ID=cleanup-group \
RUN_ID=cleanup-run \
BATCH_SIZE=1 \
MAX_WAIT_MS=1000 \
OPENFAAS_FUNCTION_URL=http://127.0.0.1:8080/function/frame-batch-processor \
docker compose -p batchedfaas-sut -f "$ROOT_DIR/deploy/sut-compose.yml" down --remove-orphans
