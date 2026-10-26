#!/usr/bin/env bash
set -Eeuo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

client_bootstrap=""
run_id=""
batch_size=16
max_wait_ms=1000

while [[ $# -gt 0 ]]; do
  case "$1" in
    --client-bootstrap=*) client_bootstrap="${1#*=}" ;;
    --client-bootstrap) client_bootstrap="$2"; shift ;;
    --run-id=*) run_id="${1#*=}" ;;
    --run-id) run_id="$2"; shift ;;
    --batch-size=*) batch_size="${1#*=}" ;;
    --batch-size) batch_size="$2"; shift ;;
    --max-wait-ms=*) max_wait_ms="${1#*=}" ;;
    --max-wait-ms) max_wait_ms="$2"; shift ;;
    -h|--help)
      echo "usage: $0 --client-bootstrap IP:9094 --run-id ID --batch-size N --max-wait-ms N"
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

if [[ -z "$client_bootstrap" || -z "$run_id" ]]; then
  echo "--client-bootstrap and --run-id are required" >&2
  exit 2
fi

client_ip="${client_bootstrap%:*}"
sut_ip="$(ip route get "$client_ip" 2>/dev/null | awk '/src/ {for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}')"
if [[ -z "$sut_ip" ]]; then
  sut_ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
fi
if [[ -z "$sut_ip" ]]; then
  echo "cannot determine SUT IP" >&2
  exit 1
fi

gateway="http://${sut_ip}:8080"
functions_ok=0
if curl --connect-timeout 3 --max-time 5 -fsS "$gateway/system/functions" \
  >/tmp/batchedfaas-functions.json 2>/dev/null \
  && grep -q frame-batch-processor /tmp/batchedfaas-functions.json; then
  functions_ok=1
elif command -v faas-cli >/dev/null 2>&1 \
  && faas-cli list --gateway "$gateway" 2>/dev/null \
  | grep -q frame-batch-processor; then
  functions_ok=1
fi
if [[ "$functions_ok" != 1 ]]; then
  echo "OpenFaaS/faasd gateway unavailable or frame-batch-processor is not deployed" >&2
  exit 1
fi

compose=(docker compose -p batchedfaas-sut -f "$ROOT_DIR/deploy/sut-compose.yml")

export KAFKA_BOOTSTRAP_SERVERS="$client_bootstrap"
export INPUT_TOPIC=frames
export OUTPUT_TOPIC=results
export KAFKA_GROUP_ID="batch-faas-batcher-${run_id}"
export RUN_ID="$run_id"
export BATCH_SIZE="$batch_size"
export MAX_WAIT_MS="$max_wait_ms"
export OPENFAAS_FUNCTION_URL="http://${sut_ip}:8080/function/frame-batch-processor"
export OPENFAAS_USERNAME="${OPENFAAS_USERNAME:-admin}" OPENFAAS_PASSWORD="${OPENFAAS_PASSWORD:-$(sudo -n cat /var/lib/faasd/secrets/basic-auth-password 2>/dev/null || true)}"
if [[ -z "$OPENFAAS_PASSWORD" ]]; then
  echo "cannot read faasd password; set OPENFAAS_PASSWORD or ensure /var/lib/faasd/secrets/basic-auth-password is readable" >&2
  exit 1
fi

"${compose[@]}" up -d --build --force-recreate batcher
for _ in {1..60}; do
  if curl --connect-timeout 2 --max-time 3 -fsS \
    "http://${sut_ip}:8090/readyz" >/dev/null 2>&1; then
    echo "SUT ready: batcher is connected to Kafka"
    exit 0
  fi
  sleep 1
done

echo "batcher did not become ready; inspect: ${compose[*]} logs --tail=200 batcher" >&2
"${compose[@]}" ps >&2
exit 1
