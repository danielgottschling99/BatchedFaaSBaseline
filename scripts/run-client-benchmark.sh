#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -p batchedfaas-client -f "$ROOT_DIR/deploy/client-compose.yml")

export HOST_UID="${HOST_UID:-$(id -u)}"
export HOST_GID="${HOST_GID:-$(id -g)}"

client_ip=10.156.0.4
sut_host=10.156.0.3
mode=unscaled
frames=30
run_id=""
batch_size=16
max_wait_ms=1000
result_timeout_seconds=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --client-ip=*) client_ip="${1#*=}" ;;
    --client-ip) client_ip="$2"; shift ;;
    --sut-host=*) sut_host="${1#*=}" ;;
    --sut-host) sut_host="$2"; shift ;;
    --mode=*) mode="${1#*=}" ;;
    --mode) mode="$2"; shift ;;
    --frames=*) frames="${1#*=}" ;;
    --frames) frames="$2"; shift ;;
    --run-id=*) run_id="${1#*=}" ;;
    --run-id) run_id="$2"; shift ;;
    --batch-size=*) batch_size="${1#*=}" ;;
    --batch-size) batch_size="$2"; shift ;;
    --max-wait-ms=*) max_wait_ms="${1#*=}" ;;
    --max-wait-ms) max_wait_ms="$2"; shift ;;
    --result-timeout-seconds=*) result_timeout_seconds="${1#*=}" ;;
    --result-timeout-seconds) result_timeout_seconds="$2"; shift ;;
    -h|--help)
      echo "see README.md for usage"
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

if [[ -z "$run_id" ]]; then
  echo "--run-id is required; use a fresh ID for every run" >&2
  exit 2
fi
if [[ ! "$run_id" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "run ID contains unsupported characters" >&2
  exit 2
fi

case "$mode" in
  unscaled|steady) pattern=steady ;;
  burst) pattern=burst ;;
  step) pattern=step ;;
  *)
    echo "unsupported --mode: $mode" >&2
    exit 2
    ;;
esac

group_id="batch-faas-consumer-${run_id}"
consumer_name="batch-faas-consumer-${run_id}"
consumer_cid=""
consumer_log_pid=""

cleanup_consumer() {
  [[ -z "$consumer_log_pid" ]] || kill "$consumer_log_pid" >/dev/null 2>&1 || true
  [[ -z "$consumer_cid" ]] || docker rm -f "$consumer_cid" >/dev/null 2>&1 || true
}
trap cleanup_consumer EXIT

CLIENT_IP="$client_ip" "${COMPOSE[@]}" up -d kafka
for _ in {1..60}; do
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 1
done
if ! "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 >/dev/null; then
  echo "Kafka unavailable; inspect client Kafka logs" >&2
  exit 1
fi
for topic in frames results; do
  if ! "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists \
    --topic "$topic" \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 >/dev/null; then
    echo "missing/uncreatable Kafka topic: $topic" >&2
    exit 1
  fi
done

echo "Waiting for SUT readiness for run_id=${run_id} before starting the consumer or producer"
ready=0
for _ in {1..180}; do
  if curl --connect-timeout 2 --max-time 3 -fsS \
    "http://${sut_host}:8090/readyz?run_id=${run_id}" 2>/dev/null \
    | grep -Fqx "ready run_id=${run_id}"; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  echo "SUT not ready: check gateway, function deployment, advertised Kafka address, and batcher logs" >&2
  exit 1
fi

echo "SUT is ready; starting the consumer before publishing frames"
"${COMPOSE[@]}" build consumer producer >/dev/null
consumer_cid=$("${COMPOSE[@]}" run -d --no-deps --name "$consumer_name" consumer \
  --bootstrap-servers kafka:9092 \
  --topic results \
  --group-id "$group_id" \
  --run-id "$run_id" \
  --expected-frames "$frames" \
  --sink-metrics-out /workspace/consumer/e2e-sink-metrics.jsonl)
sleep 2
"${COMPOSE[@]}" run --rm --no-deps producer \
  --input-dir /frames \
  --bootstrap-servers kafka:9092 \
  --topic frames \
  --pattern "$pattern" \
  --run-id "$run_id" \
  --expected-frames "$frames" \
  --max-frames "$frames" \
  --loop \
  --source-metrics-out /workspace/producer/e2e-source-metrics.jsonl
if [[ -z "$result_timeout_seconds" ]]; then
  result_timeout_seconds=$((120 + frames * 3 + (frames * max_wait_ms + 999) / 1000))
fi
if [[ ! "$result_timeout_seconds" =~ ^[0-9]+$ || "$result_timeout_seconds" -le 0 ]]; then
  echo "--result-timeout-seconds must be a positive integer" >&2
  exit 2
fi
deadline=$(( $(date +%s) + result_timeout_seconds ))
echo "Waiting up to ${result_timeout_seconds}s for ${frames} results"
docker logs -f --since=0s "$consumer_cid" &
consumer_log_pid=$!
while [[ "$(docker inspect -f '{{.State.Running}}' "$consumer_cid" 2>/dev/null || echo false)" == true ]]; do
  if (( $(date +%s) >= deadline )); then
    echo "timeout: fewer results than expected" >&2
    docker logs "$consumer_cid" >&2 || true
    exit 1
  fi
  sleep 1
done
consumer_rc=$(docker wait "$consumer_cid" 2>/dev/null || echo 1)
kill "$consumer_log_pid" >/dev/null 2>&1 || true
consumer_log_pid=""
docker logs "$consumer_cid" || true
if [[ "$consumer_rc" != 0 ]]; then
  echo "benchmark failed: inspect sink metrics for missing, duplicate, or error results" >&2
  exit "$consumer_rc"
fi
echo "Benchmark completed!"
