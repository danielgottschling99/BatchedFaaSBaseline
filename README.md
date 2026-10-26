# BatchedFaaS benchmark

This repository contains the batch based FaaS baseline used in the thesis. A
producer publishes JPEG frames to Kafka, the batcher groups frames and invokes
an OpenFaaS function, and a consumer validate the returned frames and writes
benchmark metrics.

```text
client VM                                      SUT VM
Kafka :9094 <- frames/results -> batcher :8090 -> faasd :8080 -> function
producer + consumer
```

The repository supports a distributed two-VM experiment and a local experiment
where Kafka, the batcher, producer, and consumer run on the host while the
function runs in a Multipass faasd VM.

## Required setup

For the distributed experiment, both VM's need Docker Engine, Docker Compose v2,
and a checkout of this repository. The client must accept TCP connections from
the SUT on port 9094. The SUT must accept connections from the client on ports
8080 and 8090.

For the local experiment, the host needs Docker, Go 1.22 or newer, and
`faas-cli`. The host must be able to reach the Multipass VM on port 8080.

The distributed SUT also need `curl`, `iproute2` (`ip`), `faas-cli`, and
access to the container registry containing the function image. Multipass is
not required for the distributed setup.

### Prepare faasd on an existing SUT VM

On the SUT VM, install faasd directly:

```bash
git clone https://github.com/openfaas/faasd --depth=1
cd faasd
./hack/install.sh
```

### Create the Multipass faasd VM

For the local workflow, create the VM on the host once:

```bash
multipass launch --name faasd-vm --cpus 4 --memory 8G --disk 20G
multipass shell faasd-vm
```

Inside the VM, install the tools required by faasd and install faasd it self:

```bash
sudo apt-get update
sudo apt-get install -y git curl
git clone https://github.com/openfaas/faasd --depth=1
cd faasd
./hack/install.sh
```

Leave the VM running between trials. If it was stoped, start it again with:

```bash
multipass start faasd-vm
```

Find its current address and retrieve the generated faasd password from the
host with:

```bash
multipass info faasd-vm
multipass exec faasd-vm -- sudo cat /var/lib/faasd/secrets/basic-auth-password
```

Build and push the function image from the repository root. If the registry is
private, log the Docker daemon of the machine running faasd into it as well.

```bash
docker build -t ghcr.io/<your-user>/thesis-frame-batch-fn:latest ./function
docker push ghcr.io/<your-user>/thesis-frame-batch-fn:latest
```

Set the `image` field in [`stack.yml`](stack.yml) to the pushed image, then
deploy it on the SUT:

```bash
faas-cli login \
  --gateway http://127.0.0.1:8080 \
  --username admin \
  --password '<faasd-password>'
faas-cli deploy -f stack.yml --gateway http://127.0.0.1:8080
curl -fsS http://127.0.0.1:8080/system/functions
```

For the local workflow, run the login, deploy, and verification commands from
the host and replace `127.0.0.1` with the Multipass VM address.

The response must contain `frame-batch-processor` succesfully.

## Run an experiment

Use a fresh run ID for every trial. Start the client script in one terminal;
it starts Kafka and waits for the SUT. Then run the SUT script in a second
terminal. The scripts is expected to be run from the repository root.

On the client VM:

```bash
cd /path/to/BatchedFaaS
./scripts/run-client-benchmark.sh \
  --client-ip=10.156.0.4 \
  --sut-host=10.156.0.3 \
  --mode=unscaled \
  --frames=300 \
  --run-id=batch-faas-001 \
  --batch-size=16 \
  --max-wait-ms=1000 \
  --result-timeout-seconds=900
```

On the SUT VM, using the same run ID:

```bash
cd /path/to/BatchedFaaS
./scripts/run-sut.sh \
  --client-bootstrap=10.156.0.4:9094 \
  --run-id=batch-faas-001 \
  --batch-size=16 \
  --max-wait-ms=1000
```

The client script start Kafka, creates the `frames` and `results` topics,
waits for SUT readiness, starts the result consumer, publishes the requested
frames, and validates the results. The SUT script verifies the deployed
function, starts the batcher, and reports ready after the batcher connects to
Kafka.

Supported traffic modes are `unscaled`/`steady`, `burst`, and `step`. The
`frames/` directory is replayed as needed when `--frames` is larger than the
dataset.

## Run locally with a Multipass faasd VM

This workflow runs Kafka, the batcher, producer, and consumer on the host. The
function remains deployed in the Multipass faasd VM. It does not use the
two-VM benchmark scripts and does not requires a second client machine.

Start Kafka from the repository root:

```bash
docker compose up -d kafka
for topic in frames results; do
  docker exec thesis-kafka /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists \
    --topic "$topic" \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
done
```

Get the current faasd VM address on the host:

```bash
FAASD_IP="$(multipass info faasd-vm | awk '/IPv4/ {print $2}')"
FAASD_PASSWORD="$(multipass exec faasd-vm -- sudo cat /var/lib/faasd/secrets/basic-auth-password)"
echo "$FAASD_IP"
```

Deploy the function using the VM address if it is not already deployed:

```bash
faas-cli login \
  --gateway "http://${FAASD_IP}:8080" \
  --username admin \
  --password "$FAASD_PASSWORD"
faas-cli deploy -f stack.yml --gateway "http://${FAASD_IP}:8080"
```

Start the batcher on the host in another terminal. It calls the function through
the faasd gateway in the VM:

```bash
FAASD_IP="$(multipass info faasd-vm | awk '/IPv4/ {print $2}')"
FAASD_PASSWORD="$(multipass exec faasd-vm -- sudo cat /var/lib/faasd/secrets/basic-auth-password)"
cd batcher
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export INPUT_TOPIC=frames
export OUTPUT_TOPIC=results
export OPENFAAS_FUNCTION_URL=http://${FAASD_IP}:8080/function/frame-batch-processor
export OPENFAAS_USERNAME=admin
export OPENFAAS_PASSWORD="$FAASD_PASSWORD"
export BATCH_SIZE=16
export MAX_WAIT_MS=1000
export KAFKA_GROUP_ID=batch-faas-batcher-local-001
export RUN_ID=batch-faas-local-001
export HEALTH_ADDR=:8090
go run ./cmd/batcher
```

Start the consumer before the producer:

```bash
cd consumer
go run ./cmd/consumer \
  --bootstrap-servers localhost:9092 \
  --topic results \
  --group-id batch-faas-consumer-local-001 \
  --run-id batch-faas-local-001 \
  --expected-frames 300 \
  --sink-metrics-out e2e-sink-metrics.jsonl
```

Then publish the frames from a fourth terminal:

```bash
cd producer
go run ./cmd/producer \
  --input-dir ../frames \
  --bootstrap-servers localhost:9092 \
  --topic frames \
  --run-id batch-faas-local-001 \
  --pattern steady \
  --expected-frames 300 \
  --max-frames 300 \
  --loop \
  --source-metrics-out e2e-source-metrics.jsonl
```

Inspect the metrics as described below. Stop the Go process with `Ctrl-C`
and stop Kafka when finished:

```bash
docker compose down
```

## Metrics and validation

After a successful run, inspect the JSONL records for the run ID:

```bash
RUN_ID=batch-faas-001
grep "$RUN_ID" producer/e2e-source-metrics.jsonl | tail -1
grep "$RUN_ID" consumer/e2e-sink-metrics.jsonl | tail -1
```

For a local run, set `RUN_ID` to `batch-faas-local-001` instead.

The source record contain emitted frames, bytes sent, and source duration.
The sink record contains result counts, errors, completion time, effective FPS,
and success status. A successful trial should report:

```text
frames_sent == expected_frames
result_frames == expected_frames
unique_result_frames == expected_frames
duplicate_frames == 0
errors == 0
success == true
```

Metrics files are append-only. Keep them for analysis and use a new run ID for
the next trial.

## Cleanup

These commands remove only this project's client and SUT containers:

```bash
./scripts/cleanup-client.sh
./scripts/cleanup-sut.sh
```

Do not reinstall faasd between runs. If the function code or image changes,
push the new image and redeploy `stack.yml` first.

## Troubleshooting

- Kafka is unavailable: inspect `docker compose -p batchedfaas-client -f deploy/client-compose.yml ps` and logs. Confirm that port 9094 is reachable from the SUT.
- The SUT never becomes ready: inspect `docker compose -p batchedfaas-sut -f deploy/sut-compose.yml logs --tail=200 batcher`.
- The function is unavailable: run `curl http://<sut-ip>:8080/system/functions` and redeploy `stack.yml` if necessary.
- Results are missing or duplicated: use a fresh run ID, confirm that the client and SUT use the same ID, and inspect the sink JSONL record.
