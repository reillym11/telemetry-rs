# telemetry-rs

Rust implementations of the vehicle telemetry stream jobs. Part of a mock EV vehicle-to-cloud
platform for a product demo; all data is synthetic.

- `crates/telemetry-core`: shared runtime. Kafka transactions (exactly-once output), state stores
  backed by compacted changelog topics, restore, rebalance, metrics and health endpoints, shutdown.
- `jobs/ota-status-tracker`: reference port of the legacy Spark job. See its
  [SEMANTICS.md](jobs/ota-status-tracker/SEMANTICS.md) and
  [privacy-manifest.yaml](jobs/ota-status-tracker/privacy-manifest.yaml).
- `charts/telemetry-job`: generic Helm chart, one values file per job.
- `contracts/`: schemas, topic registry, sample fixture and parity golden, vendored at the SHAs in
  `contracts/SOURCE`.

Design: [docs/DESIGN.md](docs/DESIGN.md). Adding a job: [docs/PORTING.md](docs/PORTING.md).

## Setup

- Rust 1.98.1 (installed by rustup from `rust-toolchain.toml`)
- `cmake`, `g++` and `make` (librdkafka is built from source)
- Docker, for images and parity runs
- Helm 3, for chart checks

## Build and test

```bash
cargo build --release --locked
cargo test --locked
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo audit
helm lint charts/telemetry-job -f charts/telemetry-job/values/ota-status-tracker.yaml
helm template ota charts/telemetry-job -f charts/telemetry-job/values/ota-status-tracker.yaml
```

`cargo test` replays the vendored sample through the OTA job and compares it with the parity golden,
validates every output against the vendored schema, checks restore, and runs a property test over
arrival order.

## Run

```bash
docker build --build-arg JOB=ota-status-tracker -t telemetry-rs/ota-status-tracker:dev .
```

Add `--build-arg REGISTRY=mirror.gcr.io/library` if Docker Hub rate-limits you. Configuration is
environment variables:

| Variable | Default | |
|---|---|---|
| `KAFKA_BOOTSTRAP` | required | |
| `INPUT_TOPICS` | required | comma-separated |
| `OUTPUT_TOPIC` | required | `OUTPUT_PATH` is not supported yet |
| `CONSUMER_GROUP` | required | |
| `TRANSACTIONAL_ID` | required | one live process per id |
| `GROUP_INSTANCE_ID` | `TRANSACTIONAL_ID` | static membership |
| `STATE_TOPIC_PREFIX` | required | state topic is `<prefix>.<store>` |
| `EXIT_WHEN_CAUGHT_UP` | `false` | exit 0 once committed offsets reach the end offsets seen at startup |
| `BATCH_MAX_RECORDS` | `500` | records per transaction |
| `BATCH_MAX_MS` | `200` | maximum transaction duration |
| `TXN_TIMEOUT_MS` | `60000` | |
| `METRICS_PORT` | `9090` | `/metrics`, `/healthz`, `/readyz` |
| `LOG_LEVEL` | `info` | |

## Parity

[telemetry-parity](https://github.com/reillym11/telemetry-parity) replays the same
fixture through the legacy Spark job and this image and compares the results. Use the harness at the
`TELEMETRY_PARITY_SHA` in `contracts/SOURCE`:

```bash
cd telemetry-parity && scripts/fetch-deps.sh
RUST_IMAGE=telemetry-rs/ota-status-tracker:dev ./run.sh --job ota-status-tracker --fixture full \
  --chaos --chaos-at 0.4 --manifest ../telemetry-rs/jobs/ota-status-tracker/privacy-manifest.yaml
```

The sample has 43 OTA records, which fit in one transaction at the default batch size, so chaos on
the sample needs `BATCH_MAX_RECORDS=5`. CI builds that variant as `<image>-chaos`. Each MR pipeline
publishes the sample parity report as the "Parity report" artifact.
