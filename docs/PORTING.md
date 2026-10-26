# Porting a job onto telemetry-core

`telemetry-core` owns Kafka, transactions, state, restore, metrics and shutdown. A job supplies
decoding, the per-record logic and encoding. `jobs/ota-status-tracker` is the reference.

## 1. Read the legacy job, not only its docs

Write `jobs/<job>/SEMANTICS.md` from the Scala code and its tests: keys, ordering and tie rules,
dedup, windows and timeouts, what is emitted and when, and every output field including nulls and
timestamp formats. Where the legacy code and the topic description disagree, the code wins; list the
difference under "Legacy behaviors kept as they are".

## 2. Add the crate

```text
jobs/<job>/
  Cargo.toml          # depends on telemetry-core; add it to the workspace members
  src/main.rs         # fn main() -> ExitCode { telemetry_core::run(MyJob::default()) }
  src/lib.rs          # the Job impl
  src/reducer.rs      # pure logic, no Kafka types
  tests/fixture.rs    # sample fixture vs golden, schema validation, restore, property test
  SEMANTICS.md
  privacy-manifest.yaml
```

## 3. Implement `Job`

```rust
impl telemetry_core::Job for MyJob {
    type Input = Event;
    type Output = Out;
    fn decode(&self, payload: &[u8]) -> Result<Option<Event>>;   // Ok(None) skips the record
    fn process(&mut self, input: Event) -> Result<Vec<Out>>;     // read and write state here
    fn encode(&self, out: &Out) -> Result<Record>;               // output key and JSON value
    fn stores(&mut self) -> Vec<&mut dyn StateStore>;            // every KeyedState the job owns
}
```

Rules the runtime relies on:

- **All state lives in `KeyedState`.** Anything kept in a plain field is lost on restart and is not
  rolled back when a transaction aborts. `KeyedState::get` sees writes made earlier in the same
  transaction; `put` and `delete` are staged and only become committed state after Kafka commits.
- **`process` must be deterministic** given the state and the input. No clocks, random numbers or
  hash-map iteration order in output.
- **Skipped records still commit their offsets.** Return `Ok(None)` from `decode` for heartbeats and
  other records the legacy job filtered out. A record that fails to decode is counted in
  `decode_errors_total`, logged by offset only, and skipped.
- **State values must carry the sensitive fields they hold** (for example `vin`), and the privacy
  manifest must list them. telemetry-parity check E3 scans state topic values.
- **Never log record contents.** Log topic, partition, offset and counts.

## 4. Store naming

A store named `latest` writes to `<STATE_TOPIC_PREFIX>.latest`. The runtime creates the topic
(compacted, same partition count as the input) and writes each change to the input record's partition,
so a partition's state moves with the partition on rebalance.

## 5. Tests

- Unit tests for the ordering rule: newer, older late, equal rank, duplicate, nulls.
- `tests/fixture.rs`: replay `contracts/fixtures/sample` and compare the last record per key with
  `contracts/golden/<job>`; validate every output against the vendored schema; replay the changelog
  into a fresh job and check a full redelivery emits nothing new.
- A proptest over arrival order with the topic's maximum lateness.

## 6. Packaging and parity

1. `docker build --build-arg JOB=<job> -t telemetry-rs/<job>:dev .`
2. Add `charts/telemetry-job/values/<job>.yaml`; run `helm lint` and `helm template` with it.
3. In telemetry-parity at the SHA in `contracts/SOURCE`:
   `RUST_IMAGE=telemetry-rs/<job>:dev ./run.sh --job <job> --fixture full --chaos-at 0.4 --manifest <path>`.
   Run `full` without chaos and with `--chaos-at 0.75` too.
4. Set `JOB` in `.gitlab-ci.yml` (or add a matrix entry) and open the MR with the `port` template.

## Not supported yet

`OUTPUT_PATH` (Parquet on S3, needed by `battery-health-aggregator`) is rejected at startup. A Parquet
sink has to be added to `telemetry-core` before that job can be ported; it needs its own
exactly-once design, because an object store write is not part of the Kafka transaction.
