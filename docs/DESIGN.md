# telemetry-rs design

Final design checkpoint for `telemetry-core` and the `ota-status-tracker` reference port.

## Process model

- One process runs one job instance with one `transactional.id`.
- Kafka processing runs on one blocking thread (`spawn_blocking`): a `BaseConsumer`, a transactional
  `ThreadedProducer`, and the job's state stores. Nothing on this thread is shared.
- Tokio runs the HTTP server (`/metrics`, `/healthz`, `/readyz`) and the SIGTERM/SIGINT handler.

## Startup

1. Create the producer and call `init_transactions`. This fences any older process with the same
   `transactional.id` and aborts its open transaction, so the read_committed restore below never
   waits on a transaction nobody will finish.
2. Create missing state topics: `<STATE_TOPIC_PREFIX>.<store>`, `cleanup.policy=compact`, as many
   partitions as the input topic.
3. Subscribe with `enable.auto.commit=false`, `isolation.level=read_committed`,
   `group.instance.id` (defaults to `TRANSACTIONAL_ID`), `session.timeout.ms=10000`.
4. On assignment: read the group's committed offsets, restore state, record each partition's high
   watermark (used by `EXIT_WHEN_CAUGHT_UP`), then mark `/readyz` ready.

## Transaction loop

```text
poll -> first record after a commit: begin_transaction
     -> decode; process against staged state; produce outputs and changelog records
     -> BATCH_MAX_RECORDS records or BATCH_MAX_MS since begin, or the batch ends:
          send_offsets_to_transaction(next offsets, consumer group metadata)
          commit_transaction
          apply staged state; advance local committed offsets
```

A batch works like a Spark micro-batch. Its first record reads the end offset of every assigned input
partition. Once every partition has reached its end offset, the processor calls `Job::end_batch` in the
open transaction, sends what it returns and the changelog records it staged, and commits. A batch can
span several transactions. Jobs use `end_batch` to advance an event-time watermark and fire timeouts,
so a key's output does not wait for that key's next record. With `EXIT_WHEN_CAUGHT_UP` the process
exits only after the last batch has ended, as Spark's `AvailableNow` runs a final no-data batch.

- The transaction stays open while records are processed, so `BATCH_MAX_MS` bounds the transaction,
  not only the poll.
- Records that `decode` skips (heartbeats, undecodable records) still advance offsets.
- Output records use the producer's `murmur2_random` partitioner, which matches Java's default for
  non-null keys. Changelog records set the partition explicitly to the input record's partition.

## Errors

| Class (`RDKafkaError`) | Action |
|---|---|
| `txn_requires_abort()` | `abort_transaction`, discard staged state, seek assigned partitions to their committed offsets, continue |
| `is_retriable()` | Retry the same call with backoff from 100 ms to 5 s, at most 30 s, then fatal |
| `is_fatal()`, fenced, anything else | Log and exit 1 |

A zombie (an old process still running after a new one called `init_transactions`) gets a fenced error
on its next transactional call and exits. With static membership the new process also takes over
the `group.instance.id`; the old consumer then gets a fatal `FencedInstanceId` error from `poll` and
exits 1 (verified against Kafka 3.9.1). Offsets sent with stale group metadata are rejected.

## State

`KeyedState<K, V>` keeps a committed map and a staged overlay for the open transaction. `get` reads
the overlay first. `put` and `delete` write the overlay and queue a changelog record or tombstone.
Commit merges the overlay; abort drops it. The committed map is never written mid-transaction, so
abort needs no undo log.

Restore uses a separate consumer (`read_committed`, `enable.partition.eof`) assigned to the changelog
partitions with the same numbers as the assigned input partitions, read from the beginning to the end.
It sets a `group.id` because librdkafka requires one to assign partitions, but never subscribes or
commits, so the group never exists on the broker.

## Rebalance

The rebalance callback runs inside `poll` on the processing thread.

- **Revoke:** abort the open transaction (committing inside the callback is not safe), drop staged
  state, drop committed state for the revoked partitions.
- **Assign:** `poll` returns no message for the call that serves the rebalance, so the loop restores
  the new partitions and seeks to committed offsets before any of their records is processed.

## Shutdown

SIGTERM or SIGINT sets a flag. The loop stops polling, commits the open transaction (aborts if the
commit fails), and closes the consumer. If processing has not finished 25 s after the signal, the
process exits 1; anything uncommitted is aborted by the next `init_transactions`.

With `group.instance.id` set, librdkafka does not send LeaveGroup on close. A restarted pod with the
same instance id rejoins without a rebalance; a pod that does not come back is removed after
`session.timeout.ms`.

## Failpoint (test only)

`TELEMETRY_FAILPOINT_AT_OFFSETS=N` exists only for telemetry-parity's `--chaos-mode failpoint`
(telemetry-parity `docs/RUST_JOB_CONTRACT.md`). Unset, which is the default and what the chart
deploys, it does nothing. When set, the first transaction whose commit would bring the committed
input offsets, summed over partitions, to `N` or more does this in `commit`, in order:

1. its outputs and changelog records are already produced;
2. `send_offsets_to_transaction`;
3. flush the producer, so the records are on the broker;
4. SIGKILL its own process, before `commit_transaction`.

The restarted process's `init_transactions` aborts that transaction, so read_committed readers never
see its records and read_uncommitted readers do. The log line carries only offset totals.

## Observability

Metrics (OpenMetrics on `METRICS_PORT`): `records_in_total`, `records_out_total`,
`decode_errors_total`, `txn_committed_total`, `txn_aborted_total`, `txn_duration_seconds`,
`consumer_lag{topic,partition}`, `state_keys{store}`, `restore_duration_seconds`.

Logs are JSON (`tracing`). They carry topic, partition, offset, counts and durations, never record
keys or values, so no VIN or location reaches a log.

## Packaging

- `Dockerfile`: pinned `rust:1.98.1-slim-bookworm` and `debian:bookworm-slim` (by digest), cargo-chef
  for dependency caching, `--build-arg JOB=<job>` selects the binary, librdkafka built from source
  (`cmake-build`, no SSL or SASL), runtime user 10001.
- `charts/telemetry-job`: one-replica StatefulSet per job. `TRANSACTIONAL_ID` and `GROUP_INSTANCE_ID`
  are `<prefix>-<pod name>`, which is stable across restarts. Headless Service, optional
  ServiceMonitor, PDB with `maxUnavailable: 1` (with one replica, `minAvailable: 1` would block every
  node drain), probes on `/healthz` and `/readyz`, read-only root filesystem, all capabilities dropped.

## CI

`build` (cargo build, image push), `test` (cargo test, helm lint and template), `lint` (fmt, clippy,
cargo audit), `parity` (telemetry-parity at the SHA in `contracts/SOURCE`, sample fixture, `--chaos`,
`--chaos-mode failpoint`, privacy manifest). The parity job runs a second image tag that only sets
`BATCH_MAX_RECORDS=5`, so the 43-record sample spans several transactions and the failpoint crash
lands after some of them have committed.

## Dependency notes

- `rdkafka 0.38.0` with `rdkafka-sys 4.9.0` (librdkafka 2.10.0). `rdkafka-sys 4.10` (librdkafka
  2.12.1) includes `curl/curl.h` even when curl is disabled, so a CMake build without curl fails.
- `tokio 1.53.3` and `anyhow 1.0.104` fix RUSTSEC-2026-0386 and RUSTSEC-2026-0190.

## Known limits

- `OUTPUT_PATH` (Parquet) is not implemented; the runtime rejects it at startup.
- `Job::end_batch` sees only the partitions assigned to this process, so a watermark kept by a job is
  per process. That matches Spark's global watermark with one replica, the chart default.
- One process per `transactional.id`. Scaling out means more StatefulSet replicas, each with its own
  id from the pod name, sharing the consumer group.
