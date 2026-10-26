# ota-status-tracker semantics

Reference port of `telemetry-spark-jobs/jobs/ota-status-tracker` (`OtaStatusTracker.scala`). The
effective output is identical to the legacy job's; the delivery guarantee is stronger.

## Topics and keys

| | Topic | Key | Partitions | Partitioner |
|---|---|---|---|---|
| Input | `vehicle.ota.status.v1` | `vin` | 6 | murmur2 (Java) |
| Output | `ota.status.latest.v1` (compacted) | `vin\|campaign_id` | 6 | murmur2 (Java), `partitioner=murmur2_random` |
| State | `<STATE_TOPIC_PREFIX>.latest` (compacted) | `"vin\|campaign_id"` as JSON | same as input | the input record's partition, set explicitly |

The key joins the two values with `|` and skips a null `campaign_id`, as Spark's `concat_ws` does.
The schema forbids a null `campaign_id` on `ota_status` events, so this only matters for invalid input.

Downstream consumers: `ota-orchestrator` and `service` (from `topics.yaml`).

## Ordering rule

1. Records with `event_type` equal to `heartbeat` or null are skipped. Their offsets still commit.
2. State per `vin|campaign_id` is the winning event. An event replaces the winner only if
   `(event_time, producer_seq)` is strictly greater, compared as (epoch milliseconds, sequence).
3. Older late events and duplicates (equal rank) never change state and emit nothing.
4. Every change emits one output record built from the new winner: `vin`, `campaign_id`, `state`,
   `target_version`, `failure_code` (explicit `null` when absent), `last_event_id`,
   `last_event_time` (RFC 3339 UTC, three fractional digits, `Z`).

The result per key does not depend on arrival order, except for two distinct events with equal rank
(see below). `tests/fixture.rs` checks this with a property test over random arrival orders with up
to 300 seconds of lateness and duplicates.

## State model

- One `KeyedState<String, Winner>` store named `latest`. The changelog value carries `vin`,
  `campaign_id`, `event_id`, `event_time_ms`, `producer_seq`, `state`, `target_version` and
  `failure_code`.
- State never expires and nothing is deleted, matching the legacy job (`GroupStateTimeout.NoTimeout`).
- On assignment the job restores the store from the changelog (read_committed, beginning to end) before
  it processes a record.

## Delivery guarantee

| | Legacy (Spark) | Rust |
|---|---|---|
| Output | At least once: a Kafka sink replays the last micro-batch after a failure | Exactly once for `read_committed` consumers: output, changelog and input offsets commit in one Kafka transaction |
| State | Spark checkpoint directory | Kafka changelog topic |
| Intermediate records | One per key per micro-batch | One per state change |

A `read_uncommitted` consumer can see records of aborted transactions. Downstream consumers should use
`isolation.level=read_committed`. Both jobs can emit intermediate records; the contract compares the
last record per key, so the extra records are not a behavioral difference.

## Behavioral differences

None in effective output. The harness compares the last record per key (check A) and per-key order (C).

## Legacy behaviors kept as they are

- **Equal rank, different events.** If two different events share `(event_time, producer_seq)`, the
  first to arrive wins. The generator never produces this. The topic schema says "latest" means the
  highest `producer_seq`; the legacy job ranks by event time first. They agree on all generated data.
- **Watermark.** The legacy job declares a 10 minute watermark, but `flatMapGroupsWithState` with
  `NoTimeout` uses it neither to drop late rows nor to expire state. The port has no watermark.
