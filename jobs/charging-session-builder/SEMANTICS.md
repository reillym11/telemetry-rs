# charging-session-builder semantics

Port of `telemetry-spark-jobs/jobs/charging-session-builder` (`ChargingSessionBuilder.scala`,
`Sessions.scala`, `Ids.scala`). `src/reducer.rs` is a line-by-line port of `Sessions.advance`,
`segment` and `build`. The effective output is identical to the legacy job's; the delivery guarantee is
stronger.

## Topics and keys

| | Topic | Key | Partitions | Partitioner |
|---|---|---|---|---|
| Input | `vehicle.charging.events.v1` | `vin` | 6 | murmur2 (Java) |
| Output | `charging.sessions.v1` | `vin` | 6 | murmur2 (Java), `partitioner=murmur2_random` |
| State | `<STATE_TOPIC_PREFIX>.sessions` (compacted) | `"vin"` as JSON | same as input | the input record's partition, set explicitly |
| State | `<STATE_TOPIC_PREFIX>.watermark` (compacted) | `"watermark"` as JSON | same as input | the lowest assigned partition |

Every session of a VIN goes to the VIN's partition. Downstream consumers: `energy-insights` and
`service` (from `topics.yaml`). `service_charging_consumer.py` deduplicates by `session_id` and prints
`session_id`, `vin`, `close_reason` and `energy_added_kwh`; `energy-insights` is not in the repo.

## Ordering rule

1. Every record is decoded, heartbeats included. A record that does not parse is skipped (its offset
   still commits).
2. Events of a VIN are ordered by `(event_time, producer_seq)` (epoch milliseconds, sequence), a stable
   sort over pending events in arrival order.
3. A session (run) starts at a `charge_start`. A following event joins the open run if it is at most
   30 minutes (`GAP_MS`, inclusive) after the run's last event. A `charge_stop` that joins closes the run
   with `close_reason = charge_stop`. A new `charge_start`, or an event more than 30 minutes after the
   run's last event, closes the open run with `close_reason = gap_timeout`.
4. Non-heartbeat events outside any run are stray events. They are never emitted; they stay pending
   while they are within 30 minutes of the horizon, so a late `charge_start` can still claim them.
5. Output per session: `session_id` (below); `fleet`, `market`, `connector_type`, `charger_type`,
   `start_time`, `start_soc_pct`, `charger_lat`, `charger_lon` from the first event; `end_time`,
   `energy_added_kwh`, `end_soc_pct` from the last event; `peak_power_kw` the largest non-null
   `power_kw`; `event_count` the number of distinct `event_id`s. Timestamps are RFC 3339 UTC with three
   fractional digits and `Z`. Sessions emitted together are in run order.

For events within the contract's 300 second lateness, the result does not depend on arrival order.
`tests/fixture.rs` checks this with a property test over random arrival orders with up to 300 seconds
of lateness and duplicates.

## State model and emission

- One `KeyedState<String, SessionState>` store named `sessions`, keyed by `vin`. The changelog value
  carries `vin`, `pending` (events not yet emitted, without `vin`), `max_seen_ms` (highest
  `event_time` seen for the VIN, heartbeats and dropped events included) and `emitted_until_ms` (end of
  the last emitted session).
- A second store, `watermark`, holds Spark's event-time watermark (`.withWatermark("event_time",
  "10 minutes")`): the highest `event_time` seen across all VINs minus 10 minutes, as of the end of
  the last batch. It starts at 0, never decreases, and has no sensitive fields.
- A batch is every record up to the input end offsets read when it starts, like a Spark micro-batch
  (`Job::end_batch`, docs/DESIGN.md). A record whose `event_time` is at or before the watermark is
  dropped before `advance` and does not move `max_seen_ms`, as Spark drops late rows before
  `flatMapGroupsWithState` when it uses `EventTimeTimeout`.
- Each record runs `advance` with that one event and the watermark, so the horizon is
  `max(max_seen_ms, watermark)`. A run is emitted, once, when `horizon - last event time > 30 minutes`.
  A `charge_stop` alone does not emit.
- At the end of every batch the job advances the watermark, then runs `advance` with no events for
  every VIN. This is Spark's `EventTimeTimeout`: the timeout is the last open event plus 30 minutes and
  fires once the watermark is past it, in the next batch, which `AvailableNow` always runs. So a
  vehicle that charges and goes offline has its session emitted once event time across all vehicles
  is 40 minutes past its last event, without another event from that vehicle. The process exits only
  after the last batch has ended.
- State is written only when it changes. It never expires and nothing is deleted, matching the legacy
  job, which always calls `state.update`.
- On assignment the job restores the store from the changelog (read_committed, beginning to end) before
  it processes a record.

## Where duplicates are removed

| Where | What |
|---|---|
| Job (legacy and Rust) | A non-heartbeat event is added to pending only if its `event_id` is not already pending and its `event_time` is after `emitted_until_ms`. So a duplicate of an emitted session's event is dropped, and `event_count` counts distinct ids. |
| Sink | Legacy: none (Spark's Kafka sink is at least once, a replayed micro-batch can emit a session again). Rust: output, changelog and offsets commit in one Kafka transaction, so `read_committed` readers see each session once. |
| Downstream | `service_charging_consumer.py` keeps the first record per `session_id`. |

## Delivery guarantee

| | Legacy (Spark) | Rust |
|---|---|---|
| Output | At least once: a Kafka sink replays the last micro-batch after a failure | Exactly once for `read_committed` consumers: output, changelog and input offsets commit in one Kafka transaction |
| State | Spark checkpoint directory | Kafka changelog topic |

A `read_uncommitted` consumer can see records of aborted transactions. Downstream consumers should use
`isolation.level=read_committed`.

## Sensitive fields

| Field | Tag | Transformation |
|---|---|---|
| `vin` | vehicle-id | Copied unchanged into the output value, the output key and the state key and value. |
| `session_id` | vehicle-id | UUIDv5, namespace `f5179d15-0fd7-5b5f-bf53-0852eb22d3d5` (`Ids.Namespace`), over the UTF-8 bytes of `vin + "\|" + lowercase(charge_start event_id)` (`Ids.sessionId`). |
| `charger_lat`, `charger_lon` | precise-location | Copied unchanged from the session's `charge_start` event; no rounding. The legacy job does not call `GeoUtils.coarsen`. Each pending event keeps its own coordinates in state, as the legacy Spark state did. |

No sensitive value is logged. The schema declares `x-precision: 5` for both coordinates on input and
output; the job passes the input values through, so they agree.

## Behavioral differences

None, in effective output or emission timing. The harness compares the last record per `session_id`
(check A) and per-key order (C), not timing, so `tests/watermark.rs` checks timing against the legacy
job: the sessions the legacy job emitted for a vehicle that went offline, and the late events it
dropped (two `AvailableNow` runs on one checkpoint).

Batch boundaries are not Spark's exactly: a crash restarts the batch from the committed offsets, and
the watermark is per process (Spark's is global; the chart runs one replica). Neither changes content
for events within the contract's 300 second lateness.

## Legacy behaviors kept as they are

- **Heartbeats** never join a session but advance `max_seen_ms`, so they trigger emission.
- **A `charge_start` within 30 minutes** of an open run closes it as `gap_timeout`, before any gap.
- **Late events at or before `emitted_until_ms`** are dropped, even if they are not duplicates.
- **Events at or before the watermark** are dropped, heartbeats included, even if they would start a
  new session.
- **Events after a `charge_stop`** within 30 minutes do not join the closed run; they are stray.
- **Equal rank, different events.** Two different events with equal `(event_time, producer_seq)` keep
  arrival order. The generator never produces this.
- **Null fields.** A null `event_type` is treated as neither start, stop nor heartbeat. A session whose
  needed fields are null (for example `soc_pct`) fails the job, as Scala's `Option.get` does. The schema
  forbids these nulls.

## Disagreements between the legacy code and the schema descriptions

None is privacy-relevant. The port follows the code.

- **Gap.** The schema says a session closes when "the next event for the VIN (including heartbeat)"
  is more than 30 minutes later. The code segments only non-heartbeat events; heartbeats only move the
  horizon. They agree on generated data.
- **Late data.** The schema says jobs must accept 300 seconds of lateness and drop nothing earlier. The
  code drops events at or before the end of the last emitted session and stray events more than 30
  minutes behind the horizon, and events at or before the 10 minute watermark. Generated sessions are
  more than the gap plus the maximum lateness apart, and lateness is at most 300 seconds, so no
  generated event is dropped.
