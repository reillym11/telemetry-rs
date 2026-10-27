# charging-session-builder semantics

Port of `telemetry-spark-jobs/jobs/charging-session-builder` (`ChargingSessionBuilder.scala`,
`Sessions.scala`, `SessionsSpec.scala`) and the shared code it calls (`KafkaIO`, `Json`, `Ids`,
`Schemas`, `JobArgs`). The effective output and the batch in which each session is emitted are the
legacy job's. The delivery guarantee is stronger.

## Topics and keys

| | Topic | Key | Partitions | Partitioner |
|---|---|---|---|---|
| Input | `vehicle.charging.events.v1` | `vin` | 6 | murmur2 (Java) |
| Output | `charging.sessions.v1` (delete) | `vin` | 6 | murmur2 (Java), `partitioner=murmur2_random` |
| State | `<STATE_TOPIC_PREFIX>.sessions` (compacted) | `vin` as JSON | same as input | the VIN's input partition |
| State | `<STATE_TOPIC_PREFIX>.batch` (compacted) | `"vin\|slot"` as JSON | same as input | the record's input partition |
| State | `<STATE_TOPIC_PREFIX>.watermark` (compacted) | `"watermark"` as JSON | same as input | the partition of the first record that created it |

Downstream consumers: `service` (`downstream-consumers/service_charging_consumer.py`) and
`energy-insights` (from `topics.yaml`).

## Session rule (per VIN)

The legacy `Sessions.advance`, `segment` and `build`, ported line for line in `src/reducer.rs`.

1. State per VIN: `pending` events, `max_seen` (highest event time seen, heartbeats included),
   `emitted_until` (last event time of the last emitted session) and the event-time timeout.
2. New rows of a VIN: every row raises `max_seen`. Heartbeats never become pending. A non-heartbeat
   event becomes pending only if its `event_time > emitted_until` and its `event_id` is not already
   pending (one row per `event_id`, the first to arrive).
3. Pending events are sorted by `(event_time, producer_seq)` and cut into runs: a `charge_start`
   opens a run (closing an open one as not stopped); any other event joins the open run if it is at
   most 30 minutes (1,800,000 ms) after the run's last event, and a `charge_stop` closes the run as
   stopped; an event more than 30 minutes after the open run's last event closes that run and is a
   stray; an event with no open run is a stray. A null `event_type` counts as a progress event.
4. `horizon = max(max_seen, watermark)`. A run is emitted when `horizon - last > 30 min`. A stray is
   dropped when `horizon - event_time > 30 min`. Everything else stays pending.
5. The timeout is the earliest pending run end or stray, plus 30 minutes. No timeout when nothing is
   pending. VIN state is never deleted.

Output fields: `session_id` = UUIDv5(namespace `f5179d15-0fd7-5b5f-bf53-0852eb22d3d5`,
`vin + "|" + lowercase(charge_start event_id)`); `vin`, `fleet`, `market`, `connector_type`,
`charger_type`, `charger_lat`, `charger_lon`, `start_soc_pct` and `start_time` from the run's first
event (always the `charge_start`); `end_time`, `energy_added_kwh` and `end_soc_pct` from its last
event; `peak_power_kw` the max non-null `power_kw`; `event_count` the distinct `event_id`s;
`close_reason` `charge_stop` if a `charge_stop` closed the run, else `gap_timeout`. Timestamps are
RFC 3339 UTC with three fractional digits and `Z`. Fields are written in the legacy order, nulls
explicit (`ignoreNullFields=false`).

## When output is emitted

The legacy query is `withWatermark("event_time", "10 minutes")`,
`flatMapGroupsWithState(Append, EventTimeTimeout)` and `Trigger.AvailableNow`. One legacy run is one
data micro-batch (all input available at start, no `maxOffsetsPerTrigger`) and then a final no-data
batch, because the watermark advanced. The port does the same with telemetry-core batches: every
record up to the end offsets read when the batch started, then `Job::end_batch`.

| Legacy (one AvailableNow run) | Port (one telemetry-core batch) |
|---|---|
| Watermark `W` from earlier runs (initially 0). Rows with `event_time <= W` are dropped before grouping | `process` drops rows with `event_time <= W` (stored watermark) |
| Every row, dropped or not, feeds the run's max event time | `process` records the batch's max event time in the `watermark` store |
| Each VIN with rows gets all of its rows at once: `advance(state, rows, W)` | `process` buffers rows in the `batch` store in arrival order; `end_batch` calls `advance(state, rows, W)` per VIN |
| In the same batch, VINs without rows whose timeout `< W` (strict) get `advance(state, [], W)` | `end_batch` does the same |
| Watermark becomes `max(W, max event time - 10 min)` | `end_batch` stores the same |
| Final no-data batch: VINs whose timeout `< new W` get `advance(state, [], new W)` | `end_batch` does the same, in the same transaction |

So a session is emitted in the batch where its VIN's own later events or the global watermark (other
VINs' events) pass its last event by more than 30 minutes. It does not wait for the VIN's next record,
and a `charge_stop` alone does not emit. Emission is across keys (the watermark is global) and the
timeout check is per key.

This was checked against the legacy jar: `tests/timing/legacy_run.sh` runs `run1..run3.jsonl` as three
AvailableNow runs sharing one checkpoint (spark-runner image at `TELEMETRY_SPARK_JOBS_SHA`). The
results are in `legacy-runN.jsonl`, and `tests/fixture.rs` requires the same sessions from the same
batches. They show:

- A VIN that stops sending is closed by the final no-data batch once other VINs move the watermark past
  its last event plus 30 minutes.
- A timeout equal to the watermark does not fire.
- A row exactly at the watermark is dropped, and one 1 ms after it is kept.

A crash inside a batch loses nothing. Buffered rows and the batch's max event time are committed
state, and the watermark only moves in `end_batch`. The restarted process's first batch ends with the
same `advance` calls, as if one batch had run.

## Duplicates

- In the job, as in legacy: duplicates within a batch by `event_id` (first arrival kept), duplicates
  of pending events by `event_id`, and duplicates of emitted events by `event_time <= emitted_until`.
- At the sink: legacy is at least once (a Kafka sink can repeat a micro-batch), so
  `service_charging_consumer.py` dedups by `session_id`. The port writes output, state and offsets in
  one transaction, so `read_committed` readers see each session once. That consumer uses the default
  `read_uncommitted`, so it can see records from aborted transactions. These are identical copies of
  sessions committed later (the job is deterministic), and its `session_id` dedup removes them.

## Sensitive fields

- `vin` (vehicle-id): copied into the output value and key, the `sessions` and `batch` values, and the
  state keys, and hashed into `session_id`. Never logged.
- `session_id` (vehicle-id): `Ids.sessionId`, above. The `event_id` is lowercased; the VIN is not.
- `charger_lat`, `charger_lon` (precise-location): copied unchanged from the session's `charge_start`
  at input precision (5 decimals). The job never calls `GeoUtils.coarsen` or rounds them. They sit in
  the `batch` and `sessions` state values while their events are pending. The legacy job kept the same
  pending events in its Spark checkpoint. Never logged.

## State model

- `sessions: KeyedState<String, SessionState>`: legacy `SessionState` plus the timeout. It is created
  in `process` on the VIN's first row, so it lives on the VIN's partition.
- `batch: KeyedState<String, Slot>`: the open batch's rows in slots `vin|0`, `vin|1`, ... in arrival
  order. `end_batch` clears each slot to `{vin, event: null}` instead of writing a tombstone, because
  telemetry-parity reads every state record's value. Slots are reused, so there are at most as many
  per VIN as its most rows in one batch.
- `watermark: KeyedState<String, Watermark>`: `current_ms` and the open batch's `batch_max_ms`. One
  key, so one global watermark per process, like Spark's with one replica (docs/DESIGN.md, Known
  limits).
- On assignment the stores are restored from their changelogs before any record is processed.

## Delivery guarantee

| | Legacy (Spark) | Rust |
|---|---|---|
| Output | At least once | Exactly once for `read_committed` consumers |
| State | Spark checkpoint directory | Kafka changelog topics |
| Batch boundaries | One micro-batch per scheduled run | One batch per telemetry-core batch (records available when it starts) |

## Behavioral differences

None in effective output or emission timing for the same batch boundaries. The harness compares
`session_id` sets and values (check A). `tests/fixture.rs` covers the timing, which the harness does
not compare.

## Legacy behaviors kept as they are

Each item notes whether the output can depend on micro-batch boundaries or arrival order.

- **Other VINs close a session.** The schema says a gap closes a session when the VIN's next event is
  more than 30 minutes later. Legacy also closes it when the global watermark passes the last event
  by 30 minutes, with no event from that VIN. Batch-dependent: yes, in when the session is emitted.
  Its content does not change for input within the schema's 300 s lateness.
- **Late rows are dropped.** The schema says to drop nothing. Legacy drops rows at or before the
  watermark (max event time minus 10 minutes, as of the previous batch). Batch-dependent and
  order-dependent: yes, but only for rows more than 10 minutes behind the topic's max event time,
  which the schema's 300 s bound and the generator rule out.
- **A new `charge_start` within 30 minutes** closes the open run with `close_reason = gap_timeout`,
  although no gap passed. Not batch-dependent.
- **Late events of an emitted session.** Events at or before `emitted_until` are dropped. An event
  that would have extended a run, but arrives after the run was emitted, becomes a stray. Batch- and
  order-dependent: yes, but a run is emitted only once the horizon is more than 30 minutes past its
  last event. For input within 300 s lateness, with sessions more than 30 minutes plus the lateness
  apart (the generator's rule), no such event exists. The property test checks random arrival orders
  and batch splits.
- **Equal `(event_time, producer_seq)` for different `event_id`s.** Legacy order then follows Scala
  `HashMap` iteration; the port orders by arrival. `producer_seq` is strictly increasing per VIN, so
  valid input never has this.
- **Records outside the schema.** A null `producer_seq` or `event_time`, or unparsable JSON, fails the
  legacy query on its `ChargingEvent` decode. telemetry-core counts and skips undecodable records
  (`decode_errors_total`). A session event with a null `energy_added_kwh`, `soc_pct`, `charger_lat`,
  `charger_lon` or all-null `power_kw` fails the legacy `build` (`Option.get`, `max`). The port fails
  the batch the same way, so the job exits 1 and retries. The schema makes all of these fields
  required on non-heartbeat events, so valid input reaches neither path.
