# battery-health-aggregator semantics

Port of `telemetry-spark-jobs/jobs/battery-health-aggregator` (`BatteryHealthAggregator.scala`,
`Buckets.scala`), read at telemetry-spark-jobs `169768600588` (the `TELEMETRY_SPARK_JOBS_SHA` the pinned
harness runs; the job and `common/` are unchanged on main). This file describes the legacy behavior the
port must reproduce. The port is blocked: the output is a Parquet sink, and telemetry-core rejects
`OUTPUT_PATH` at startup (see "Port status").

## Topics and keys

| | Topic | Key | Partitions | Partitioner |
|---|---|---|---|---|
| Input | `vehicle.battery.health.v1` | `vin` | 6 | murmur2 (Java) |
| Output | `battery.health.hourly`: Parquet at `s3a://telemetry-lake/battery_health_hourly` (harness: `OUTPUT_PATH`), partitioned by `date` | none (business key `(vin, window_start)`) | none | none |
| State (planned) | `<STATE_TOPIC_PREFIX>.buckets` (compacted) | `"vin"` as JSON | same as input | the input record's partition, set explicitly |
| State (planned) | `<STATE_TOPIC_PREFIX>.watermark` (compacted) | `"watermark"` as JSON | same as input | the lowest assigned partition |

The output has no Kafka key or partition, so harness checks C and D are N/A. Parquet files are
partitioned by `date` only; `vin` is never part of an object path.

Downstream consumers: `warranty-analytics` (from `topics.yaml` and the schema `x-consumers`). It is not
in `telemetry-spark-jobs/downstream-consumers/`, which has consumers for `charging.sessions.v1` and
`service.dtc.alerts.v1` only. Nothing in the repos reads `battery.health.hourly`.

## Ordering rule

1. Input JSON is parsed with `Schemas.BatteryHealth` (`Json.parse`, `from_json`, timestamps
   `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`). Only `event_id`, `vin`, `event_time`, `event_type`, `soh_pct`,
   `cell_voltage_delta_mv` and `pack_temp_c` are used.
2. Records are grouped by `vin`. Within a micro-batch, all of a VIN's records go to one `Buckets.advance`
   call, in arrival order.
3. Only `event_type == "battery_sample"` records are aggregated. Heartbeats, null and unknown types are
   not aggregated but do move the VIN's `maxSeenMs`, so they trigger emission.
4. A sample belongs to the UTC hour `floorDiv(event_time_ms, 3600000) * 3600000`, start inclusive,
   end exclusive (a sample at 01:00:00.000 is in the 01:00 hour).
5. One row per `(vin, hour)` with at least one sample:
   - `vin`; `window_start`; `window_end = window_start + 1 h`; `date = to_date(window_start)` in UTC.
   - `sample_count`: number of distinct `event_id`s accepted into the hour.
   - `soh_pct_min`: min of `BigDecimal(soh_pct.toString)`, `setScale(2)`, cast `decimal(5,2)`.
   - `soh_pct_avg`: `sum / sample_count` in Scala `BigDecimal` (DECIMAL128 context, 34 digits) of
     `BigDecimal(soh_pct.toString)`, then `setScale(2, HALF_UP)`, cast `decimal(5,2)`. Exact for
     inputs with 2 decimals. Example: 95.01 and 95.02 give 95.02 (`BucketsSpec`).
   - `cell_voltage_delta_mv_min` / `_max`: min / max of `cell_voltage_delta_mv.toInt` (INT32).
   - `pack_temp_c_max`: max of `BigDecimal(pack_temp_c.toString)`, `setScale(1)`, cast `decimal(4,1)`.
   - Parquet types: `vin` STRING, `window_start`/`window_end` TIMESTAMP(MILLIS, UTC), `sample_count`
     and the deltas INT32, decimals as declared, `date` the Hive partition directory.
6. Rows emitted together for a VIN are in `window_start` order.

The aggregates are order independent (min, max, exact sum). Which samples are accepted can depend on
arrival order and micro-batch boundaries only beyond the 10 minute grace (see below), never for input
within the contract's 300 seconds of lateness.

## State model and emission

- Per VIN (`GroupState[BucketState]`): `pending` (accepted samples of hours not yet emitted, each with
  `vin`), `maxSeenMs` (highest `event_time` of any record of the VIN that passed the watermark filter,
  heartbeats included) and `emittedUntilMs` (end of the last emitted hour).
- Spark watermark: `.withWatermark("event_time", "10 minutes")`, the highest `event_time` across all
  VINs (all records, heartbeats and late rows included) minus 10 minutes, as of the end of the previous
  micro-batch. It is global across keys, starts at 0, and never decreases. Records with `event_time`
  at or before it are dropped before `advance` and do not move `maxSeenMs`.
- `advance(state, events, watermark)`: `horizon = max(maxSeenMs, watermark)`. Every hour with
  `window_end + 10 min <= horizon` is emitted, once. The rest stay pending.
- Per key: a VIN's hour is emitted in the micro-batch in which that VIN's own records (a sample or its
  heartbeat) reach `window_end + 10 min`.
- Across keys (timeout): `GroupStateTimeout.EventTimeTimeout` with timeout `earliest open hour end
  + 10 min - 1 ms`. It fires once the watermark is past it, i.e. watermark `>= window_end + 10 min`, and
  then `advance` runs with no events and the watermark as horizon, emitting every hour that is due. The
  watermark moves only between batches, so a timeout fires in the next batch.
- End of input: `Trigger.AvailableNow` runs the data batch(es), then a final no-data batch with the
  advanced watermark, which fires the timeouts. A VIN that stops sending without a heartbeat has its
  hours emitted there, once event time across all VINs is 10 minutes past `window_end + 10 min`.
- On the generated fixtures every VIN's heartbeat is 2 hours after its last record, so all its hours are
  emitted per key. The timeout path matters for VINs without a heartbeat and for multi-batch runs.
- State never expires: `state.update` is always called, nothing is removed.

Port plan (no new behavior): keep pending samples and the watermark in `KeyedState` stores, buffer a
VIN's records of the current batch and run `advance` per VIN in `Job::end_batch`, advance the
watermark there, then run `advance` with no events for every VIN with open hours. This reproduces
"one `advance` per VIN per batch" and the timeouts, as charging-session-builder does with `end_batch`.

### Timing confirmed on the legacy jar

Two `AvailableNow` runs of the spark-runner jar (`telemetry-parity/spark-runner:169768600588`) on one
checkpoint, 6 partitions, custom input, times 2026-10-01 UTC. Output grouped by `_spark_metadata` batch.

| Run | Input (arrival order per VIN) | Batch | Rows emitted |
|---|---|---|---|
| 1 | A: 00:05, 00:15, then silent, no heartbeat. D: 00:50, 01:20, 00:55. B: 00:10 ... 02:30 every 10 to 30 min | 0 (watermark 0) | B 00:00 (n=2), B 01:00 (n=3), D 00:00 (n=2: 00:50 and 00:55) |
| 1 | none (no-data batch, watermark 02:20) | 1 | A 00:00 (n=2), D 01:00 (n=1): event-time timeouts |
| 2 | C: 01:30, 02:20:00.000, 02:20:00.001. B: 02:15. A: 00:20. Heartbeats B and C at 05:00 | 2 (watermark 02:20) | B 02:00 (n=2: 02:15 dropped), C 02:00 (n=1: only 02:20:00.001 kept) |
| 2 | none (watermark 04:50) | 3 | nothing |

This shows: per-key emission from the VIN's own records; emission of silent VINs only through the
global watermark in the next batch; one `advance` per VIN per batch (D's 00:55, 25 minutes late, still
joined its hour because it arrived in the same batch as 01:20); rows at or before the watermark dropped,
the boundary value included.

## Where duplicates are removed

| Where | What |
|---|---|
| Job | A sample is accepted only if `event_time >= emittedUntilMs` and its `event_id` is not already pending; within one call, duplicates by `event_id` keep the first in arrival order. A duplicate of an emitted hour's sample is dropped by time. So `sample_count` counts distinct ids. |
| Sink | Spark's file sink commits each batch's files to `_spark_metadata`; readers that use the log see each row once. Files of a failed batch stay in the directory. The harness reads every `.parquet` file under the prefix and does not use the log. |
| Downstream | None in the repos (`warranty-analytics` is not present). |

## Delivery guarantee

| | Legacy (Spark) | Rust (planned) |
|---|---|---|
| Output | Exactly once for readers of `_spark_metadata`; orphan files possible after a crash | Must leave no duplicate `(vin, window_start)` row in any `.parquet` file under `OUTPUT_PATH`, including after SIGKILL (RUST_JOB_CONTRACT item 5) |
| State | Spark checkpoint directory | Kafka changelog topics |

## Sensitive fields

| Field | Tag | Transformation |
|---|---|---|
| `vin` | vehicle-id | Copied unchanged from input into the output row and into state (group key and each pending sample). Not in any object path. |

No other input or output field is sensitive. The job calls no shared helper that transforms data
(`GeoUtils` and `Ids` are not used); `JobArgs`, `Spark`, `KafkaIO.read` and `Json.parse` only read
and parse. No record value is logged.

## Behavioral differences

None planned, in content or emission timing. The harness compares rows by `(vin, window_start)`
(check A), not timing; a timing test must compare against the legacy run above.

## Legacy behaviors kept as they are

- **Heartbeats** are never aggregated but move `maxSeenMs` and trigger emission.
- **Hour boundary** is start inclusive: a sample at exactly `HH:00:00.000` is in hour `HH`.
- **Grace is 10 minutes after the hour end, inclusive**, on the VIN's own `maxSeenMs` or the watermark.
- **Late samples before `emittedUntilMs`** are dropped, even if not duplicates.
- **Records at or before the watermark** are dropped, heartbeats included.
- **One `advance` per VIN per batch**: a sample later than the grace still joins its hour if it
  arrives in the same batch as the record that closes the hour.
- **Same `event_id`, different content**: the first pending copy wins, so content depends on arrival
  order. The generator only produces exact copies.
- **Null or extra-precision values**: a `battery_sample` with null `soh_pct`, `cell_voltage_delta_mv` or
  `pack_temp_c` fails the job (`Option.get`); `soh_pct` with more than 2 decimals or `pack_temp_c` with
  more than 1 fails it (`setScale` without rounding). `cell_voltage_delta_mv` above `Int.MaxValue`
  wraps (`toInt`). The schemas forbid or never produce these.

## Disagreements between the legacy code and the schema descriptions

None is privacy-relevant. The port follows the code.

| Schema says | Code does | Can output depend on batch boundaries or arrival order? |
|---|---|---|
| "deduplicate by event_id first, ignore arrival order ... drop nothing" | Drops samples before `emittedUntilMs` and records at or before the global watermark; same-id copies keep the first arrival | Yes, beyond the 10 minute grace: the watermark moves per batch and is global across VINs; `emittedUntilMs` depends on per-VIN arrival order. No within 300 s lateness: a record arrives at most 300 s after a later one, so it is after the watermark and after `emittedUntilMs`. |
| `soh_pct_avg` = HALF_UP of integer hundredths / count | DECIMAL128 division of exact decimals, then HALF_UP | No (sum is exact). Agrees for 2-decimal input; ties are exact in 34 digits. |
| `soh_pct` `x-decimals: 2`, `pack_temp_c` `x-decimals: 1` | More decimals fail the job | No; schema-invalid input only. |
| `sample_count` = distinct `event_id`s in the window | Distinct accepted `event_id`s | Only if a duplicate arrives after its hour was emitted (dropped either way, so they agree). |
| Input heartbeat is each VIN's last record, at last event + 2 h | Relies on it only for per-key emission; without it, emission waits for the global watermark | Timing only (next batch), not content, within 300 s lateness. |

## Port status

Blocked on a telemetry-core capability. `OUTPUT_PATH` is rejected at startup
(`crates/telemetry-core/src/processor.rs`, docs/DESIGN.md "Known limits", docs/PORTING.md "Not supported
yet"), and the workspace has no Parquet or object-store dependency. A Parquet sink with an exactly-once
design is a separate telemetry-core change, not part of this port.
