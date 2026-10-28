# battery-health-aggregator semantics

Semantics of `telemetry-spark-jobs/jobs/battery-health-aggregator` (`BatteryHealthAggregator.scala`,
`Buckets.scala`), read from the code at `TELEMETRY_SPARK_JOBS_SHA` in telemetry-parity `deps.lock`
(identical to `main` for this job and `common/`). The shared modules it calls are `JobArgs`, `Spark`,
`KafkaIO.read`, `Json.parse` and `Schemas.BatteryHealth`; it does not call `GeoUtils` or `Ids`.

The port is not implemented yet. It needs a Parquet sink in `telemetry-core` first (see
"Port prerequisite" at the end).

## Topics and keys

| | Topic or path | Key | Partitions | Partitioner |
|---|---|---|---|---|
| Input | `vehicle.battery.health.v1` | `vin` | 6 | murmur2 (Java) |
| Output | Parquet at `s3a://telemetry-lake/battery_health_hourly` (`battery.health.hourly`), Hive-partitioned by `date` | business key `(vin, window_start)` | none | none |
| State (legacy) | Spark state store in the checkpoint, grouped by `vin` | `vin` | 6 shuffle partitions (`spark.sql.shuffle.partitions`) | Spark hash partitioning |

- The job reads `startingOffsets=earliest`, parses the value with `Json.parse(Schemas.BatteryHealth)`
  (timestamp format `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`, session time zone UTC), and groups by `vin`.
- Output rows carry `vin`, `date`, `window_start`, `window_end`, `sample_count`, `soh_pct_min`,
  `soh_pct_avg`, `cell_voltage_delta_mv_min`, `cell_voltage_delta_mv_max`, `pack_temp_c_max`.
  `date` is `to_date(window_start)` in UTC and exists only as the `date=YYYY-MM-DD` directory, not as
  a column in the files. Timestamps are `TIMESTAMP(MILLIS, isAdjustedToUTC=true)`
  (`spark.sql.parquet.outputTimestampType=TIMESTAMP_MILLIS`), decimals are `DECIMAL(5,2)` and
  `DECIMAL(4,1)`, counts and deltas are `INT32`, as `x-parquet-type` declares.
- Output has no Kafka key or partition, so harness checks C (ordering) and D (partition placement)
  are N/A for this job. Files are written one per non-empty shuffle partition per micro-batch.

## Ordering rule

1. Every parsed record is passed to the VIN's group, heartbeats included.
2. Only `event_type == "battery_sample"` records are aggregated. Heartbeats and any other or null
   `event_type` only advance the VIN's `maxSeenMs`.
3. A sample is added to pending only if `event_time >= emittedUntilMs` and its `event_id` is not
   already pending. Within one micro-batch, the first record per `event_id` in arrival order is kept.
4. Samples are bucketed by UTC hour: `window_start = floorDiv(event_time_ms, 3600000) * 3600000`,
   `window_end = window_start + 1 h`. A sample at exactly `hh:00:00.000` belongs to the new hour.
5. Aggregates per emitted hour, all order-independent:
   - `sample_count`: distinct pending `event_id`s.
   - `soh_pct_min`: min of `BigDecimal(soh_pct.toString)`, `setScale(2)` (no rounding mode: an input
     with more than 2 decimals throws and fails the job).
   - `soh_pct_avg`: exact `BigDecimal` sum of `soh_pct.toString` values divided by the count
     (`MathContext.DECIMAL128`), then `setScale(2, HALF_UP)`. For inputs with 2 decimals this equals
     the schema's integer-hundredths rule: a tie is an exact finite decimal and a non-tie is at least
     `1/(2n)` hundredths from one. Example from the edge fixture: 96.5 and 96.49 give 96.50.
   - `cell_voltage_delta_mv_min` and `_max`: min and max of `cell_voltage_delta_mv.toInt`.
   - `pack_temp_c_max`: max of `BigDecimal(pack_temp_c.toString)`, `setScale(1)` (throws like
     `soh_pct_min`).
   - Then `cast` to `decimal(5,2)` and `decimal(4,1)`. The schema ranges fit, so the cast never
     overflows.
6. Hours emitted together by one call are in `window_start` order; Parquet does not preserve it.

Within one micro-batch the result does not depend on arrival order: the VIN's whole batch is passed to
`Buckets.advance` at once, so `maxSeen` covers every event in the batch before anything is emitted.
Across micro-batches it can (see "Disagreements").

## State model and emission

- Per VIN, `BucketState(pending, maxSeenMs, emittedUntilMs)`: pending samples (full `BatterySample`
  with `vin`, sorted by `(event_time, event_id)`), the highest `event_time` seen for the VIN (all event
  types, but not rows dropped by the watermark), and the end of the last emitted hour. It starts at
  `(Nil, Long.MinValue, Long.MinValue)`, is always updated and is never removed.
- Spark watermark (`.withWatermark("event_time", "10 minutes")`), across all VINs: the highest
  `event_time` in the input up to the end of the previous micro-batch minus 10 minutes. It starts at
  0, never decreases, and moves only between micro-batches.
- A row whose `event_time` is at or before the watermark is dropped before `flatMapGroupsWithState`
  (`EventTimeTimeout`), heartbeats included, and does not move `maxSeenMs`.
- Emission per VIN, when the VIN has rows in the batch: `horizon = max(maxSeenMs, watermark)`; every
  pending hour with `window_end + 10 min <= horizon` is emitted once. So a VIN's own later event
  (often its heartbeat) closes its hours as soon as it arrives.
- Emission across VINs, when the VIN has no rows in the batch: the event-time timeout is the earliest
  open hour's `window_end + 10 min - 1 ms`. It fires once the watermark is past it, which is
  `watermark >= window_end + 10 min`, and runs the same rule with no events.
- `Trigger.AvailableNow`: every micro-batch of available data, then one final no-data batch with the
  advanced watermark, which fires timeouts. There is no end-of-input flush: an hour whose
  `window_end + 10 min` is after both the VIN's `maxSeenMs` and the final watermark stays in state
  and is not written.

Confirmed by running the legacy jar (spark-runner image at the `deps.lock` SHA) twice with
`AvailableNow` on one checkpoint, day 2026-10-01, VINs `SYNTIMING{A..E}0000001`:

| Run | Input (arrival order) | Spark batch | Watermark | Written |
|---|---|---|---|---|
| 1 | A 00:05; B 00:05, 00:30, 01:30, 02:05 | 0 (data) | 0 | B 00:00 (2 samples) |
| 1 | | 1 (no data) | 01:55 | A 00:00 (1): timeout, A sent nothing after 00:05 |
| 2 | A 01:20; B 01:58; A 00:58; D 01:55:00.000; E 01:55:00.001; C 03:00 | 2 (data) | 01:55 | nothing; 3 rows dropped by the watermark (A 01:20, A 00:58, D at exactly the watermark) |
| 2 | | 3 (no data) | 02:50 | B 01:00 (2: 01:30, 01:58), E 01:00 (1): timeouts |

C 03:00 was never written. This is the expected output for the port's timing test.

## Where duplicates are removed

| Where | What |
|---|---|
| Job | `fresh` keeps one record per `event_id` within the batch and skips ids already pending; samples before `emittedUntilMs` are skipped, so a duplicate of an emitted hour's sample is dropped. Generator duplicates are exact copies, so which copy wins does not matter. |
| Sink | Spark's file sink records each batch's files in `_spark_metadata`; a replayed batch after a failure writes files under new names and only the committed batch's files are listed in the log. Readers that list `*.parquet` directly (the harness does) can see files of a failed attempt. |
| Downstream | Nothing. `warranty-analytics` is not in the repo; `downstream-consumers/` has no battery consumer. |

## Sensitive fields

| Field | Tag | Transformation |
|---|---|---|
| `vin` | vehicle-id | Copied unchanged from the input into the output row (`ss.head.vin`), the group key, and every pending sample in state. |

No other input or output field has `x-privacy` other than `none`. No location fields, no hashing or
coarsening. The job does not log record values. `topics.yaml` lists only `vin` for both topics, which
matches the code.

## Downstream consumers

`warranty-analytics` (from `topics.yaml` and the schema `x-consumers`), not in the repo. The Python
consumers in `downstream-consumers/` read `charging.sessions.v1` and `service.dtc.alerts.v1` only.

## Disagreements between the legacy code and the schema descriptions

None is privacy-relevant. The port follows the code.

| Schema says | Code does | Depends on micro-batch boundaries or arrival order? |
|---|---|---|
| "Jobs must allow at least 300 seconds of lateness and drop nothing." | Drops rows at or before the global 10-minute watermark and samples before the VIN's `emittedUntilMs`. | Yes in general: the watermark moves per batch, across VINs and partitions. Not for generated data: in arrival order the global disorder is at most 299.8 s (full fixture), under the 10-minute delay, so nothing is dropped. A model of the code drops nothing on sample, edge and full in one batch or in batches of 5, 500 or 3000 records; with partition 0 one batch ahead of the others it drops 19028 of 21806 full-fixture rows. |
| "Ignore arrival order." | Within one batch the result is order-independent; across batches a sample can be dropped or emitted late depending on what arrived before it. | Yes, across batches (same cause as above). Generated data has no same-VIN disorder. |
| "One row per (vin, window_start) with at least one sample." | Only closed hours are written; no end-of-input flush. | Yes: whether an hour closes depends on later events of the VIN or on other VINs moving the watermark. Not for generated data: every VIN's last record is a heartbeat 2 hours after its last sample, which closes all its hours (sample 63, edge 3, full 5220 rows, all in the data batch). |
| `soh_pct_avg`: "integer arithmetic, never binary floating point". | `BigDecimal` from `Double.toString`, `DECIMAL128` division, then HALF_UP. | No. Same result for inputs with at most 2 decimals. |
| Input `x-decimals` 2 and 1. | `setScale` without a rounding mode throws on more decimals; a null domain field on a `battery_sample` throws on `Option.get`. | No. The schema forbids both; the job fails instead of writing. |

## Legacy behaviors kept as they are

- Heartbeats and non-`battery_sample` events close hours through `maxSeenMs` but are never counted.
- Rows at or before the watermark are dropped, heartbeats included, even when they would not be late
  for their own VIN.
- Samples before `emittedUntilMs` are dropped, even if they are not duplicates.
- Hours still open at end of input are not written.
- Duplicate `event_id`s with different content: the first in the batch wins. The generator never
  produces this.

## Port notes

- Reproduce timing with `Job::end_batch` and a separate watermark store, as charging-session-builder
  does: drop at or before the watermark, emit on `max(maxSeen, watermark)`, fire timeouts for VINs
  without rows in the batch, then advance the watermark.
- Legacy passes a VIN's whole batch to `advance` at once; per-record processing would differ only for
  same-VIN disorder over 10 minutes, which the contract (300 s) and the fixtures rule out.

## Port prerequisite

`telemetry-core` rejects `OUTPUT_PATH` at startup (`docs/PORTING.md`, "Not supported yet"). The port
needs a Parquet sink on S3 with no duplicate `(vin, window_start)` rows after SIGKILL and restart, and
no stray files, because the harness reads every `*.parquet` under the prefix. That is a new
`telemetry-core` capability and needs its own PR before this job can be ported.
