# battery-health-aggregator semantics

Port of `telemetry-spark-jobs/jobs/battery-health-aggregator` (`BatteryHealthAggregator.scala`,
`Buckets.scala`). The effective output and the emission rule match the legacy job. The output is
Parquet on S3, written by the telemetry-core Parquet sink (docs/DESIGN.md, Parquet sink).

## Topics and keys

| | Topic / path | Key | Partitions | Partitioner |
|---|---|---|---|---|
| Input | `vehicle.battery.health.v1` | `vin` | 6 | murmur2 (Java) |
| Output | `s3a://telemetry-lake/battery_health_hourly`, Parquet, `date=YYYY-MM-DD/` directories | one row per `(vin, window_start)` | n/a | n/a |
| State | `<STATE_TOPIC_PREFIX>.buckets` (compacted) | `"vin"` as JSON | same as input | the input record's partition, set explicitly |
| State | `<STATE_TOPIC_PREFIX>.watermark` (compacted) | `"watermark"` as JSON | same as input | the first assigned partition |

Columns and physical types are the ones Spark writes: `vin` STRING, `window_start` and `window_end`
INT64 TIMESTAMP(MILLIS, UTC), `sample_count` INT32, `soh_pct_min` and `soh_pct_avg` INT32
DECIMAL(5,2), `cell_voltage_delta_mv_min` and `_max` INT32, `pack_temp_c_max` INT32 DECIMAL(4,1),
Snappy. `date` is `to_date(window_start)` in UTC and appears only as the directory name, as with
`partitionBy("date")`. File names do not contain a VIN.

Downstream consumers: `warranty-analytics` (from `topics.yaml`; not in `downstream-consumers/`).

## Ordering rule

Records are grouped by `vin`. Within a VIN, the legacy `Buckets.advance` runs once per micro-batch
with all of the batch's records:

1. Only `event_type == "battery_sample"` records are aggregated. Heartbeats (and any other type)
   only raise the VIN's highest event time `max_seen`.
2. A sample is new if its `event_time >= emitted_until` and its `event_id` is not already pending.
   Within a batch the first arrival of each `event_id` is kept. So duplicates are removed in the
   job, not in the sink or downstream.
3. Samples are bucketed by UTC hour, `floorDiv(t, 1h) * 1h`.
4. An hour is emitted when `window_start + 1h + 10min <= max(max_seen, watermark)`. Emitted hours
   move `emitted_until` to their end, so later samples for them are dropped.
5. Aggregates: `sample_count`, min and HALF_UP average of `soh_pct` at scale 2, min and max of
   `cell_voltage_delta_mv` (`Long.toInt`), and max of `pack_temp_c` at scale 1. Each double is
   taken as `BigDecimal(d.toString)` (shortest round-trip decimal). Min `soh_pct` and max
   `pack_temp_c` use `setScale` without rounding, which throws if digits would be lost. The port
   returns an error there, so the job fails as the legacy job does. A null required field on a
   `battery_sample` (Scala `Option.get`) also fails the job. A value that does not fit the column
   precision is null, as Spark's non-ANSI `cast` gives.

`tests/fixture.rs` checks that arrival order does not change the result, using a property test with
random arrival orders, up to 300 seconds of lateness, duplicates and arbitrary batch cuts.

## State model and emission timing

- `buckets`: `KeyedState<String, BucketState>` with `vin`, `pending` samples (sorted by
  `(event_time, event_id)`), `max_seen_ms`, `emitted_until_ms`, and `arrived`, which holds the open
  batch's records until the batch ends.
- `watermark`: Spark's event-time watermark (max event time seen across all VINs minus 10 minutes,
  starting at 0), as in charging-session-builder.
- `process` drops a record whose `event_time <= watermark`, which Spark does before
  `flatMapGroupsWithState`, and appends the rest to the VIN's `arrived`. It emits nothing.
- `end_batch` (a batch is every record up to the input end offsets read when it started, as a Spark
  micro-batch):
  1. For each VIN with records in the batch, runs `advance` with them and the batch's watermark.
     This is the legacy per-key `step` with data. Emission here is per key and is driven by that
     VIN's own `max_seen`.
  2. Advances the watermark across keys. Then, for every VIN, runs `advance` with no records. This
     is Spark's `EventTimeTimeout`: the timeout is the earliest open hour's start + 70 min - 1, and
     it fires when the watermark passes it. Spark fires it in the next batch. Under `AvailableNow`
     that is the final no-data batch, which emits the hours of VINs that stopped sending.
     Emitting at the end of the current batch gives the same rows: any record of a timed-out hour
     arriving in the next batch would be at or before the watermark and dropped.
- Every input VIN ends with a heartbeat two hours after its last sample (input schema), so every
  hour is emitted at the latest when that heartbeat arrives. `tests/timing.rs` compares the
  emission of a VIN that stops sending while another continues with what the legacy job emitted for
  the same input, batch by batch.
- On assignment the job restores both stores from their changelogs before it processes a record.

## Delivery guarantee

| | Legacy (Spark) | Rust |
|---|---|---|
| Output | Spark file sink: files plus the `_spark_metadata` log; readers that ignore the log can see files of failed batches | Files are uploaded before the Kafka transaction's offsets are sent. On abort and on startup, files of uncommitted transactions are deleted, so a plain listing sees each row once |
| State | Spark checkpoint directory | Kafka changelog topics |
| Files | one or more per partition per micro-batch | one per `date` per transaction, `part-<committed input offset total at transaction start>.parquet` |

The sink names files by the committed input offset total, which spans every input partition, so it
needs a single replica (the chart default).

## Behavioral differences

None in effective output or emission rule.

## Legacy behaviors kept as they are, and schema disagreements

- **Lateness.** The output schema says jobs must allow at least 300 seconds of lateness and drop
  nothing. The legacy job allows 10 minutes and then drops: samples at or before the watermark, and
  samples for an already emitted hour. Within 300 seconds nothing is dropped, so the two agree on
  contract input. Beyond that, the output depends on arrival order and on micro-batch boundaries
  (the watermark advances per batch). The port keeps Spark's batch-level behavior. Not
  privacy-relevant.
- **Exact arithmetic.** The schema asks for HALF_UP on exact decimal values. The legacy job reads
  doubles and converts them with `BigDecimal(d.toString)`, then divides the exact sum with
  DECIMAL128 before HALF_UP. The port uses the same conversion and an exact quotient. These agree
  except for a quotient with a run of more than 30 repeated digits, which cannot happen for a
  realistic `sample_count`. This does not depend on arrival order or batch boundaries.
- **Extra decimals fail the job.** Input with more decimals than `x-decimals` makes the legacy job
  throw (`setScale`) only if the extra digits land in the min `soh_pct` or the max `pack_temp_c`.
  The port fails in the same cases. The schema forbids such input.
- **Sensitive fields.** `vin` is the only sensitive field. It is copied unchanged into the output
  and the state, as in legacy. No code/schema disagreement is privacy-relevant.
