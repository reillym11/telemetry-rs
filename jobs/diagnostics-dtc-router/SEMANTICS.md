# diagnostics-dtc-router semantics

Port of `telemetry-spark-jobs/jobs/diagnostics-dtc-router` (`DiagnosticsDtcRouter.scala`,
`DiagnosticsDtcRouterSpec.scala`) and the shared helpers it calls: `GeoUtils.coarsen`, `Ids.alertId`,
`Json.parse` / `Json.toKafkaRecord`, `Schemas.DiagnosticsDtc`, `KafkaIO`. Written from the code at
telemetry-spark-jobs `1697686` (the `TELEMETRY_SPARK_JOBS_SHA` the pinned harness runs).

**Status: blocked before implementation.** The legacy code and the output schema disagree on how the
sensitive `lat` / `lon` fields are rounded (see "Disagreements"). The port does not pick a side until an
engineer decides.

## Topics and keys

| | Topic | Key | Partitions | Partitioner |
|---|---|---|---|---|
| Input | `vehicle.diagnostics.dtc.v1` | `vin` | 6 | murmur2 (Java) |
| Output | `service.dtc.alerts.v1` (delete) | `vin` | 6 | murmur2 (Java), `partitioner=murmur2_random` |
| State (planned) | `<STATE_TOPIC_PREFIX>.seen` (compacted) | `event_id` as JSON | same as input | the input record's partition |
| State (planned) | `<STATE_TOPIC_PREFIX>.watermark` (compacted) | `"watermark"` as JSON | same as input | the lowest assigned partition |

The legacy job writes `col("vin").cast("string")` as the key, so every alert of a VIN goes to the VIN's
partition. Downstream consumers: `service-scheduling` (from `topics.yaml`).

## Routing and ordering

1. Every input value is parsed with `from_json` and `Schemas.DiagnosticsDtc` (`timestampFormat`
   `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`, `severity` as long, `lat` / `lon` as double).
2. `routable`: `event_type = "dtc" AND severity >= 3 AND status = "active"`. A null in any of the three
   makes the predicate null, so the row is dropped. Heartbeats, cleared DTCs and severity 1 and 2 are
   dropped here, **before** the watermark operator.
3. `withWatermark("event_time", "10 minutes")` then `dropDuplicatesWithinWatermark("event_id")`.
4. One output per surviving row: `alert_id`, `vin`, `dtc_code`, `severity`, `ecu`, `event_time`
   (RFC 3339 UTC, three fractional digits, `Z`), `lat`, `lon` (coarsened, below). No other fields.

There is no ordering rule between alerts: each alert is a function of one input row. Alerts of a VIN
are written in the order Spark processes the rows; the contract compares alerts by `alert_id`.

## State and when output is emitted

- **Emission is immediate.** An alert is emitted in the micro-batch in which the first routable copy of
  its `event_id` arrives. No watermark, timeout or trigger delays or produces output; Append mode with
  `dropDuplicatesWithinWatermark` never holds rows back.
- **Watermark (across keys).** Spark's watermark is the highest `event_time` seen by the watermark
  operator in previous batches minus 10 minutes, global across all VINs and partitions. Because the
  filter runs first, only routable rows move it; heartbeats (`last event + 2 hours`) do not. It starts
  at 0, never decreases, and is updated at the end of each micro-batch for use in the next one.
- **Late rows are dropped.** In a batch, a routable row with `event_time` at or before the watermark is
  dropped by the dedup operator, even if its `event_id` is new.
- **Dedup state (per `event_id`).** The first copy stores `event_id -> event_time + 10 minutes`. At the
  end of each batch, entries whose expiry is at or before the watermark are evicted. A later copy with
  the same `event_time` is then already late, so in effect an `event_id` emits once.
- **End of input.** `Trigger.AvailableNow` processes the available offsets (one batch: no
  `maxOffsetsPerTrigger`) and may run a final no-data batch that advances the watermark and evicts
  state. That batch emits nothing for this job.
- The planned port keeps the watermark in its own store and advances it, and evicts `seen`, in
  `Job::end_batch`, as charging-session-builder does; `end_batch` emits nothing.

## Where duplicates are removed

| Where | What |
|---|---|
| Job (legacy) | `dropDuplicatesWithinWatermark("event_id")` on routable rows, case-sensitive `event_id`. |
| Sink (legacy) | None. Spark's Kafka sink is at least once; a replayed micro-batch writes alerts again. |
| Downstream | `service_scheduling_consumer.py` keeps the first record per `alert_id` (`Deduper("alert_id")`). |

## Sensitive fields

| Field | Tag | Transformation |
|---|---|---|
| `vin` | vehicle-id | Copied unchanged into the output value and the output key. Legacy dedup state holds only `event_id` and an expiry, not `vin`. |
| `alert_id` | vehicle-id (output) | `Ids.alertId`: UUIDv5, namespace `f5179d15-0fd7-5b5f-bf53-0852eb22d3d5`, over the UTF-8 bytes of `lowercase(event_id)`. Dedup does not lowercase. |
| `lat`, `lon` | precise-location in, coarse-location out | `GeoUtils.coarsen` UDF: `BigDecimal(value.toString).setScale(2, RoundingMode.HALF_EVEN).toDouble` on the parsed double, each coordinate independently. If either input is null the UDF result is null, so both outputs are null (the schema forbids null on `dtc`). |

No other field of the input reaches the output; `odometer_km`, `status`, envelope fields other than
`vin` and `event_time` are dropped.

## Downstream consumers

- `service-scheduling` (`downstream-consumers/service_scheduling_consumer.py`): dedupes by `alert_id`
  and prints `alert_id`, `vin`, `dtc_code`, `severity`, `lat`, `lon`.

## Disagreements between the legacy code and the schema descriptions

1. **Rounding of `lat` / `lon` (privacy-relevant).** `service.dtc.alerts.v1.schema.json` says HALF_UP,
   ties away from zero, on exact decimals. `GeoUtils.coarsen` uses HALF_EVEN since telemetry-spark-jobs
   `67626d8` ("Use HALF_EVEN in GeoUtils.coarsen"); the schema was not changed. They differ on exact ties
   at the third decimal (`x.xx500`). The edge fixture has three routable ties and the edge golden
   (legacy at `1697686`) has `37.12, -122.12`, `-37.12, 122.12`, `33.44, -112.08`; the schema gives
   `37.13, -122.13`, `-37.13, 122.13`, `33.45, -112.08`. The sample fixture has no ties. Does not depend
   on micro-batch boundaries or arrival order (a pure function of one row).
2. **Dedup before routing.** The schema says deduplicate by `event_id` first, then route; the code
   routes first. Same result when copies of an `event_id` are identical, as the contract requires. If
   copies differed, the code emits the first routable copy, so the output could depend on arrival order.
3. **"Drop nothing" with 300 seconds of lateness.** The code drops routable rows at or before a
   10 minute global watermark. With lateness at most 300 seconds nothing is dropped, but whether a row
   more than 10 minutes behind the global maximum is dropped depends on micro-batch boundaries and on
   arrival order across VINs.
4. **One alert per distinct `event_id`.** The code forgets an `event_id` once its `event_time` plus
   10 minutes is at or before the watermark; a copy with a different, later `event_time` would alert
   again. Depends on micro-batch boundaries. The legacy sink can also duplicate alerts after a failure.
5. **`event_id` case.** `alert_id` lowercases the `event_id`; dedup does not. The schema requires
   lowercase `event_id`, so they agree on valid input. Does not depend on batch boundaries.

## Behavioral differences

To be decided: see disagreement 1.
