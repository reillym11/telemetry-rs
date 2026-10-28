# diagnostics-dtc-router semantics

Semantics of `telemetry-spark-jobs/jobs/diagnostics-dtc-router` (`DiagnosticsDtcRouter.scala`) and the
shared helpers it calls (`GeoUtils.coarsen`, `Ids.alertId`, `Json`, `KafkaIO`, `Schemas.DiagnosticsDtc`),
read at the legacy commit pinned by telemetry-parity (`TELEMETRY_SPARK_JOBS_SHA=169768600588`, same code
as telemetry-spark-jobs main). Timing and edge cases below were confirmed by running that commit's jar
(`spark-runner:169768600588`) on a small custom input, two `AvailableNow` runs on one checkpoint.

**Status: blocked before implementation.** The legacy code and the output schema disagree on how a
sensitive field (`lat`, `lon`, coarse-location) is rounded. See "Disagreements" below. The port is not
implemented until an engineer decides.

## Topics and keys

| | Topic | Key | Partitions | Partitioner |
|---|---|---|---|---|
| Input | `vehicle.diagnostics.dtc.v1` | `vin` | 6 | murmur2 (Java) |
| Output | `service.dtc.alerts.v1` | `vin` | 6 | murmur2 (Java); a Rust producer needs `partitioner=murmur2_random` |

The output key is the input `vin` cast to string (`Json.toKafkaRecord(..., col("vin"))`), so every alert
of a VIN goes to the VIN's partition. Downstream consumers: `service-scheduling` (from `topics.yaml`).

## Routing and output

1. Each Kafka value is parsed with `from_json` against `Schemas.DiagnosticsDtc` (timestamps
   `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`). A value that does not parse becomes an all-null row and is dropped by
   the filter.
2. A row is routable if `event_type = "dtc"` and `severity >= 3` and `status = "active"` (exact,
   case-sensitive string compares; a null in any of the three drops the row). Heartbeats, cleared codes
   and severity 1-2 are dropped.
3. The filter runs **before** `withWatermark("event_time", "10 minutes")` and
   `dropDuplicatesWithinWatermark("event_id")` (see "State and emission").
4. Every surviving row emits one alert, fields in this order: `alert_id`, `vin`, `dtc_code`, `severity`,
   `ecu`, `event_time`, `lat`, `lon`. `ignoreNullFields=false`, so nulls are written as `null`.
   `event_time` is RFC 3339 UTC with three fractional digits and `Z` (for example
   `2026-10-01T01:00:00.000Z`). Numbers are Spark JSON doubles (`30.0`, `-122.12`).

## Ordering

- No ordering rule in the job. `dropDuplicatesWithinWatermark` shuffles by `event_id` into 6 shuffle
  partitions (`spark.sql.shuffle.partitions=6`), so within a micro-batch the alerts of one VIN are not
  written in arrival order or event-time order. Observed: VIN A arrived as 01:00, 01:05, 01:06 in one
  batch and was written as 01:00, 01:06, 01:05 on partition 3.
- Across micro-batches, a batch's alerts are written after the previous batch's.
- telemetry-parity check C (`order_by=vin`, `time_field=event_time`) fails the Rust side if
  `event_time` decreases within a partition for a VIN; legacy violations are informational only.
  An alert emitted on arrival for a same-VIN late event would decrease. The sample, edge and full
  fixtures have no late routable event (measured: 0 routable events arrive after a later routable
  event, globally or per VIN), so arrival-order emission passes check C on them.

## State and emission

- **Emission is immediate.** An alert is written in the micro-batch that first sees its `event_id`. No
  watermark, timeout or end-of-input flush delays output, and `AvailableNow`'s final no-data batch emits
  nothing (it only evicts dedup state). Confirmed: run 1 wrote all its alerts with no later events.
- **State:** Spark's dedup store, keyed by `event_id`, value = expiry `event_time + 10 minutes` of the first
  copy. A key is evicted once the watermark passes its expiry. No sensitive field is kept in state.
- **Watermark:** global across keys and partitions: the highest `event_time` among **routable** rows seen
  in earlier batches minus 10 minutes. Heartbeats and filtered rows do not move it. Confirmed: a
  heartbeat at 03:00 left the watermark at 01:20 (from a routable 01:30 event).
- **Late drop:** a routable row with `event_time` at or before the watermark of the previous batch is
  dropped. Confirmed in run 2 with watermark 01:20: 01:15 dropped, 01:20:00.000 dropped, 01:25 emitted.
  Rows in the same batch as the event that raised the max are never late: in run 1 a 01:05 event arriving
  after a 01:30 event was emitted.
- A port reproduces this with `Job::end_batch` advancing a watermark kept in its own state store (as
  charging-session-builder does) and a dedup store keyed by `event_id`. No new telemetry-core
  capability is needed.

## Where duplicates are removed

| Where | What |
|---|---|
| Job | `dropDuplicatesWithinWatermark("event_id")` on routable rows: one alert per `event_id` while its state lives. A copy arriving after eviction is also at or before the watermark (expiry `t + 10 min` < watermark implies `t` <= watermark), so it is late-dropped; exact copies are never re-emitted. Confirmed: copies of run-1 events in run 2 emitted nothing. |
| Sink | None. Spark's Kafka sink is at least once; a replayed micro-batch rewrites its alerts. |
| Downstream | `service_scheduling_consumer.py` keeps the first record per `alert_id` (`Deduper("alert_id")`). |

## Sensitive fields

| Field | Tag | Transformation |
|---|---|---|
| `vin` | vehicle-id | Copied unchanged into the output value and the output key. |
| `alert_id` | vehicle-id | `Ids.alertId(event_id)` = UUIDv5, namespace `f5179d15-0fd7-5b5f-bf53-0852eb22d3d5`, name = UTF-8 bytes of `event_id.toLowerCase`. Does not use `vin`. |
| `lat`, `lon` | precise-location in, coarse-location out | `GeoUtils.coarsen`: `BigDecimal(value.toString).setScale(2, HALF_EVEN).toDouble`, i.e. exact decimal arithmetic on the `Double`'s shortest string, ties to even. `-0.004` becomes `0.0` (no negative zero). If either is null, the Scala UDF (primitive `Double` args) returns null and **both** `lat` and `lon` are written as `null` (confirmed: `lat=null, lon=-122.5` gave `lat:null, lon:null`). |

The job logs no record values. Input `lat`/`lon` are never written at 5 decimals.

## Downstream consumers

- `service-scheduling`: `downstream-consumers/service_scheduling_consumer.py` reads
  `service.dtc.alerts.v1` (confluent-kafka, librdkafka default `isolation.level=read_committed`,
  `auto.offset.reset=earliest`), dedupes by `alert_id` keeping the first, and prints `alert_id`, `vin`,
  `dtc_code`, `severity`, `lat`, `lon`. It does not read `ecu` or `event_time` and does not depend on order.

## Delivery guarantee

| | Legacy (Spark) | Rust (telemetry-core) |
|---|---|---|
| Output | At least once: a Kafka sink replays the last micro-batch after a failure | Exactly once for `read_committed` consumers |
| State | Spark checkpoint directory | Kafka changelog topics |

## Behavioral differences

None planned. The port would reproduce every legacy behavior below, including immediate emission and
the watermark late drop. Implementation is blocked on the rounding disagreement.

## Legacy behaviors kept as they are

- Filter before dedup and before the watermark; only routable rows advance the watermark.
- Routable rows at or before the previous batch's watermark are dropped, even within the 300 s contract lateness if the watermark got ahead (it cannot on generated data).
- A null `lat` or `lon` nulls both output coordinates.
- Within a micro-batch, the first copy of an `event_id` in shuffle order wins (copies are exact, so content
  is the same).

## Disagreements between the legacy code and the schema descriptions

| # | Disagreement | Privacy-relevant | Depends on micro-batch boundaries or arrival order |
|---|---|---|---|
| 1 | **Coordinate rounding.** Schema: round to 2 decimals with HALF_UP, ties away from zero, on exact integers of 1e-5 degrees. Code: HALF_EVEN since telemetry-spark-jobs `67626d8` ("Use HALF_EVEN in GeoUtils.coarsen"), which also rewrote `GeoUtilsSpec` and `DiagnosticsDtcRouterSpec` to expect it. Every `.xx5` tie with an even second decimal differs by 0.01 degrees: edge fixture `37.12500 -> 37.12` (schema `37.13`), `-122.12500 -> -122.12` (`-122.13`), `-37.125`, `122.125`, `33.445 -> 33.44` (`33.45`). The legacy edge golden in telemetry-parity (`golden/edge/diagnostics-dtc-router`) has the HALF_EVEN values; the sample and full fixtures have no differing tie. | **Yes** (coarse-location output) | No. Pure per-record function. |
| 2 | **Late data.** Schema: allow at least 300 s of lateness and drop nothing. Code: drops routable rows at or before a global 10 minute watermark. No row in the fixtures is affected (generator lateness <= 300 s < 10 min). | No | **Yes.** Whether a row is dropped depends on which batch it lands in and on other VINs' event times (global watermark). |
| 3 | **Dedup order.** Schema: deduplicate by `event_id` first, then route. Code: routes first, then dedups. Same result while copies are exact (the generator's are); differs only if two records share an `event_id` but differ in routing fields. | No | Yes, in that invalid case: which copy wins depends on arrival order and batch. |
| 4 | **Null coordinates.** Output schema requires numeric `lat` and `lon`; the code writes both as `null` when either input is null. The input schema forbids null coordinates on `dtc` events. | Location fields, but only nulling (no precision change) | No. |

## Open decision (stop condition)

Disagreement 1 changes the value of a coarse-location field, so per the port playbook this is a stop
before implementation. Options for an engineer:

1. **Port reproduces legacy HALF_EVEN** (code wins, `docs/PORTING.md`). Parity passes on edge and full
   (the goldens are HALF_EVEN). The schema description stays wrong; data-contracts would need a fix in a
   separate change.
2. **Legacy goes back to HALF_UP** (revert `67626d8` in telemetry-spark-jobs, regenerate goldens), then the
   port implements HALF_UP. This changes legacy behavior and the goldens, outside a port PR.
3. **Schema owners confirm which rule is intended** before either job changes.
