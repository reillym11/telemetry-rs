//! Replay the vendored sample through the job and compare with the vendored parity golden
//! (the Parquet files the legacy Spark job wrote for the same input).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int32Type, TimestampMillisecondType};
use arrow_array::RecordBatch;
use battery_health_aggregator::reducer::{self, BucketState, Sample};
use battery_health_aggregator::{spec, BatteryHealthAggregator};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use proptest::prelude::*;
use serde_json::Value;
use telemetry_core::{parquet_sink, Job};

fn contracts() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts")
}

fn read_json(path: PathBuf) -> Value {
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

fn sample() -> Vec<String> {
    std::fs::read_to_string(contracts().join("fixtures/sample/vehicle.battery.health.v1.jsonl"))
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

/// Resolves the topic schema's `../common.schema.json` reference from the vendored copy.
struct Vendored(Value);

impl jsonschema::Retrieve for Vendored {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        match uri.as_str() {
            "https://contracts.telemetry.example/common.schema.json" => Ok(self.0.clone()),
            other => Err(format!("unexpected schema reference {other}").into()),
        }
    }
}

fn output_validator() -> jsonschema::Validator {
    let common = read_json(contracts().join("common.schema.json"));
    let schema = read_json(contracts().join("topics/battery.health.hourly.schema.json"));
    jsonschema::options()
        .with_retriever(Vendored(common))
        .build(&schema)
        .unwrap()
}

/// Run records through the job as one batch, as the processor does, committing state after every
/// `commit_every` records. Returns every encoded row.
fn replay(
    job: &mut BatteryHealthAggregator,
    payloads: &[String],
    commit_every: usize,
) -> Vec<Value> {
    let mut out = Vec::new();
    let commit = |job: &mut BatteryHealthAggregator| {
        for s in job.stores() {
            s.take_changelog();
            s.commit();
        }
    };
    for (i, p) in payloads.iter().enumerate() {
        if let Some(input) = job.decode(p.as_bytes()).unwrap() {
            for s in job.stores() {
                s.set_partition(0);
            }
            assert!(
                job.process(input).unwrap().is_empty(),
                "rows are emitted at batch end"
            );
        }
        if (i + 1) % commit_every == 0 {
            commit(job);
        }
    }
    for o in job.end_batch().unwrap() {
        out.push(serde_json::from_slice(&job.encode(&o).unwrap().value).unwrap());
    }
    commit(job);
    out
}

/// One row in a comparable form: (date, vin, window_start, window_end, sample_count, soh_min,
/// soh_avg, delta_min, delta_max, temp_max), decimals as unscaled integers.
type Row = (String, String, i64, i64, i32, i128, i128, i32, i32, i128);

fn rows(date: &str, b: &RecordBatch) -> Vec<Row> {
    let s = |n: &str| b.column_by_name(n).unwrap().as_string::<i32>().clone();
    let ts = |n: &str| {
        b.column_by_name(n)
            .unwrap()
            .as_primitive::<TimestampMillisecondType>()
            .clone()
    };
    let i = |n: &str| {
        b.column_by_name(n)
            .unwrap()
            .as_primitive::<Int32Type>()
            .clone()
    };
    let d = |n: &str| {
        b.column_by_name(n)
            .unwrap()
            .as_primitive::<Decimal128Type>()
            .clone()
    };
    let (vin, ws, we, n) = (
        s("vin"),
        ts("window_start"),
        ts("window_end"),
        i("sample_count"),
    );
    let (smin, savg, tmax) = (d("soh_pct_min"), d("soh_pct_avg"), d("pack_temp_c_max"));
    let (dmin, dmax) = (
        i("cell_voltage_delta_mv_min"),
        i("cell_voltage_delta_mv_max"),
    );
    (0..b.num_rows())
        .map(|r| {
            (
                date.to_string(),
                vin.value(r).to_string(),
                ws.value(r),
                we.value(r),
                n.value(r),
                smin.value(r),
                savg.value(r),
                dmin.value(r),
                dmax.value(r),
                tmax.value(r),
            )
        })
        .collect()
}

fn read_parquet(bytes: bytes::Bytes, date: &str) -> Vec<Row> {
    ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .flat_map(|b| rows(date, &b.unwrap()))
        .collect()
}

/// Physical schema of a Parquet file, one line per column (type, logical type, repetition).
fn physical_schema(bytes: bytes::Bytes) -> Vec<String> {
    let r = SerializedFileReader::new(bytes).unwrap();
    r.metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| {
            format!(
                "{} {:?} {:?} {:?}",
                c.name(),
                c.physical_type(),
                c.logical_type_ref(),
                c.self_type().get_basic_info().repetition()
            )
        })
        .collect()
}

fn golden_files() -> Vec<(String, PathBuf)> {
    let root = contracts().join("golden/battery-health-aggregator/parquet");
    let mut files = Vec::new();
    for dir in std::fs::read_dir(&root).unwrap() {
        let dir = dir.unwrap().path();
        let date = dir
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .strip_prefix("date=")
            .unwrap()
            .to_string();
        for f in std::fs::read_dir(&dir).unwrap() {
            let f = f.unwrap().path();
            if f.extension().is_some_and(|e| e == "parquet") {
                files.push((date.clone(), f));
            }
        }
    }
    files.sort();
    files
}

fn load(p: &Path) -> bytes::Bytes {
    bytes::Bytes::from(std::fs::read(p).unwrap())
}

fn golden() -> BTreeSet<Row> {
    golden_files()
        .iter()
        .flat_map(|(d, f)| read_parquet(load(f), d))
        .collect()
}

/// Encode rows per date as the sink does and read them back.
fn through_sink(out: &[Value]) -> (BTreeSet<Row>, Vec<String>) {
    let dates: BTreeSet<&str> = out.iter().map(|v| v["date"].as_str().unwrap()).collect();
    let mut all = BTreeSet::new();
    let mut schema = Vec::new();
    for d in dates {
        let part: Vec<Value> = out.iter().filter(|v| v["date"] == d).cloned().collect();
        let bytes = bytes::Bytes::from(parquet_sink::encode(&spec(), &part).unwrap());
        schema = physical_schema(bytes.clone());
        all.extend(read_parquet(bytes, d));
    }
    (all, schema)
}

#[test]
fn sample_matches_the_parity_golden() {
    let input = sample();
    assert_eq!(input.len(), 288);
    let out = replay(&mut BatteryHealthAggregator::default(), &input, 500);
    let (rust, schema) = through_sink(&out);
    let golden = golden();
    assert_eq!(golden.len(), 63);
    assert_eq!(rust, golden);
    assert_eq!(
        out.len(),
        rust.len(),
        "each (vin, window_start) is emitted once"
    );
    let (_, gf) = &golden_files()[0];
    assert_eq!(
        schema,
        physical_schema(load(gf)),
        "same physical Parquet types as Spark"
    );

    let validator = output_validator();
    for v in &out {
        assert!(
            validator.is_valid(v),
            "schema violation: {:?}",
            validator
                .iter_errors(v)
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn transaction_boundaries_do_not_change_the_result() {
    let input = sample();
    let one = replay(&mut BatteryHealthAggregator::default(), &input, 500);
    let small = replay(&mut BatteryHealthAggregator::default(), &input, 5);
    assert_eq!(through_sink(&one).0, through_sink(&small).0);
}

#[test]
fn replay_after_restore_emits_nothing_new() {
    let input = sample();
    let mut job = BatteryHealthAggregator::default();
    let mut changelog = Vec::new();
    let mut collect = |job: &mut BatteryHealthAggregator| {
        for (i, s) in job.stores().into_iter().enumerate() {
            changelog.extend(s.take_changelog().into_iter().map(|c| (i, c)));
            s.commit();
        }
    };
    for p in &input {
        if let Some(i) = job.decode(p.as_bytes()).unwrap() {
            for s in job.stores() {
                s.set_partition(2);
            }
            job.process(i).unwrap();
            collect(&mut job);
        }
    }
    job.end_batch().unwrap();
    collect(&mut job);
    let mut restored = BatteryHealthAggregator::default();
    for (i, c) in &changelog {
        restored.stores()[*i]
            .restore(c.partition, &c.key, c.value.as_deref())
            .unwrap();
    }
    assert_eq!(restored.stores()[0].len(), job.stores()[0].len());
    assert_eq!(restored.watermark_ms(), job.watermark_ms());
    assert!(
        replay(&mut restored, &input, 500).is_empty(),
        "a full re-delivery after restore must not emit an hour again"
    );
}

const H0: i64 = 1_790_812_800_000; // 2026-10-01T00:00:00Z
const MIN: i64 = 60_000;

fn battery(id: usize, t_ms: i64) -> Sample {
    Sample {
        event_id: format!("e{id}"),
        event_time_ms: t_ms,
        event_type: Some("battery_sample".into()),
        soh_pct: Some(90.0 + (id % 7) as f64 * 0.01),
        cell_voltage_delta_mv: Some(10 + (id % 5) as i64),
        pack_temp_c: Some(20.0 + (id % 9) as f64 * 0.1),
    }
}

proptest! {
    /// Samples every 10 minutes, delivered with up to 300 s of lateness and duplicates, split into
    /// batches anywhere, then a heartbeat two hours after the last sample: the job emits exactly
    /// the rows that one batch of all records in producer order produces, each once.
    #[test]
    fn arrival_order_does_not_change_the_result(
        n in 1usize..40,
        lateness in prop::collection::vec(0i64..=300_000, 40),
        dups in prop::collection::vec(any::<bool>(), 40),
        cuts in prop::collection::vec(any::<bool>(), 80),
    ) {
        let vin = "SYNTESTVIN0000001";
        let samples: Vec<Sample> = (0..n).map(|i| battery(i, H0 + 7 * MIN + i as i64 * 10 * MIN)).collect();
        let last = samples.last().unwrap().event_time_ms;
        let mut heartbeat = battery(999, last + 120 * MIN);
        heartbeat.event_type = Some("heartbeat".into());
        heartbeat.soh_pct = None;

        let (_, expected, _) = reducer::advance(
            &BucketState::empty(vin),
            &samples.iter().cloned().chain([heartbeat.clone()]).collect::<Vec<_>>(),
            0,
        ).unwrap();

        let mut arrivals: Vec<(i64, Sample)> = Vec::new();
        for (i, s) in samples.iter().enumerate() {
            arrivals.push((s.event_time_ms + lateness[i], s.clone()));
            if dups[i] {
                arrivals.push((s.event_time_ms + lateness[(i + 1) % 40], s.clone()));
            }
        }
        arrivals.sort_by_key(|(t, _)| *t);
        arrivals.push((i64::MAX, heartbeat));

        let mut job = BatteryHealthAggregator::default();
        let mut got = Vec::new();
        for (i, (_, s)) in arrivals.iter().enumerate() {
            let payload = serde_json::json!({"event_id": s.event_id, "vin": vin,
                "event_time": reducer::format_ts(s.event_time_ms), "event_type": s.event_type,
                "soh_pct": s.soh_pct, "cell_voltage_delta_mv": s.cell_voltage_delta_mv,
                "pack_temp_c": s.pack_temp_c}).to_string();
            let input = job.decode(payload.as_bytes()).unwrap().unwrap();
            for st in job.stores() {
                st.set_partition(0);
            }
            job.process(input).unwrap();
            if cuts[i % 80] {
                got.extend(job.end_batch().unwrap());
            }
            for st in job.stores() {
                st.take_changelog();
                st.commit();
            }
        }
        got.extend(job.end_batch().unwrap());
        for _ in 0..2 {
            got.extend(job.end_batch().unwrap());
        }
        let key = |r: &reducer::HourlyHealth| r.window_start.clone();
        got.sort_by_key(key);
        let mut expected = expected;
        expected.sort_by_key(key);
        prop_assert_eq!(got, expected);
    }
}
