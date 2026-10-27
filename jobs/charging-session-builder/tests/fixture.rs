//! Replay inputs through the job the way telemetry-core does (records, then `end_batch` once the batch
//! ends) and compare with the vendored parity golden and with the legacy job's timing.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use charging_session_builder::reducer::format_ts;
use charging_session_builder::ChargingSessionBuilder;
use proptest::prelude::*;
use serde_json::{json, Value};
use telemetry_core::state::ChangelogRecord;
use telemetry_core::Job;

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn contracts() -> PathBuf {
    dir().join("../../contracts")
}

fn lines(path: PathBuf) -> Vec<String> {
    std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

fn read_json(path: PathBuf) -> Value {
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

fn sample() -> Vec<String> {
    lines(contracts().join("fixtures/sample/vehicle.charging.events.v1.jsonl"))
}

/// The golden as `session_id -> value`.
fn golden() -> BTreeMap<String, String> {
    lines(contracts().join("golden/charging-session-builder/output.jsonl"))
        .iter()
        .map(|l| {
            let v: Value = serde_json::from_str(l).unwrap();
            let value = v["value"].as_str().unwrap().to_string();
            (session_id(&value), value)
        })
        .collect()
}

fn session_id(value: &str) -> String {
    let v: Value = serde_json::from_str(value).unwrap();
    v["session_id"].as_str().unwrap().to_string()
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

/// One job instance, committing after every record and after `end_batch`, keeping the changelog.
struct Runner {
    job: ChargingSessionBuilder,
    changelog: Vec<Vec<ChangelogRecord>>,
}

impl Runner {
    fn new() -> Self {
        let mut job = ChargingSessionBuilder::default();
        let changelog = job.stores().iter().map(|_| Vec::new()).collect();
        Runner { job, changelog }
    }

    /// A fresh instance restored from this one's changelog, as after a crash.
    fn restart(&self) -> Self {
        let mut r = Runner::new();
        for (i, log) in self.changelog.iter().enumerate() {
            for c in log {
                r.job.stores()[i]
                    .restore(c.partition, &c.key, c.value.as_deref())
                    .unwrap();
            }
        }
        r.changelog = self.changelog.clone();
        r
    }

    fn commit(&mut self) {
        for (i, s) in self.job.stores().into_iter().enumerate() {
            self.changelog[i].extend(s.take_changelog());
            s.commit();
        }
    }

    fn record(&mut self, payload: &str) -> Vec<String> {
        let input = self.job.decode(payload.as_bytes()).unwrap().unwrap();
        let v: Value = serde_json::from_str(payload).unwrap();
        let p = (v["vin"].as_str().unwrap().len() % 6) as i32;
        for s in self.job.stores() {
            s.set_partition(p);
        }
        let out = self.job.process(input).unwrap();
        self.commit();
        self.encode(out)
    }

    fn end_batch(&mut self) -> Vec<String> {
        for s in self.job.stores() {
            s.set_partition(0);
        }
        let out = self.job.end_batch().unwrap();
        self.commit();
        self.encode(out)
    }

    fn encode(&self, out: Vec<charging_session_builder::reducer::ChargingSession>) -> Vec<String> {
        out.iter()
            .map(|o| {
                let r = self.job.encode(o).unwrap();
                assert_eq!(r.key, o.vin);
                String::from_utf8(r.value).unwrap()
            })
            .collect()
    }

    /// One batch: every record, then `end_batch`. Returns what the batch emitted.
    fn batch(&mut self, payloads: &[String]) -> Vec<String> {
        let mut out: Vec<String> = payloads.iter().flat_map(|p| self.record(p)).collect();
        out.extend(self.end_batch());
        out
    }
}

fn by_id(values: &[String]) -> BTreeMap<String, String> {
    let m: BTreeMap<String, String> = values.iter().map(|v| (session_id(v), v.clone())).collect();
    assert_eq!(m.len(), values.len(), "a session was emitted twice");
    m
}

#[test]
fn sample_matches_the_parity_golden_byte_for_byte() {
    let input = sample();
    assert_eq!(input.len(), 288);
    let emitted = Runner::new().batch(&input);
    assert_eq!(by_id(&emitted), golden());
    assert_eq!(emitted.len(), 11);

    let schema = read_json(contracts().join("topics/charging.sessions.v1.schema.json"));
    let validator = jsonschema::options()
        .with_retriever(Vendored(read_json(contracts().join("common.schema.json"))))
        .build(&schema)
        .unwrap();
    for v in &emitted {
        let v: Value = serde_json::from_str(v).unwrap();
        let errors: Vec<String> = validator.iter_errors(&v).map(|e| e.to_string()).collect();
        assert!(errors.is_empty(), "schema violation: {errors:?}");
    }
}

#[test]
fn sample_gives_the_same_sessions_in_3_and_7_batches() {
    let input = sample();
    for n in [3, 7] {
        let mut r = Runner::new();
        let size = input.len().div_ceil(n);
        let emitted: Vec<String> = input.chunks(size).flat_map(|c| r.batch(c)).collect();
        assert_eq!(by_id(&emitted), golden(), "{n} batches");
    }
}

#[test]
fn a_crash_mid_batch_restores_the_buffered_records_and_emits_the_same_sessions() {
    let input = sample();
    for cut in [1, 40, 115, 287] {
        let mut first = Runner::new();
        let before: Vec<String> = input[..cut].iter().flat_map(|p| first.record(p)).collect();
        assert!(
            before.is_empty(),
            "nothing is emitted before the batch ends"
        );
        let mut restarted = first.restart();
        assert_eq!(
            by_id(&restarted.batch(&input[cut..])),
            golden(),
            "cut at {cut}"
        );
    }
}

#[test]
fn redelivery_after_restore_emits_nothing_new() {
    let input = sample();
    let mut r = Runner::new();
    r.batch(&input);
    let mut restored = r.restart();
    assert!(restored.batch(&input).is_empty());
}

/// Expected output per run comes from the legacy jar: tests/timing/legacy_run.sh ran run1..run3.jsonl as
/// three AvailableNow runs sharing one checkpoint and wrote legacy-runN.jsonl.
#[test]
fn emission_timing_matches_three_legacy_runs() {
    let mut r = Runner::new();
    for i in 1..=3 {
        let input = lines(dir().join(format!("tests/timing/run{i}.jsonl")));
        let legacy: BTreeSet<String> =
            lines(dir().join(format!("tests/timing/legacy-run{i}.jsonl")))
                .into_iter()
                .collect();
        let emitted: BTreeSet<String> = r.batch(&input).into_iter().collect();
        assert_eq!(emitted, legacy, "run {i}");
    }
}

#[test]
fn a_vin_that_stops_sending_is_closed_by_the_watermark_in_the_same_batch() {
    let input = lines(dir().join("tests/timing/run1.jsonl"));
    let mut r = Runner::new();
    let before_end: Vec<String> = input.iter().flat_map(|p| r.record(p)).collect();
    assert!(before_end.is_empty());
    let at_end = r.end_batch();
    assert_eq!(at_end.len(), 1);
    assert!(at_end[0].contains("\"vin\":\"SYNTMGA0000000000\""));
}

const VIN: &str = "SYNPROPTEST000001";
const MIN: i64 = 60_000;
const T0: i64 = 1_790_812_800_000;

fn payload(id: usize, t: i64, kind: &str, seq: i64, x: f64) -> String {
    let hb = kind == "heartbeat";
    let num = |v: f64| if hb { Value::Null } else { json!(v) };
    json!({"event_id": format!("00000000-0000-4000-8000-{id:012}"), "vin": VIN,
        "event_time": format_ts(t), "event_type": kind, "producer_seq": seq, "schema_version": "1",
        "vehicle_model": "AIR", "fleet": "consumer", "market": "us-phoenix", "firmware_version": "2026.10.1",
        "connector_type": if hb { Value::Null } else { json!("CCS1") },
        "charger_type": if hb { Value::Null } else { json!("dc_fast") },
        "power_kw": num(x * 3.0), "energy_added_kwh": num(x), "soc_pct": num(20.0 + x),
        "charger_lat": num(33.44838), "charger_lon": num(-112.07404)})
    .to_string()
}

proptest! {
    /// Sessions as the generator builds them (progress every 1-5 minutes, sessions more than the gap plus
    /// the maximum lateness apart, some without charge_stop, a heartbeat 2 hours after the last event),
    /// delivered with up to 300 s of lateness and duplicates and split into random batches: the sessions
    /// equal those of the in-order, single-batch run.
    #[test]
    fn arrival_order_and_batching_do_not_change_the_sessions(
        sessions in prop::collection::vec((2usize..12, any::<bool>(), 36i64..120), 1..5),
        steps in prop::collection::vec(1i64..=5, 60),
        lateness in prop::collection::vec(0i64..=300_000, 80),
        dup in prop::collection::vec(0u8..10, 80),
        cuts in prop::collection::vec(0usize..80, 0..4),
    ) {
        let mut events: Vec<(i64, &str)> = Vec::new();
        let mut t = T0;
        let mut k = 0;
        for (n, stop, gap) in &sessions {
            events.push((t, "charge_start"));
            for i in 1..*n {
                t += steps[k % steps.len()] * MIN;
                k += 1;
                events.push((t, if *stop && i == n - 1 { "charge_stop" } else { "charge_progress" }));
            }
            t += gap * MIN;
        }
        let last = events.last().unwrap().0;
        let clean: Vec<String> = events.iter().enumerate()
            .map(|(i, (t, kind))| payload(i + 1, *t, kind, i as i64 + 1, i as f64 * 1.25))
            .chain([payload(events.len() + 1, last + 120 * MIN, "heartbeat", events.len() as i64 + 1, 0.0)])
            .collect();
        let expected = by_id(&Runner::new().batch(&clean));
        prop_assert_eq!(expected.len(), sessions.len());

        let mut arrivals: Vec<(i64, usize)> = Vec::new();
        for (i, (t, _)) in events.iter().enumerate() {
            arrivals.push((t + lateness[i % 80], i));
            if dup[i % 80] == 0 {
                arrivals.push((t + lateness[i % 80] + 1_000 + lateness[(i + 7) % 80] / 5, i));
            }
        }
        arrivals.sort();
        let mut shuffled: Vec<String> = arrivals.iter().map(|(_, i)| clean[*i].clone()).collect();
        shuffled.push(clean.last().unwrap().clone());
        let mut bounds: Vec<usize> = cuts.iter().map(|c| c % shuffled.len()).collect();
        bounds.push(shuffled.len());
        bounds.sort();
        let mut r = Runner::new();
        let mut emitted = Vec::new();
        let mut from = 0;
        for b in bounds {
            emitted.extend(r.batch(&shuffled[from..b]));
            from = b;
        }
        prop_assert_eq!(by_id(&emitted), expected);
    }
}
