//! Replay the vendored sample through the job and compare with the vendored parity golden.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use diagnostics_dtc_router::reducer::{self, format_ts};
use diagnostics_dtc_router::DiagnosticsDtcRouter;
use proptest::prelude::*;
use serde_json::Value;
use telemetry_core::Job;

fn contracts() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts")
}

fn read_json(path: PathBuf) -> Value {
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
}

fn sample() -> Vec<String> {
    std::fs::read_to_string(contracts().join("fixtures/sample/vehicle.diagnostics.dtc.v1.jsonl"))
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
    let schema = read_json(contracts().join("topics/service.dtc.alerts.v1.schema.json"));
    jsonschema::options()
        .with_retriever(Vendored(common))
        .build(&schema)
        .unwrap()
}

/// Run records through the job as one batch, one transaction per record, then end the batch.
/// Returns every (key, value) emitted.
fn replay(job: &mut DiagnosticsDtcRouter, payloads: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut emit = |job: &mut DiagnosticsDtcRouter, outputs: Vec<_>| {
        for o in outputs {
            let r = job.encode(&o).unwrap();
            out.push((r.key, String::from_utf8(r.value).unwrap()));
        }
        for s in job.stores() {
            s.take_changelog();
            s.commit();
        }
    };
    for p in payloads {
        if let Some(input) = job.decode(p.as_bytes()).unwrap() {
            for s in job.stores() {
                s.set_partition(0);
            }
            let o = job.process(input).unwrap();
            emit(job, o);
        }
    }
    let o = job.end_batch().unwrap();
    emit(job, o);
    out
}

/// The effective output: the last record per alert_id.
fn by_alert(records: &[(String, String)]) -> BTreeMap<String, (String, String)> {
    records
        .iter()
        .map(|(k, v)| {
            let id: Value = serde_json::from_str(v).unwrap();
            (
                id["alert_id"].as_str().unwrap().to_string(),
                (k.clone(), v.clone()),
            )
        })
        .collect()
}

#[test]
fn sample_matches_the_parity_golden_byte_for_byte() {
    let input = sample();
    assert_eq!(input.len(), 25);
    let emitted = replay(&mut DiagnosticsDtcRouter::default(), &input);

    let golden: Vec<(String, String)> =
        std::fs::read_to_string(contracts().join("golden/diagnostics-dtc-router/output.jsonl"))
            .unwrap()
            .lines()
            .map(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                (
                    v["key"].as_str().unwrap().to_string(),
                    v["value"].as_str().unwrap().to_string(),
                )
            })
            .collect();
    let rust = by_alert(&emitted);
    assert_eq!(rust, by_alert(&golden));
    assert_eq!(rust.len(), 1);
    assert_eq!(emitted.len(), rust.len(), "each alert is emitted once");

    let validator = output_validator();
    for (_, v) in &emitted {
        let v: Value = serde_json::from_str(v).unwrap();
        assert!(
            validator.is_valid(&v),
            "schema violation: {:?}",
            validator
                .iter_errors(&v)
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn replay_after_restore_emits_nothing_new() {
    let input = sample();
    let mut job = DiagnosticsDtcRouter::default();
    let mut changelog = Vec::new();
    let mut collect = |job: &mut DiagnosticsDtcRouter| {
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
    let mut restored = DiagnosticsDtcRouter::default();
    for (i, c) in &changelog {
        restored.stores()[*i]
            .restore(c.partition, &c.key, c.value.as_deref())
            .unwrap();
    }
    assert_eq!(restored.seen_len(), job.seen_len());
    assert!(
        replay(&mut restored, &input).is_empty(),
        "a re-delivery after restore must not emit an alert again"
    );
}

const T0: i64 = 1_790_812_800_000; // 2026-10-01T00:00:00Z
const VINS: [&str; 3] = [
    "SYNTESTVIN0000001",
    "SYNTESTVIN0000002",
    "SYNTESTVIN0000003",
];

fn payload(id: usize, t_ms: i64, routable: bool) -> String {
    serde_json::json!({"event_id": format!("e{id}"), "vin": VINS[id % 3], "event_time": format_ts(t_ms),
        "event_type": if routable { "dtc" } else { "heartbeat" }, "severity": 3 + (id % 3) as i64,
        "status": "active", "dtc_code": "P0A80", "ecu": "bms",
        "lat": 30.0 + (id % 1000) as f64 / 1000.0 + 0.00005, "lon": -97.74305})
    .to_string()
}

proptest! {
    /// Events delivered with up to 300 s of lateness (the contract's bound) and duplicates, split
    /// into batches at arbitrary points: every routable event_id is emitted exactly once, with the
    /// same value as a single batch in event-time order.
    #[test]
    fn arrival_order_and_batch_boundaries_do_not_change_the_result(
        steps in prop::collection::vec(1i64..600_000, 1..60),
        routable in prop::collection::vec(any::<bool>(), 60),
        lateness in prop::collection::vec(0i64..=300_000, 60),
        dup in prop::collection::vec(any::<bool>(), 60),
        cuts in prop::collection::btree_set(0usize..120, 0..6),
    ) {
        let mut t = T0;
        let events: Vec<(usize, i64, bool)> = steps
            .iter()
            .enumerate()
            .map(|(i, s)| { t += s; (i, t, routable[i]) })
            .collect();
        let mut arrivals: Vec<(i64, usize)> = Vec::new();
        for (i, t, _) in &events {
            arrivals.push((t + lateness[*i], *i));
            if dup[*i] {
                arrivals.push((t + lateness[(*i + 7) % 60], *i));
            }
        }
        arrivals.sort();
        let payloads: Vec<String> =
            arrivals.iter().map(|(_, i)| payload(*i, events[*i].1, events[*i].2)).collect();

        let mut job = DiagnosticsDtcRouter::default();
        let mut got = Vec::new();
        let mut start = 0;
        for cut in cuts.iter().copied().filter(|c| *c < payloads.len()).chain([payloads.len()]) {
            got.extend(replay(&mut job, &payloads[start..cut.max(start)]));
            start = cut.max(start);
        }

        let ordered: Vec<String> = events.iter().map(|(i, t, r)| payload(*i, *t, *r)).collect();
        let expected = replay(&mut DiagnosticsDtcRouter::default(), &ordered);
        let ids: BTreeSet<String> = by_alert(&got).into_keys().collect();
        prop_assert_eq!(got.len(), ids.len(), "an alert is emitted at most once");
        prop_assert_eq!(by_alert(&got), by_alert(&expected));
        let routable_ids: BTreeSet<String> = events
            .iter()
            .filter(|e| e.2)
            .map(|(i, _, _)| reducer::alert_id(&format!("e{i}")))
            .collect();
        prop_assert_eq!(ids, routable_ids);
    }
}
