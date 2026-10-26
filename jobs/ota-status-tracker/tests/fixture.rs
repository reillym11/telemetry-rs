//! Replay the vendored sample through the job and compare with the vendored parity golden.

use std::collections::BTreeMap;
use std::path::PathBuf;

use ota_status_tracker::reducer::{self, Winner};
use ota_status_tracker::OtaStatusTracker;
use proptest::prelude::*;
use serde_json::Value;
use telemetry_core::Job;

fn contracts() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts")
}

fn read_json(path: PathBuf) -> Value {
    serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
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
    let schema = read_json(contracts().join("topics/ota.status.latest.v1.schema.json"));
    jsonschema::options()
        .with_retriever(Vendored(common))
        .build(&schema)
        .unwrap()
}

/// Run records through the job as one transaction per record. Returns every (key, value) emitted.
fn replay(job: &mut OtaStatusTracker, payloads: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for p in payloads {
        if let Some(input) = job.decode(p.as_bytes()).unwrap() {
            for s in job.stores() {
                s.set_partition(0);
            }
            for o in job.process(input).unwrap() {
                let r = job.encode(&o).unwrap();
                out.push((r.key, String::from_utf8(r.value).unwrap()));
            }
            for s in job.stores() {
                s.take_changelog();
                s.commit();
            }
        }
    }
    out
}

fn last_per_key(records: impl IntoIterator<Item = (String, String)>) -> BTreeMap<String, String> {
    records.into_iter().collect()
}

#[test]
fn sample_matches_the_parity_golden_byte_for_byte() {
    let input: Vec<String> =
        std::fs::read_to_string(contracts().join("fixtures/sample/vehicle.ota.status.v1.jsonl"))
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
    assert_eq!(input.len(), 43);
    let emitted = replay(&mut OtaStatusTracker::default(), &input);

    let golden: Vec<(String, String)> =
        std::fs::read_to_string(contracts().join("golden/ota-status-tracker/output.jsonl"))
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
    let rust = last_per_key(emitted.clone());
    assert_eq!(rust, last_per_key(golden));
    assert_eq!(rust.len(), 12);

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
    let input: Vec<String> =
        std::fs::read_to_string(contracts().join("fixtures/sample/vehicle.ota.status.v1.jsonl"))
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
    let mut job = OtaStatusTracker::default();
    let mut changelog = Vec::new();
    for p in &input {
        if let Some(i) = job.decode(p.as_bytes()).unwrap() {
            for s in job.stores() {
                s.set_partition(2);
            }
            job.process(i).unwrap();
            for s in job.stores() {
                changelog.extend(s.take_changelog());
                s.commit();
            }
        }
    }
    let mut restored = OtaStatusTracker::default();
    for c in &changelog {
        restored.stores()[0]
            .restore(c.partition, &c.key, c.value.as_deref())
            .unwrap();
    }
    assert_eq!(restored.stores()[0].len(), 12);
    assert!(
        replay(&mut restored, &input).is_empty(),
        "a full re-delivery after restore must not change state"
    );
}

fn event(id: usize, t_ms: i64, seq: i64) -> Winner {
    Winner {
        vin: "SYNTESTVIN0000001".into(),
        campaign_id: Some("OTA-2026.10.1".into()),
        event_id: format!("e{id}"),
        event_time_ms: t_ms,
        producer_seq: seq,
        state: Some(["downloaded", "installed", "failed"][id % 3].into()),
        target_version: Some("2026.10.1".into()),
        failure_code: (id % 3 == 2).then(|| "E_TIMEOUT".into()),
    }
}

proptest! {
    /// Events in producer order, then delivered with up to 300 s of lateness and duplicates: the final
    /// state is always the highest-ranked event, and the last output is its projection.
    #[test]
    fn arrival_order_does_not_change_the_result(
        gaps in prop::collection::vec(1i64..120_000, 1..30),
        lateness in prop::collection::vec(0i64..=300_000, 30),
        dup in prop::collection::vec(any::<bool>(), 30),
    ) {
        let mut t = 1_790_000_000_000i64;
        let events: Vec<Winner> = gaps.iter().enumerate().map(|(i, g)| { t += g; event(i, t, i as i64 + 1) }).collect();
        let mut arrivals: Vec<(i64, usize)> = Vec::new();
        for (i, e) in events.iter().enumerate() {
            arrivals.push((e.event_time_ms + lateness[i], i));
            if dup[i] {
                arrivals.push((e.event_time_ms + lateness[(i + 7) % 30], i));
            }
        }
        arrivals.sort();
        let payloads: Vec<String> = arrivals.iter().map(|(_, i)| {
            let e = &events[*i];
            serde_json::json!({"event_id": e.event_id, "vin": e.vin, "event_time": reducer::format_ts(e.event_time_ms),
                "event_type": "ota_status", "producer_seq": e.producer_seq, "campaign_id": e.campaign_id,
                "target_version": e.target_version, "state": e.state, "failure_code": e.failure_code}).to_string()
        }).collect();
        let out = replay(&mut OtaStatusTracker::default(), &payloads);
        let last = &out.last().unwrap().1;
        let expected = serde_json::to_string(&reducer::to_output(events.last().unwrap())).unwrap();
        prop_assert_eq!(last, &expected);
    }
}
