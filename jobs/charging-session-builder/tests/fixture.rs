//! Replay the vendored sample through the job and compare with the vendored parity golden.

use std::collections::BTreeMap;
use std::path::PathBuf;

use charging_session_builder::reducer::{self, Event, SessionState};
use charging_session_builder::ChargingSessionBuilder;
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
    std::fs::read_to_string(contracts().join("fixtures/sample/vehicle.charging.events.v1.jsonl"))
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
    let schema = read_json(contracts().join("topics/charging.sessions.v1.schema.json"));
    jsonschema::options()
        .with_retriever(Vendored(common))
        .build(&schema)
        .unwrap()
}

/// Run records through the job as one transaction per record. Returns every (key, value) emitted.
fn replay(job: &mut ChargingSessionBuilder, payloads: &[String]) -> Vec<(String, String)> {
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

/// The effective output: the last record per session_id.
fn by_session(records: &[(String, String)]) -> BTreeMap<String, (String, String)> {
    records
        .iter()
        .map(|(k, v)| {
            let id: Value = serde_json::from_str(v).unwrap();
            (
                id["session_id"].as_str().unwrap().to_string(),
                (k.clone(), v.clone()),
            )
        })
        .collect()
}

#[test]
fn sample_matches_the_parity_golden_byte_for_byte() {
    let input = sample();
    assert_eq!(input.len(), 288);
    let emitted = replay(&mut ChargingSessionBuilder::default(), &input);

    let golden: Vec<(String, String)> =
        std::fs::read_to_string(contracts().join("golden/charging-session-builder/output.jsonl"))
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
    let rust = by_session(&emitted);
    assert_eq!(rust, by_session(&golden));
    assert_eq!(rust.len(), 11);
    assert_eq!(emitted.len(), rust.len(), "each session is emitted once");

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
    let mut job = ChargingSessionBuilder::default();
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
    let mut restored = ChargingSessionBuilder::default();
    for c in &changelog {
        restored.stores()[0]
            .restore(c.partition, &c.key, c.value.as_deref())
            .unwrap();
    }
    assert_eq!(restored.stores()[0].len(), job.stores()[0].len());
    assert!(
        replay(&mut restored, &input).is_empty(),
        "a full re-delivery after restore must not emit a session again"
    );
}

const VIN: &str = "SYNTESTVIN0000001";

fn event(id: usize, t_ms: i64, ty: &str) -> Event {
    Event {
        event_id: format!("e{id}"),
        event_time_ms: t_ms,
        event_type: Some(ty.into()),
        producer_seq: id as i64 + 1,
        fleet: Some("robotaxi".into()),
        market: Some("us-austin".into()),
        connector_type: Some("CCS1".into()),
        charger_type: Some("dc_fast".into()),
        power_kw: (ty != "heartbeat").then_some(50.0 + id as f64),
        energy_added_kwh: (ty != "heartbeat").then_some(id as f64 * 1.5),
        soc_pct: (ty != "heartbeat").then_some(20.0 + id as f64),
        charger_lat: (ty != "heartbeat").then_some(30.26715),
        charger_lon: (ty != "heartbeat").then_some(-97.74306),
    }
}

fn payload(e: &Event) -> String {
    serde_json::json!({"event_id": e.event_id, "vin": VIN, "event_time": reducer::format_ts(e.event_time_ms),
        "event_type": e.event_type, "producer_seq": e.producer_seq, "fleet": e.fleet, "market": e.market,
        "connector_type": e.connector_type, "charger_type": e.charger_type, "power_kw": e.power_kw,
        "energy_added_kwh": e.energy_added_kwh, "soc_pct": e.soc_pct, "charger_lat": e.charger_lat,
        "charger_lon": e.charger_lon})
    .to_string()
}

proptest! {
    /// Sessions in producer order (separated by more than the gap plus 300 s), delivered with up to
    /// 300 s of lateness and duplicates, then a heartbeat far ahead: the job emits exactly the sessions
    /// that one batch of all events produces, each once.
    #[test]
    fn arrival_order_does_not_change_the_result(
        sessions in prop::collection::vec((1usize..8, any::<bool>(), 1_860_000i64..7_200_000), 1..5),
        steps in prop::collection::vec(1i64..300_000, 40),
        lateness in prop::collection::vec(0i64..=300_000, 60),
        dup in prop::collection::vec(any::<bool>(), 60),
    ) {
        let mut t = 1_790_000_000_000i64;
        let mut events = Vec::new();
        for (progress, stopped, gap) in &sessions {
            t += gap;
            events.push(event(events.len(), t, "charge_start"));
            for _ in 0..*progress {
                t += steps[events.len() % 40];
                events.push(event(events.len(), t, "charge_progress"));
            }
            if *stopped {
                t += steps[(events.len() + 3) % 40];
                events.push(event(events.len(), t, "charge_stop"));
            }
        }
        let mut arrivals: Vec<(i64, usize)> = Vec::new();
        for (i, e) in events.iter().enumerate() {
            arrivals.push((e.event_time_ms + lateness[i % 60], i));
            if dup[i % 60] {
                arrivals.push((e.event_time_ms + lateness[(i + 7) % 60], i));
            }
        }
        arrivals.sort();
        let mut payloads: Vec<String> = arrivals.iter().map(|(_, i)| payload(&events[*i])).collect();
        payloads.push(payload(&event(events.len(), t + 2 * reducer::GAP_MS, "heartbeat")));

        let out = replay(&mut ChargingSessionBuilder::default(), &payloads);
        let (_, expected, _) = reducer::advance(&SessionState::empty(VIN), &events, i64::MAX).unwrap();
        let expected: Vec<String> = expected.iter().map(|s| serde_json::to_string(s).unwrap()).collect();
        let got: Vec<String> = out.into_iter().map(|(_, v)| v).collect();
        prop_assert_eq!(got, expected);
    }
}
