//! Emission timing against the legacy job. The expected sessions are what the legacy Spark job
//! emitted for the same input (two AvailableNow runs on one checkpoint, see SEMANTICS.md).

use charging_session_builder::reducer::format_ts;
use charging_session_builder::ChargingSessionBuilder;
use serde_json::Value;
use telemetry_core::Job;

const T0: i64 = 1_790_812_800_000; // 2026-10-01T00:00:00Z
const MIN: i64 = 60_000;

fn ev(vin: &str, ty: &str, minute: i64, seq: usize) -> String {
    let power = (ty != "heartbeat").then_some(50.0);
    serde_json::json!({"event_id": format!("{vin}-{seq}"), "vin": vin,
        "event_time": format_ts(T0 + minute * MIN), "event_type": ty, "producer_seq": seq,
        "fleet": "robotaxi", "market": "us-austin", "connector_type": "CCS1", "charger_type": "dc_fast",
        "power_kw": power, "energy_added_kwh": power.map(|_| seq as f64), "soc_pct": power.map(|_| 40.0),
        "charger_lat": power.map(|_| 30.26715), "charger_lon": power.map(|_| -97.74306)})
    .to_string()
}

/// One batch, as the processor runs it: every record, then `end_batch`. Returns
/// (vin, start_time, end_time, close_reason, event_count) per emitted session.
fn batch(
    job: &mut ChargingSessionBuilder,
    payloads: &[String],
) -> Vec<(String, String, String, String, i64)> {
    let mut out = Vec::new();
    for p in payloads {
        let input = job.decode(p.as_bytes()).unwrap().unwrap();
        for s in job.stores() {
            s.set_partition(0);
        }
        out.extend(job.process(input).unwrap());
    }
    out.extend(job.end_batch().unwrap());
    for s in job.stores() {
        s.take_changelog();
        s.commit();
    }
    out.iter()
        .map(|s| {
            let v: Value = serde_json::to_value(s).unwrap();
            let f = |k: &str| v[k].as_str().unwrap().to_string();
            (
                f("vin"),
                f("start_time"),
                f("end_time"),
                f("close_reason"),
                v["event_count"].as_i64().unwrap(),
            )
        })
        .collect()
}

fn session(
    vin: &str,
    start: &str,
    end: &str,
    reason: &str,
    n: i64,
) -> (String, String, String, String, i64) {
    (vin.into(), start.into(), end.into(), reason.into(), n)
}

#[test]
fn offline_vehicles_are_emitted_by_the_watermark_and_late_events_are_dropped() {
    let mut job = ChargingSessionBuilder::default();

    // VIN_A charges at 0 to 5 min and goes offline, VIN_C charges at 100 to 110 min and goes
    // offline, VIN_B sends heartbeats until 180 min. Legacy emitted A and C in the same run.
    let mut run1 = vec![
        ev("VIN_A", "charge_start", 0, 1),
        ev("VIN_A", "charge_progress", 5, 2),
    ];
    run1.extend((0..=36).map(|i| ev("VIN_B", "heartbeat", i * 5, i as usize + 1)));
    run1.extend([
        ev("VIN_C", "charge_start", 100, 1),
        ev("VIN_C", "charge_progress", 105, 2),
        ev("VIN_C", "charge_stop", 110, 3),
    ]);
    let mut got = batch(&mut job, &run1);
    got.sort();
    assert_eq!(
        got,
        vec![
            session(
                "VIN_A",
                "2026-10-01T00:00:00.000Z",
                "2026-10-01T00:05:00.000Z",
                "gap_timeout",
                2
            ),
            session(
                "VIN_C",
                "2026-10-01T01:40:00.000Z",
                "2026-10-01T01:50:00.000Z",
                "charge_stop",
                3
            ),
        ]
    );
    assert_eq!(job.watermark_ms(), T0 + 170 * MIN);

    // VIN_D's events at 60 to 70 min arrive after the watermark (170 min) passed them. Legacy dropped
    // them, so VIN_D's heartbeat at 185 min emits nothing.
    let mut run2 = vec![
        ev("VIN_D", "charge_start", 60, 1),
        ev("VIN_D", "charge_progress", 65, 2),
        ev("VIN_D", "charge_stop", 70, 3),
        ev("VIN_D", "heartbeat", 185, 4),
    ];
    run2.extend((37..=40).map(|i| ev("VIN_B", "heartbeat", i * 5, i as usize + 1)));
    assert_eq!(batch(&mut job, &run2), vec![]);
    assert_eq!(job.watermark_ms(), T0 + 190 * MIN);
}

#[test]
fn a_session_is_not_emitted_before_the_watermark_passes_its_timeout() {
    let mut job = ChargingSessionBuilder::default();
    batch(
        &mut job,
        &[
            ev("VIN_A", "charge_start", 0, 1),
            ev("VIN_A", "charge_progress", 4, 2),
        ],
    );
    // Timeout is 4 + 30 = 34 min. A watermark of exactly 34 min (max 44) does not emit.
    assert_eq!(batch(&mut job, &[ev("VIN_B", "heartbeat", 44, 1)]), vec![]);
    let got = batch(&mut job, &[ev("VIN_B", "heartbeat", 45, 2)]);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].0, "VIN_A");
}
