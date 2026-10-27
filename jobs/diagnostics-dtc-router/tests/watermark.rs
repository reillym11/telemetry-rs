//! Late-row dropping and dedup expiry against the legacy job. The expected alerts are what the
//! legacy Spark job emitted for the same input: three AvailableNow runs on one checkpoint (see
//! SEMANTICS.md, "Timing evidence").

use diagnostics_dtc_router::reducer::format_ts;
use diagnostics_dtc_router::DiagnosticsDtcRouter;
use serde_json::Value;
use telemetry_core::Job;

const T0: i64 = 1_790_812_800_000; // 2026-10-01T00:00:00Z
const MIN: i64 = 60_000;
const A: &str = "SYNTIMING00000VNA";
const B: &str = "SYNTIMING00000VNB";
const C: &str = "SYNTIMING00000VNC";
const D: &str = "SYNTIMING00000VND";

fn ev(id: &str, vin: &str, minute: i64, event_type: &str, severity: i64) -> String {
    serde_json::json!({"event_id": id, "vin": vin, "event_time": format_ts(T0 + minute * MIN),
        "event_type": event_type, "producer_seq": 1, "fleet": "robotaxi", "market": "us-austin",
        "vehicle_model": "GRAVITY", "firmware_version": "2026.9.2", "schema_version": "1",
        "odometer_km": 1000, "dtc_code": "P0A80", "ecu": "bms", "severity": severity,
        "status": "active", "lat": 30.26715, "lon": -97.74306})
    .to_string()
}

fn dtc(id: &str, vin: &str, minute: i64) -> String {
    ev(id, vin, minute, "dtc", 3)
}

/// One batch, as the processor runs it: every record, then `end_batch`. Returns (vin, event_time)
/// per emitted alert, sorted.
fn batch(job: &mut DiagnosticsDtcRouter, payloads: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for p in payloads {
        if let Some(input) = job.decode(p.as_bytes()).unwrap() {
            for s in job.stores() {
                s.set_partition(0);
            }
            out.extend(job.process(input).unwrap());
        }
    }
    assert!(job.end_batch().unwrap().is_empty(), "end_batch never emits");
    for s in job.stores() {
        s.take_changelog();
        s.commit();
    }
    let mut got: Vec<(String, String)> = out
        .iter()
        .map(|a| {
            let v: Value = serde_json::to_value(a).unwrap();
            (
                v["vin"].as_str().unwrap().to_string(),
                v["event_time"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    got.sort();
    got
}

fn alert(vin: &str, minute: i64) -> (String, String) {
    (vin.to_string(), format_ts(T0 + minute * MIN))
}

#[test]
fn late_rows_are_dropped_and_dedup_state_expires_as_in_legacy() {
    let mut job = DiagnosticsDtcRouter::default();

    // Run 1: alerts are emitted at once; the duplicate e1 is dropped. The heartbeat at 200 min and
    // the severity 2 row at 300 min do not move the watermark: it becomes 60 - 10 = 50 min.
    let run1 = [
        dtc("e1", A, 0),
        dtc("e2", A, 45),
        dtc("e3", B, 60),
        dtc("e1", A, 0),
        ev("h1", B, 200, "heartbeat", 3),
        ev("s1", C, 300, "dtc", 2),
    ];
    assert_eq!(
        batch(&mut job, &run1),
        vec![alert(A, 0), alert(A, 45), alert(B, 60)]
    );
    assert_eq!(job.watermark_ms(), T0 + 50 * MIN);
    assert_eq!(job.seen_len(), 2, "e1 expired at 10 min <= 50 min");

    // Run 2: e4 at exactly the watermark is late. e1 again at 55 min alerts again because its
    // entry expired; e2 at 58 min is still in the dedup state; e3 is a duplicate.
    let run2 = [
        dtc("e4", C, 50),
        dtc("e5", C, 51),
        dtc("e1", A, 55),
        dtc("e2", A, 58),
        dtc("e3", B, 60),
        dtc("e6", D, 90),
    ];
    assert_eq!(
        batch(&mut job, &run2),
        vec![alert(A, 55), alert(C, 51), alert(D, 90)]
    );
    assert_eq!(job.watermark_ms(), T0 + 80 * MIN);

    // Run 3 (watermark 80 min): e5 again at 81 min alerts again (expired at 61 min); e6 is a
    // duplicate; e7 at 79 min and e8 at 80 min are late.
    let run3 = [
        dtc("e5", C, 81),
        dtc("e6", D, 90),
        dtc("e7", A, 79),
        dtc("e8", A, 80),
    ];
    assert_eq!(batch(&mut job, &run3), vec![alert(C, 81)]);
}

#[test]
fn the_watermark_does_not_move_within_a_batch() {
    let mut job = DiagnosticsDtcRouter::default();
    // One batch: the event at 100 min does not make the later-arriving event at 0 min late.
    assert_eq!(
        batch(&mut job, &[dtc("x1", A, 100), dtc("x2", B, 0)]),
        vec![alert(A, 100), alert(B, 0)]
    );
    assert_eq!(job.watermark_ms(), T0 + 90 * MIN);
    assert_eq!(batch(&mut job, &[dtc("x3", B, 90)]), vec![]);
}
