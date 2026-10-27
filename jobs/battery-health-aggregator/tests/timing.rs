//! Emission timing against the legacy job. The expected rows are what the legacy Spark job wrote for
//! the same input in two AvailableNow runs on one checkpoint: VIN A stops sending after 00:55 while
//! VIN B continues. See SEMANTICS.md, State model and emission timing.

use battery_health_aggregator::reducer::{format_ts, HourlyHealth};
use battery_health_aggregator::BatteryHealthAggregator;
use telemetry_core::Job;

const H0: i64 = 1_790_812_800_000; // 2026-10-01T00:00:00Z
const MIN: i64 = 60_000;
const A: &str = "SYNTIMINGA0000001";
const B: &str = "SYNTIMINGB0000002";

fn ev(vin: &str, minute: i64, seq: usize, soh: f64, delta: i64, temp: f64) -> String {
    serde_json::json!({"event_id": format!("{vin}-{seq}"), "vin": vin,
        "event_time": format_ts(H0 + minute * MIN), "event_type": "battery_sample",
        "producer_seq": seq, "soh_pct": soh, "cell_voltage_delta_mv": delta, "pack_temp_c": temp})
    .to_string()
}

/// One batch, as the processor runs it: every record, then `end_batch`.
fn batch(job: &mut BatteryHealthAggregator, payloads: &[String]) -> Vec<HourlyHealth> {
    for p in payloads {
        let input = job.decode(p.as_bytes()).unwrap().unwrap();
        for s in job.stores() {
            s.set_partition(0);
        }
        assert!(job.process(input).unwrap().is_empty());
    }
    let out = job.end_batch().unwrap();
    for s in job.stores() {
        s.take_changelog();
        s.commit();
    }
    out
}

type Row = (String, i64, i32, f64, f64, i32, i32, f64);

fn rows(mut out: Vec<HourlyHealth>) -> Vec<Row> {
    out.sort_by(|a, b| (&a.vin, &a.window_start).cmp(&(&b.vin, &b.window_start)));
    out.into_iter()
        .map(|h| {
            let hour = (chrono::DateTime::parse_from_rfc3339(&h.window_start)
                .unwrap()
                .timestamp_millis()
                - H0)
                / (60 * MIN);
            (
                h.vin,
                hour,
                h.sample_count,
                h.soh_pct_min.unwrap(),
                h.soh_pct_avg.unwrap(),
                h.cell_voltage_delta_mv_min,
                h.cell_voltage_delta_mv_max,
                h.pack_temp_c_max.unwrap(),
            )
        })
        .collect()
}

#[test]
fn a_vin_that_stops_sending_is_emitted_when_the_watermark_passes_its_hour() {
    let mut job = BatteryHealthAggregator::default();
    let first: Vec<String> = (0..6)
        .map(|i| {
            ev(
                A,
                5 + 10 * i as i64,
                i,
                95.0 + i as f64 * 0.01,
                10 + i as i64,
                25.0 + i as f64 * 0.1,
            )
        })
        .chain((0..9).map(|i| {
            ev(
                B,
                5 + 10 * i as i64,
                i,
                90.0 + i as f64 * 0.01,
                20 + i as i64,
                30.0 + i as f64 * 0.1,
            )
        }))
        .collect();
    // Legacy run 1: A's hour 00 by timeout (A's last sample is 00:55, the watermark is 01:15), and
    // B's hour 00 by B's own data (01:25).
    assert_eq!(
        rows(batch(&mut job, &first)),
        vec![
            (A.into(), 0, 6, 95.00, 95.03, 10, 15, 25.5),
            (B.into(), 0, 6, 90.00, 90.03, 20, 25, 30.5),
        ]
    );
    let second: Vec<String> = (0..5)
        .map(|i| ev(B, 95 + 10 * i as i64, 9 + i, 90.5, 30, 31.0))
        .chain([
            ev(A, 58, 100, 80.0, 99, 40.0),
            ev(A, 80, 101, 94.0, 5, 20.0),
        ])
        .collect();
    // Legacy run 2: B's hour 01 only. A's 00:58 sample is late for an emitted hour and dropped;
    // A's 01:20 sample stays pending (01:20 + 50 min is past the 02:05 watermark).
    assert_eq!(
        rows(batch(&mut job, &second)),
        vec![(B.into(), 1, 6, 90.06, 90.29, 26, 30, 31.0)]
    );
    assert!(
        batch(&mut job, &[]).is_empty(),
        "a no-data batch emits nothing more"
    );
}
