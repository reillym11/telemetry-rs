//! The legacy session logic, ported exactly (Sessions.scala `advance`, `segment`, `build`, and
//! Ids.scala `sessionId`).

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

pub const GAP_MS: i64 = 30 * 60 * 1000;

/// `Ids.Namespace`: UUIDv5(NAMESPACE_DNS, "contracts.telemetry.example").
const NAMESPACE: [u8; 16] = [
    0xf5, 0x17, 0x9d, 0x15, 0x0f, 0xd7, 0x5b, 0x5f, 0xbf, 0x53, 0x08, 0x52, 0xeb, 0x22, 0xd3, 0xd5,
];

/// The fields of a vehicle.charging.events.v1 record the job reads. Other fields are ignored.
#[derive(Debug, Deserialize)]
struct RawEvent {
    event_id: String,
    vin: String,
    event_time: String,
    event_type: Option<String>,
    producer_seq: i64,
    fleet: Option<String>,
    market: Option<String>,
    connector_type: Option<String>,
    charger_type: Option<String>,
    power_kw: Option<f64>,
    energy_added_kwh: Option<f64>,
    soc_pct: Option<f64>,
    charger_lat: Option<f64>,
    charger_lon: Option<f64>,
}

/// One decoded input record. Heartbeats are kept: they advance the VIN's highest event time.
#[derive(Debug, Clone, PartialEq)]
pub struct Input {
    pub vin: String,
    pub event: Event,
}

/// A charging event without its VIN (the VIN is the state key and a field of [`SessionState`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub event_id: String,
    /// Epoch milliseconds, as Spark's `Timestamp.getTime`.
    pub event_time_ms: i64,
    pub event_type: Option<String>,
    pub producer_seq: i64,
    pub fleet: Option<String>,
    pub market: Option<String>,
    pub connector_type: Option<String>,
    pub charger_type: Option<String>,
    pub power_kw: Option<f64>,
    pub energy_added_kwh: Option<f64>,
    pub soc_pct: Option<f64>,
    pub charger_lat: Option<f64>,
    pub charger_lon: Option<f64>,
}

impl Event {
    fn is(&self, t: &str) -> bool {
        self.event_type.as_deref() == Some(t)
    }
}

/// Per-VIN state (`SessionState`): events not yet emitted, highest event time seen (heartbeats too),
/// end of the last emitted session. This is also the changelog value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionState {
    pub vin: String,
    pub pending: Vec<Event>,
    pub max_seen_ms: i64,
    pub emitted_until_ms: i64,
}

impl SessionState {
    /// `Sessions.Empty`.
    pub fn empty(vin: &str) -> Self {
        SessionState {
            vin: vin.to_string(),
            pending: Vec::new(),
            max_seen_ms: i64::MIN,
            emitted_until_ms: i64::MIN,
        }
    }
}

/// One charging.sessions.v1 value. Field order matches the legacy JSON.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ChargingSession {
    pub session_id: String,
    pub vin: String,
    pub fleet: Option<String>,
    pub market: Option<String>,
    pub connector_type: Option<String>,
    pub charger_type: Option<String>,
    pub start_time: String,
    pub end_time: String,
    pub close_reason: String,
    pub energy_added_kwh: f64,
    pub peak_power_kw: f64,
    pub start_soc_pct: f64,
    pub end_soc_pct: f64,
    pub event_count: i64,
    pub charger_lat: f64,
    pub charger_lon: f64,
}

pub fn decode(payload: &[u8]) -> Result<Input> {
    let e: RawEvent =
        serde_json::from_slice(payload).context("decode vehicle.charging.events.v1")?;
    let t = DateTime::parse_from_rfc3339(&e.event_time).context("event_time")?;
    Ok(Input {
        vin: e.vin,
        event: Event {
            event_id: e.event_id,
            event_time_ms: t.timestamp_millis(),
            event_type: e.event_type,
            producer_seq: e.producer_seq,
            fleet: e.fleet,
            market: e.market,
            connector_type: e.connector_type,
            charger_type: e.charger_type,
            power_kw: e.power_kw,
            energy_added_kwh: e.energy_added_kwh,
            soc_pct: e.soc_pct,
            charger_lat: e.charger_lat,
            charger_lon: e.charger_lon,
        },
    })
}

/// `order`: a stable sort by (event_time, producer_seq).
fn order(es: &mut [Event]) {
    es.sort_by_key(|e| (e.event_time_ms, e.producer_seq));
}

/// A run of events that forms one session (`Sessions.Run`).
#[derive(Debug, Clone, PartialEq)]
pub struct Run {
    pub events: Vec<Event>,
    pub stopped: bool,
}

impl Run {
    fn last(&self) -> i64 {
        self.events
            .last()
            .expect("a run is never empty")
            .event_time_ms
    }
}

/// `Sessions.segment`: splits events into runs and stray events (outside any session).
pub fn segment(events: &[Event]) -> (Vec<Run>, Vec<Event>) {
    let mut sorted = events.to_vec();
    order(&mut sorted);
    let mut runs = Vec::new();
    let mut stray = Vec::new();
    let mut open: Option<Vec<Event>> = None;
    fn close(open: &mut Option<Vec<Event>>, runs: &mut Vec<Run>, stopped: bool) {
        if let Some(b) = open.take() {
            runs.push(Run { events: b, stopped });
        }
    }
    for e in sorted {
        if e.is("charge_start") {
            close(&mut open, &mut runs, false);
            open = Some(vec![e]);
            continue;
        }
        match open.as_mut() {
            Some(b) if e.event_time_ms - b.last().unwrap().event_time_ms <= GAP_MS => {
                let stop = e.is("charge_stop");
                b.push(e);
                if stop {
                    close(&mut open, &mut runs, true);
                }
            }
            Some(_) => {
                close(&mut open, &mut runs, false);
                stray.push(e);
            }
            None => stray.push(e),
        }
    }
    close(&mut open, &mut runs, false);
    (runs, stray)
}

fn required(v: Option<f64>, field: &str) -> Result<f64> {
    // Scala's Option.get throws on a null here; the schema forbids it on non-heartbeat events.
    v.ok_or_else(|| anyhow!("{field} is null on a session event"))
}

/// `Sessions.build`.
pub fn build(vin: &str, run: &Run) -> Result<ChargingSession> {
    let start = &run.events[0];
    let last = run.events.last().expect("a run is never empty");
    let peak = run
        .events
        .iter()
        .filter_map(|e| e.power_kw)
        .reduce(f64::max)
        .ok_or_else(|| anyhow!("power_kw is null on every session event"))?;
    let mut ids: Vec<&str> = run.events.iter().map(|e| e.event_id.as_str()).collect();
    ids.sort_unstable();
    ids.dedup();
    Ok(ChargingSession {
        session_id: session_id(vin, &start.event_id),
        vin: vin.to_string(),
        fleet: start.fleet.clone(),
        market: start.market.clone(),
        connector_type: start.connector_type.clone(),
        charger_type: start.charger_type.clone(),
        start_time: format_ts(start.event_time_ms),
        end_time: format_ts(last.event_time_ms),
        close_reason: if run.stopped {
            "charge_stop"
        } else {
            "gap_timeout"
        }
        .to_string(),
        energy_added_kwh: required(last.energy_added_kwh, "energy_added_kwh")?,
        peak_power_kw: peak,
        start_soc_pct: required(start.soc_pct, "soc_pct")?,
        end_soc_pct: required(last.soc_pct, "soc_pct")?,
        event_count: ids.len() as i64,
        charger_lat: required(start.charger_lat, "charger_lat")?,
        charger_lon: required(start.charger_lon, "charger_lon")?,
    })
}

/// `Sessions.advance`: adds `events` (any type, any order) and emits every session whose last event
/// is more than GAP_MS before the horizon, the larger of the VIN's highest event time and
/// `watermark_ms`. Returns the new state, the emitted sessions, and the legacy event-time timeout.
pub fn advance(
    state: &SessionState,
    events: &[Event],
    watermark_ms: i64,
) -> Result<(SessionState, Vec<ChargingSession>, Option<i64>)> {
    let max_seen = events
        .iter()
        .map(|e| e.event_time_ms)
        .fold(state.max_seen_ms, i64::max);
    let mut pending = state.pending.clone();
    for e in events {
        let known = pending.iter().any(|p| p.event_id == e.event_id);
        if !e.is("heartbeat") && e.event_time_ms > state.emitted_until_ms && !known {
            pending.push(e.clone());
        }
    }
    let horizon = max_seen.max(watermark_ms);

    let (runs, stray) = segment(&pending);
    let (done, open): (Vec<Run>, Vec<Run>) = runs
        .into_iter()
        .partition(|r| horizon.saturating_sub(r.last()) > GAP_MS);
    let kept_stray: Vec<Event> = stray
        .into_iter()
        .filter(|e| horizon.saturating_sub(e.event_time_ms) <= GAP_MS)
        .collect();
    let emitted_until = done
        .iter()
        .map(Run::last)
        .fold(state.emitted_until_ms, i64::max);
    let timeout = open
        .iter()
        .map(Run::last)
        .chain(kept_stray.iter().map(|e| e.event_time_ms))
        .min()
        .map(|t| t + GAP_MS);
    let mut next_pending: Vec<Event> = open.into_iter().flat_map(|r| r.events).collect();
    next_pending.extend(kept_stray);
    order(&mut next_pending);
    let out = done
        .iter()
        .map(|r| build(&state.vin, r))
        .collect::<Result<Vec<_>>>()?;
    let next = SessionState {
        vin: state.vin.clone(),
        pending: next_pending,
        max_seen_ms: max_seen,
        emitted_until_ms: emitted_until,
    };
    Ok((next, out, timeout))
}

/// `Ids.sessionId`: UUIDv5 over `vin + "|" + lowercase(charge_start event_id)`, UTF-8.
pub fn session_id(vin: &str, charge_start_event_id: &str) -> String {
    let mut h = Sha1::new();
    h.update(NAMESPACE);
    h.update(format!("{vin}|{}", charge_start_event_id.to_lowercase()).as_bytes());
    let d = h.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&d[..16]);
    b[6] = (b[6] & 0x0f) | 0x50;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// RFC 3339 UTC with exactly three fractional digits and `Z`, as the legacy job writes.
pub fn format_ts(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("event_time is within chrono's range")
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIN: &str = "SYNTESTVIN0000001";
    const T0: i64 = 1_790_000_000_000;
    const MIN: i64 = 60_000;

    fn ev(id: &str, t: i64, ty: &str, seq: i64) -> Event {
        let hb = ty == "heartbeat";
        Event {
            event_id: id.into(),
            event_time_ms: t,
            event_type: Some(ty.into()),
            producer_seq: seq,
            fleet: Some("robotaxi".into()),
            market: Some("us-austin".into()),
            connector_type: Some("CCS1".into()),
            charger_type: Some("dc_fast".into()),
            power_kw: (!hb).then_some(100.0 + seq as f64),
            energy_added_kwh: (!hb).then_some(seq as f64),
            soc_pct: (!hb).then_some(20.0 + seq as f64),
            charger_lat: (!hb).then_some(30.26715),
            charger_lon: (!hb).then_some(-97.74306),
        }
    }

    /// Feeds events one at a time, as the job does. Returns the final state and all sessions.
    fn feed(events: &[Event]) -> (SessionState, Vec<ChargingSession>) {
        let mut s = SessionState::empty(VIN);
        let mut out = Vec::new();
        for e in events {
            let (n, o, _) = advance(&s, std::slice::from_ref(e), i64::MIN).unwrap();
            s = n;
            out.extend(o);
        }
        (s, out)
    }

    #[test]
    fn a_gap_of_exactly_thirty_minutes_stays_in_the_session() {
        let (runs, stray) = segment(&[
            ev("a", T0, "charge_start", 1),
            ev("b", T0 + GAP_MS, "charge_progress", 2),
            ev("c", T0 + 2 * GAP_MS + 1, "charge_progress", 3),
        ]);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].events.len(), 2);
        assert!(!runs[0].stopped);
        assert_eq!(stray.len(), 1);
    }

    #[test]
    fn a_stopped_session_waits_for_the_gap_to_pass() {
        let (_, out) = feed(&[
            ev("a", T0, "charge_start", 1),
            ev("b", T0 + MIN, "charge_stop", 2),
            ev("h", T0 + MIN + GAP_MS, "heartbeat", 3),
        ]);
        assert!(
            out.is_empty(),
            "the horizon must be more than 30 minutes past"
        );
        let (_, out) = feed(&[
            ev("a", T0, "charge_start", 1),
            ev("b", T0 + MIN, "charge_stop", 2),
            ev("h", T0 + MIN + GAP_MS + 1, "heartbeat", 3),
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].close_reason, "charge_stop");
        assert_eq!(out[0].event_count, 2);
    }

    #[test]
    fn output_fields_come_from_the_first_and_last_events() {
        let mut first = ev("Start-1", T0, "charge_start", 1);
        first.power_kw = Some(250.0);
        let (_, out) = feed(&[
            first,
            ev("p", T0 + MIN, "charge_progress", 2),
            ev("s", T0 + 2 * MIN, "charge_stop", 3),
            ev("h", T0 + 40 * MIN, "heartbeat", 4),
        ]);
        let s = &out[0];
        assert_eq!(s.session_id, session_id(VIN, "start-1"));
        assert_eq!(s.start_time, format_ts(T0));
        assert_eq!(s.end_time, format_ts(T0 + 2 * MIN));
        assert_eq!(s.start_soc_pct, 21.0);
        assert_eq!(s.end_soc_pct, 23.0);
        assert_eq!(s.energy_added_kwh, 3.0);
        assert_eq!(s.peak_power_kw, 250.0);
        assert_eq!((s.charger_lat, s.charger_lon), (30.26715, -97.74306));
    }

    #[test]
    fn a_late_event_joins_its_session_in_event_time_order() {
        let (_, out) = feed(&[
            ev("a", T0, "charge_start", 1),
            ev("c", T0 + 2 * MIN, "charge_stop", 3),
            ev("b", T0 + MIN, "charge_progress", 2),
            ev("h", T0 + 40 * MIN, "heartbeat", 4),
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_count, 3);
        assert_eq!(out[0].close_reason, "charge_stop");
    }

    #[test]
    fn a_late_start_claims_the_pending_strays() {
        let (_, out) = feed(&[
            ev("b", T0 + MIN, "charge_progress", 2),
            ev("a", T0, "charge_start", 1),
            ev("h", T0 + 40 * MIN, "heartbeat", 3),
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_count, 2);
        assert_eq!(out[0].start_time, format_ts(T0));
    }

    #[test]
    fn duplicates_are_counted_once_and_dropped_after_emission() {
        let events = [
            ev("a", T0, "charge_start", 1),
            ev("a", T0, "charge_start", 1),
            ev("b", T0 + MIN, "charge_stop", 2),
            ev("h", T0 + 40 * MIN, "heartbeat", 3),
            ev("a", T0, "charge_start", 1),
            ev("b", T0 + MIN, "charge_stop", 2),
            ev("h2", T0 + 80 * MIN, "heartbeat", 4),
        ];
        let (s, out) = feed(&events);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_count, 2);
        assert!(s.pending.is_empty());
        assert_eq!(s.emitted_until_ms, T0 + MIN);
    }

    #[test]
    fn a_late_event_at_or_before_the_last_emitted_end_is_dropped() {
        let (s, out) = feed(&[
            ev("a", T0, "charge_start", 1),
            ev("c", T0 + 2 * MIN, "charge_stop", 3),
            ev("h", T0 + 40 * MIN, "heartbeat", 4),
            ev("b", T0 + MIN, "charge_progress", 2),
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_count, 2);
        assert!(s.pending.is_empty());
    }

    #[test]
    fn a_new_start_closes_the_open_session_as_gap_timeout() {
        let (_, out) = feed(&[
            ev("a", T0, "charge_start", 1),
            ev("b", T0 + MIN, "charge_progress", 2),
            ev("c", T0 + 2 * MIN, "charge_start", 3),
            ev("d", T0 + 3 * MIN, "charge_stop", 4),
            ev("h", T0 + 40 * MIN, "heartbeat", 5),
        ]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].close_reason, "gap_timeout");
        assert_eq!(out[0].event_count, 2);
        assert_eq!(out[1].close_reason, "charge_stop");
        assert!(out[0].start_time < out[1].start_time);
    }

    #[test]
    fn a_session_without_a_stop_closes_by_gap() {
        let (_, out) = feed(&[
            ev("a", T0, "charge_start", 1),
            ev("b", T0 + MIN, "charge_progress", 2),
            ev("h", T0 + MIN + GAP_MS + 1, "heartbeat", 3),
        ]);
        assert_eq!(out[0].close_reason, "gap_timeout");
        assert_eq!(out[0].end_time, format_ts(T0 + MIN));
    }

    #[test]
    fn strays_expire_after_the_gap_and_are_never_emitted() {
        let (s, out) = feed(&[
            ev("x", T0, "charge_progress", 1),
            ev("h", T0 + GAP_MS, "heartbeat", 2),
        ]);
        assert!(out.is_empty());
        assert_eq!(s.pending.len(), 1);
        let (s, out) = feed(&[
            ev("x", T0, "charge_progress", 1),
            ev("h", T0 + GAP_MS + 1, "heartbeat", 2),
        ]);
        assert!(out.is_empty());
        assert!(s.pending.is_empty());
    }

    #[test]
    fn equal_time_orders_by_producer_seq_then_arrival() {
        let mut stop = ev("z", T0 + MIN, "charge_stop", 2);
        stop.energy_added_kwh = Some(9.0);
        let mut progress = ev("y", T0 + MIN, "charge_progress", 3);
        progress.energy_added_kwh = Some(7.0);
        let (_, out) = feed(&[
            ev("a", T0, "charge_start", 1),
            progress.clone(),
            stop.clone(),
            ev("h", T0 + 40 * MIN, "heartbeat", 4),
        ]);
        // producer_seq 2 (the stop) sorts first and closes the run; the progress is stray.
        assert_eq!(out[0].energy_added_kwh, 9.0);
        assert_eq!(out[0].event_count, 2);

        let mut a = ev("p", T0 + MIN, "charge_progress", 2);
        a.energy_added_kwh = Some(1.0);
        let mut b = ev("q", T0 + MIN, "charge_progress", 2);
        b.energy_added_kwh = Some(2.0);
        let gap = ev("h", T0 + 40 * MIN, "heartbeat", 4);
        let start = ev("a", T0, "charge_start", 1);
        let (_, first) = feed(&[start.clone(), a.clone(), b.clone(), gap.clone()]);
        let (_, second) = feed(&[start, b, a, gap]);
        assert_eq!(
            first[0].energy_added_kwh, 2.0,
            "equal rank keeps arrival order"
        );
        assert_eq!(second[0].energy_added_kwh, 1.0);
    }

    #[test]
    fn heartbeats_and_null_event_types() {
        let mut odd = ev("n", T0 + MIN, "charge_progress", 2);
        odd.event_type = None;
        let (_, out) = feed(&[
            ev("a", T0, "charge_start", 1),
            odd,
            ev("hb", T0 + 2 * MIN, "heartbeat", 3),
            ev("h", T0 + 40 * MIN, "heartbeat", 4),
        ]);
        assert_eq!(
            out[0].event_count, 2,
            "a null type joins, a heartbeat does not"
        );
    }

    #[test]
    fn null_session_fields_fail_as_the_legacy_job_does() {
        let mut start = ev("a", T0, "charge_start", 1);
        start.soc_pct = None;
        let s = SessionState::empty(VIN);
        let (s, _, _) = advance(&s, &[start], i64::MIN).unwrap();
        assert!(advance(&s, &[ev("h", T0 + 40 * MIN, "heartbeat", 2)], i64::MIN).is_err());
    }

    #[test]
    fn the_watermark_also_advances_the_horizon_and_sets_the_timeout() {
        let s = SessionState::empty(VIN);
        let (s, out, timeout) = advance(&s, &[ev("a", T0, "charge_start", 1)], i64::MIN).unwrap();
        assert!(out.is_empty());
        assert_eq!(timeout, Some(T0 + GAP_MS));
        let (_, out, timeout) = advance(&s, &[], T0 + GAP_MS + 1).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(timeout, None);
    }

    #[test]
    fn session_id_is_uuid_v5_of_vin_and_lowercase_start_id() {
        let id = session_id(VIN, "ABC");
        assert_eq!(id, session_id(VIN, "abc"));
        assert_eq!(&id[14..15], "5");
        assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"));
    }

    #[test]
    fn decode_keeps_heartbeats_and_parses_millis() {
        let i = decode(br#"{"event_id":"e","vin":"V","event_time":"2026-10-01T00:00:00.123Z","event_type":"heartbeat","producer_seq":1}"#).unwrap();
        assert_eq!(i.event.event_time_ms % 1000, 123);
        assert!(i.event.is("heartbeat"));
        assert_eq!(format_ts(i.event.event_time_ms), "2026-10-01T00:00:00.123Z");
    }
}
