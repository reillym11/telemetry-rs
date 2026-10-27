//! The legacy session logic, ported exactly (Sessions.scala `advance`, `segment`, `build`; Ids.scala
//! `sessionId`). No Kafka types.

use std::collections::HashSet;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Sessions.GapMs`.
pub const GAP_MS: i64 = 30 * 60 * 1000;
/// `withWatermark("event_time", "10 minutes")`.
pub const WATERMARK_DELAY_MS: i64 = 10 * 60 * 1000;
/// Spark's watermark before the first batch has advanced it.
pub const INITIAL_WATERMARK_MS: i64 = 0;
/// `Ids.Namespace`.
const NAMESPACE: Uuid = Uuid::from_u128(0xf5179d15_0fd7_5b5f_bf53_0852eb22d3d5);

/// One vehicle.charging.events.v1 record, as the legacy `ChargingEvent` reads it. Heartbeats are kept:
/// they advance the VIN's highest event time and the watermark. This is also the state value for
/// pending and buffered events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChargingEvent {
    pub event_id: String,
    pub vin: String,
    /// Epoch milliseconds, as `Timestamp.getTime`.
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

#[derive(Deserialize)]
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

pub fn decode(payload: &[u8]) -> Result<ChargingEvent> {
    let r: RawEvent =
        serde_json::from_slice(payload).context("decode vehicle.charging.events.v1")?;
    let t = DateTime::parse_from_rfc3339(&r.event_time).context("event_time")?;
    Ok(ChargingEvent {
        event_id: r.event_id,
        vin: r.vin,
        event_time_ms: t.timestamp_millis(),
        event_type: r.event_type,
        producer_seq: r.producer_seq,
        fleet: r.fleet,
        market: r.market,
        connector_type: r.connector_type,
        charger_type: r.charger_type,
        power_kw: r.power_kw,
        energy_added_kwh: r.energy_added_kwh,
        soc_pct: r.soc_pct,
        charger_lat: r.charger_lat,
        charger_lon: r.charger_lon,
    })
}

impl ChargingEvent {
    fn is(&self, kind: &str) -> bool {
        self.event_type.as_deref() == Some(kind)
    }
}

/// Per-VIN state (`SessionState` plus the event-time timeout Spark keeps beside it). This is the
/// changelog value of the `sessions` store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionState {
    pub vin: String,
    /// Events not yet emitted, in (event_time, producer_seq) order.
    pub pending: Vec<ChargingEvent>,
    /// Highest event time seen for the VIN, heartbeats included.
    pub max_seen_ms: i64,
    /// End of the last emitted session.
    pub emitted_until_ms: i64,
    /// `GroupState.setTimeoutTimestamp`; fires once the watermark is strictly greater.
    pub timeout_ms: Option<i64>,
}

impl SessionState {
    /// `Sessions.Empty`.
    pub fn empty(vin: &str) -> Self {
        SessionState {
            vin: vin.to_string(),
            pending: Vec::new(),
            max_seen_ms: i64::MIN,
            emitted_until_ms: i64::MIN,
            timeout_ms: None,
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
    pub close_reason: &'static str,
    pub energy_added_kwh: f64,
    pub peak_power_kw: f64,
    pub start_soc_pct: f64,
    pub end_soc_pct: f64,
    pub event_count: i64,
    pub charger_lat: f64,
    pub charger_lon: f64,
}

struct Run {
    events: Vec<ChargingEvent>,
    stopped: bool,
}

impl Run {
    fn last(&self) -> i64 {
        self.events
            .last()
            .expect("a run is never empty")
            .event_time_ms
    }
}

/// `order`: a stable sort by (event_time, producer_seq).
fn order(es: &mut [ChargingEvent]) {
    es.sort_by_key(|e| (e.event_time_ms, e.producer_seq));
}

/// `segment`: split events into runs (charge_start, then progress/stop within the gap) and strays.
fn segment(mut events: Vec<ChargingEvent>) -> (Vec<Run>, Vec<ChargingEvent>) {
    order(&mut events);
    let mut runs = Vec::new();
    let mut stray = Vec::new();
    let mut open: Option<Vec<ChargingEvent>> = None;
    fn close(open: &mut Option<Vec<ChargingEvent>>, runs: &mut Vec<Run>, stopped: bool) {
        if let Some(events) = open.take() {
            runs.push(Run { events, stopped });
        }
    }
    for e in events {
        if e.is("charge_start") {
            close(&mut open, &mut runs, false);
            open = Some(vec![e]);
            continue;
        }
        match open.as_mut() {
            Some(b) if e.event_time_ms - b.last().expect("non-empty").event_time_ms <= GAP_MS => {
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

/// `Ids.sessionId`: UUIDv5 over `vin|lowercase(charge_start event_id)`.
pub fn session_id(vin: &str, charge_start_event_id: &str) -> String {
    let name = format!("{vin}|{}", charge_start_event_id.to_lowercase());
    Uuid::new_v5(&NAMESPACE, name.as_bytes()).to_string()
}

/// `build`. The legacy `Option.get` and `max` on an empty list fail the query; here they fail the batch.
fn build(run: &Run) -> Result<ChargingSession> {
    let start = run.events.first().expect("a run is never empty");
    let last = run.events.last().expect("a run is never empty");
    let missing = |f: &str| format!("{f} is null on a session event");
    let ids: HashSet<&str> = run.events.iter().map(|e| e.event_id.as_str()).collect();
    Ok(ChargingSession {
        session_id: session_id(&start.vin, &start.event_id),
        vin: start.vin.clone(),
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
        },
        energy_added_kwh: last
            .energy_added_kwh
            .with_context(|| missing("energy_added_kwh"))?,
        peak_power_kw: run
            .events
            .iter()
            .filter_map(|e| e.power_kw)
            .reduce(f64::max)
            .with_context(|| missing("power_kw"))?,
        start_soc_pct: start.soc_pct.with_context(|| missing("soc_pct"))?,
        end_soc_pct: last.soc_pct.with_context(|| missing("soc_pct"))?,
        event_count: ids.len() as i64,
        charger_lat: start.charger_lat.with_context(|| missing("charger_lat"))?,
        charger_lon: start.charger_lon.with_context(|| missing("charger_lon"))?,
    })
}

/// `advance`: add `events` (any type, any order) and emit every session whose last event is more than
/// GAP_MS before the horizon, the larger of the VIN's highest event time and the watermark. Returns the
/// new state (with its next timeout) and the emitted sessions.
pub fn advance(
    state: &SessionState,
    events: &[ChargingEvent],
    watermark_ms: i64,
) -> Result<(SessionState, Vec<ChargingSession>)> {
    let max_seen = events
        .iter()
        .map(|e| e.event_time_ms)
        .fold(state.max_seen_ms, i64::max);
    let mut known: HashSet<&str> = state.pending.iter().map(|e| e.event_id.as_str()).collect();
    let mut pending = state.pending.clone();
    for e in events {
        // `groupBy(_.event_id).values.map(_.head)`: one event per id, the first in arrival order.
        if !e.is("heartbeat")
            && e.event_time_ms > state.emitted_until_ms
            && known.insert(e.event_id.as_str())
        {
            pending.push(e.clone());
        }
    }
    let horizon = max_seen.max(watermark_ms);
    let (runs, stray) = segment(pending);
    let (done, open): (Vec<Run>, Vec<Run>) = runs
        .into_iter()
        .partition(|r| horizon.saturating_sub(r.last()) > GAP_MS);
    let kept_stray: Vec<ChargingEvent> = stray
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
    let mut next_pending: Vec<ChargingEvent> = open
        .into_iter()
        .flat_map(|r| r.events)
        .chain(kept_stray)
        .collect();
    order(&mut next_pending);
    let out = done.iter().map(build).collect::<Result<Vec<_>>>()?;
    Ok((
        SessionState {
            vin: state.vin.clone(),
            pending: next_pending,
            max_seen_ms: max_seen,
            emitted_until_ms: emitted_until,
            timeout_ms: timeout,
        },
        out,
    ))
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
    const MIN: i64 = 60 * 1000;
    const T0: i64 = 1_790_812_800_000;

    /// Builds events as SessionsSpec's `ev` does: ids and sequence numbers from a counter.
    struct Gen(i64);

    impl Gen {
        fn ev(
            &mut self,
            kind: &str,
            minute: f64,
            energy: f64,
            power: f64,
            soc: f64,
        ) -> ChargingEvent {
            self.0 += 1;
            let hb = kind == "heartbeat";
            let opt = |v: f64| (!hb).then_some(v);
            ChargingEvent {
                event_id: format!("00000000-0000-4000-8000-{:012}", self.0),
                vin: VIN.into(),
                event_time_ms: T0 + (minute * MIN as f64) as i64,
                event_type: Some(kind.into()),
                producer_seq: self.0,
                fleet: Some("consumer".into()),
                market: Some("us-bay-area".into()),
                connector_type: (!hb).then(|| "NACS".into()),
                charger_type: (!hb).then(|| "dc_fast".into()),
                power_kw: opt(power),
                energy_added_kwh: opt(energy),
                soc_pct: opt(soc),
                charger_lat: opt(37.12345),
                charger_lon: opt(-122.04321),
            }
        }
        fn at(&mut self, kind: &str, minute: f64) -> ChargingEvent {
            self.ev(kind, minute, 0.0, 50.0, 50.0)
        }
    }

    fn run(batches: &[Vec<ChargingEvent>]) -> Vec<ChargingSession> {
        let mut state = SessionState::empty(VIN);
        let mut out = Vec::new();
        for b in batches {
            let (next, emitted) = advance(&state, b, i64::MIN).unwrap();
            state = next;
            out.extend(emitted);
        }
        out
    }

    #[test]
    fn a_gap_of_more_than_30_minutes_splits_sessions_exactly_30_does_not() {
        let mut g = Gen(0);
        let mut events = vec![
            g.at("charge_start", 0.0),
            g.ev("charge_progress", 30.0, 5.0, 50.0, 50.0),
            g.ev("charge_progress", 60.0, 9.0, 50.0, 50.0),
            g.at("charge_start", 91.0),
            g.ev("charge_progress", 100.0, 3.0, 50.0, 50.0),
        ];
        events.push(g.at("heartbeat", 200.0));
        let out = run(&[events]);
        assert_eq!(
            out.iter().map(|s| s.event_count).collect::<Vec<_>>(),
            [3, 2]
        );
        assert!(out.iter().all(|s| s.close_reason == "gap_timeout"));
        assert_eq!(out[0].end_time, format_ts(T0 + 60 * MIN));
    }

    #[test]
    fn charge_stop_sets_close_reason_but_does_not_emit_until_the_gap_passes() {
        let mut g = Gen(0);
        let start = g.ev("charge_start", 0.0, 0.0, 50.0, 40.0);
        let p = g.ev("charge_progress", 10.0, 6.0, 120.0, 48.0);
        let stop = g.ev("charge_stop", 20.0, 12.5, 0.0, 55.0);
        let batch = vec![start.clone(), p, stop.clone()];
        assert!(run(std::slice::from_ref(&batch)).is_empty());
        let out = run(&[batch, vec![g.at("heartbeat", 51.0)]]);
        assert_eq!(out.len(), 1);
        let s = &out[0];
        assert_eq!(s.close_reason, "charge_stop");
        assert_eq!(
            (
                s.start_soc_pct,
                s.end_soc_pct,
                s.energy_added_kwh,
                s.peak_power_kw
            ),
            (40.0, 55.0, 12.5, 120.0)
        );
        assert_eq!(
            (s.start_time.clone(), s.end_time.clone()),
            (
                format_ts(start.event_time_ms),
                format_ts(stop.event_time_ms)
            )
        );
    }

    #[test]
    fn a_late_event_joins_the_right_session() {
        let mut g = Gen(0);
        let start = g.at("charge_start", 0.0);
        let p1 = g.ev("charge_progress", 5.0, 2.0, 60.0, 50.0);
        let p2 = g.ev("charge_progress", 10.0, 4.0, 150.0, 50.0);
        let stop = g.ev("charge_stop", 15.0, 6.0, 0.0, 50.0);
        let out = run(&[
            vec![start, p1],
            vec![stop],
            vec![p2],
            vec![g.at("heartbeat", 120.0)],
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!((out[0].event_count, out[0].peak_power_kw), (4, 150.0));
    }

    #[test]
    fn a_late_charge_start_still_starts_the_session() {
        let mut g = Gen(0);
        let start = g.ev("charge_start", 0.0, 0.0, 50.0, 33.0);
        let p = g.ev("charge_progress", 5.0, 2.0, 50.0, 50.0);
        let stop = g.ev("charge_stop", 9.0, 3.0, 50.0, 50.0);
        let out = run(&[
            vec![p],
            vec![start.clone(), stop],
            vec![g.at("heartbeat", 60.0)],
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!((out[0].event_count, out[0].start_soc_pct), (3, 33.0));
        assert_eq!(out[0].session_id, session_id(VIN, &start.event_id));
    }

    #[test]
    fn duplicates_are_ignored_before_and_after_the_session_is_emitted() {
        let mut g = Gen(0);
        let start = g.at("charge_start", 0.0);
        let p = g.ev("charge_progress", 5.0, 2.0, 50.0, 50.0);
        let stop = g.ev("charge_stop", 9.0, 3.0, 50.0, 50.0);
        let hb = g.at("heartbeat", 60.0);
        let out = run(&[
            vec![start, p.clone(), p.clone()],
            vec![p.clone(), stop.clone(), stop.clone()],
            vec![hb],
            vec![stop, p],
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_count, 3);
    }

    #[test]
    fn a_missing_charge_stop_gives_gap_timeout_ended_at_the_last_event() {
        let mut g = Gen(0);
        let out = run(&[
            vec![
                g.at("charge_start", 0.0),
                g.ev("charge_progress", 10.0, 4.0, 50.0, 61.0),
            ],
            vec![g.at("heartbeat", 41.0)],
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].close_reason, "gap_timeout");
        assert_eq!(
            (out[0].end_time.clone(), out[0].end_soc_pct),
            (format_ts(T0 + 10 * MIN), 61.0)
        );
    }

    #[test]
    fn session_id_is_uuidv5_over_vin_and_lowercase_charge_start_event_id() {
        let mut g = Gen(0);
        let start = g.at("charge_start", 0.0);
        let out = run(&[
            vec![start.clone(), g.ev("charge_stop", 5.0, 1.0, 50.0, 50.0)],
            vec![g.at("heartbeat", 40.0)],
        ]);
        assert_eq!(out[0].session_id, session_id(VIN, &start.event_id));
        assert_eq!(
            (out[0].charger_lat, out[0].charger_lon),
            (37.12345, -122.04321)
        );
        // telemetry-parity golden/edge: SYNEDGECHG0000001's session.
        let (vin, id) = ("SYNEDGECHG0000001", "55becb84-e3e0-50ac-9b97-6d14e5c35392");
        assert_eq!(session_id(vin, id), "6fb439b3-0ed0-532a-83ba-e40c75ac2e52");
        assert_eq!(session_id(vin, &id.to_uppercase()), session_id(vin, id));
    }

    #[test]
    fn events_outside_a_session_are_ignored() {
        let mut g = Gen(0);
        let out = run(&[
            vec![
                g.ev("charge_progress", 0.0, 1.0, 50.0, 50.0),
                g.at("charge_start", 40.0),
                g.ev("charge_stop", 50.0, 2.0, 50.0, 50.0),
                g.ev("charge_progress", 55.0, 3.0, 50.0, 50.0),
            ],
            vec![g.at("heartbeat", 200.0)],
        ]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].event_count, 2);
    }

    #[test]
    fn the_watermark_closes_a_session_and_the_timeout_is_last_event_plus_30_minutes() {
        let mut g = Gen(0);
        let events = vec![
            g.at("charge_start", 0.0),
            g.ev("charge_progress", 4.0, 1.0, 50.0, 50.0),
        ];
        let (s1, out1) = advance(&SessionState::empty(VIN), &events, i64::MIN).unwrap();
        assert!(out1.is_empty());
        assert_eq!(s1.timeout_ms, Some(T0 + 34 * MIN));
        let (_, early) = advance(&s1, &[], T0 + 34 * MIN).unwrap();
        assert!(early.is_empty());
        let (s2, out2) = advance(&s1, &[], T0 + 34 * MIN + 1).unwrap();
        assert_eq!(out2.len(), 1);
        assert!(s2.pending.is_empty() && s2.timeout_ms.is_none());
    }

    #[test]
    fn heartbeats_are_never_pending_but_advance_max_seen() {
        let mut g = Gen(0);
        let hb = g.at("heartbeat", 7.0);
        let (s, out) = advance(&SessionState::empty(VIN), &[hb], i64::MIN).unwrap();
        assert!(out.is_empty() && s.pending.is_empty());
        assert_eq!(s.max_seen_ms, T0 + 7 * MIN);
    }

    #[test]
    fn a_null_field_on_a_session_event_fails_like_the_legacy_get() {
        let mut g = Gen(0);
        let mut stop = g.ev("charge_stop", 5.0, 1.0, 50.0, 50.0);
        stop.energy_added_kwh = None;
        let r = advance(
            &SessionState::empty(VIN),
            &[g.at("charge_start", 0.0), stop, g.at("heartbeat", 60.0)],
            i64::MIN,
        );
        assert!(r.is_err());
    }

    #[test]
    fn output_timestamps_have_milliseconds_and_z() {
        assert_eq!(format_ts(T0), "2026-10-01T00:00:00.000Z");
        let e = decode(br#"{"event_id":"a","vin":"V","event_time":"2026-10-01T00:00:00.5Z","event_type":"heartbeat","producer_seq":1,"fleet":"consumer","market":"us-houston","connector_type":null,"charger_type":null,"power_kw":null,"energy_added_kwh":null,"soc_pct":null,"charger_lat":null,"charger_lon":null}"#).unwrap();
        assert_eq!(format_ts(e.event_time_ms), "2026-10-01T00:00:00.500Z");
    }
}
