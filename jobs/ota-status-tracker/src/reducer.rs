//! The legacy reducer, ported exactly (OtaStatusTracker.scala `latest`, `rank`, `toOutput`).

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

/// The fields of a vehicle.ota.status.v1 record the job reads. Other fields are ignored.
#[derive(Debug, Deserialize)]
pub struct OtaEvent {
    pub event_id: String,
    pub vin: String,
    pub event_time: String,
    pub event_type: Option<String>,
    pub producer_seq: i64,
    pub campaign_id: Option<String>,
    pub target_version: Option<String>,
    pub state: Option<String>,
    pub failure_code: Option<String>,
}

/// The winning event for one (vin, campaign_id). This is also the changelog value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Winner {
    pub vin: String,
    pub campaign_id: Option<String>,
    pub event_id: String,
    /// Epoch milliseconds, as Spark's `Timestamp.getTime`.
    pub event_time_ms: i64,
    pub producer_seq: i64,
    pub state: Option<String>,
    pub target_version: Option<String>,
    pub failure_code: Option<String>,
}

/// One ota.status.latest.v1 value. Field order matches the legacy JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OtaLatest {
    pub vin: String,
    pub campaign_id: Option<String>,
    pub state: Option<String>,
    pub target_version: Option<String>,
    pub failure_code: Option<String>,
    pub last_event_id: String,
    pub last_event_time: String,
}

/// Decode an input value. `Ok(None)` for heartbeats: the legacy filter `event_type =!= "heartbeat"`
/// also drops a null event_type.
pub fn decode(payload: &[u8]) -> Result<Option<Winner>> {
    let e: OtaEvent = serde_json::from_slice(payload).context("decode vehicle.ota.status.v1")?;
    match e.event_type.as_deref() {
        None | Some("heartbeat") => return Ok(None),
        Some(_) => {}
    }
    let t = DateTime::parse_from_rfc3339(&e.event_time).context("event_time")?;
    Ok(Some(Winner {
        vin: e.vin,
        campaign_id: e.campaign_id,
        event_id: e.event_id,
        event_time_ms: t.timestamp_millis(),
        producer_seq: e.producer_seq,
        state: e.state,
        target_version: e.target_version,
        failure_code: e.failure_code,
    }))
}

/// State and output key: `concat_ws("|", vin, campaign_id)`, which skips a null campaign_id.
pub fn key(vin: &str, campaign_id: Option<&str>) -> String {
    match campaign_id {
        Some(c) => format!("{vin}|{c}"),
        None => vin.to_string(),
    }
}

fn rank(e: &Winner) -> (i64, i64) {
    (e.event_time_ms, e.producer_seq)
}

/// A new event replaces the current winner only if its rank is strictly greater. Equal ranks keep
/// the current winner, so duplicates never change state.
pub fn wins(current: Option<&Winner>, e: &Winner) -> bool {
    current.is_none_or(|cur| rank(e) > rank(cur))
}

pub fn to_output(e: &Winner) -> OtaLatest {
    OtaLatest {
        vin: e.vin.clone(),
        campaign_id: e.campaign_id.clone(),
        state: e.state.clone(),
        target_version: e.target_version.clone(),
        failure_code: e.failure_code.clone(),
        last_event_id: e.event_id.clone(),
        last_event_time: format_ts(e.event_time_ms),
    }
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

    fn ev(id: &str, time: &str, seq: i64, state: &str, failure: Option<&str>) -> Winner {
        decode(
            serde_json::json!({"event_id": id, "vin": "SYNTESTVIN0000001", "event_time": time,
                "event_type": "ota_status", "producer_seq": seq, "campaign_id": "OTA-2026.10.1",
                "target_version": "2026.10.1", "state": state, "failure_code": failure})
            .to_string()
            .as_bytes(),
        )
        .unwrap()
        .unwrap()
    }

    fn fold(events: &[Winner]) -> Option<Winner> {
        events.iter().fold(None, |cur, e| {
            if wins(cur.as_ref(), e) {
                Some(e.clone())
            } else {
                cur
            }
        })
    }

    #[test]
    fn newer_event_wins() {
        let a = ev("a", "2026-10-01T00:00:00.000Z", 1, "downloaded", None);
        let b = ev("b", "2026-10-01T00:05:00.000Z", 2, "installed", None);
        assert_eq!(fold(&[a, b.clone()]), Some(b));
    }

    #[test]
    fn older_late_event_does_not_overwrite() {
        let a = ev("a", "2026-10-01T00:00:00.000Z", 1, "downloaded", None);
        let b = ev("b", "2026-10-01T00:05:00.000Z", 2, "installed", None);
        assert_eq!(fold(&[b.clone(), a]), Some(b));
    }

    #[test]
    fn producer_seq_breaks_event_time_ties() {
        let a = ev("a", "2026-10-01T00:00:00.000Z", 1, "downloaded", None);
        let b = ev("b", "2026-10-01T00:00:00.000Z", 2, "installed", None);
        assert_eq!(fold(&[b.clone(), a.clone()]), Some(b.clone()));
        assert_eq!(fold(&[a, b.clone()]), Some(b));
    }

    #[test]
    fn equal_rank_keeps_the_first_arrival() {
        let a = ev("a", "2026-10-01T00:00:00.000Z", 1, "downloaded", None);
        let b = ev("b", "2026-10-01T00:00:00.000Z", 1, "installed", None);
        assert_eq!(fold(&[a.clone(), b.clone()]), Some(a.clone()));
        assert_eq!(fold(&[b.clone(), a]), Some(b));
    }

    #[test]
    fn duplicate_does_not_change_state() {
        let a = ev("a", "2026-10-01T00:00:00.000Z", 1, "downloaded", None);
        assert!(!wins(Some(&a), &a.clone()));
    }

    #[test]
    fn failure_code_and_winner_id_and_time_reach_the_output() {
        let f = ev(
            "f",
            "2026-10-01T01:40:30.88Z",
            3,
            "failed",
            Some("E_TIMEOUT"),
        );
        let o = to_output(&f);
        assert_eq!(
            (o.state.as_deref(), o.failure_code.as_deref()),
            (Some("failed"), Some("E_TIMEOUT"))
        );
        assert_eq!(
            (o.last_event_id.as_str(), o.last_event_time.as_str()),
            ("f", "2026-10-01T01:40:30.880Z")
        );
        let ok = to_output(&ev("g", "2026-10-01T00:00:00Z", 1, "installed", None));
        assert!(serde_json::to_string(&ok)
            .unwrap()
            .contains("\"failure_code\":null"));
        assert_eq!(ok.last_event_time, "2026-10-01T00:00:00.000Z");
    }

    #[test]
    fn heartbeats_and_null_event_types_are_skipped() {
        for t in [serde_json::json!("heartbeat"), serde_json::Value::Null] {
            let v = serde_json::json!({"event_id": "h", "vin": "SYNTESTVIN0000001",
                "event_time": "2026-10-01T00:00:00.000Z", "event_type": t, "producer_seq": 1,
                "campaign_id": null, "target_version": null, "state": null, "failure_code": null});
            assert_eq!(decode(v.to_string().as_bytes()).unwrap(), None);
        }
    }

    #[test]
    fn key_joins_vin_and_campaign() {
        assert_eq!(key("V", Some("OTA-2026.10.1")), "V|OTA-2026.10.1");
        assert_eq!(key("V", None), "V");
    }
}
