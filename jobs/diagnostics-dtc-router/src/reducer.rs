//! The legacy row logic, ported exactly: the `routable` filter, `Ids.alertId`, `GeoUtils.coarsen`
//! and the output columns of DiagnosticsDtcRouter.scala.

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

/// `DiagnosticsDtcRouter.MinSeverity`.
pub const MIN_SEVERITY: i64 = 3;

/// `Ids.Namespace`: UUIDv5(NAMESPACE_DNS, "contracts.telemetry.example").
const NAMESPACE: [u8; 16] = [
    0xf5, 0x17, 0x9d, 0x15, 0x0f, 0xd7, 0x5b, 0x5f, 0xbf, 0x53, 0x08, 0x52, 0xeb, 0x22, 0xd3, 0xd5,
];

/// The fields of a vehicle.diagnostics.dtc.v1 record the job reads. Other fields are ignored.
#[derive(Debug, Deserialize)]
struct RawEvent {
    event_id: Option<String>,
    vin: Option<String>,
    event_time: Option<String>,
    event_type: Option<String>,
    severity: Option<i64>,
    status: Option<String>,
    dtc_code: Option<String>,
    ecu: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
}

/// One routable input record.
#[derive(Debug, Clone, PartialEq)]
pub struct Input {
    /// The dedup key, as received (dedup does not lowercase it).
    pub event_id: String,
    /// Epoch milliseconds, as Spark's timestamp at the parsed millisecond precision.
    pub event_time_ms: i64,
    pub alert: Alert,
}

/// A service.dtc.alerts.v1 value. Field order and nulls match Spark's `to_json` with
/// `ignoreNullFields=false`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Alert {
    pub alert_id: String,
    pub vin: String,
    pub dtc_code: Option<String>,
    pub severity: i64,
    pub ecu: Option<String>,
    pub event_time: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
}

/// `routable`: `event_type = "dtc" AND severity >= 3 AND status = "active"`. A null in any of them
/// drops the row.
fn routable(e: &RawEvent) -> bool {
    e.event_type.as_deref() == Some("dtc")
        && e.severity.is_some_and(|s| s >= MIN_SEVERITY)
        && e.status.as_deref() == Some("active")
}

/// Decode a record. `Ok(None)` for a row the legacy filter drops; an error for a routable row
/// without the `event_id`, `vin` or `event_time` the contract requires.
pub fn decode(payload: &[u8]) -> Result<Option<Input>> {
    let e: RawEvent =
        serde_json::from_slice(payload).context("decode vehicle.diagnostics.dtc.v1")?;
    if !routable(&e) {
        return Ok(None);
    }
    let event_id = e.event_id.ok_or_else(|| anyhow!("missing event_id"))?;
    let vin = e.vin.ok_or_else(|| anyhow!("missing vin"))?;
    let t = e.event_time.ok_or_else(|| anyhow!("missing event_time"))?;
    let event_time_ms = DateTime::parse_from_rfc3339(&t)
        .context("event_time")?
        .timestamp_millis();
    let (lat, lon) = match (e.lat, e.lon) {
        (Some(lat), Some(lon)) => (Some(coarsen(lat)), Some(coarsen(lon))),
        // A Scala UDF with primitive Double arguments returns null if either argument is null.
        _ => (None, None),
    };
    Ok(Some(Input {
        alert: Alert {
            alert_id: alert_id(&event_id),
            vin,
            dtc_code: e.dtc_code,
            severity: e.severity.expect("routable"),
            ecu: e.ecu,
            event_time: format_ts(event_time_ms),
            lat,
            lon,
        },
        event_id,
        event_time_ms,
    }))
}

/// `GeoUtils.to2`: `BigDecimal(value.toString).setScale(2, RoundingMode.HALF_EVEN).toDouble`.
/// Rounds the shortest decimal form of the double, with exact decimal arithmetic.
pub fn coarsen(value: f64) -> f64 {
    let s = format!("{value}");
    let (neg, digits) = match s.strip_prefix('-') {
        Some(d) => (true, d),
        None => (false, s.as_str()),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let frac = frac.as_bytes();
    let digit = |i: usize| frac.get(i).map_or(0, |b| i128::from(b - b'0'));
    let int: i128 = int.parse().expect("f64 Display is a plain decimal");
    let mut hundredths = int * 100 + digit(0) * 10 + digit(1);
    let rest = frac.get(2..).unwrap_or(&[]);
    let up = match rest.first() {
        None => false,
        Some(b'5') if rest[1..].iter().all(|&b| b == b'0') => hundredths % 2 == 1,
        Some(&b) => b >= b'5',
    };
    if up {
        hundredths += 1;
    }
    if hundredths == 0 {
        return 0.0;
    }
    let sign = if neg { "-" } else { "" };
    format!("{sign}{}.{:02}", hundredths / 100, hundredths % 100)
        .parse()
        .expect("valid decimal")
}

/// `Ids.alertId`: UUIDv5 over the lowercased source event_id.
pub fn alert_id(source_event_id: &str) -> String {
    let mut h = Sha1::new();
    h.update(NAMESPACE);
    h.update(source_event_id.to_lowercase().as_bytes());
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

/// `yyyy-MM-dd'T'HH:mm:ss.SSSXXX` in UTC.
pub fn format_ts(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("event_time is within chrono's range")
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(event_type: &str, severity: Option<i64>, status: &str) -> String {
        serde_json::json!({"event_id": "E1", "vin": "SYNTESTVIN0000001", "event_time": "2026-10-01T00:00:00.123+02:00",
            "event_type": event_type, "severity": severity, "status": status, "dtc_code": "P0A80",
            "ecu": "bms", "lat": 37.12500, "lon": -122.12500, "odometer_km": 1})
        .to_string()
    }

    #[test]
    fn routes_only_active_dtcs_of_severity_three_or_more() {
        assert!(decode(rec("dtc", Some(3), "active").as_bytes())
            .unwrap()
            .is_some());
        assert!(decode(rec("dtc", Some(5), "active").as_bytes())
            .unwrap()
            .is_some());
        assert!(decode(rec("dtc", Some(2), "active").as_bytes())
            .unwrap()
            .is_none());
        assert!(decode(rec("dtc", None, "active").as_bytes())
            .unwrap()
            .is_none());
        assert!(decode(rec("dtc", Some(4), "cleared").as_bytes())
            .unwrap()
            .is_none());
        assert!(decode(rec("heartbeat", Some(4), "active").as_bytes())
            .unwrap()
            .is_none());
        assert!(decode(br#"{"event_type":"heartbeat","vin":"V"}"#)
            .unwrap()
            .is_none());
    }

    #[test]
    fn a_routable_row_without_required_fields_does_not_decode() {
        assert!(
            decode(br#"{"event_type":"dtc","severity":3,"status":"active","vin":"V"}"#).is_err()
        );
    }

    #[test]
    fn builds_the_alert_columns() {
        let i = decode(rec("dtc", Some(4), "active").as_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(i.event_id, "E1");
        assert_eq!(i.event_time_ms % 1000, 123);
        assert_eq!(i.alert.event_time, "2026-09-30T22:00:00.123Z");
        assert_eq!(i.alert.alert_id, alert_id("e1"));
        assert_eq!((i.alert.lat, i.alert.lon), (Some(37.12), Some(-122.12)));
        let json = serde_json::to_string(&i.alert).unwrap();
        assert!(json.starts_with(r#"{"alert_id":"#));
        assert!(json.ends_with(r#""lat":37.12,"lon":-122.12}"#));
    }

    #[test]
    fn a_null_coordinate_nulls_both() {
        let p = br#"{"event_id":"E1","vin":"V","event_time":"2026-10-01T00:00:00.000Z","event_type":"dtc","severity":3,"status":"active","dtc_code":null,"ecu":null,"lat":37.1,"lon":null}"#;
        let a = decode(p).unwrap().unwrap().alert;
        assert_eq!((a.lat, a.lon), (None, None));
        assert!(serde_json::to_string(&a)
            .unwrap()
            .contains(r#""dtc_code":null"#));
    }

    #[test]
    fn coarsen_rounds_half_even_on_the_decimal_form() {
        for (input, want) in [
            (37.125, 37.12),
            (37.135, 37.14),
            (-122.125, -122.12),
            (-37.125, -37.12),
            (33.445, 33.44),
            (-112.075, -112.08),
            (37.12501, 37.13),
            (37.12499, 37.12),
            (29.52127, 29.52),
            (-95.33472, -95.33),
            (0.005, 0.0),
            (0.015, 0.02),
            (30.0, 30.0),
            (-179.995, -180.0),
            (89.999, 90.0),
        ] {
            assert_eq!(coarsen(input), want, "{input}");
        }
        assert!(coarsen(-0.004).is_sign_positive(), "BigDecimal has no -0");
    }

    #[test]
    fn alert_id_is_uuid_v5_of_lowercase_event_id() {
        let id = alert_id("ABC");
        assert_eq!(id, alert_id("abc"));
        assert_eq!(&id[14..15], "5");
        assert!(matches!(&id[19..20], "8" | "9" | "a" | "b"));
    }
}
