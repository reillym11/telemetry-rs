//! The legacy bucket logic, ported exactly (Buckets.scala `windowStart`, `aggregate`, `advance`).

use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

pub const HOUR_MS: i64 = 60 * 60 * 1000;
pub const GRACE_MS: i64 = 10 * 60 * 1000;

/// The fields of a vehicle.battery.health.v1 record the job reads (the legacy `select`).
#[derive(Debug, Deserialize)]
struct RawSample {
    event_id: String,
    vin: String,
    event_time: String,
    event_type: Option<String>,
    soh_pct: Option<f64>,
    cell_voltage_delta_mv: Option<i64>,
    pack_temp_c: Option<f64>,
}

/// One decoded input record. Heartbeats are kept: they advance the VIN's highest event time.
#[derive(Debug, Clone, PartialEq)]
pub struct Input {
    pub vin: String,
    pub sample: Sample,
}

/// `BatterySample` without its VIN (the VIN is the state key and a field of [`BucketState`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub event_id: String,
    /// Epoch milliseconds, as `Timestamp.getTime`.
    pub event_time_ms: i64,
    pub event_type: Option<String>,
    pub soh_pct: Option<f64>,
    pub cell_voltage_delta_mv: Option<i64>,
    pub pack_temp_c: Option<f64>,
}

/// Per-VIN state (`BucketState`) plus the records that arrived in the open batch, which Spark
/// hands to `step` together at the end of the batch. This is also the changelog value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BucketState {
    pub vin: String,
    pub pending: Vec<Sample>,
    pub max_seen_ms: i64,
    pub emitted_until_ms: i64,
    #[serde(default)]
    pub arrived: Vec<Sample>,
}

impl BucketState {
    /// `Buckets.Empty`.
    pub fn empty(vin: &str) -> Self {
        BucketState {
            vin: vin.to_string(),
            pending: Vec::new(),
            max_seen_ms: i64::MIN,
            emitted_until_ms: i64::MIN,
            arrived: Vec::new(),
        }
    }
}

/// One battery.health.hourly row. Decimals are exact values at their column scale; `None` is
/// what Spark's non-ANSI `cast` to the column's decimal type gives on overflow.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HourlyHealth {
    pub vin: String,
    pub date: String,
    pub window_start: String,
    pub window_end: String,
    pub sample_count: i32,
    pub soh_pct_min: Option<f64>,
    pub soh_pct_avg: Option<f64>,
    pub cell_voltage_delta_mv_min: i32,
    pub cell_voltage_delta_mv_max: i32,
    pub pack_temp_c_max: Option<f64>,
}

pub fn format_ts(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("timestamp in range")
        .to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn decode(payload: &[u8]) -> Result<Input> {
    let s: RawSample =
        serde_json::from_slice(payload).context("decode vehicle.battery.health.v1")?;
    let t = DateTime::parse_from_rfc3339(&s.event_time).context("event_time")?;
    Ok(Input {
        vin: s.vin,
        sample: Sample {
            event_id: s.event_id,
            event_time_ms: t.timestamp_millis(),
            event_type: s.event_type,
            soh_pct: s.soh_pct,
            cell_voltage_delta_mv: s.cell_voltage_delta_mv,
            pack_temp_c: s.pack_temp_c,
        },
    })
}

/// `Buckets.windowStart`: `floorDiv(t, HourMs) * HourMs`.
pub fn window_start(t: i64) -> i64 {
    t.div_euclid(HOUR_MS) * HOUR_MS
}

/// An exact decimal: `unscaled * 10^-scale`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Dec {
    unscaled: i128,
    scale: u32,
}

impl Dec {
    /// `BigDecimal(d.toString)`: the shortest decimal that round-trips the double.
    fn from_f64(d: f64) -> Result<Dec> {
        if !d.is_finite() {
            bail!("non-finite number");
        }
        let text = format!("{d}");
        let (int, frac) = text.split_once('.').unwrap_or((&text, ""));
        let unscaled: i128 = format!("{int}{frac}")
            .parse()
            .context("decimal out of range")?;
        Ok(Dec {
            unscaled,
            scale: frac.len() as u32,
        })
    }

    fn at_scale(self, scale: u32) -> i128 {
        self.unscaled * 10i128.pow(scale - self.scale)
    }

    /// `setScale(scale)` without a rounding mode: throws (ArithmeticException) if rounding is needed.
    fn set_scale_exact(self, scale: u32, field: &str) -> Result<i128> {
        if self.scale <= scale {
            return Ok(self.at_scale(scale));
        }
        let div = 10i128.pow(self.scale - scale);
        if self.unscaled % div != 0 {
            bail!("{field} has more than {scale} decimal places (Rounding necessary)");
        }
        Ok(self.unscaled / div)
    }
}

/// `(soh.sum / n).setScale(2, HALF_UP)`. The sum is exact; Scala divides with DECIMAL128 (34
/// significant digits) before the HALF_UP step, which gives the same result as the exact quotient
/// unless the quotient has a run of over 30 repeated digits, impossible for realistic `n`.
fn avg_half_up(values: &[Dec], scale: u32) -> i128 {
    let s = values.iter().map(|d| d.scale).max().unwrap_or(0).max(scale);
    let sum: i128 = values.iter().map(|d| d.at_scale(s)).sum();
    let n = values.len() as i128;
    // value = sum / (n * 10^s); target unscaled = round_half_up(sum * 10^scale / (n * 10^s))
    let num = sum;
    let den = n * 10i128.pow(s - scale);
    let q = num.div_euclid(den);
    let r = num.rem_euclid(den);
    let twice = 2 * r;
    if num >= 0 {
        if twice >= den {
            q + 1
        } else {
            q
        }
    } else if twice > den {
        // HALF_UP rounds ties away from zero; for negatives that is towards -inf.
        q + 1
    } else {
        q
    }
}

/// Spark's `cast(decimal(p, s))` on an exact value at scale `s`: null when it does not fit.
fn to_column(unscaled: i128, precision: u32, scale: u32) -> Option<f64> {
    (unscaled.abs() < 10i128.pow(precision)).then(|| unscaled as f64 / 10f64.powi(scale as i32))
}

fn required<T: Copy>(v: Option<T>, field: &str) -> Result<T> {
    // Scala's Option.get throws on a null here; the schema forbids it on battery_sample events.
    v.ok_or_else(|| anyhow!("{field} is null on a battery_sample"))
}

/// `Buckets.aggregate`.
pub fn aggregate(vin: &str, start: i64, samples: &[Sample]) -> Result<HourlyHealth> {
    let soh = samples
        .iter()
        .map(|s| Dec::from_f64(required(s.soh_pct, "soh_pct")?))
        .collect::<Result<Vec<_>>>()?;
    // `.toInt` on a Long keeps the low 32 bits.
    let delta = samples
        .iter()
        .map(|s| Ok(required(s.cell_voltage_delta_mv, "cell_voltage_delta_mv")? as i32))
        .collect::<Result<Vec<i32>>>()?;
    let temp = samples
        .iter()
        .map(|s| Dec::from_f64(required(s.pack_temp_c, "pack_temp_c")?))
        .collect::<Result<Vec<_>>>()?;
    let cmp = |a: &&Dec, b: &&Dec| {
        let s = a.scale.max(b.scale);
        a.at_scale(s).cmp(&b.at_scale(s))
    };
    let soh_min = soh
        .iter()
        .min_by(cmp)
        .expect("non-empty")
        .set_scale_exact(2, "soh_pct")?;
    let soh_avg = avg_half_up(&soh, 2);
    let temp_max = temp
        .iter()
        .max_by(cmp)
        .expect("non-empty")
        .set_scale_exact(1, "pack_temp_c")?;
    Ok(HourlyHealth {
        vin: vin.to_string(),
        date: format_ts(start)[..10].to_string(),
        window_start: format_ts(start),
        window_end: format_ts(start + HOUR_MS),
        sample_count: samples.len() as i32,
        soh_pct_min: to_column(soh_min, 5, 2),
        soh_pct_avg: to_column(soh_avg, 5, 2),
        cell_voltage_delta_mv_min: *delta.iter().min().expect("non-empty"),
        cell_voltage_delta_mv_max: *delta.iter().max().expect("non-empty"),
        pack_temp_c_max: to_column(temp_max, 4, 1),
    })
}

/// `Buckets.advance`. `state.arrived` is ignored; pass the batch's records as `events`.
/// Returns the next state (with `arrived` empty), the rows to emit, and the event-time timeout.
pub fn advance(
    state: &BucketState,
    events: &[Sample],
    watermark_ms: i64,
) -> Result<(BucketState, Vec<HourlyHealth>, Option<i64>)> {
    let max_seen = events
        .iter()
        .map(|e| e.event_time_ms)
        .fold(state.max_seen_ms, i64::max);
    let known: HashSet<&str> = state.pending.iter().map(|s| s.event_id.as_str()).collect();
    // `.groupBy(_.event_id).values.map(_.head)`: the first arrival of each new event_id.
    let mut seen = HashSet::new();
    let fresh = events.iter().filter(|e| {
        e.event_type.as_deref() == Some("battery_sample")
            && e.event_time_ms >= state.emitted_until_ms
            && !known.contains(e.event_id.as_str())
            && seen.insert(e.event_id.as_str())
    });
    let horizon = max_seen.max(watermark_ms);
    let mut by_hour: BTreeMap<i64, Vec<Sample>> = BTreeMap::new();
    for s in state.pending.iter().chain(fresh) {
        by_hour
            .entry(window_start(s.event_time_ms))
            .or_default()
            .push(s.clone());
    }
    let mut rows = Vec::new();
    let mut emitted_until = state.emitted_until_ms;
    let mut open = Vec::new();
    let mut timeout = None;
    for (start, samples) in by_hour {
        if start + HOUR_MS + GRACE_MS <= horizon {
            rows.push(aggregate(&state.vin, start, &samples)?);
            emitted_until = emitted_until.max(start + HOUR_MS);
        } else {
            timeout.get_or_insert(start + HOUR_MS + GRACE_MS - 1);
            open.extend(samples);
        }
    }
    open.sort_by(|a, b| (a.event_time_ms, &a.event_id).cmp(&(b.event_time_ms, &b.event_id)));
    let next = BucketState {
        vin: state.vin.clone(),
        pending: open,
        max_seen_ms: max_seen,
        emitted_until_ms: emitted_until,
        arrived: Vec::new(),
    };
    Ok((next, rows, timeout))
}

#[cfg(test)]
mod tests {
    use super::*;

    const H0: i64 = 1_790_812_800_000; // 2026-10-01T00:00:00Z
    const MIN: i64 = 60_000;

    fn s(id: &str, minute: i64, soh: f64, delta: i64, temp: f64) -> Sample {
        Sample {
            event_id: id.into(),
            event_time_ms: H0 + minute * MIN,
            event_type: Some("battery_sample".into()),
            soh_pct: Some(soh),
            cell_voltage_delta_mv: Some(delta),
            pack_temp_c: Some(temp),
        }
    }

    fn at(minute: i64) -> Sample {
        s(&format!("e{minute}"), minute, 95.0, 10, 25.0)
    }

    fn hb(minute: i64) -> Sample {
        Sample {
            event_id: format!("hb{minute}"),
            event_time_ms: H0 + minute * MIN,
            event_type: Some("heartbeat".into()),
            soh_pct: None,
            cell_voltage_delta_mv: None,
            pack_temp_c: None,
        }
    }

    fn empty() -> BucketState {
        BucketState::empty("SYNTESTVIN0000001")
    }

    #[test]
    fn buckets_by_utc_hour_with_floor_div() {
        assert_eq!(window_start(H0 + 59 * MIN), H0);
        assert_eq!(window_start(H0 + 60 * MIN), H0 + HOUR_MS);
        assert_eq!(window_start(-1), -HOUR_MS);
    }

    #[test]
    fn an_hour_is_emitted_once_its_end_plus_grace_is_reached() {
        let (st, rows, _) = advance(&empty(), &[at(5), hb(69)], i64::MIN).unwrap();
        assert!(rows.is_empty());
        let (_, rows, _) = advance(&st, &[hb(70)], i64::MIN).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].window_start, "2026-10-01T00:00:00.000Z");
        assert_eq!(rows[0].window_end, "2026-10-01T01:00:00.000Z");
        assert_eq!(rows[0].date, "2026-10-01");
    }

    #[test]
    fn the_event_time_timeout_is_hour_end_plus_ten_minutes() {
        let (st, rows, timeout) = advance(&empty(), &[at(5)], i64::MIN).unwrap();
        assert!(rows.is_empty());
        assert_eq!(timeout, Some(H0 + 70 * MIN - 1));
        assert!(advance(&st, &[], H0 + 70 * MIN - 1).unwrap().1.is_empty());
        assert_eq!(advance(&st, &[], H0 + 70 * MIN).unwrap().1.len(), 1);
    }

    #[test]
    fn late_samples_join_an_open_hour_and_are_dropped_once_it_is_emitted() {
        let (st, _, _) = advance(&empty(), &[at(10), at(65)], i64::MIN).unwrap();
        let (st, rows, _) = advance(&st, &[at(5)], i64::MIN).unwrap();
        assert!(rows.is_empty());
        let (st, rows, _) = advance(&st, &[hb(70)], i64::MIN).unwrap();
        assert_eq!(rows[0].sample_count, 2);
        assert_eq!(st.emitted_until_ms, H0 + HOUR_MS);
        let (st, rows, _) = advance(&st, &[at(30)], i64::MIN).unwrap();
        assert!(rows.is_empty());
        assert_eq!(st.pending.len(), 1, "only the 01:05 sample stays pending");
    }

    #[test]
    fn duplicate_event_ids_count_once_within_a_batch_and_across_batches() {
        let a = at(5);
        let (st, _, _) = advance(&empty(), &[a.clone(), a.clone()], i64::MIN).unwrap();
        let (_, rows, _) = advance(&st, &[a, hb(70)], i64::MIN).unwrap();
        assert_eq!(rows[0].sample_count, 1);
    }

    #[test]
    fn heartbeats_advance_time_but_are_not_counted() {
        let (st, rows, timeout) = advance(&empty(), &[hb(5)], i64::MIN).unwrap();
        assert!(rows.is_empty() && st.pending.is_empty() && timeout.is_none());
        assert_eq!(st.max_seen_ms, H0 + 5 * MIN);
    }

    #[test]
    fn aggregates_are_exact_with_half_up_average() {
        let samples = [s("a", 0, 96.5, 12, 24.5), s("b", 10, 96.49, 13, 25.0)];
        let r = aggregate("V", H0, &samples).unwrap();
        assert_eq!(r.sample_count, 2);
        assert_eq!(r.soh_pct_min, Some(96.49));
        assert_eq!(r.soh_pct_avg, Some(96.5), "96.495 rounds half up");
        assert_eq!(
            (r.cell_voltage_delta_mv_min, r.cell_voltage_delta_mv_max),
            (12, 13)
        );
        assert_eq!(r.pack_temp_c_max, Some(25.0));
        let three = [
            s("a", 0, 91.02, 1, 1.0),
            s("b", 1, 91.01, 1, 1.0),
            s("c", 2, 91.01, 1, 1.0),
        ];
        assert_eq!(aggregate("V", H0, &three).unwrap().soh_pct_avg, Some(91.01));
        let tie = [s("a", 0, 90.01, 1, 1.0), s("b", 1, 90.02, 1, 1.0)];
        assert_eq!(aggregate("V", H0, &tie).unwrap().soh_pct_avg, Some(90.02));
    }

    #[test]
    fn values_beyond_the_column_scale_fail_as_legacy_set_scale_does() {
        assert!(aggregate("V", H0, &[s("a", 0, 90.123, 1, 1.0)]).is_err());
        assert!(aggregate("V", H0, &[s("a", 0, 90.0, 1, 1.25)]).is_err());
        // Only the minimum soh and maximum temperature are set to scale without rounding.
        let r = aggregate(
            "V",
            H0,
            &[s("a", 0, 90.0, 1, 1.25), s("b", 1, 91.005, 1, 2.0)],
        )
        .unwrap();
        assert_eq!(r.soh_pct_avg, Some(90.5));
        assert_eq!(r.pack_temp_c_max, Some(2.0));
    }

    #[test]
    fn a_missing_battery_field_is_an_error() {
        let mut a = at(1);
        a.soh_pct = None;
        assert!(aggregate("V", H0, &[a]).is_err());
    }
}
