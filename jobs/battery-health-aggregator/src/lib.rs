//! battery-health-aggregator: hourly battery health per VIN, written as Parquet. See SEMANTICS.md.

pub mod reducer;

use anyhow::Result;
use telemetry_core::{Column, ColumnType, Job, KeyedState, ParquetSpec, Record, StateStore};

use reducer::{BucketState, HourlyHealth, Input};

/// `.withWatermark("event_time", "10 minutes")`.
pub const WATERMARK_DELAY_MS: i64 = 10 * 60 * 1000;
const WATERMARK_KEY: &str = "watermark";

pub struct BatteryHealthAggregator {
    buckets: KeyedState<String, BucketState>,
    /// Spark's event-time watermark, as of the end of the last batch. It starts at 0 as in Spark.
    watermark: KeyedState<String, i64>,
}

impl Default for BatteryHealthAggregator {
    fn default() -> Self {
        BatteryHealthAggregator {
            buckets: KeyedState::new("buckets"),
            watermark: KeyedState::new("watermark"),
        }
    }
}

impl BatteryHealthAggregator {
    pub fn watermark_ms(&self) -> i64 {
        self.watermark
            .get(&WATERMARK_KEY.to_string())
            .copied()
            .unwrap_or(0)
    }
}

impl Job for BatteryHealthAggregator {
    type Input = Input;
    type Output = HourlyHealth;

    fn decode(&self, payload: &[u8]) -> Result<Option<Input>> {
        reducer::decode(payload).map(Some)
    }

    /// Spark drops rows at or before the watermark before `flatMapGroupsWithState` sees them. The
    /// rest are kept with the VIN until the end of the batch, where Spark calls `step` once per VIN
    /// with all of them.
    fn process(&mut self, input: Input) -> Result<Vec<HourlyHealth>> {
        if input.sample.event_time_ms <= self.watermark_ms() {
            return Ok(Vec::new());
        }
        let mut state = match self.buckets.get(&input.vin) {
            Some(s) => s.clone(),
            None => BucketState::empty(&input.vin),
        };
        state.arrived.push(input.sample);
        self.buckets.put(input.vin, state)?;
        Ok(Vec::new())
    }

    /// The end of a Spark micro-batch: run `advance` for every VIN with records in the batch, with
    /// the batch's watermark. Then advance the watermark to the highest event time seen minus 10
    /// minutes and run `advance` with it for every VIN, as Spark's `EventTimeTimeout` does in the
    /// next batch (the final no-data batch of `AvailableNow`). A VIN whose timeout has not passed is
    /// left unchanged by `advance`.
    fn end_batch(&mut self) -> Result<Vec<HourlyHealth>> {
        let mut vins = self.buckets.keys();
        vins.sort();
        let old = self.watermark_ms();
        let mut out = Vec::new();
        for vin in &vins {
            let state = self.buckets.get(vin).cloned().expect("listed key");
            if state.arrived.is_empty() {
                continue;
            }
            let (next, emitted, _timeout) = reducer::advance(&state, &state.arrived, old)?;
            self.buckets.put(vin.clone(), next)?;
            out.extend(emitted);
        }
        let max_seen = vins
            .iter()
            .filter_map(|v| self.buckets.get(v).map(|s| s.max_seen_ms))
            .max();
        let watermark = match max_seen {
            Some(m) => old.max(m.saturating_sub(WATERMARK_DELAY_MS)),
            None => old,
        };
        if watermark == old {
            return Ok(out);
        }
        self.watermark.put(WATERMARK_KEY.to_string(), watermark)?;
        for vin in vins {
            let state = self.buckets.get(&vin).cloned().expect("listed key");
            let (next, emitted, _timeout) = reducer::advance(&state, &[], watermark)?;
            if next != state {
                self.buckets.put(vin, next)?;
            }
            out.extend(emitted);
        }
        Ok(out)
    }

    fn parquet(&self) -> Option<ParquetSpec> {
        Some(spec())
    }

    fn encode(&self, h: &HourlyHealth) -> Result<Record> {
        Ok(Record {
            key: h.vin.clone(),
            value: serde_json::to_vec(h)?,
        })
    }

    fn stores(&mut self) -> Vec<&mut dyn StateStore> {
        vec![&mut self.buckets, &mut self.watermark]
    }
}

/// The legacy output columns and the physical types Spark writes for them, partitioned by `date`.
pub fn spec() -> ParquetSpec {
    let col = |name, ty, nullable| Column { name, ty, nullable };
    let dec = |precision, scale| ColumnType::Decimal { precision, scale };
    ParquetSpec {
        columns: vec![
            col("vin", ColumnType::Utf8, true),
            col("window_start", ColumnType::TimestampMillis, true),
            col("window_end", ColumnType::TimestampMillis, true),
            col("sample_count", ColumnType::Int32, false),
            col("soh_pct_min", dec(5, 2), true),
            col("soh_pct_avg", dec(5, 2), true),
            col("cell_voltage_delta_mv_min", ColumnType::Int32, false),
            col("cell_voltage_delta_mv_max", ColumnType::Int32, false),
            col("pack_temp_c_max", dec(4, 1), true),
        ],
        partition_by: "date",
    }
}
