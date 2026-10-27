//! diagnostics-dtc-router: one service alert per routable DTC event. See SEMANTICS.md.

pub mod reducer;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use telemetry_core::{Job, KeyedState, Record, StateStore};

use reducer::{Alert, Input};

/// `.withWatermark("event_time", "10 minutes")`.
pub const WATERMARK_DELAY_MS: i64 = 10 * 60 * 1000;
const WATERMARK_KEY: &str = "watermark";
const MAX_EVENT_TIME_KEY: &str = "max_event_time";

/// `dropDuplicatesWithinWatermark` state for one `event_id`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Seen {
    /// `event_time + 10 minutes`; evicted once the watermark reaches it.
    pub expires_at_ms: i64,
}

pub struct DiagnosticsDtcRouter {
    seen: KeyedState<String, Seen>,
    /// `watermark`: Spark's event-time watermark as of the end of the last batch (starts at 0).
    /// `max_event_time`: the highest event_time of any routable row, duplicates and late rows included.
    watermark: KeyedState<String, i64>,
}

impl Default for DiagnosticsDtcRouter {
    fn default() -> Self {
        DiagnosticsDtcRouter {
            seen: KeyedState::new("seen"),
            watermark: KeyedState::new("watermark"),
        }
    }
}

impl DiagnosticsDtcRouter {
    fn get(&self, key: &str) -> Option<i64> {
        self.watermark.get(&key.to_string()).copied()
    }

    pub fn watermark_ms(&self) -> i64 {
        self.get(WATERMARK_KEY).unwrap_or(0)
    }

    pub fn seen_len(&self) -> usize {
        self.seen.keys().len()
    }
}

impl Job for DiagnosticsDtcRouter {
    type Input = Input;
    type Output = Alert;

    fn decode(&self, payload: &[u8]) -> Result<Option<Input>> {
        reducer::decode(payload)
    }

    /// Every routable row moves the watermark's maximum. A row at or before the watermark, or whose
    /// `event_id` is in the dedup state, is dropped; otherwise the alert is emitted now.
    fn process(&mut self, input: Input) -> Result<Vec<Alert>> {
        if self
            .get(MAX_EVENT_TIME_KEY)
            .is_none_or(|m| input.event_time_ms > m)
        {
            self.watermark
                .put(MAX_EVENT_TIME_KEY.to_string(), input.event_time_ms)?;
        }
        if input.event_time_ms <= self.watermark_ms() || self.seen.get(&input.event_id).is_some() {
            return Ok(Vec::new());
        }
        self.seen.put(
            input.event_id,
            Seen {
                expires_at_ms: input.event_time_ms.saturating_add(WATERMARK_DELAY_MS),
            },
        )?;
        Ok(vec![input.alert])
    }

    /// Advance the watermark to the highest event time minus 10 minutes, then evict dedup entries that
    /// expire at or before it, as Spark's no-data batch after an `AvailableNow` batch does. Emits nothing.
    fn end_batch(&mut self) -> Result<Vec<Alert>> {
        let old = self.watermark_ms();
        let watermark = match self.get(MAX_EVENT_TIME_KEY) {
            Some(m) => old.max(m.saturating_sub(WATERMARK_DELAY_MS)),
            None => old,
        };
        if watermark == old {
            return Ok(Vec::new());
        }
        self.watermark.put(WATERMARK_KEY.to_string(), watermark)?;
        let mut ids = self.seen.keys();
        ids.sort();
        for id in ids {
            if self
                .seen
                .get(&id)
                .is_some_and(|s| s.expires_at_ms <= watermark)
            {
                self.seen.delete(&id)?;
            }
        }
        Ok(Vec::new())
    }

    fn encode(&self, a: &Alert) -> Result<Record> {
        Ok(Record {
            key: a.vin.clone(),
            value: serde_json::to_vec(a)?,
        })
    }

    fn stores(&mut self) -> Vec<&mut dyn StateStore> {
        vec![&mut self.seen, &mut self.watermark]
    }
}
