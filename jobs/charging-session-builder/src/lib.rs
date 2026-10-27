//! charging-session-builder: charging sessions per VIN. See SEMANTICS.md.

pub mod reducer;

use anyhow::Result;
use telemetry_core::{Job, KeyedState, Record, StateStore};

use reducer::{ChargingSession, Input, SessionState};

/// `.withWatermark("event_time", "10 minutes")`.
pub const WATERMARK_DELAY_MS: i64 = 10 * 60 * 1000;
const WATERMARK_KEY: &str = "watermark";

pub struct ChargingSessionBuilder {
    sessions: KeyedState<String, SessionState>,
    /// Spark's event-time watermark, as of the end of the last batch. It starts at 0 as in Spark.
    watermark: KeyedState<String, i64>,
}

impl Default for ChargingSessionBuilder {
    fn default() -> Self {
        ChargingSessionBuilder {
            sessions: KeyedState::new("sessions"),
            watermark: KeyedState::new("watermark"),
        }
    }
}

impl ChargingSessionBuilder {
    pub fn watermark_ms(&self) -> i64 {
        self.watermark
            .get(&WATERMARK_KEY.to_string())
            .copied()
            .unwrap_or(0)
    }
}

impl Job for ChargingSessionBuilder {
    type Input = Input;
    type Output = ChargingSession;

    fn decode(&self, payload: &[u8]) -> Result<Option<Input>> {
        reducer::decode(payload).map(Some)
    }

    /// Spark drops rows at or before the watermark before `flatMapGroupsWithState` sees them.
    fn process(&mut self, input: Input) -> Result<Vec<ChargingSession>> {
        let watermark = self.watermark_ms();
        if input.event.event_time_ms <= watermark {
            return Ok(Vec::new());
        }
        let state = match self.sessions.get(&input.vin) {
            Some(s) => s.clone(),
            None => SessionState::empty(&input.vin),
        };
        let (next, out, _timeout) =
            reducer::advance(&state, std::slice::from_ref(&input.event), watermark)?;
        if next != state {
            self.sessions.put(input.vin, next)?;
        }
        Ok(out)
    }

    /// Advance the watermark to the highest event time seen minus 10 minutes, then run `advance`
    /// with it for every VIN, as Spark's `EventTimeTimeout` does in the next batch. A VIN whose
    /// timeout has not passed is left unchanged by `advance`.
    fn end_batch(&mut self) -> Result<Vec<ChargingSession>> {
        let mut vins = self.sessions.keys();
        vins.sort();
        let max_seen = vins
            .iter()
            .filter_map(|v| self.sessions.get(v).map(|s| s.max_seen_ms))
            .max();
        let old = self.watermark_ms();
        let watermark = match max_seen {
            Some(m) => old.max(m.saturating_sub(WATERMARK_DELAY_MS)),
            None => old,
        };
        if watermark == old {
            return Ok(Vec::new());
        }
        self.watermark.put(WATERMARK_KEY.to_string(), watermark)?;
        let mut out = Vec::new();
        for vin in vins {
            let state = self.sessions.get(&vin).cloned().expect("listed key");
            let (next, emitted, _timeout) = reducer::advance(&state, &[], watermark)?;
            if next != state {
                self.sessions.put(vin, next)?;
            }
            out.extend(emitted);
        }
        Ok(out)
    }

    fn encode(&self, s: &ChargingSession) -> Result<Record> {
        Ok(Record {
            key: s.vin.clone(),
            value: serde_json::to_vec(s)?,
        })
    }

    fn stores(&mut self) -> Vec<&mut dyn StateStore> {
        vec![&mut self.sessions, &mut self.watermark]
    }
}
