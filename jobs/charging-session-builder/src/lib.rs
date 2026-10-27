//! charging-session-builder: charging sessions per VIN, emitted when the legacy job emits them. See
//! SEMANTICS.md.

pub mod reducer;

use std::collections::BTreeMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use telemetry_core::{Job, KeyedState, Record, StateStore};

use reducer::{ChargingEvent, ChargingSession, SessionState};

const WATERMARK_KEY: &str = "watermark";

/// The event-time watermark, kept as one key in its own store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Watermark {
    /// Watermark of the open batch: late rows at or before it are dropped and timeouts below it fire.
    pub current_ms: i64,
    /// Highest event time read in the open batch, dropped rows included.
    pub batch_max_ms: Option<i64>,
}

impl Default for Watermark {
    fn default() -> Self {
        Watermark {
            current_ms: reducer::INITIAL_WATERMARK_MS,
            batch_max_ms: None,
        }
    }
}

/// A Spark micro-batch hands each VIN all of its new rows at once, so records are buffered (`batch`,
/// keyed `vin|event_id`) and applied in `end_batch`, followed by the timeouts of the final no-data batch.
pub struct ChargingSessionBuilder {
    sessions: KeyedState<String, SessionState>,
    batch: KeyedState<String, ChargingEvent>,
    watermark: KeyedState<String, Watermark>,
}

impl Default for ChargingSessionBuilder {
    fn default() -> Self {
        ChargingSessionBuilder {
            sessions: KeyedState::new("sessions"),
            batch: KeyedState::new("batch"),
            watermark: KeyedState::new("watermark"),
        }
    }
}

impl ChargingSessionBuilder {
    fn watermark(&self) -> Watermark {
        self.watermark
            .get(&WATERMARK_KEY.to_string())
            .cloned()
            .unwrap_or_default()
    }

    /// `flatMapGroupsWithState` for one VIN: its new rows, or none when its timeout fires.
    fn step(
        &mut self,
        vin: &str,
        events: &[ChargingEvent],
        watermark_ms: i64,
        out: &mut Vec<ChargingSession>,
    ) -> Result<()> {
        let key = vin.to_string();
        let state = self
            .sessions
            .get(&key)
            .cloned()
            .unwrap_or_else(|| SessionState::empty(vin));
        let (next, emitted) = reducer::advance(&state, events, watermark_ms)?;
        out.extend(emitted);
        self.sessions.put(key, next)
    }

    /// Spark's timeout pass: every VIN whose timeout is strictly below the watermark, in VIN order.
    fn fire_timeouts(&mut self, watermark_ms: i64, out: &mut Vec<ChargingSession>) -> Result<()> {
        let mut due: Vec<String> = self
            .sessions
            .keys()
            .into_iter()
            .filter(|k| {
                self.sessions
                    .get(k)
                    .and_then(|s| s.timeout_ms)
                    .is_some_and(|t| t < watermark_ms)
            })
            .collect();
        due.sort();
        for vin in due {
            self.step(&vin, &[], watermark_ms, out)?;
        }
        Ok(())
    }
}

impl Job for ChargingSessionBuilder {
    type Input = ChargingEvent;
    type Output = ChargingSession;

    fn decode(&self, payload: &[u8]) -> Result<Option<ChargingEvent>> {
        reducer::decode(payload).map(Some)
    }

    fn process(&mut self, e: ChargingEvent) -> Result<Vec<ChargingSession>> {
        let mut wm = self.watermark();
        if wm.batch_max_ms.is_none_or(|m| e.event_time_ms > m) {
            wm.batch_max_ms = Some(e.event_time_ms);
            self.watermark.put(WATERMARK_KEY.to_string(), wm.clone())?;
        }
        // Spark drops rows at or before the watermark before they reach the VIN's state.
        if e.event_time_ms <= wm.current_ms {
            return Ok(Vec::new());
        }
        // Create the VIN's state here, so it lives on the VIN's input partition.
        if self.sessions.get(&e.vin).is_none() {
            self.sessions
                .put(e.vin.clone(), SessionState::empty(&e.vin))?;
        }
        let key = format!("{}|{}", e.vin, e.event_id);
        if self.batch.get(&key).is_none() {
            self.batch.put(key, e)?;
        }
        Ok(Vec::new())
    }

    /// One legacy `AvailableNow` run: the data batch with the current watermark, then the watermark
    /// advances and the final no-data batch fires the timeouts below it.
    fn end_batch(&mut self) -> Result<Vec<ChargingSession>> {
        let wm = self.watermark();
        let mut by_vin: BTreeMap<String, Vec<ChargingEvent>> = BTreeMap::new();
        for k in self.batch.keys() {
            if let Some(e) = self.batch.get(&k) {
                by_vin.entry(e.vin.clone()).or_default().push(e.clone());
            }
            self.batch.delete(&k)?;
        }
        let mut out = Vec::new();
        for (vin, mut events) in by_vin {
            events.sort_by(|a, b| {
                (a.event_time_ms, a.producer_seq, &a.event_id).cmp(&(
                    b.event_time_ms,
                    b.producer_seq,
                    &b.event_id,
                ))
            });
            self.step(&vin, &events, wm.current_ms, &mut out)?;
        }
        self.fire_timeouts(wm.current_ms, &mut out)?;
        if let Some(max) = wm.batch_max_ms {
            let next = wm
                .current_ms
                .max(max.saturating_sub(reducer::WATERMARK_DELAY_MS));
            self.watermark.put(
                WATERMARK_KEY.to_string(),
                Watermark {
                    current_ms: next,
                    batch_max_ms: None,
                },
            )?;
            if next > wm.current_ms {
                self.fire_timeouts(next, &mut out)?;
            }
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
        vec![&mut self.sessions, &mut self.batch, &mut self.watermark]
    }
}
