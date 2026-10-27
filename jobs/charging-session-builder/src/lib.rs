//! charging-session-builder: charging sessions per VIN, emitted when the legacy job emits them. See
//! SEMANTICS.md.

pub mod reducer;

use std::collections::BTreeSet;

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

/// Per-VIN scalars of the legacy `SessionState` plus its timeout. The pending events are in `events`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VinState {
    pub vin: String,
    pub max_seen_ms: i64,
    pub emitted_until_ms: i64,
    pub timeout_ms: Option<i64>,
}

/// One event slot of a VIN. Slots `vin|0`, `vin|1`, ... are used from 0 without gaps: first the
/// pending events (`pending`), in order, then the open batch's rows, in arrival order. A slot no
/// longer needed is cleared to `event: None`, so the changelog never needs a tombstone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Slot {
    pub vin: String,
    pub pending: bool,
    pub event: Option<ChargingEvent>,
}

fn slot_key(vin: &str, i: usize) -> String {
    format!("{vin}|{i}")
}

/// A Spark micro-batch hands each VIN all of its new rows at once, so records are buffered and
/// applied in `end_batch`, followed by the timeouts of the final no-data batch.
pub struct ChargingSessionBuilder {
    sessions: KeyedState<String, VinState>,
    events: KeyedState<String, Slot>,
    watermark: KeyedState<String, Watermark>,
}

impl Default for ChargingSessionBuilder {
    fn default() -> Self {
        ChargingSessionBuilder {
            sessions: KeyedState::new("sessions"),
            events: KeyedState::new("events"),
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

    /// The VIN's occupied slots, in slot order.
    fn slots(&self, vin: &str) -> Vec<(bool, ChargingEvent)> {
        (0..)
            .map_while(|i| {
                let s = self.events.get(&slot_key(vin, i))?;
                Some((s.pending, s.event.clone()?))
            })
            .collect()
    }

    /// `flatMapGroupsWithState` for one VIN: its new rows of the batch, or none when its timeout fires.
    fn step(&mut self, vin: &str, watermark_ms: i64, out: &mut Vec<ChargingSession>) -> Result<()> {
        let key = vin.to_string();
        let slots = self.slots(vin);
        let scalars = self.sessions.get(&key).cloned();
        let mut state = SessionState::empty(vin);
        if let Some(s) = scalars {
            state.max_seen_ms = s.max_seen_ms;
            state.emitted_until_ms = s.emitted_until_ms;
            state.timeout_ms = s.timeout_ms;
        }
        let mut rows = Vec::new();
        for (pending, e) in &slots {
            if *pending {
                state.pending.push(e.clone());
            } else {
                rows.push(e.clone());
            }
        }
        let (next, emitted) = reducer::advance(&state, &rows, watermark_ms)?;
        out.extend(emitted);
        for i in 0..slots.len().max(next.pending.len()) {
            let event = next.pending.get(i).cloned();
            self.events.put(
                slot_key(vin, i),
                Slot {
                    vin: key.clone(),
                    pending: event.is_some(),
                    event,
                },
            )?;
        }
        self.sessions.put(
            key.clone(),
            VinState {
                vin: key,
                max_seen_ms: next.max_seen_ms,
                emitted_until_ms: next.emitted_until_ms,
                timeout_ms: next.timeout_ms,
            },
        )
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
            self.step(&vin, watermark_ms, out)?;
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
            let s = SessionState::empty(&e.vin);
            self.sessions.put(
                e.vin.clone(),
                VinState {
                    vin: s.vin,
                    max_seen_ms: s.max_seen_ms,
                    emitted_until_ms: s.emitted_until_ms,
                    timeout_ms: s.timeout_ms,
                },
            )?;
        }
        let i = self.slots(&e.vin).len();
        self.events.put(
            slot_key(&e.vin, i),
            Slot {
                vin: e.vin.clone(),
                pending: false,
                event: Some(e),
            },
        )?;
        Ok(Vec::new())
    }

    /// One legacy `AvailableNow` run: the data batch with the current watermark, then the watermark
    /// advances and the final no-data batch fires the timeouts below it.
    fn end_batch(&mut self) -> Result<Vec<ChargingSession>> {
        let wm = self.watermark();
        let with_rows: BTreeSet<String> = self
            .events
            .keys()
            .into_iter()
            .filter_map(|k| {
                let s = self.events.get(&k)?;
                (!s.pending && s.event.is_some()).then(|| s.vin.clone())
            })
            .collect();
        let mut out = Vec::new();
        for vin in with_rows {
            self.step(&vin, wm.current_ms, &mut out)?;
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
        vec![&mut self.sessions, &mut self.events, &mut self.watermark]
    }
}
