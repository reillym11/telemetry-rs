//! charging-session-builder: charging sessions per VIN. See SEMANTICS.md.

pub mod reducer;

use anyhow::Result;
use telemetry_core::{Job, KeyedState, Record, StateStore};

use reducer::{ChargingSession, Input, SessionState};

pub struct ChargingSessionBuilder {
    sessions: KeyedState<String, SessionState>,
}

impl Default for ChargingSessionBuilder {
    fn default() -> Self {
        ChargingSessionBuilder {
            sessions: KeyedState::new("sessions"),
        }
    }
}

impl Job for ChargingSessionBuilder {
    type Input = Input;
    type Output = ChargingSession;

    fn decode(&self, payload: &[u8]) -> Result<Option<Input>> {
        reducer::decode(payload).map(Some)
    }

    fn process(&mut self, input: Input) -> Result<Vec<ChargingSession>> {
        let state = match self.sessions.get(&input.vin) {
            Some(s) => s.clone(),
            None => SessionState::empty(&input.vin),
        };
        let (next, out, _timeout) =
            reducer::advance(&state, std::slice::from_ref(&input.event), i64::MIN)?;
        if next != state {
            self.sessions.put(input.vin, next)?;
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
        vec![&mut self.sessions]
    }
}
