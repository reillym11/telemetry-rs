//! ota-status-tracker: latest OTA state per (vin, campaign_id). See SEMANTICS.md.

pub mod reducer;

use anyhow::Result;
use telemetry_core::{Job, KeyedState, Record, StateStore};

use reducer::{OtaLatest, Winner};

pub struct OtaStatusTracker {
    latest: KeyedState<String, Winner>,
}

impl Default for OtaStatusTracker {
    fn default() -> Self {
        OtaStatusTracker {
            latest: KeyedState::new("latest"),
        }
    }
}

impl Job for OtaStatusTracker {
    type Input = Winner;
    type Output = OtaLatest;

    fn decode(&self, payload: &[u8]) -> Result<Option<Winner>> {
        reducer::decode(payload)
    }

    fn process(&mut self, e: Winner) -> Result<Vec<OtaLatest>> {
        let k = reducer::key(&e.vin, e.campaign_id.as_deref());
        if !reducer::wins(self.latest.get(&k), &e) {
            return Ok(vec![]);
        }
        let out = reducer::to_output(&e);
        self.latest.put(k, e)?;
        Ok(vec![out])
    }

    fn encode(&self, o: &OtaLatest) -> Result<Record> {
        Ok(Record {
            key: reducer::key(&o.vin, o.campaign_id.as_deref()),
            value: serde_json::to_vec(o)?,
        })
    }

    fn stores(&mut self) -> Vec<&mut dyn StateStore> {
        vec![&mut self.latest]
    }
}
