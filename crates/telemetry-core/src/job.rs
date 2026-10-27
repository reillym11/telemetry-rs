//! The interface a port implements. Everything Kafka-related lives in the processor.

use anyhow::Result;

use crate::parquet_sink::ParquetSpec;
use crate::state::StateStore;

/// One encoded output record. The key decides the partition (murmur2, as the Java client does).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub key: String,
    pub value: Vec<u8>,
}

pub trait Job: Send + 'static {
    type Input;
    type Output;

    /// Decode one input value. `Ok(None)` skips the record (for example a heartbeat); its offset
    /// still commits. `Err` also skips it and is counted, never logged with the payload.
    fn decode(&self, payload: &[u8]) -> Result<Option<Self::Input>>;

    /// Apply one record. Read and write state only through the job's [`KeyedState`] stores: writes
    /// are staged and become visible to later records in the same transaction, and are applied for
    /// good only after the transaction commits.
    ///
    /// [`KeyedState`]: crate::state::KeyedState
    fn process(&mut self, input: Self::Input) -> Result<Vec<Self::Output>>;

    /// Called once at the end of every batch, inside its transaction, after the batch's last
    /// record. A batch is every record up to the input end offsets read when it started, as in a
    /// Spark micro-batch. Jobs with an event-time watermark or timeouts advance them here. State
    /// writes go to each existing key's own partition. The default does nothing.
    fn end_batch(&mut self) -> Result<Vec<Self::Output>> {
        Ok(Vec::new())
    }

    /// For a job whose output is `OUTPUT_PATH`: the Parquet columns `encode`'s JSON value maps to.
    /// The key of each [`Record`] is then unused. The default is `None` (Kafka output only).
    fn parquet(&self) -> Option<ParquetSpec> {
        None
    }

    fn encode(&self, output: &Self::Output) -> Result<Record>;

    /// Every state store the job owns. The order must be stable.
    fn stores(&mut self) -> Vec<&mut dyn StateStore>;
}
