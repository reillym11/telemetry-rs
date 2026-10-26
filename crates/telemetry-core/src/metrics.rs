//! Prometheus metrics served at /metrics.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{exponential_buckets, Histogram};
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabels {
    pub topic: String,
    pub partition: i32,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StoreLabels {
    pub store: String,
}

pub struct Metrics {
    pub registry: Registry,
    pub records_in: Counter,
    pub records_out: Counter,
    pub decode_errors: Counter,
    pub txn_committed: Counter,
    pub txn_aborted: Counter,
    pub txn_duration: Histogram,
    pub consumer_lag: Family<PartitionLabels, Gauge>,
    pub state_keys: Family<StoreLabels, Gauge>,
    pub restore_duration: Histogram,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        let m = Metrics {
            records_in: Counter::default(),
            records_out: Counter::default(),
            decode_errors: Counter::default(),
            txn_committed: Counter::default(),
            txn_aborted: Counter::default(),
            txn_duration: Histogram::new(exponential_buckets(0.001, 2.0, 16)),
            consumer_lag: Family::default(),
            state_keys: Family::default(),
            restore_duration: Histogram::new(exponential_buckets(0.01, 2.0, 14)),
            registry: Registry::default(),
        };
        // Counters get the _total suffix from the library.
        registry.register("records_in", "Input records consumed", m.records_in.clone());
        registry.register(
            "records_out",
            "Output records produced",
            m.records_out.clone(),
        );
        registry.register(
            "decode_errors",
            "Input records skipped because they did not decode",
            m.decode_errors.clone(),
        );
        registry.register(
            "txn_committed",
            "Committed transactions",
            m.txn_committed.clone(),
        );
        registry.register("txn_aborted", "Aborted transactions", m.txn_aborted.clone());
        registry.register(
            "txn_duration_seconds",
            "Begin to commit, per transaction",
            m.txn_duration.clone(),
        );
        registry.register(
            "consumer_lag",
            "Input end offset minus committed offset",
            m.consumer_lag.clone(),
        );
        registry.register(
            "state_keys",
            "Committed keys per state store",
            m.state_keys.clone(),
        );
        registry.register(
            "restore_duration_seconds",
            "Changelog restore time per assignment",
            m.restore_duration.clone(),
        );
        Metrics { registry, ..m }
    }

    pub fn encode(&self) -> String {
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &self.registry)
            .expect("writing to a String cannot fail");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_the_required_metric_names() {
        let m = Metrics::new();
        m.records_in.inc();
        m.consumer_lag
            .get_or_create(&PartitionLabels {
                topic: "t".into(),
                partition: 0,
            })
            .set(3);
        m.state_keys
            .get_or_create(&StoreLabels {
                store: "latest".into(),
            })
            .set(1);
        let text = m.encode();
        for name in [
            "records_in_total",
            "records_out_total",
            "txn_committed_total",
            "txn_aborted_total",
            "txn_duration_seconds_bucket",
            "consumer_lag{",
            "state_keys{",
            "restore_duration_seconds_bucket",
        ] {
            assert!(text.contains(name), "missing {name}");
        }
    }
}
