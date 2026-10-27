//! Configuration from the harness contract's environment variables.

use std::time::Duration;

use anyhow::{bail, Context, Result};

/// Where a job writes its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sink {
    /// `OUTPUT_TOPIC`: a Kafka topic written inside the transaction.
    Topic(String),
    /// `OUTPUT_PATH`: an object-store path (Parquet jobs; not used by Kafka jobs).
    Path(String),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bootstrap: String,
    pub input_topics: Vec<String>,
    pub sink: Sink,
    pub s3_endpoint: Option<String>,
    pub consumer_group: String,
    pub transactional_id: String,
    /// Static membership id. Defaults to `TRANSACTIONAL_ID`, which is already a stable per-instance id.
    pub group_instance_id: String,
    pub state_topic_prefix: String,
    pub exit_when_caught_up: bool,
    pub batch_max_records: usize,
    pub batch_max: Duration,
    pub txn_timeout: Duration,
    pub metrics_port: u16,
    pub log_level: String,
    /// `TELEMETRY_FAILPOINT_AT_OFFSETS`: test only, see docs/DESIGN.md (Failpoint).
    pub failpoint_at_offsets: Option<i64>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let get = |k: &str| get(k).filter(|v| !v.trim().is_empty());
        let req = |k: &str| get(k).with_context(|| format!("{k} is required"));
        let num = |k: &str, default: u64| -> Result<u64> {
            match get(k) {
                Some(v) => v
                    .trim()
                    .parse()
                    .with_context(|| format!("{k} must be a non-negative integer")),
                None => Ok(default),
            }
        };
        let sink = match (get("OUTPUT_TOPIC"), get("OUTPUT_PATH")) {
            (Some(t), None) => Sink::Topic(t),
            (None, Some(p)) => Sink::Path(p),
            (Some(_), Some(_)) => bail!("set only one of OUTPUT_TOPIC and OUTPUT_PATH"),
            (None, None) => bail!("OUTPUT_TOPIC or OUTPUT_PATH is required"),
        };
        let input_topics: Vec<String> = req("INPUT_TOPICS")?
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if input_topics.is_empty() {
            bail!("INPUT_TOPICS lists no topics");
        }
        let transactional_id = req("TRANSACTIONAL_ID")?;
        let batch_max_records = num("BATCH_MAX_RECORDS", 500)? as usize;
        if batch_max_records == 0 {
            bail!("BATCH_MAX_RECORDS must be at least 1");
        }
        let exit = get("EXIT_WHEN_CAUGHT_UP")
            .unwrap_or_default()
            .to_ascii_lowercase();
        Ok(Config {
            bootstrap: req("KAFKA_BOOTSTRAP")?,
            input_topics,
            sink,
            s3_endpoint: get("S3_ENDPOINT"),
            consumer_group: req("CONSUMER_GROUP")?,
            group_instance_id: get("GROUP_INSTANCE_ID").unwrap_or_else(|| transactional_id.clone()),
            transactional_id,
            state_topic_prefix: req("STATE_TOPIC_PREFIX")?,
            exit_when_caught_up: matches!(exit.as_str(), "true" | "1" | "yes"),
            batch_max_records,
            batch_max: Duration::from_millis(num("BATCH_MAX_MS", 200)?),
            txn_timeout: Duration::from_millis(num("TXN_TIMEOUT_MS", 60_000)?),
            metrics_port: u16::try_from(num("METRICS_PORT", 9090)?)
                .context("METRICS_PORT is out of range")?,
            log_level: get("LOG_LEVEL").unwrap_or_else(|| "info".into()),
            failpoint_at_offsets: match get("TELEMETRY_FAILPOINT_AT_OFFSETS") {
                Some(_) => Some(num("TELEMETRY_FAILPOINT_AT_OFFSETS", 0)? as i64),
                None => None,
            },
        })
    }

    /// Changelog topic for a state store.
    pub fn state_topic(&self, store: &str) -> String {
        format!("{}.{}", self.state_topic_prefix, store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn harness_env() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("KAFKA_BOOTSTRAP", "kafka:9092"),
            ("INPUT_TOPICS", "vehicle.ota.status.v1"),
            ("OUTPUT_TOPIC", "rust.ota.status.latest.v1"),
            ("S3_ENDPOINT", "http://minio:9000"),
            ("CONSUMER_GROUP", "rust.ota-status-tracker"),
            ("TRANSACTIONAL_ID", "rust.ota-status-tracker.tx"),
            ("STATE_TOPIC_PREFIX", "rust.ota-status-tracker.state"),
            ("EXIT_WHEN_CAUGHT_UP", "true"),
        ])
    }

    fn parse(env: &HashMap<&str, &str>) -> Result<Config> {
        Config::from_lookup(|k| env.get(k).map(|v| v.to_string()))
    }

    #[test]
    fn harness_contract_with_defaults() {
        let c = parse(&harness_env()).unwrap();
        assert_eq!(c.sink, Sink::Topic("rust.ota.status.latest.v1".into()));
        assert_eq!(c.group_instance_id, "rust.ota-status-tracker.tx");
        assert!(c.exit_when_caught_up);
        assert_eq!(
            (
                c.batch_max_records,
                c.batch_max.as_millis(),
                c.txn_timeout.as_millis()
            ),
            (500, 200, 60_000)
        );
        assert_eq!(c.metrics_port, 9090);
        assert_eq!(c.failpoint_at_offsets, None);
        assert_eq!(
            c.state_topic("latest"),
            "rust.ota-status-tracker.state.latest"
        );
    }

    #[test]
    fn optional_overrides_and_errors() {
        let mut env = harness_env();
        env.insert("GROUP_INSTANCE_ID", "pod-0");
        env.insert("BATCH_MAX_RECORDS", "5");
        let c = parse(&env).unwrap();
        assert_eq!(
            (c.group_instance_id.as_str(), c.batch_max_records),
            ("pod-0", 5)
        );
        env.insert("TELEMETRY_FAILPOINT_AT_OFFSETS", "18");
        assert_eq!(parse(&env).unwrap().failpoint_at_offsets, Some(18));
        env.insert("TELEMETRY_FAILPOINT_AT_OFFSETS", "-1");
        assert!(parse(&env).is_err());
        env.remove("TELEMETRY_FAILPOINT_AT_OFFSETS");
        env.insert("OUTPUT_PATH", "s3a://x");
        assert!(parse(&env).is_err());
        env.remove("OUTPUT_PATH");
        env.insert("BATCH_MAX_RECORDS", "0");
        assert!(parse(&env).is_err());
        env.remove("BATCH_MAX_RECORDS");
        env.remove("KAFKA_BOOTSTRAP");
        assert!(parse(&env).is_err());
    }
}
