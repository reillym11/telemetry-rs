//! The transactional consume, process, produce loop. See docs/DESIGN.md for the full design.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Message, ToBytes};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::{Offset, TopicPartitionList};
use tokio::runtime::Handle;

use crate::config::{Config, Sink};
use crate::job::Job;
use crate::metrics::{Metrics, PartitionLabels, StoreLabels};
use crate::state::StateStore;

type Tp = (String, i32);
type TxProducer = ThreadedProducer<DefaultProducerContext>;

const IDLE_POLL: Duration = Duration::from_millis(100);
const RESTORE_TIMEOUT: Duration = Duration::from_secs(120);
const RETRY_BUDGET: Duration = Duration::from_secs(30);

/// How a failed transactional call is handled (docs/DESIGN.md, error-handling table).
#[derive(Debug, PartialEq, Eq)]
enum Class {
    Retry,
    Abort,
    Fatal,
}

fn classify(e: &KafkaError) -> Class {
    match e {
        KafkaError::Transaction(t) if t.is_fatal() => Class::Fatal,
        KafkaError::Transaction(t) if t.txn_requires_abort() => Class::Abort,
        KafkaError::Transaction(t) if t.is_retriable() => Class::Retry,
        _ => Class::Fatal,
    }
}

/// Retry `op` while it fails with a retriable error, with backoff from 100 ms to 5 s and a 30 s budget.
fn retrying<T>(what: &str, mut op: impl FnMut() -> Result<T, KafkaError>) -> Result<T, KafkaError> {
    let start = Instant::now();
    let mut backoff = Duration::from_millis(100);
    loop {
        match op() {
            Err(e) if classify(&e) == Class::Retry && start.elapsed() < RETRY_BUDGET => {
                tracing::warn!(op = what, error = %e, backoff_ms = backoff.as_millis() as u64, "retriable error");
                sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
            r => return r,
        }
    }
}

enum Event {
    Assigned(Vec<Tp>),
    Revoked {
        partitions: Vec<Tp>,
        aborted: bool,
        abort_error: Option<KafkaError>,
    },
}

/// Rebalance callbacks run inside `poll` on the processing thread. A transaction is never left
/// open across a revoke: it is aborted here, before the partitions are given up.
struct Ctx {
    producer: Arc<TxProducer>,
    txn_open: AtomicBool,
    txn_timeout: Duration,
    events: Mutex<Vec<Event>>,
}

impl ClientContext for Ctx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, message: &str) {
        log_librdkafka(level, fac, message);
    }
}

impl ConsumerContext for Ctx {
    fn pre_rebalance(&self, _c: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Revoke(tpl) = rebalance {
            let aborted = self.txn_open.swap(false, Ordering::SeqCst);
            let abort_error = if aborted {
                self.producer.abort_transaction(self.txn_timeout).err()
            } else {
                None
            };
            self.push(Event::Revoked {
                partitions: tps(tpl),
                aborted,
                abort_error,
            });
        }
    }

    fn post_rebalance(&self, _c: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => self.push(Event::Assigned(tps(tpl))),
            Rebalance::Error(e) => tracing::error!(error = %e, "rebalance error"),
            Rebalance::Revoke(_) => {}
        }
    }
}

impl Ctx {
    fn push(&self, e: Event) {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(e);
    }
}

struct LogContext;
impl ClientContext for LogContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, message: &str) {
        log_librdkafka(level, fac, message);
    }
}
impl ConsumerContext for LogContext {}

// librdkafka log lines carry broker, topic and offset details, never message payloads.
fn log_librdkafka(level: RDKafkaLogLevel, fac: &str, message: &str) {
    match level {
        RDKafkaLogLevel::Emerg
        | RDKafkaLogLevel::Alert
        | RDKafkaLogLevel::Critical
        | RDKafkaLogLevel::Error => {
            tracing::error!(target: "librdkafka", fac, "{message}")
        }
        RDKafkaLogLevel::Warning => tracing::warn!(target: "librdkafka", fac, "{message}"),
        RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
            tracing::info!(target: "librdkafka", fac, "{message}")
        }
        RDKafkaLogLevel::Debug => tracing::debug!(target: "librdkafka", fac, "{message}"),
    }
}

fn tps(tpl: &TopicPartitionList) -> Vec<Tp> {
    tpl.elements()
        .iter()
        .map(|e| (e.topic().to_string(), e.partition()))
        .collect()
}

pub struct Processor<J: Job> {
    cfg: Config,
    job: J,
    output_topic: String,
    consumer: BaseConsumer<Ctx>,
    producer: Arc<TxProducer>,
    metrics: Arc<Metrics>,
    ready: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    assigned: BTreeSet<Tp>,
    /// Next offset to read per partition, as of the last successful commit.
    committed: HashMap<Tp, i64>,
    /// Next offset per partition inside the open transaction.
    pending: HashMap<Tp, i64>,
    /// Input end offsets recorded at the first assignment (exit-when-caught-up).
    end_offsets: HashMap<Tp, i64>,
    /// Input end offsets read when the open batch started (see `Job::end_batch`).
    batch_end: Option<HashMap<Tp, i64>>,
    txn_started: Option<Instant>,
    txn_records: usize,
}

impl<J: Job> Processor<J> {
    /// Startup: init_transactions (fences zombies and aborts their open transaction), create state
    /// topics, subscribe. Restore happens on assignment, inside `run`.
    pub fn start(
        cfg: Config,
        mut job: J,
        metrics: Arc<Metrics>,
        ready: Arc<AtomicBool>,
        shutdown: Arc<AtomicBool>,
        rt: Handle,
    ) -> Result<Self> {
        let output_topic = match &cfg.sink {
            Sink::Topic(t) => t.clone(),
            Sink::Path(_) => {
                bail!("this runtime writes Kafka outputs only; OUTPUT_PATH is not supported yet")
            }
        };
        let producer: Arc<TxProducer> = Arc::new(
            ClientConfig::new()
                .set("bootstrap.servers", &cfg.bootstrap)
                .set("transactional.id", &cfg.transactional_id)
                .set("enable.idempotence", "true")
                .set("acks", "all")
                .set("partitioner", "murmur2_random")
                .set(
                    "transaction.timeout.ms",
                    cfg.txn_timeout.as_millis().to_string(),
                )
                .create()
                .context("create producer")?,
        );
        retrying("init_transactions", || {
            producer.init_transactions(cfg.txn_timeout)
        })
        .context("init_transactions")?;
        tracing::info!(
            "producer initialised; any previous instance with this transactional.id is fenced"
        );

        let partitions = input_partitions(&producer, &cfg)?;
        let stores: Vec<String> = job
            .stores()
            .iter()
            .map(|s| cfg.state_topic(s.name()))
            .collect();
        create_state_topics(&cfg, &stores, partitions, &rt)?;

        let ctx = Ctx {
            producer: producer.clone(),
            txn_open: AtomicBool::new(false),
            txn_timeout: cfg.txn_timeout,
            events: Mutex::new(Vec::new()),
        };
        let consumer: BaseConsumer<Ctx> = ClientConfig::new()
            .set("bootstrap.servers", &cfg.bootstrap)
            .set("group.id", &cfg.consumer_group)
            .set("group.instance.id", &cfg.group_instance_id)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("isolation.level", "read_committed")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            .set("heartbeat.interval.ms", "1000")
            .create_with_context(ctx)
            .context("create consumer")?;
        let topics: Vec<&str> = cfg.input_topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topics).context("subscribe")?;
        tracing::info!(topics = ?topics, "subscribed");

        Ok(Processor {
            cfg,
            job,
            output_topic,
            consumer,
            producer,
            metrics,
            ready,
            shutdown,
            assigned: BTreeSet::new(),
            committed: HashMap::new(),
            pending: HashMap::new(),
            end_offsets: HashMap::new(),
            batch_end: None,
            txn_started: None,
            txn_records: 0,
        })
    }

    pub fn run(mut self) -> Result<()> {
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                return self.shutdown_gracefully();
            }
            let timeout = match self.txn_started {
                Some(t) => self
                    .cfg
                    .batch_max
                    .saturating_sub(t.elapsed())
                    .max(Duration::from_millis(1)),
                None => IDLE_POLL,
            };
            let polled = self.consumer.poll(timeout).map(|m| {
                m.map(|m| {
                    (
                        (m.topic().to_string(), m.partition()),
                        m.offset(),
                        m.payload().map(<[u8]>::to_vec),
                    )
                })
            });
            self.handle_events()?;
            if let Some(m) = polled {
                let (tp, offset, payload) = match m {
                    Ok(m) => m,
                    // A fatal consumer error (e.g. another instance took our group.instance.id)
                    // leaves the consumer unusable, so this process is a zombie and must exit.
                    Err(e @ KafkaError::MessageConsumptionFatal(_)) => {
                        return Err(anyhow!(e).context("fatal consumer error"));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "consumer error");
                        continue;
                    }
                };
                if self.assigned.contains(&tp) {
                    let result = self
                        .process(&tp, offset, payload.as_deref())
                        .and_then(|_| self.end_batch_if_done());
                    if let Err(e) = result {
                        match e.downcast_ref::<KafkaError>().map(classify) {
                            Some(Class::Abort) | Some(Class::Retry) => self.abort_and_rewind(&e)?,
                            _ => return Err(e),
                        }
                    }
                }
            }
            if let Some(t) = self.txn_started {
                if self.txn_records >= self.cfg.batch_max_records
                    || t.elapsed() >= self.cfg.batch_max
                {
                    self.commit()?;
                }
            }
            if self.txn_started.is_none()
                && self.batch_end.is_none()
                && self.cfg.exit_when_caught_up
                && self.caught_up()
            {
                tracing::info!(
                    committed = self.committed.values().sum::<i64>(),
                    "caught up, exiting"
                );
                return Ok(());
            }
        }
    }

    fn process(&mut self, tp: &Tp, offset: i64, payload: Option<&[u8]>) -> Result<()> {
        if self.batch_end.is_none() {
            let mut end = HashMap::new();
            for (t, p) in &self.assigned {
                let (_, high) = self
                    .consumer
                    .fetch_watermarks(t, *p, Duration::from_secs(30))?;
                end.insert((t.clone(), *p), high);
            }
            self.batch_end = Some(end);
        }
        if self.txn_started.is_none() {
            self.producer
                .begin_transaction()
                .context("begin_transaction")?;
            self.consumer
                .context()
                .txn_open
                .store(true, Ordering::SeqCst);
            self.txn_started = Some(Instant::now());
            self.txn_records = 0;
        }
        self.metrics.records_in.inc();
        let input = match payload.map(|p| self.job.decode(p)) {
            Some(Ok(Some(i))) => Some(i),
            Some(Ok(None)) => None,
            None | Some(Err(_)) => {
                self.metrics.decode_errors.inc();
                tracing::warn!(topic = %tp.0, partition = tp.1, offset, "skipping a record that does not decode");
                None
            }
        };
        if let Some(input) = input {
            for s in self.job.stores() {
                s.set_partition(tp.1);
            }
            let outputs = self.job.process(input)?;
            self.send_outputs(&outputs)?;
        }
        self.pending.insert(tp.clone(), offset + 1);
        self.txn_records += 1;
        Ok(())
    }

    /// Once every partition has reached the end offset read when the batch started: run
    /// `Job::end_batch` in the open transaction and commit it.
    fn end_batch_if_done(&mut self) -> Result<()> {
        let done = self
            .batch_end
            .as_ref()
            .is_some_and(|end| batch_done(end, &self.committed, &self.pending));
        if !done || self.txn_started.is_none() {
            return Ok(());
        }
        if let Some((_, p)) = self.assigned.iter().next() {
            for s in self.job.stores() {
                s.set_partition(*p);
            }
        }
        let outputs = self.job.end_batch()?;
        self.send_outputs(&outputs)?;
        self.batch_end = None;
        self.commit()
    }

    fn send_outputs(&mut self, outputs: &[J::Output]) -> Result<()> {
        for o in outputs {
            let r = self.job.encode(o)?;
            send(
                &self.producer,
                BaseRecord::to(&self.output_topic)
                    .key(&r.key)
                    .payload(&r.value),
            )?;
            self.metrics.records_out.inc();
        }
        let prefix = self.cfg.state_topic_prefix.clone();
        for s in self.job.stores() {
            let topic = format!("{prefix}.{}", s.name());
            for c in s.take_changelog() {
                let rec = BaseRecord::<Vec<u8>, Vec<u8>>::to(&topic)
                    .key(&c.key)
                    .partition(c.partition);
                match &c.value {
                    Some(v) => send(&self.producer, rec.payload(v))?,
                    None => send(&self.producer, rec)?,
                }
            }
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let started = self
            .txn_started
            .context("commit without an open transaction")?;
        let mut tpl = TopicPartitionList::new();
        for ((t, p), o) in &self.pending {
            tpl.add_partition_offset(t, *p, Offset::Offset(*o))?;
        }
        let cgm = self
            .consumer
            .group_metadata()
            .context("consumer group metadata")?;
        let timeout = self.cfg.txn_timeout;
        let failpoint = self
            .cfg
            .failpoint_at_offsets
            .filter(|n| committed_after(&self.committed, &self.pending) >= *n);
        let result = retrying("send_offsets_to_transaction", || {
            self.producer
                .send_offsets_to_transaction(&tpl, &cgm, timeout)
        })
        .and_then(|_| {
            if let Some(n) = failpoint {
                self.fire_failpoint(n, timeout)?;
            }
            retrying("commit_transaction", || {
                self.producer.commit_transaction(timeout)
            })
        });
        if let Err(e) = result {
            return match classify(&e) {
                Class::Abort => self.abort_and_rewind(&anyhow!(e)),
                _ => Err(anyhow!(e).context("commit_transaction")),
            };
        }
        self.consumer
            .context()
            .txn_open
            .store(false, Ordering::SeqCst);
        for s in self.job.stores() {
            s.commit();
        }
        self.committed.extend(self.pending.drain());
        self.txn_started = None;
        self.metrics.txn_committed.inc();
        self.metrics
            .txn_duration
            .observe(started.elapsed().as_secs_f64());
        tracing::debug!(records = self.txn_records, "transaction committed");
        self.update_gauges();
        Ok(())
    }

    /// Test only (docs/DESIGN.md, Failpoint): the outputs, changelog records and offsets of the open
    /// transaction are already sent; flush them to the broker, then SIGKILL before commit_transaction.
    fn fire_failpoint(&self, at: i64, timeout: Duration) -> Result<(), KafkaError> {
        self.producer.flush(timeout)?;
        tracing::warn!(
            failpoint_at_offsets = at,
            committed_after = committed_after(&self.committed, &self.pending),
            "TELEMETRY_FAILPOINT_AT_OFFSETS reached: SIGKILL before commit_transaction"
        );
        // SAFETY: kill(2) on our own pid has no memory-safety preconditions.
        unsafe { libc::kill(libc::getpid(), libc::SIGKILL) };
        std::process::abort()
    }

    /// Abort, discard staged state, and seek every assigned partition back to its committed offset.
    fn abort_and_rewind(&mut self, cause: &anyhow::Error) -> Result<()> {
        tracing::warn!(error = %format!("{cause:#}"), "aborting transaction");
        if self
            .consumer
            .context()
            .txn_open
            .swap(false, Ordering::SeqCst)
        {
            let timeout = self.cfg.txn_timeout;
            retrying("abort_transaction", || {
                self.producer.abort_transaction(timeout)
            })
            .context("abort_transaction")?;
        }
        self.discard_txn();
        let mut tpl = TopicPartitionList::new();
        for tp in &self.assigned {
            let o = self
                .committed
                .get(tp)
                .map_or(Offset::Beginning, |o| Offset::Offset(*o));
            tpl.add_partition_offset(&tp.0, tp.1, o)?;
        }
        if tpl.count() > 0 {
            let res = self
                .consumer
                .seek_partitions(tpl, Duration::from_secs(10))
                .context("seek after abort")?;
            for e in res.elements() {
                e.error().with_context(|| {
                    format!("seek {}[{}] after abort", e.topic(), e.partition())
                })?;
            }
        }
        Ok(())
    }

    fn discard_txn(&mut self) {
        for s in self.job.stores() {
            s.abort();
        }
        self.pending.clear();
        self.txn_started = None;
        self.txn_records = 0;
        self.metrics.txn_aborted.inc();
    }

    fn handle_events(&mut self) -> Result<()> {
        let events = std::mem::take(
            &mut *self
                .consumer
                .context()
                .events
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        for e in events {
            match e {
                Event::Revoked {
                    partitions,
                    aborted,
                    abort_error,
                } => {
                    if let Some(err) = abort_error {
                        return Err(anyhow!(err).context("abort_transaction on revoke"));
                    }
                    if aborted {
                        tracing::info!("open transaction aborted on revoke");
                        self.discard_txn();
                    }
                    for tp in &partitions {
                        self.assigned.remove(tp);
                        self.committed.remove(tp);
                        if let Some(end) = self.batch_end.as_mut() {
                            end.remove(tp);
                        }
                    }
                    let dropped: Vec<i32> = partitions
                        .iter()
                        .map(|(_, p)| *p)
                        .filter(|p| !self.assigned.iter().any(|(_, q)| q == p))
                        .collect();
                    for s in self.job.stores() {
                        s.drop_partitions(&dropped);
                    }
                    tracing::info!(partitions = ?partitions, "partitions revoked");
                }
                Event::Assigned(partitions) => self.on_assigned(partitions)?,
            }
        }
        Ok(())
    }

    /// Restore state for the new partitions before any of their records is processed. `poll`
    /// returns without a message when it serves a rebalance, so none has been handed out yet.
    fn on_assigned(&mut self, partitions: Vec<Tp>) -> Result<()> {
        if partitions.is_empty() {
            return Ok(());
        }
        tracing::info!(partitions = ?partitions, "partitions assigned");
        let mut tpl = TopicPartitionList::new();
        for (t, p) in &partitions {
            tpl.add_partition(t, *p);
        }
        let committed = self
            .consumer
            .committed_offsets(tpl, Duration::from_secs(30))
            .context("read committed offsets")?;
        for e in committed.elements() {
            if let Offset::Offset(o) = e.offset() {
                self.committed
                    .insert((e.topic().to_string(), e.partition()), o);
            }
        }
        let numbers: BTreeSet<i32> = partitions.iter().map(|(_, p)| *p).collect();
        let start = Instant::now();
        let topics: Vec<String> = self
            .job
            .stores()
            .iter()
            .map(|s| self.cfg.state_topic(s.name()))
            .collect();
        for (i, topic) in topics.iter().enumerate() {
            let store = &mut self.job.stores()[i];
            restore(&self.cfg, topic, &numbers, &mut **store)?;
        }
        self.metrics
            .restore_duration
            .observe(start.elapsed().as_secs_f64());
        for tp in &partitions {
            self.assigned.insert(tp.clone());
            if !self.end_offsets.contains_key(tp) {
                let (_, high) =
                    self.consumer
                        .fetch_watermarks(&tp.0, tp.1, Duration::from_secs(30))?;
                self.end_offsets.insert(tp.clone(), high);
            }
        }
        tracing::info!(
            restore_ms = start.elapsed().as_millis() as u64,
            input_end_total = self.end_offsets.values().sum::<i64>(),
            committed_total = self.committed.values().sum::<i64>(),
            "restore complete"
        );
        self.update_gauges();
        self.ready.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn caught_up(&self) -> bool {
        !self.assigned.is_empty()
            && self.assigned.iter().all(|tp| {
                self.end_offsets
                    .get(tp)
                    .is_some_and(|end| self.committed.get(tp).copied().unwrap_or(0) >= *end)
            })
    }

    fn update_gauges(&mut self) {
        for tp in &self.assigned {
            let end = self.end_offsets.get(tp).copied().unwrap_or(0);
            let lag = (end - self.committed.get(tp).copied().unwrap_or(0)).max(0);
            self.metrics
                .consumer_lag
                .get_or_create(&PartitionLabels {
                    topic: tp.0.clone(),
                    partition: tp.1,
                })
                .set(lag);
        }
        let sizes: Vec<(String, usize)> = self
            .job
            .stores()
            .iter()
            .map(|s| (s.name().to_string(), s.len()))
            .collect();
        for (store, n) in sizes {
            self.metrics
                .state_keys
                .get_or_create(&StoreLabels { store })
                .set(n as i64);
        }
    }

    /// Stop polling, commit the in-flight transaction (abort it if that fails), close the consumer.
    fn shutdown_gracefully(mut self) -> Result<()> {
        if self.txn_started.is_some() {
            if let Err(e) = self.commit() {
                tracing::warn!(error = %format!("{e:#}"), "could not commit on shutdown");
            }
            if self.txn_started.is_some()
                && self
                    .consumer
                    .context()
                    .txn_open
                    .swap(false, Ordering::SeqCst)
            {
                let _ = self.producer.abort_transaction(Duration::from_secs(5));
            }
        }
        // Dropping the consumer closes it. With static membership the broker keeps the member until
        // session.timeout.ms, so a restarted pod with the same group.instance.id rejoins without a rebalance.
        drop(self.consumer);
        tracing::info!("shut down");
        Ok(())
    }
}

fn send<K: ToBytes + ?Sized>(
    producer: &TxProducer,
    mut rec: BaseRecord<'_, K, Vec<u8>>,
) -> Result<()> {
    loop {
        match producer.send(rec) {
            Ok(()) => return Ok(()),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                rec = r;
                sleep(Duration::from_millis(5));
            }
            Err((e, _)) => return Err(e.into()),
        }
    }
}

fn input_partitions(producer: &TxProducer, cfg: &Config) -> Result<i32> {
    let mut counts = BTreeSet::new();
    for t in &cfg.input_topics {
        let md = producer
            .client()
            .fetch_metadata(Some(t), Duration::from_secs(30))
            .context("input topic metadata")?;
        let n = md.topics().first().map_or(0, |m| m.partitions().len()) as i32;
        if n == 0 {
            bail!("input topic {t} does not exist");
        }
        counts.insert(n);
    }
    if counts.len() != 1 {
        bail!("input topics must have the same partition count for co-partitioned state, got {counts:?}");
    }
    Ok(*counts.iter().next().expect("one count"))
}

fn create_state_topics(
    cfg: &Config,
    topics: &[String],
    partitions: i32,
    rt: &Handle,
) -> Result<()> {
    if topics.is_empty() {
        return Ok(());
    }
    for t in topics {
        if !t.starts_with(&cfg.state_topic_prefix) {
            bail!("state topic {t} is outside STATE_TOPIC_PREFIX");
        }
    }
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        .create()
        .context("create admin client")?;
    let new: Vec<NewTopic> = topics
        .iter()
        .map(|t| {
            NewTopic::new(t, partitions, TopicReplication::Fixed(-1))
                .set("cleanup.policy", "compact")
        })
        .collect();
    let results = rt
        .block_on(admin.create_topics(&new, &AdminOptions::new()))
        .context("create state topics")?;
    for r in results {
        match r {
            Ok(t) => tracing::info!(topic = %t, partitions, "created state topic"),
            Err((t, RDKafkaErrorCode::TopicAlreadyExists)) => {
                tracing::info!(topic = %t, "state topic exists")
            }
            Err((t, code)) => bail!("create state topic {t}: {code}"),
        }
    }
    Ok(())
}

/// Read each changelog partition from the beginning to its end with read_committed.
fn restore(
    cfg: &Config,
    topic: &str,
    partitions: &BTreeSet<i32>,
    store: &mut dyn StateStore,
) -> Result<()> {
    store.drop_partitions(&partitions.iter().copied().collect::<Vec<_>>());
    let consumer: BaseConsumer<LogContext> = ClientConfig::new()
        .set("bootstrap.servers", &cfg.bootstrap)
        // librdkafka needs a group.id to assign partitions. This consumer never subscribes or
        // commits, so the broker never registers the group.
        .set("group.id", format!("{}.restore", cfg.consumer_group))
        .set("isolation.level", "read_committed")
        .set("enable.partition.eof", "true")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create_with_context(LogContext)
        .context("create restore consumer")?;
    let mut tpl = TopicPartitionList::new();
    for p in partitions {
        tpl.add_partition_offset(topic, *p, Offset::Beginning)?;
    }
    consumer
        .assign(&tpl)
        .context("assign changelog partitions")?;
    let mut remaining = partitions.clone();
    let mut records = 0u64;
    let deadline = Instant::now() + RESTORE_TIMEOUT;
    while !remaining.is_empty() {
        if Instant::now() > deadline {
            bail!("restore of {topic} did not reach the end of partitions {remaining:?}");
        }
        match consumer.poll(Duration::from_millis(200)) {
            Some(Ok(m)) => {
                let key = m.key().context("changelog record without a key")?;
                store.restore(m.partition(), key, m.payload())?;
                records += 1;
            }
            Some(Err(KafkaError::PartitionEOF(p))) => {
                remaining.remove(&p);
            }
            Some(Err(e)) => return Err(anyhow!(e).context(format!("restore {topic}"))),
            None => {}
        }
    }
    tracing::info!(
        topic,
        partitions = partitions.len(),
        records,
        keys = store.len(),
        "restored state store"
    );
    Ok(())
}

/// Whether every partition of the batch has been read up to the end offset recorded at its start.
fn batch_done(
    end: &HashMap<Tp, i64>,
    committed: &HashMap<Tp, i64>,
    pending: &HashMap<Tp, i64>,
) -> bool {
    end.iter().all(|(tp, e)| {
        pending
            .get(tp)
            .or_else(|| committed.get(tp))
            .copied()
            .unwrap_or(0)
            >= *e
    })
}

/// Committed input offsets, summed over partitions, once the open transaction commits.
fn committed_after(committed: &HashMap<Tp, i64>, pending: &HashMap<Tp, i64>) -> i64 {
    let mut next = committed.clone();
    next.extend(pending.iter().map(|(tp, o)| (tp.clone(), *o)));
    next.values().sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_after_counts_pending_over_committed() {
        let tp = |p| ("in".to_string(), p);
        let committed = HashMap::from([(tp(0), 5), (tp(1), 3)]);
        let pending = HashMap::from([(tp(0), 9), (tp(2), 4)]);
        assert_eq!(committed_after(&committed, &pending), 9 + 3 + 4);
        assert_eq!(committed_after(&committed, &HashMap::new()), 8);
    }

    #[test]
    fn batch_is_done_once_every_partition_reaches_its_end() {
        let tp = |p| ("in".to_string(), p);
        let end = HashMap::from([(tp(0), 10), (tp(1), 4), (tp(2), 0)]);
        let committed = HashMap::from([(tp(0), 6), (tp(1), 4)]);
        assert!(!batch_done(&end, &committed, &HashMap::new()));
        assert!(!batch_done(&end, &committed, &HashMap::from([(tp(0), 9)])));
        assert!(batch_done(&end, &committed, &HashMap::from([(tp(0), 10)])));
    }
}
