//! Shared runtime for telemetry stream jobs: a transactional consume, process, produce loop with
//! Kafka-changelog state, metrics, health endpoints and JSON logs. See docs/DESIGN.md.

pub mod config;
pub mod job;
pub mod metrics;
pub mod parquet_sink;
mod processor;
mod server;
pub mod state;

use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub use config::{Config, Sink};
pub use job::{Job, Record};
pub use parquet_sink::{Column, ColumnType, ParquetSpec};
pub use state::{KeyedState, StateStore};

/// Hard limit after SIGTERM or SIGINT, below the chart's terminationGracePeriodSeconds (30).
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(25);

/// Run a job until it is caught up (when `EXIT_WHEN_CAUGHT_UP=true`), it is signalled, or a fatal
/// error occurs. Exit codes: 0 on success or graceful shutdown, 1 on any error.
pub fn run<J: Job>(job: J) -> ExitCode {
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            init_logging("info");
            tracing::error!(error = %format!("{e:#}"), "invalid configuration");
            return ExitCode::from(1);
        }
    };
    init_logging(&cfg.log_level);
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(error = %e, "could not start the tokio runtime");
            return ExitCode::from(1);
        }
    };
    let metrics = Arc::new(metrics::Metrics::new());
    let ready = Arc::new(AtomicBool::new(false));
    let shutdown = Arc::new(AtomicBool::new(false));
    rt.block_on(async {
        tokio::spawn(server::serve(
            cfg.metrics_port,
            metrics.clone(),
            ready.clone(),
        ));
        tokio::spawn(watch_signals(shutdown.clone()));
        let handle = tokio::runtime::Handle::current();
        let processing = tokio::task::spawn_blocking(move || {
            processor::Processor::start(cfg, job, metrics, ready, shutdown, handle)?.run()
        });
        match processing.await {
            Ok(Ok(())) => ExitCode::SUCCESS,
            Ok(Err(e)) => {
                tracing::error!(error = %format!("{e:#}"), "fatal error, exiting");
                ExitCode::from(1)
            }
            Err(e) => {
                tracing::error!(error = %e, "processing thread panicked");
                ExitCode::from(1)
            }
        }
    })
}

async fn watch_signals(shutdown: Arc<AtomicBool>) {
    use tokio::signal::unix::{signal, SignalKind};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        tracing::error!("could not install signal handlers");
        return;
    };
    tokio::select! {
        _ = term.recv() => tracing::info!(signal = "SIGTERM", "shutting down"),
        _ = int.recv() => tracing::info!(signal = "SIGINT", "shutting down"),
    }
    shutdown.store(true, Ordering::SeqCst);
    tokio::time::sleep(SHUTDOWN_DEADLINE).await;
    tracing::error!(
        deadline_s = SHUTDOWN_DEADLINE.as_secs(),
        "graceful shutdown did not finish, exiting"
    );
    std::process::exit(1);
}

fn init_logging(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_current_span(false)
        .with_env_filter(filter)
        .try_init();
}
