//! /metrics, /healthz and /readyz on METRICS_PORT.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

use crate::metrics::Metrics;

#[derive(Clone)]
struct AppState {
    metrics: Arc<Metrics>,
    ready: Arc<AtomicBool>,
}

pub async fn serve(port: u16, metrics: Arc<Metrics>, ready: Arc<AtomicBool>) {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(ready_handler))
        .with_state(AppState { metrics, ready });
    match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(listener) => {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(error = %e, "metrics server stopped");
            }
        }
        Err(e) => tracing::error!(error = %e, port, "could not bind the metrics port"),
    }
}

async fn metrics_handler(State(s): State<AppState>) -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        s.metrics.encode(),
    )
}

async fn ready_handler(State(s): State<AppState>) -> impl IntoResponse {
    if s.ready.load(Ordering::SeqCst) {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "restoring")
    }
}
