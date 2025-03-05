use crate::http::HttpServerError;
use crate::runner::ReloadHandle;
use anyhow::{Context, Result, anyhow};
use axum::{Router, extract::State, response::Html};
use metrics_exporter_prometheus::PrometheusHandle;
use std::str;
use std::{net::SocketAddr, sync::Arc};
use tracing::{error, trace};

/// Exports metrics over HTTP.
pub(crate) struct LogFilterHttpExporter {
    recorder_handle: PrometheusHandle,
    address: SocketAddr,
    tracing_handle: ReloadHandle,
}

impl LogFilterHttpExporter {
    /// Creates a new [`LogFilterHttpExporter`] that listens on the given `address`.
    ///
    /// Observers expose their output by being converted into strings.
    pub fn new(
        recorder_handle: PrometheusHandle,
        address: SocketAddr,
        tracing_handle: ReloadHandle,
    ) -> Self {
        LogFilterHttpExporter {
            recorder_handle,
            address,
            tracing_handle,
        }
    }

    /// Starts an HTTP server on the `address` the exporter was originally configured with,
    /// responding to any request with the output of the configured observer.
    pub async fn async_run(self) {
        if let Err(err) = self.async_run_inner().await {
            error!("Metrics HTTP server failed: {}", err);
        }
    }

    async fn async_run_inner(self) -> Result<()> {
        let state = AppState {
            recorder_handle: Arc::new(self.recorder_handle),
            tracing_handle: Arc::new(self.tracing_handle),
        };

        let app = Router::new()
            .route("/", axum::routing::get(root))
            .route("/metrics", axum::routing::get(serve_metrics))
            .route("/filter", axum::routing::put(put_filter))
            .with_state(state);

        let address = self.address;
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("Failed to bind to {}", address))?;
        axum::serve(listener, app).await.map_err(|e| anyhow!(e))
    }
}

async fn root() -> Html<&'static str> {
    Html("try /filter or /metrics")
}

async fn serve_metrics(State(state): State<AppState>) -> Html<String> {
    Html(state.recorder_handle.as_ref().render())
}

async fn put_filter(
    State(state): State<AppState>,
    new_filter_string: String,
) -> Result<Html<&'static str>, HttpServerError> {
    trace!("setting filter to: {new_filter_string}");
    let new_filter = new_filter_string.parse::<tracing_subscriber::filter::EnvFilter>()?;
    state.tracing_handle.reload(new_filter)?;
    tracing::info!("filter set to: {new_filter_string}");
    Ok(Html("Filter set"))
}

#[derive(Clone)]
struct AppState {
    tracing_handle: Arc<ReloadHandle>,
    recorder_handle: Arc<PrometheusHandle>,
}
