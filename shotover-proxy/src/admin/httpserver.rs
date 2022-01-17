use hyper::{
    service::{make_service_fn, service_fn},
    Method, Request, StatusCode, {Body, Response, Server},
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use metrics_exporter_prometheus::PrometheusHandle;
use std::convert::Infallible;
use std::{net::SocketAddr, sync::Arc};
use tracing::{error, trace};
use tracing_subscriber::reload::Handle;
use tracing_subscriber::EnvFilter;

/// Exports metrics over HTTP.
pub struct LogFilterHttpExporter<S> {
    recorder_handle: PrometheusHandle,
    address: SocketAddr,
    handle: Handle<EnvFilter, S>,
}

fn set_from<S>(bytes: Bytes, handle: &Handle<EnvFilter, S>) -> Result<(), String>
where
    S: tracing::Subscriber + 'static,
{
    use std::str;
    let body = str::from_utf8(bytes.as_ref()).map_err(|e| format!("{e}"))?;
    trace!(request.body = ?body);
    let new_filter = body
        .parse::<tracing_subscriber::filter::EnvFilter>()
        .map_err(|e| format!("{e}"))?;
    handle.reload(new_filter).map_err(|e| format!("{e}"))
}

fn rsp(status: StatusCode, body: impl Into<Body>) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(body.into())
        .expect("builder with known status code must not fail")
}

impl<S> LogFilterHttpExporter<S>
where
    S: tracing::Subscriber + 'static,
{
    /// Creates a new [`LogFilterHttpExporter`] that listens on the given `address`.
    ///
    /// Observers expose their output by being converted into strings.
    pub fn new(
        recorder_handle: PrometheusHandle,
        address: SocketAddr,
        handle: Handle<EnvFilter, S>,
    ) -> Self {
        LogFilterHttpExporter {
            recorder_handle,
            address,
            handle,
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
        let controller = Arc::new(self.recorder_handle);
        let handle = Arc::new(self.handle);

        let make_svc = make_service_fn(move |_| {
            let controller = controller.clone();
            let handle = handle.clone();

            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let controller = controller.clone();
                    let handle = handle.clone();

                    async move {
                        let response = match (req.method(), req.uri().path()) {
                            (&Method::GET, "/metrics") => {
                                Response::new(Body::from(controller.as_ref().render()))
                            }
                            (&Method::PUT, "/filter") => {
                                trace!("setting filter");
                                match hyper::body::to_bytes(req).await {
                                    Ok(body) => match set_from(body, &handle) {
                                        Err(error) => {
                                            error!(%error, "setting filter failed!");
                                            rsp(StatusCode::INTERNAL_SERVER_ERROR, error)
                                        }
                                        Ok(()) => rsp(StatusCode::NO_CONTENT, Body::empty()),
                                    },
                                    Err(error) => {
                                        error!(%error, "setting filter failed - Couldn't read bytes");
                                        rsp(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:?}"))
                                    }
                                }
                            }
                            _ => rsp(StatusCode::NOT_FOUND, "try '/filter'"),
                        };
                        Ok::<_, Infallible>(response)
                    }
                }))
            }
        });

        let address = self.address;
        Server::try_bind(&address)
            .map_err(|e| anyhow!("Failed to bind to {}: {}", address, e))?
            .serve(make_svc)
            .await
            .map_err(|e| anyhow!(e))
    }
}
