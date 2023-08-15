use crate::runner::ReloadHandle;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use hyper::{
    service::{make_service_fn, service_fn},
    Method, Request, StatusCode, {Body, Response, Server},
};
use metrics_exporter_prometheus::PrometheusHandle;
use std::convert::Infallible;
use std::str;
use std::{net::SocketAddr, sync::Arc};
use tracing::{error, trace};

/// Exports metrics over HTTP.
pub(crate) struct LogFilterHttpExporter {
    recorder_handle: PrometheusHandle,
    address: SocketAddr,
    tracing_handle: ReloadHandle,
}

/// Sets the `tracing_suscriber` filter level to the value of `bytes` on `handle`
fn set_filter(bytes: Bytes, handle: &ReloadHandle) -> Result<()> {
    let body = str::from_utf8(bytes.as_ref())?;
    trace!(request.body = ?body);
    let new_filter = body.parse::<tracing_subscriber::filter::EnvFilter>()?;
    handle.reload(new_filter)
}

fn rsp(status: StatusCode, body: impl Into<Body>) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(body.into())
        .expect("builder with known status code must not fail")
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
        let recorder_handle = Arc::new(self.recorder_handle);
        let tracing_handle = Arc::new(self.tracing_handle);

        let make_svc = make_service_fn(move |_| {
            let recorder_handle = recorder_handle.clone();
            let tracing_handle = tracing_handle.clone();

            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let recorder_handle = recorder_handle.clone();
                    let tracing_handle = tracing_handle.clone();

                    async move {
                        let response = match (req.method(), req.uri().path()) {
                            (&Method::GET, "/metrics") => {
                                Response::new(Body::from(recorder_handle.as_ref().render()))
                            }
                            (&Method::PUT, "/filter") => {
                                trace!("setting filter");
                                match hyper::body::to_bytes(req).await {
                                    Ok(body) => match set_filter(body, &tracing_handle) {
                                        Err(error) => {
                                            error!(?error, "setting filter failed!");
                                            rsp(
                                                StatusCode::INTERNAL_SERVER_ERROR,
                                                format!("{:?}", error),
                                            )
                                        }
                                        Ok(()) => rsp(StatusCode::NO_CONTENT, Body::empty()),
                                    },
                                    Err(error) => {
                                        error!(%error, "setting filter failed - Couldn't read bytes");
                                        rsp(StatusCode::INTERNAL_SERVER_ERROR, format!("{error:?}"))
                                    }
                                }
                            }
                            _ => rsp(StatusCode::NOT_FOUND, "try '/filter' or `/metrics`"),
                        };
                        Ok::<_, Infallible>(response)
                    }
                }))
            }
        });

        let address = self.address;
        Server::try_bind(&address)
            .with_context(|| format!("Failed to bind to {}", address))?
            .serve(make_svc)
            .await
            .map_err(|e| anyhow!(e))
    }
}
