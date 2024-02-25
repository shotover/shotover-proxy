use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

// Make our own error that wraps `anyhow::Error`.
pub(crate) struct HttpServerError(pub anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for HttpServerError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("HTTP 500 error: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for HttpServerError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
