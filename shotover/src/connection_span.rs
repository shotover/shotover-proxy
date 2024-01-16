//! This module purposefully only contains this one function to create a span.
//! This allows us to enable/disable just this one span via the tracing filter: `shotover::connection_span=debug`
//!
//! Do not add more code here!

use tracing::Span;

pub fn span(connection_count: u64, source: &str) -> Span {
    tracing::debug_span!("connection", id = connection_count, source = source)
}
