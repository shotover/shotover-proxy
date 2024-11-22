use pretty_assertions::assert_eq;
use redis::Commands;
use std::collections::HashSet;
use test_helpers::connection::cassandra::{assert_query_result, CassandraConnection, ResultValue};
use test_helpers::metrics::get_metrics_value;

/// gets the current miss count from the cache instrumentation.
async fn get_cache_miss_value() -> u64 {
    let value = get_metrics_value("shotover_cache_miss_count").await;
    value
        .parse()
        .map_err(|_| format!("Failed to parse {value} to integer"))
        .unwrap()
}

async fn assert_increment(
    session: &CassandraConnection,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    let before = get_cache_miss_value().await;
    assert_query_result(session, query, expected_rows).await;
    let after = get_cache_miss_value().await;
    assert_eq!(
        before + 1,
        after,
        "expected cache_miss count to be incremented but was not. before: {before} after: {after} query: {query}",
    );
}

async fn assert_unchanged(
    session: &CassandraConnection,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    let before = get_cache_miss_value().await;
    assert_query_result(session, query, expected_rows).await;
    let after = get_cache_miss_value().await;
    assert_eq!(
        before,
        after,
        "expected cache_miss count to be unchanged but was changed. before: {before} after: {after} query: {query}",
    );
}

pub async fn assert_query_is_cached(
    session: &CassandraConnection,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    // A query can be demonstrated as being cached if it is first recorded as a cache miss and then not recorded as a cache miss
    assert_increment(session, query, expected_rows).await;
    assert_unchanged(session, query, expected_rows).await;
}

pub async fn assert_query_is_uncacheable(
    session: &CassandraConnection,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    // A query can be demonstrated as not being cached if it never shows up as a cache miss
    assert_unchanged(session, query, expected_rows).await;
    assert_unchanged(session, query, expected_rows).await;
}

pub fn assert_sorted_set_equals(
    valkey_connection: &mut redis::Connection,
    key: &str,
    expected_values: &[&str],
) {
    let expected_values: HashSet<String> = expected_values.iter().map(|x| x.to_string()).collect();
    let values: HashSet<String> = valkey_connection.hkeys(key).unwrap();
    assert_eq!(values, expected_values)
}
