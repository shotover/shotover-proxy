use crate::helpers::cassandra::{assert_query_result, ResultValue};
use cassandra_cpp::Session;
use metrics_util::debugging::{DebugValue, Snapshotter};
use redis::Commands;
use std::collections::HashSet;

/// gets the current miss count from the cache instrumentation.
fn get_cache_miss_value(snapshotter: &Snapshotter) -> u64 {
    let mut result = 0;
    for (key, _, _, value) in snapshotter.snapshot().into_vec().iter() {
        if let DebugValue::Counter(counter) = value {
            if key.key().name() == "cache_miss" && *counter > result {
                result = *counter;
            }
        }
    }
    result
}

fn assert_increment(
    snapshotter: &Snapshotter,
    session: &Session,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    let before = get_cache_miss_value(snapshotter);
    assert_query_result(session, query, expected_rows);
    let after = get_cache_miss_value(snapshotter);
    assert_eq!(
        before + 1,
        after,
        "expected cache_miss count to be incremented but was not. before: {before} after: {after} query: {query}",
    );
}

fn assert_unchanged(
    snapshotter: &Snapshotter,
    session: &Session,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    let before = get_cache_miss_value(snapshotter);
    assert_query_result(session, query, expected_rows);
    let after = get_cache_miss_value(snapshotter);
    assert_eq!(
        before,
        after,
        "expected cache_miss count to be unchanged but was changed. before: {before} after: {after} query: {query}",
    );
}

pub fn assert_query_is_cached(
    snapshotter: &Snapshotter,
    session: &Session,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    // A query can be demonstrated as being cached if it is first recorded as a cache miss and then not recorded as a cache miss
    assert_increment(snapshotter, session, query, expected_rows);
    assert_unchanged(snapshotter, session, query, expected_rows);
}

pub fn assert_query_is_uncacheable(
    snapshotter: &Snapshotter,
    session: &Session,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    // A query can be demonstrated as not being cached if it never shows up as a cache miss
    assert_unchanged(snapshotter, session, query, expected_rows);
    assert_unchanged(snapshotter, session, query, expected_rows);
}

pub fn assert_sorted_set_equals(
    redis_connection: &mut redis::Connection,
    key: &str,
    expected_values: &[&str],
) {
    let expected_values: HashSet<String> = expected_values.iter().map(|x| x.to_string()).collect();
    let values: HashSet<String> = redis_connection.hkeys(key).unwrap();
    assert_eq!(values, expected_values)
}
