pub mod connection;
pub mod result_value;

pub use connection::{
    CassandraConnection, CassandraConnectionBuilder, CassandraDriver, Compression, Consistency,
    ProtocolVersion,
};
pub use result_value::ResultValue;

/// Execute a `query` against the `session` and assert that the result rows match `expected_rows`
pub async fn assert_query_result(
    session: &CassandraConnection,
    query: &str,
    expected_rows: &[&[ResultValue]],
) {
    let mut result_rows = session.execute(query).await;
    result_rows.sort();
    assert_rows_with_query(result_rows, expected_rows, query);
}

/// Assert that the results from an integration test match the expected rows
pub fn assert_rows(result_rows: Vec<Vec<ResultValue>>, expected_rows: &[&[ResultValue]]) {
    let mut expected_rows: Vec<_> = expected_rows.iter().map(|x| x.to_vec()).collect();
    expected_rows.sort();

    assert_eq!(result_rows, expected_rows);
}

/// Assert that the results from an integration test match the expected rows and print the query if
/// failure
pub fn assert_rows_with_query(
    result_rows: Vec<Vec<ResultValue>>,
    expected_rows: &[&[ResultValue]],
    query: &str,
) {
    let mut expected_rows: Vec<_> = expected_rows.iter().map(|x| x.to_vec()).collect();
    expected_rows.sort();

    assert_eq!(result_rows, expected_rows, "\n failed on query: {}", query);
}

/// Execute a `query` against the `session` and assert the result rows contain `row`
pub async fn assert_query_result_contains_row(
    session: &CassandraConnection,
    query: &str,
    row: &[ResultValue],
) {
    let result_rows = session.execute(query).await;
    if !result_rows.contains(&row.to_vec()) {
        panic!(
            "expected row: {:?} missing from actual rows: {:?}",
            row, result_rows
        );
    }
}

/// Execute a `query` against the `session` and assert the result rows does not contain `row`
pub async fn assert_query_result_not_contains_row(
    session: &CassandraConnection,
    query: &str,
    row: &[ResultValue],
) {
    let result_rows = session.execute(query).await;
    if result_rows.contains(&row.to_vec()) {
        panic!(
            "unexpected row: {:?} was found in actual rows: {:?}",
            row, result_rows
        );
    }
}

pub async fn run_query(session: &CassandraConnection, query: &str) {
    assert_query_result(session, query, &[]).await;
}
