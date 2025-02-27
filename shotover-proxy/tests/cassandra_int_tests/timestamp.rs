use std::time::{SystemTime, UNIX_EPOCH};
use test_helpers::connection::cassandra::{
    CassandraConnection, ResultValue, assert_query_result, run_query,
};

async fn flag(connection: &CassandraConnection) {
    let timestamp = get_timestamp();

    connection
        .execute_with_timestamp(
            "UPDATE test_timestamps.test_table SET a = 'a1-modified-1' WHERE id = 0;",
            timestamp,
        )
        .await
        .unwrap();

    assert_query_result(
        connection,
        "SELECT WRITETIME(a) FROM test_timestamps.test_table WHERE id = 0;",
        &[&[ResultValue::BigInt(timestamp)]],
    )
    .await;
}

async fn query(connection: &CassandraConnection) {
    let timestamp = get_timestamp();

    run_query(
        connection,
        &format!(
            "UPDATE test_timestamps.test_table USING TIMESTAMP {} SET a = 'a1-modified-1' WHERE id = 0;",
            timestamp
        ),
    )
    .await;

    assert_query_result(
        connection,
        "SELECT WRITETIME(a) FROM test_timestamps.test_table WHERE id = 0;",
        &[&[ResultValue::BigInt(timestamp)]],
    )
    .await;
}

// use the current system timestamp because if we send one with an
// earlier timestamp than Cassandra has as the last write
// (the insert in `fn test`) it will be ignored.
fn get_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .try_into()
        .unwrap()
}

pub async fn test(connection: &CassandraConnection) {
    run_query(connection, "CREATE KEYSPACE test_timestamps WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1  };").await;
    run_query(
        connection,
        "CREATE TABLE test_timestamps.test_table (id int PRIMARY KEY, a text);",
    )
    .await;

    assert_query_result(
        connection,
        "INSERT INTO test_timestamps.test_table (id, a) VALUES (0, 'a1');",
        &[],
    )
    .await;
    assert_query_result(
        connection,
        "INSERT INTO test_timestamps.test_table (id, a) VALUES (1, 'a2');",
        &[],
    )
    .await;

    query(connection).await;
    flag(connection).await;
}
