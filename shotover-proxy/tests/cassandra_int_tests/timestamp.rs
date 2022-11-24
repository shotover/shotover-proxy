use crate::helpers::cassandra::{assert_query_result, run_query, CassandraConnection, ResultValue};
use std::time::{SystemTime, UNIX_EPOCH};

async fn flag(connection: &CassandraConnection) {
    let timestamp = setup(connection).await;

    let _r = connection
        .execute_with_timestamp(
            "UPDATE test_timestamps.test_table SET a = 'a1-modified-1' WHERE id = 0;",
            timestamp,
        )
        .await;

    assert_query_result(
        connection,
        "SELECT WRITETIME(a) FROM test_timestamps.test_table WHERE id = 0;",
        &[&[ResultValue::BigInt(timestamp)]],
    )
    .await;
}

async fn query(connection: &CassandraConnection) {
    let timestamp = setup(connection).await;

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

async fn setup(connection: &CassandraConnection) -> i64 {
    run_query(
        connection,
        "UPDATE test_timestamps.test_table SET a = 'a1-modified' WHERE id = 0;",
    )
    .await;

    let cassandra_timestamp = if let ResultValue::BigInt(v) = connection
        .execute("SELECT WRITETIME(a) FROM test_timestamps.test_table WHERE id = 0;")
        .await[0][0]
    {
        v
    } else {
        panic!("Expected a ResultValue::BigInt");
    };

    let timestamp: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .try_into()
        .unwrap();

    assert!(cassandra_timestamp < timestamp);

    timestamp
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
