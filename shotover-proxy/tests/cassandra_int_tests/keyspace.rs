use test_helpers::connection::cassandra::{
    CassandraConnection, ResultValue, assert_query_result, assert_query_result_contains_row,
    run_query,
};

async fn test_create_keyspace(session: &CassandraConnection) {
    run_query(session, "CREATE KEYSPACE keyspace_tests_create WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    assert_query_result(
        session,
        "SELECT bootstrapped FROM system.local",
        &[&[ResultValue::Varchar("COMPLETED".into())]],
    )
    .await;
    assert_query_result_contains_row(
        session,
        "SELECT keyspace_name FROM system_schema.keyspaces;",
        &[ResultValue::Varchar("keyspace_tests_create".into())],
    )
    .await;
}

async fn test_use_keyspace(session: &CassandraConnection) {
    run_query(session, "USE system").await;

    assert_query_result(
        session,
        "SELECT bootstrapped FROM local",
        &[&[ResultValue::Varchar("COMPLETED".into())]],
    )
    .await;
}

async fn test_drop_keyspace(session: &CassandraConnection) {
    run_query(session, "CREATE KEYSPACE keyspace_tests_delete_me WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    assert_query_result(
        session,
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_delete_me';",
        &[&[ResultValue::Varchar("keyspace_tests_delete_me".into())]],
    ).await;
    run_query(session, "DROP KEYSPACE keyspace_tests_delete_me").await;
    run_query(
        session,
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_delete_me';",
    ).await;
}

async fn test_alter_keyspace(session: &CassandraConnection) {
    run_query(session, "CREATE KEYSPACE keyspace_tests_alter_me WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES = false;").await;
    run_query(
        session,
        "ALTER KEYSPACE keyspace_tests_alter_me WITH DURABLE_WRITES = true;",
    )
    .await;
    assert_query_result(
        session,
        "SELECT durable_writes FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_alter_me'",
        &[&[ResultValue::Boolean(true)]],
    ).await;
}

pub async fn test(session: &CassandraConnection) {
    test_create_keyspace(session).await;
    test_use_keyspace(session).await;
    test_drop_keyspace(session).await;
    test_alter_keyspace(session).await;
}
