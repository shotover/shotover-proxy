use test_helpers::connection::cassandra::{
    CassandraConnection, ResultValue, assert_query_result, assert_query_result_contains_row,
    assert_query_result_not_contains_row, run_query,
};

async fn test_create_table(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TABLE test_table_keyspace.my_table (id UUID PRIMARY KEY, name text, age int);",
    )
    .await;
    assert_query_result_contains_row(
        session,
        "SELECT table_name FROM system_schema.tables;",
        &[ResultValue::Varchar("my_table".into())],
    )
    .await;
}

async fn test_drop_table(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TABLE test_table_keyspace.delete_me (id UUID PRIMARY KEY, name text, age int);",
    )
    .await;

    assert_query_result_contains_row(
        session,
        "SELECT table_name FROM system_schema.tables;",
        &[ResultValue::Varchar("delete_me".into())],
    )
    .await;
    run_query(session, "DROP TABLE test_table_keyspace.delete_me;").await;
    assert_query_result_not_contains_row(
        session,
        "SELECT table_name FROM system_schema.tables;",
        &[ResultValue::Varchar("delete_me".into())],
    )
    .await;
}

async fn test_alter_table(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TABLE test_table_keyspace.alter_me (id UUID PRIMARY KEY, name text, age int);",
    )
    .await;

    assert_query_result(session, "SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'test_table_keyspace' AND table_name = 'alter_me' AND column_name = 'age';", &[&[ResultValue::Varchar("age".into())]]).await;
    run_query(
        session,
        "ALTER TABLE test_table_keyspace.alter_me RENAME id TO new_id",
    )
    .await;
    assert_query_result(session, "SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'test_table_keyspace' AND table_name = 'alter_me' AND column_name = 'new_id';", &[&[ResultValue::Varchar("new_id".into())]]).await;
}

pub async fn test(session: &CassandraConnection) {
    run_query(session, "CREATE KEYSPACE test_table_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    test_create_table(session).await;
    test_drop_table(session).await;
    test_alter_table(session).await;
}
