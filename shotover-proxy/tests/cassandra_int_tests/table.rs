use crate::helpers::cassandra::{
    assert_query_result, assert_query_result_contains_row, assert_query_result_not_contains_row,
    run_query, CassandraConnection, ResultValue,
};

use super::schema_awaiter::SchemaAwaiter;

fn test_create_table(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TABLE test_table_keyspace.my_table (id UUID PRIMARY KEY, name text, age int);",
    );
    assert_query_result_contains_row(
        session,
        "SELECT table_name FROM system_schema.tables;",
        &[ResultValue::Varchar("my_table".into())],
    );
}

fn test_drop_table(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TABLE test_table_keyspace.delete_me (id UUID PRIMARY KEY, name text, age int);",
    );

    assert_query_result_contains_row(
        session,
        "SELECT table_name FROM system_schema.tables;",
        &[ResultValue::Varchar("delete_me".into())],
    );
    run_query(session, "DROP TABLE test_table_keyspace.delete_me;");
    assert_query_result_not_contains_row(
        session,
        "SELECT table_name FROM system_schema.tables;",
        &[ResultValue::Varchar("delete_me".into())],
    );
}

async fn test_alter_table(session: &CassandraConnection, schema_awaiter: &SchemaAwaiter) {
    run_query(
        session,
        "CREATE TABLE test_table_keyspace.alter_me (id UUID PRIMARY KEY, name text, age int);",
    );

    assert_query_result(session, "SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'test_table_keyspace' AND table_name = 'alter_me' AND column_name = 'age';", &[&[ResultValue::Varchar("age".into())]]);
    schema_awaiter.await_schema_agreement().await;
    run_query(
        session,
        "ALTER TABLE test_table_keyspace.alter_me RENAME id TO new_id",
    );
    assert_query_result(session, "SELECT column_name FROM system_schema.columns WHERE keyspace_name = 'test_table_keyspace' AND table_name = 'alter_me' AND column_name = 'new_id';", &[&[ResultValue::Varchar("new_id".into())]]);
}

pub async fn test(session: &CassandraConnection, schema_awaiter: &SchemaAwaiter) {
    run_query(session, "CREATE KEYSPACE test_table_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    test_create_table(session);
    test_drop_table(session);
    test_alter_table(session, schema_awaiter).await;
}
