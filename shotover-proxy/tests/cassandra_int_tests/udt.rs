use crate::cassandra_int_tests::schema_awaiter::SchemaAwaiter;
use crate::helpers::cassandra::{run_query, CassandraConnection};

fn test_create_udt(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TYPE test_udt_keyspace.test_type_name (foo text, bar int)",
    );
    run_query(
        session,
        "CREATE TABLE test_udt_keyspace.test_table (id int PRIMARY KEY, foo test_type_name);",
    );
    run_query(
        session,
        "INSERT INTO test_udt_keyspace.test_table (id, foo) VALUES (1, {foo: 'yes', bar: 1})",
    );
}

async fn test_drop_udt(session: &CassandraConnection, schema_awaiter: &SchemaAwaiter) {
    run_query(
        session,
        "CREATE TYPE test_udt_keyspace.test_type_drop_me (foo text, bar int)",
    );
    schema_awaiter.await_schema_agreement().await;
    run_query(session, "DROP TYPE test_udt_keyspace.test_type_drop_me;");
    schema_awaiter.await_schema_agreement().await;
    let statement = "CREATE TABLE test_udt_keyspace.test_delete_table (id int PRIMARY KEY, foo test_type_drop_me);";
    let result = session.execute_expect_err(statement).to_string();
    assert_eq!(result, "Cassandra detailed error SERVER_INVALID_QUERY: Unknown type test_udt_keyspace.test_type_drop_me");
}

pub async fn test(session: &CassandraConnection, schema_awaiter: &SchemaAwaiter) {
    run_query(session, "CREATE KEYSPACE test_udt_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    test_create_udt(session);
    test_drop_udt(session, schema_awaiter).await;
}
