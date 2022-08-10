use crate::helpers::cassandra::{run_query, CassandraConnection};

async fn test_create_udt(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TYPE test_udt_keyspace.test_type_name (foo text, bar int)",
    )
    .await;
    run_query(
        session,
        "CREATE TABLE test_udt_keyspace.test_table (id int PRIMARY KEY, foo test_type_name);",
    )
    .await;
    run_query(
        session,
        "INSERT INTO test_udt_keyspace.test_table (id, foo) VALUES (1, {foo: 'yes', bar: 1})",
    )
    .await;
}

async fn test_drop_udt(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TYPE test_udt_keyspace.test_type_drop_me (foo text, bar int)",
    )
    .await;
    run_query(session, "DROP TYPE test_udt_keyspace.test_type_drop_me;").await;
    let statement = "CREATE TABLE test_udt_keyspace.test_delete_table (id int PRIMARY KEY, foo test_type_drop_me);";
    let result = session.execute_expect_err(statement).to_string();
    assert_eq!(result, "Cassandra detailed error SERVER_INVALID_QUERY: Unknown type test_udt_keyspace.test_type_drop_me");
}

pub async fn test(session: &CassandraConnection) {
    run_query(session, "CREATE KEYSPACE test_udt_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    test_create_udt(session).await;
    test_drop_udt(session).await;
}
