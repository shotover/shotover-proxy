use test_helpers::connection::cassandra::{CassandraConnection, run_query};

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
    run_query(session, "SELECT * FROM test_udt_keyspace.test_table").await;
}

async fn test_drop_udt(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE TYPE test_udt_keyspace.test_type_drop_me (foo text, bar int)",
    )
    .await;
    run_query(session, "DROP TYPE test_udt_keyspace.test_type_drop_me;").await;
    // TODO: This exposes a bug in at least cassandra 3
    // The error is supposed to be a 0x2200 syntax error but rarely cassandra will return a 0x0000 server error instead.
    // The cpp driver interprets 0x0000 as different to 0x2200 and considers the node as down and will no longer talk to it.
    // We should eventually reenable this test when we upgrade to cassandra 4 (the bug may also need to be reported and fixed in cassandra 3 and/or 4)
    // session.execute_expect_err_contains(
    //     "CREATE TABLE test_udt_keyspace.test_delete_table (id int PRIMARY KEY, foo test_type_drop_me);",
    //     "Unknown type test_udt_keyspace.test_type_drop_me",
    // );
}

pub async fn test(session: &CassandraConnection) {
    run_query(session, "CREATE KEYSPACE test_udt_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    test_create_udt(session).await;
    test_drop_udt(session).await;
}
