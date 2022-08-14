use crate::cassandra_int_tests::schema_awaiter::SchemaAwaiter;
use crate::helpers::cassandra::{assert_query_result, run_query, CassandraConnection, ResultValue};

fn drop_function(session: &CassandraConnection) {
    assert_query_result(
        session,
        "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table WHERE id=1;",
        &[&[ResultValue::Int(4)]]
    );
    run_query(session, "DROP FUNCTION test_function_keyspace.my_function");

    let statement = "SELECT test_function_keyspace.my_function(x) FROM test_function_keyspace.test_function_table WHERE id=1;";
    let result = session.execute_expect_err(statement).to_string();

    assert_eq!(
        result,
        "Cassandra detailed error SERVER_INVALID_QUERY: Unknown function 'test_function_keyspace.my_function'"
    );
}

async fn create_function(session: &CassandraConnection, schema_awaiter: &SchemaAwaiter) {
    run_query(
        session,
        "CREATE FUNCTION test_function_keyspace.my_function (a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE javascript AS 'a * b';",
    );
    schema_awaiter.await_schema_agreement().await;
    assert_query_result(
        session,
        "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table;",
        &[&[ResultValue::Int(4)], &[ResultValue::Int(9)], &[ResultValue::Int(16)]]
    );
}

pub async fn test(session: &CassandraConnection, schema_awaiter: &SchemaAwaiter) {
    run_query(
        session,
        "CREATE KEYSPACE test_function_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    );
    run_query(
        session,
        "CREATE TABLE test_function_keyspace.test_function_table (id int PRIMARY KEY, x int, y int);",
    );
    run_query(
        session,
        r#"BEGIN BATCH
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (1, 2, 2);
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (2, 3, 3);
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (3, 4, 4);
APPLY BATCH;"#,
    );

    create_function(session, schema_awaiter).await;
    drop_function(session);
}
