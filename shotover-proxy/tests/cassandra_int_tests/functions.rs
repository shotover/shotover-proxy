use test_helpers::connection::cassandra::{
    CassandraConnection, ResultValue, assert_query_result, run_query,
};

async fn drop_function(session: &CassandraConnection) {
    assert_query_result(
        session,
        "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table WHERE id=1;",
        &[&[ResultValue::Int(4)]]
    ).await;

    run_query(session, "DROP FUNCTION test_function_keyspace.my_function").await;
}

async fn create_function(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE FUNCTION test_function_keyspace.my_function (a int, b int) 
RETURNS NULL ON NULL INPUT 
RETURNS int 
LANGUAGE java 
AS $$ return a * b; $$;",
    )
    .await;

    assert_query_result(
        session,
        "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table;",
        &[&[ResultValue::Int(4)], &[ResultValue::Int(9)], &[ResultValue::Int(16)]]
    ).await;
}

pub async fn test(session: &CassandraConnection) {
    run_query(
        session,
        "CREATE KEYSPACE test_function_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    ).await;

    run_query(
        session,
        "CREATE TABLE test_function_keyspace.test_function_table (id int PRIMARY KEY, x int, y int);",
    ).await;

    run_query(
        session,
        r#"BEGIN BATCH
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (1, 2, 2);
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (2, 3, 3);
INSERT INTO test_function_keyspace.test_function_table (id, x, y) VALUES (3, 4, 4);
APPLY BATCH;"#,
    )
    .await;

    create_function(session).await;
    drop_function(session).await;
}
