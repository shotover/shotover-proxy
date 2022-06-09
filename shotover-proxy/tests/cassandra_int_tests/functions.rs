use crate::helpers::cassandra::{assert_query_result, run_query, ResultValue};
use cassandra_cpp::{stmt, Session};

fn drop_function(session: &Session) {
    assert_query_result(session, "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table WHERE id=1;", &[&[ResultValue::Int(4)]]);
    run_query(session, "DROP FUNCTION test_function_keyspace.my_function");

    let statement = stmt!("SELECT test_function_keyspace.my_function(x) FROM test_function_keyspace.test_function_table WHERE id=1;");
    let result = session.execute(&statement).wait().unwrap_err().to_string();

    assert_eq!(result, "Cassandra detailed error SERVER_INVALID_QUERY: Unknown function 'test_function_keyspace.my_function'");
}

fn create_function(session: &Session) {
    run_query(
            session,
            "CREATE FUNCTION test_function_keyspace.my_function (a int, b int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE javascript AS 'a * b';",
        );
    assert_query_result(session, "SELECT test_function_keyspace.my_function(x, y) FROM test_function_keyspace.test_function_table;",&[&[ResultValue::Int(4)], &[ResultValue::Int(9)], &[ResultValue::Int(16)]]);
}

pub fn test(session: &Session) {
    run_query(session, "CREATE KEYSPACE test_function_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
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

    create_function(session);
    drop_function(session);
}
