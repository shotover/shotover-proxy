use crate::helpers::cassandra::{assert_query_result, assert_rows, run_query, ResultValue};
use cassandra_cpp::Session;

fn delete(session: &Session) {
    let prepared = session
        .prepare("DELETE FROM test_prepare_statements.table_1 WHERE id = ?;")
        .unwrap()
        .wait()
        .unwrap();

    let mut statement = prepared.bind();
    statement.bind_int32(0, 1).unwrap();
    session.execute(&statement).wait().unwrap();

    assert_query_result(
        session,
        "SELECT * FROM test_prepare_statements.table_1 where id = 1;",
        &[],
    );
}

fn insert(session: &Session) {
    let prepared = session
        .prepare("INSERT INTO test_prepare_statements.table_1 (id, x, name) VALUES (?, ?, ?);")
        .unwrap()
        .wait()
        .unwrap();

    let mut statement = prepared.bind();
    statement.bind_int32(0, 1).unwrap();
    statement.bind_int32(1, 11).unwrap();
    statement.bind_string(2, "foo").unwrap();
    session.execute(&statement).wait().unwrap();

    statement = prepared.bind();
    statement.bind_int32(0, 2).unwrap();
    statement.bind_int32(1, 12).unwrap();
    statement.bind_string(2, "bar").unwrap();
    session.execute(&statement).wait().unwrap();

    statement = prepared.bind();
    statement.bind_int32(0, 2).unwrap();
    statement.bind_int32(1, 13).unwrap();
    statement.bind_string(2, "baz").unwrap();
    session.execute(&statement).wait().unwrap();
}

fn select(session: &Session) {
    let prepared = session
        .prepare("SELECT id, x, name FROM test_prepare_statements.table_1 WHERE id = ?")
        .unwrap()
        .wait()
        .unwrap();

    let mut statement = prepared.bind();
    statement.bind_int32(0, 1).unwrap();

    let result_rows = session
        .execute(&statement)
        .wait()
        .unwrap()
        .into_iter()
        .map(|x| x.into_iter().map(ResultValue::new).collect())
        .collect();

    assert_rows(
        result_rows,
        &[&[
            ResultValue::Int(1),
            ResultValue::Int(11),
            ResultValue::Varchar("foo".into()),
        ]],
    );
}

pub fn test(session: &Session) {
    run_query(session, "CREATE KEYSPACE test_prepare_statements WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    run_query(
        session,
        "CREATE TABLE test_prepare_statements.table_1 (id int PRIMARY KEY, x int, name varchar);",
    );

    insert(session);
    select(session);
    delete(session);
}
