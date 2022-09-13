use crate::helpers::cassandra::{
    assert_query_result, assert_rows, run_query, CassandraConnection, ResultValue,
};

async fn delete(session: &CassandraConnection) {
    let prepared = session.prepare("DELETE FROM test_prepare_statements.table_1 WHERE id = ?;");

    assert_eq!(
        session.execute_prepared(&prepared, 1),
        Vec::<Vec<ResultValue>>::new()
    );

    assert_query_result(
        session,
        "SELECT * FROM test_prepare_statements.table_1 where id = 1;",
        &[],
    )
    .await;
}

fn insert(session: &CassandraConnection) {
    let prepared = session.prepare("INSERT INTO test_prepare_statements.table_1 (id) VALUES (?);");

    assert_eq!(
        session.execute_prepared(&prepared, 1),
        Vec::<Vec<ResultValue>>::new()
    );

    assert_eq!(
        session.execute_prepared(&prepared, 2),
        Vec::<Vec<ResultValue>>::new()
    );

    assert_eq!(
        session.execute_prepared(&prepared, 2),
        Vec::<Vec<ResultValue>>::new()
    );
}

fn select(session: &CassandraConnection) {
    let prepared = session.prepare("SELECT id FROM test_prepare_statements.table_1 WHERE id = ?");

    let result_rows = session.execute_prepared(&prepared, 1);

    assert_rows(result_rows, &[&[ResultValue::Int(1)]]);
}

async fn use_statement(session: &CassandraConnection) {
    // Create prepared command with the correct keyspace
    run_query(session, "USE test_prepare_statements;").await;
    let _prepared = session.prepare("INSERT INTO table_1 (id) VALUES (?);");

    // change the keyspace to be incorrect
    run_query(session, "USE test_prepare_statements_empty;").await;

    // TODO: Somehow executing the query fails when run through shotover but succeeds when run directly against cassandra
    //       We should investigate and fix the issue in shotover
    // observe query completing against the original keyspace without errors
    // assert_eq!(
    //     session.execute_prepared(&prepared, 358),
    //     Vec::<Vec<ResultValue>>::new()
    // );

    // // observe that the query succeeded despite the keyspace being incorrect at the time.
    // assert_query_result(
    //     session,
    //     "SELECT id FROM test_prepare_statements.table_1 WHERE id = 358;",
    //     &[&[ResultValue::Int(358)]],
    // )
    // .await;
}

pub async fn test(session: &CassandraConnection) {
    run_query(session, "CREATE KEYSPACE test_prepare_statements WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    run_query(session, "CREATE KEYSPACE test_prepare_statements_empty WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    run_query(
        session,
        "CREATE TABLE test_prepare_statements.table_1 (id int PRIMARY KEY);",
    )
    .await;

    insert(session);
    select(session);
    delete(session).await;
    use_statement(session).await;
}
