use crate::helpers::cassandra::{
    assert_query_result, assert_rows, run_query, CassandraConnection, ResultValue,
};
use futures::Future;

async fn delete(session: &CassandraConnection) {
    let prepared = session
        .prepare("DELETE FROM test_prepare_statements.table_1 WHERE id = ?;")
        .await;

    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(1)])
            .await,
        Vec::<Vec<ResultValue>>::new()
    );

    assert_query_result(
        session,
        "SELECT * FROM test_prepare_statements.table_1 where id = 1;",
        &[],
    )
    .await;
}

async fn insert(session: &CassandraConnection) {
    let prepared = session
        .prepare("INSERT INTO test_prepare_statements.table_1 (id) VALUES (?);")
        .await;

    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(1)])
            .await,
        Vec::<Vec<ResultValue>>::new()
    );

    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(2)])
            .await,
        Vec::<Vec<ResultValue>>::new()
    );

    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(3)])
            .await,
        Vec::<Vec<ResultValue>>::new()
    );
}

async fn select(session: &CassandraConnection) {
    let prepared = session
        .prepare("SELECT id FROM test_prepare_statements.table_1 WHERE id = ?")
        .await;

    let result_rows = session
        .execute_prepared(&prepared, &[ResultValue::Int(1)])
        .await;

    assert_rows(result_rows, &[&[ResultValue::Int(1)]]);
}
async fn select_cross_connection<Fut>(
    connection: &CassandraConnection,
    connection_creator: impl Fn() -> Fut,
) where
    Fut: Future<Output = CassandraConnection>,
{
    let connection_before = connection_creator().await;

    // query is purposely slightly different to past queries to avoid being cached
    let prepared = connection
        .prepare("SELECT id, id FROM test_prepare_statements.table_1 WHERE id = ?")
        .await;

    let connection_after = connection_creator().await;

    assert_rows(
        connection_before
            .execute_prepared(&prepared, &[ResultValue::Int(1)])
            .await,
        &[&[ResultValue::Int(1), ResultValue::Int(1)]],
    );
    assert_rows(
        connection_after
            .execute_prepared(&prepared, &[ResultValue::Int(1)])
            .await,
        &[&[ResultValue::Int(1), ResultValue::Int(1)]],
    );
}

async fn use_statement(session: &CassandraConnection) {
    // Create prepared command with the correct keyspace
    run_query(session, "USE test_prepare_statements;").await;
    let prepared = session
        .prepare("INSERT INTO table_1 (id) VALUES (?);")
        .await;

    // observe query completing against the original keyspace without errors
    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(358)])
            .await,
        Vec::<Vec<ResultValue>>::new()
    );

    // change the keyspace to be incorrect
    run_query(session, "USE test_prepare_statements_empty;").await;

    // observe that the query succeeded despite the keyspace being incorrect at the time.
    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(358)])
            .await,
        Vec::<Vec<ResultValue>>::new()
    );
}

pub async fn test<Fut>(session: &CassandraConnection, connection_creator: impl Fn() -> Fut)
where
    Fut: Future<Output = CassandraConnection>,
{
    run_query(session, "CREATE KEYSPACE test_prepare_statements WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    run_query(session, "CREATE KEYSPACE test_prepare_statements_empty WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
    run_query(
        session,
        "CREATE TABLE test_prepare_statements.table_1 (id int PRIMARY KEY);",
    )
    .await;

    insert(session).await;
    select(session).await;
    select_cross_connection(session, connection_creator).await;
    delete(session).await;
    use_statement(session).await;

    let cql = "SELECT * FROM system.local WHERE key = 'local'";
    let prepared = session.prepare(cql).await;
    session.execute_prepared(&prepared, &[]).await;
}
