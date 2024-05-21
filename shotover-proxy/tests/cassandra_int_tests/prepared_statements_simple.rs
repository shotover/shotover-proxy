use futures::Future;
use pretty_assertions::assert_eq;
use test_helpers::connection::cassandra::{
    assert_query_result, assert_rows, run_query, CassandraConnection, Consistency, ResultValue,
};

async fn delete(session: &CassandraConnection, replication_factor: u32) {
    let prepared = session
        .prepare(&format!(
            "DELETE FROM test_prepare_statements{replication_factor}.table_1 WHERE id = ?;"
        ))
        .await;

    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(1)], Consistency::All)
            .await,
        Ok(Vec::<Vec<ResultValue>>::new())
    );

    assert_query_result(
        session,
        &format!("SELECT * FROM test_prepare_statements{replication_factor}.table_1 where id = 1;"),
        &[],
    )
    .await;
}

async fn insert(session: &CassandraConnection, replication_factor: u32) {
    let prepared = session
        .prepare(&format!(
            "INSERT INTO test_prepare_statements{replication_factor}.table_1 (id) VALUES (?);"
        ))
        .await;

    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(1)], Consistency::All)
            .await,
        Ok(Vec::<Vec<ResultValue>>::new())
    );

    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(2)], Consistency::All)
            .await,
        Ok(Vec::<Vec<ResultValue>>::new())
    );

    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(3)], Consistency::All)
            .await,
        Ok(Vec::<Vec<ResultValue>>::new())
    );
}

async fn select(session: &CassandraConnection, replication_factor: u32) {
    let prepared = session
        .prepare(&format!(
            "SELECT id FROM test_prepare_statements{replication_factor}.table_1 WHERE id = ?"
        ))
        .await;

    let result_rows = session
        .execute_prepared(&prepared, &[ResultValue::Int(1)], Consistency::All)
        .await
        .unwrap();

    assert_rows(result_rows, &[&[ResultValue::Int(1)]]);
}
async fn select_cross_connection<Fut>(
    connection: &CassandraConnection,
    connection_creator: impl Fn() -> Fut,
    replication_factor: u32,
) where
    Fut: Future<Output = CassandraConnection>,
{
    let connection_before = connection_creator().await;

    // query is purposely slightly different to past queries to avoid being cached
    let prepared = connection
        .prepare(&format!(
            "SELECT id, id FROM test_prepare_statements{replication_factor}.table_1 WHERE id = ?"
        ))
        .await;

    let connection_after = connection_creator().await;

    assert_rows(
        connection_before
            .execute_prepared(&prepared, &[ResultValue::Int(1)], Consistency::All)
            .await
            .unwrap(),
        &[&[ResultValue::Int(1), ResultValue::Int(1)]],
    );
    assert_rows(
        connection_after
            .execute_prepared(&prepared, &[ResultValue::Int(1)], Consistency::All)
            .await
            .unwrap(),
        &[&[ResultValue::Int(1), ResultValue::Int(1)]],
    );
}

async fn use_statement(session: &CassandraConnection, replication_factor: u32) {
    // Create prepared command with the correct keyspace
    run_query(
        session,
        &format!("USE test_prepare_statements{replication_factor};"),
    )
    .await;
    let prepared = session
        .prepare("INSERT INTO table_1 (id) VALUES (?);")
        .await;

    // observe query completing against the original keyspace without errors
    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(358)], Consistency::All)
            .await,
        Ok(Vec::<Vec<ResultValue>>::new())
    );

    // change the keyspace to be incorrect
    run_query(
        session,
        &format!("USE test_prepare_statements_empty{replication_factor};"),
    )
    .await;

    // observe that the query succeeded despite the keyspace being incorrect at the time.
    assert_eq!(
        session
            .execute_prepared(&prepared, &[ResultValue::Int(358)], Consistency::All)
            .await,
        Ok(Vec::<Vec<ResultValue>>::new())
    );
}

async fn setup(session: &CassandraConnection, replication_factor: u32) {
    run_query(session, &format!("CREATE KEYSPACE test_prepare_statements{replication_factor} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'datacenter1' : {replication_factor} }};")).await;
    run_query(session, &format!("CREATE KEYSPACE test_prepare_statements_empty{replication_factor} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'datacenter1' : {replication_factor} }};")).await;
    run_query(
        session,
        &format!("CREATE TABLE test_prepare_statements{replication_factor}.table_1 (id int PRIMARY KEY);"),
    )
    .await;
}

pub async fn test<Fut>(
    session: &CassandraConnection,
    connection_creator: impl Fn() -> Fut,
    replication_factor: u32,
) where
    Fut: Future<Output = CassandraConnection>,
{
    setup(session, replication_factor).await;
    insert(session, replication_factor).await;
    select(session, replication_factor).await;
    select_cross_connection(session, &connection_creator, replication_factor).await;
    delete(session, replication_factor).await;
    use_statement(session, replication_factor).await;

    let cql = "SELECT * FROM system.local WHERE key = 'local'";
    let prepared = session.prepare(cql).await;
    session
        .execute_prepared(&prepared, &[], Consistency::All)
        .await
        .unwrap();
}
