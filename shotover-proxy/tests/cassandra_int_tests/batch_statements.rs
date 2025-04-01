use rand::Rng;
use test_helpers::connection::cassandra::{
    CassandraConnection, ResultValue, assert_query_result, run_query,
};

async fn large_batch(connection: &CassandraConnection) {
    connection
        .execute("CREATE TABLE batch_keyspace.my_table (id INT PRIMARY KEY, data BLOB);")
        .await;

    let random_bytes = rand::rng().random::<[u8; 32]>();

    let mut queries = Vec::new();
    for id in 0..500 {
        let statement = format!(
            "INSERT INTO batch_keyspace.my_table (id, data) VALUES ({}, 0x{})",
            id,
            hex::encode(random_bytes)
        );
        queries.push(statement);
    }

    connection.execute_batch(queries).await;
}

async fn use_statement(connection: &CassandraConnection) {
    {
        run_query(connection, "USE batch_keyspace;").await;
        connection
            .execute_batch(vec![
                "INSERT INTO batch_table (id, lastname, firstname) VALUES (0, 'text1', 'text2')"
                    .into(),
                "INSERT INTO batch_table (id, lastname, firstname) VALUES (1, 'text1', 'text2')"
                    .into(),
            ])
            .await;
        assert_query_result(
            connection,
            "SELECT id, lastname, firstname FROM batch_table;",
            &[
                &[
                    ResultValue::Int(0),
                    ResultValue::Varchar("text1".into()),
                    ResultValue::Varchar("text2".into()),
                ],
                &[
                    ResultValue::Int(1),
                    ResultValue::Varchar("text1".into()),
                    ResultValue::Varchar("text2".into()),
                ],
            ],
        )
        .await;
    }

    {
        connection
            .execute_batch(vec![
                "DELETE FROM batch_table WHERE id = 0;".into(),
                "DELETE FROM batch_table WHERE id = 1;".into(),
            ])
            .await;
        assert_query_result(connection, "SELECT * FROM batch_table;", &[]).await;
    }
}

async fn insert_batch(connection: &CassandraConnection) {
    let mut batch = vec![];
    for i in 0..2 {
        batch.push(format!("INSERT INTO batch_keyspace.batch_table (id, lastname, firstname) VALUES ({}, 'text1', 'text2')", i));
    }
    connection.execute_batch(batch).await;

    assert_query_result(
        connection,
        "SELECT id, lastname, firstname FROM batch_keyspace.batch_table;",
        &[
            &[
                ResultValue::Int(0),
                ResultValue::Varchar("text1".into()),
                ResultValue::Varchar("text2".into()),
            ],
            &[
                ResultValue::Int(1),
                ResultValue::Varchar("text1".into()),
                ResultValue::Varchar("text2".into()),
            ],
        ],
    )
    .await;
}

async fn update_batch(connection: &CassandraConnection) {
    let mut batch = vec![];
    for i in 0..2 {
        batch.push(format!(
            "UPDATE batch_keyspace.batch_table SET lastname = 'text3' WHERE id = {};",
            i
        ));
    }
    connection.execute_batch(batch).await;

    assert_query_result(
        connection,
        "SELECT id, lastname, firstname FROM batch_keyspace.batch_table;",
        &[
            &[
                ResultValue::Int(0),
                ResultValue::Varchar("text3".into()),
                ResultValue::Varchar("text2".into()),
            ],
            &[
                ResultValue::Int(1),
                ResultValue::Varchar("text3".into()),
                ResultValue::Varchar("text2".into()),
            ],
        ],
    )
    .await;
}

async fn delete_batch(connection: &CassandraConnection) {
    let mut batch = vec![];
    for i in 0..2 {
        batch.push(format!(
            "DELETE FROM batch_keyspace.batch_table WHERE id = {};",
            i
        ));
    }
    connection.execute_batch(batch).await;
    assert_query_result(connection, "SELECT * FROM batch_keyspace.batch_table;", &[]).await;
}

async fn empty_batch(connection: &CassandraConnection) {
    let batch = vec![];
    connection.execute_batch(batch).await;
}

async fn query_protocol_batch(connection: &CassandraConnection) {
    let insert_statement = "BEGIN BATCH
INSERT INTO batch_keyspace.batch_table (id, lastname, firstname) VALUES (2, 'text1', 'text2');
INSERT INTO batch_keyspace.batch_table (id, lastname, firstname) VALUES (3, 'text1', 'text2');
APPLY BATCH;";
    run_query(connection, insert_statement).await;

    assert_query_result(
        connection,
        "SELECT id, lastname, firstname FROM batch_keyspace.batch_table;",
        &[
            &[
                ResultValue::Int(2),
                ResultValue::Varchar("text1".into()),
                ResultValue::Varchar("text2".into()),
            ],
            &[
                ResultValue::Int(3),
                ResultValue::Varchar("text1".into()),
                ResultValue::Varchar("text2".into()),
            ],
        ],
    )
    .await;

    let update_statement = "BEGIN BATCH UPDATE batch_keyspace.batch_table SET lastname = 'text3' WHERE id = 2; UPDATE batch_keyspace.batch_table SET lastname = 'text3' WHERE id = 3; APPLY BATCH;";
    run_query(connection, update_statement).await;

    assert_query_result(
        connection,
        "SELECT id, lastname, firstname FROM batch_keyspace.batch_table;",
        &[
            &[
                ResultValue::Int(2),
                ResultValue::Varchar("text3".into()),
                ResultValue::Varchar("text2".into()),
            ],
            &[
                ResultValue::Int(3),
                ResultValue::Varchar("text3".into()),
                ResultValue::Varchar("text2".into()),
            ],
        ],
    )
    .await;
}

pub async fn test(connection: &CassandraConnection) {
    // setup keyspace and table for the batch statement tests
    {
        run_query(connection, "CREATE KEYSPACE batch_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };").await;
        run_query(connection, "CREATE TABLE batch_keyspace.batch_table (id int PRIMARY KEY, lastname text, firstname text);").await;
    }

    use_statement(connection).await;
    insert_batch(connection).await;
    update_batch(connection).await;
    delete_batch(connection).await;
    empty_batch(connection).await;
    query_protocol_batch(connection).await;
    large_batch(connection).await;
}
