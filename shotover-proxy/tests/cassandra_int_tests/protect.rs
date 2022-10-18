use crate::helpers::cassandra::{assert_query_result, run_query, CassandraConnection, ResultValue};
use chacha20poly1305::Nonce;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Protected {
    _cipher: Vec<u8>,
    _nonce: Nonce,
    _enc_dek: Vec<u8>,
    _kek_id: String,
}

pub async fn test(shotover_session: &CassandraConnection, direct_session: &CassandraConnection) {
    run_query(
        shotover_session,
        "CREATE KEYSPACE test_protect_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    ).await;

    run_query(
        shotover_session,
        "CREATE TABLE test_protect_keyspace.test_table (pk varchar PRIMARY KEY, cluster varchar, col1 blob, col2 int, col3 boolean);"
    ).await;

    run_query(
        shotover_session,
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 0, true);"
    ).await;

    shotover_session.execute_batch(vec![
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk2', 'cluster', 'encrypted2', 1, true)".into(),
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk3', 'cluster', 'encrypted3', 2, false)".into()
    ]).await;

    let insert_statement = "BEGIN BATCH
    INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk4', 'cluster', 'encrypted4', 3, true);
    INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk5', 'cluster', 'encrypted5', 4, false);
    APPLY BATCH;";
    run_query(shotover_session, insert_statement).await;

    // assert that data is decrypted by shotover
    assert_query_result(
        shotover_session,
        "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
        &[
            &[
                ResultValue::Varchar("pk1".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("I am gonna get encrypted!!".into()),
                ResultValue::Int(0),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk2".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted2".into()),
                ResultValue::Int(1),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk3".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted3".into()),
                ResultValue::Int(2),
                ResultValue::Boolean(false),
            ],
            &[
                ResultValue::Varchar("pk4".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted4".into()),
                ResultValue::Int(3),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk5".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted5".into()),
                ResultValue::Int(4),
                ResultValue::Boolean(false),
            ],
        ],
    )
    .await;

    // assert that data is encrypted on cassandra side
    let result = direct_session
        .execute("SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table")
        .await;
    assert_eq!(result.len(), 5);

    for row in result {
        assert_eq!(row.len(), 5);

        // regular values are stored unencrypted
        assert_eq!(row[1], ResultValue::Varchar("cluster".into()));

        // protected values are stored encrypted
        if let ResultValue::Blob(value) = &row[2] {
            let _: Protected = bincode::deserialize(value).unwrap();
        } else {
            panic!("expected 3rd column to be ResultValue::Blob in {row:?}");
        }
    }
}
