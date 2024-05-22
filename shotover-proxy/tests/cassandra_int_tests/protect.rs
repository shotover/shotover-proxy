use chacha20poly1305::Nonce;
use pretty_assertions::assert_eq;
use serde::Deserialize;
use test_helpers::connection::cassandra::{
    assert_query_result, run_query, CassandraConnection, ResultValue,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Protected {
    _cipher: Vec<u8>,
    _nonce: Nonce,
    _enc_dek: Vec<u8>,
    _kek_id: String,
}

async fn select_all(shotover_session: &CassandraConnection) {
    assert_query_result(
        shotover_session,
        "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
        &[
            &[
                ResultValue::Varchar("pk0".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted0".into()),
                ResultValue::Int(0),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk1".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted1".into()),
                ResultValue::Int(1),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk2".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted2".into()),
                ResultValue::Int(2),
                ResultValue::Boolean(false),
            ],
            &[
                ResultValue::Varchar("pk3".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted3".into()),
                ResultValue::Int(3),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk4".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted4".into()),
                ResultValue::Int(4),
                ResultValue::Boolean(false),
            ],
        ],
    )
    .await;
}

// assert that data is decrypted by shotover
async fn select(shotover_session: &CassandraConnection) {
    select_all(shotover_session).await;
    select_all(shotover_session).await; // run again to check any caching works

    for i in 0..5 {
        assert_query_result(
            shotover_session,
            &format!("SELECT pk, cluster, col1, col2 FROM test_protect_keyspace.test_table WHERE pk = 'pk{}'", i),
            &[
                &[
                    ResultValue::Varchar(format!("pk{}", i)),
                    ResultValue::Varchar("cluster".into()),
                    ResultValue::Blob(format!("encrypted{}", i).into()),
                    ResultValue::Int(i),
                ],
            ],
        )
        .await;
    }
}

async fn setup(shotover_session: &CassandraConnection) {
    run_query(
        shotover_session,
        "CREATE KEYSPACE test_protect_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    ).await;

    run_query(
        shotover_session,
        "CREATE TABLE test_protect_keyspace.test_table (pk varchar PRIMARY KEY, cluster varchar, col1 blob, col2 int, col3 boolean);"
    ).await;
}

async fn insert_data(shotover_session: &CassandraConnection) {
    let statements = [
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk0', 'cluster', 'encrypted0', 0, true);",
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'encrypted1', 1, true)",
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk2', 'cluster', 'encrypted2', 2, false)",
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk3', 'cluster', 'encrypted3', 3, true);",
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk4', 'cluster', 'encrypted4', 4, false);",
    ];

    run_query(shotover_session, statements[0]).await;

    shotover_session
        .execute_batch(vec![statements[1].to_string(), statements[2].to_string()])
        .await;

    let insert_statement = format!(
        "BEGIN BATCH
        {} 
        {}
    APPLY BATCH;",
        statements[3], statements[4]
    );
    run_query(shotover_session, &insert_statement).await;
}

pub async fn test(shotover_session: &CassandraConnection, direct_session: &CassandraConnection) {
    setup(shotover_session).await;
    insert_data(shotover_session).await;

    select(shotover_session).await;

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
