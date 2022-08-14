use crate::helpers::cassandra::{assert_query_result, run_query, CassandraConnection, ResultValue};
use cassandra_cpp::{stmt, Batch, BatchType};
use chacha20poly1305::Nonce;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Protected {
    _cipher: Vec<u8>,
    _nonce: Nonce,
    _enc_dek: Vec<u8>,
    _kek_id: String,
}

pub fn test(shotover_session: &CassandraConnection, direct_session: &CassandraConnection) {
    run_query(shotover_session, "CREATE KEYSPACE test_protect_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    run_query(
            shotover_session,
            "CREATE TABLE test_protect_keyspace.test_table (pk varchar PRIMARY KEY, cluster varchar, col1 blob, col2 int, col3 boolean);",
        );

    run_query(
        shotover_session,
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk1', 'cluster', 'I am gonna get encrypted!!', 42, true);"
    );

    let mut batch = Batch::new(BatchType::LOGGED);
    batch.add_statement(&stmt!(
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk2', 'cluster', 'encrypted2', 422, true)"
    )).unwrap();
    batch.add_statement(&stmt!(
        "INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk3', 'cluster', 'encrypted3', 423, false)"
    )).unwrap();
    shotover_session.execute_batch(&batch);

    let insert_statement = "BEGIN BATCH
INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk4', 'cluster', 'encrypted4', 424, true);
INSERT INTO test_protect_keyspace.test_table (pk, cluster, col1, col2, col3) VALUES ('pk5', 'cluster', 'encrypted5', 425, false);
APPLY BATCH;";
    run_query(shotover_session, insert_statement);

    // assert that data is decrypted by shotover
    assert_query_result(
        shotover_session,
        "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
        &[
            &[
                ResultValue::Varchar("pk1".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("I am gonna get encrypted!!".into()),
                ResultValue::Int(42),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk2".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted2".into()),
                ResultValue::Int(422),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk3".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted3".into()),
                ResultValue::Int(423),
                ResultValue::Boolean(false),
            ],
            &[
                ResultValue::Varchar("pk4".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted4".into()),
                ResultValue::Int(424),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk5".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Blob("encrypted5".into()),
                ResultValue::Int(425),
                ResultValue::Boolean(false),
            ],
        ],
    );

    // assert that data is encrypted on cassandra side
    let result = direct_session
        .execute("SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table");
    assert_eq!(result.len(), 5);
    for row in result {
        assert_eq!(row.len(), 5);

        // regular values are stored unencrypted
        assert_eq!(row[1], ResultValue::Varchar("cluster".into()));

        // protected values are stored encrypted
        if let ResultValue::Blob(value) = &row[2] {
            let _: Protected = bincode::deserialize(value).unwrap();
        } else {
            panic!("expected 3rd column to be ResultValue::Varchar in {row:?}");
        }
    }
}
