use crate::helpers::cassandra::{assert_query_result, execute_query, run_query, ResultValue};
use cassandra_cpp::{stmt, Batch, BatchType, Session};

pub fn test(shotover_session: &Session, direct_session: &Session) {
    run_query(shotover_session, "CREATE KEYSPACE test_protect_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    run_query(
            shotover_session,
            "CREATE TABLE test_protect_keyspace.test_table (pk varchar PRIMARY KEY, cluster varchar, col1 varchar, col2 int, col3 boolean);",
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
    shotover_session.execute_batch(&batch).wait().unwrap();

    // assert that data is decrypted by shotover
    assert_query_result(
        shotover_session,
        "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
        &[
            &[
                ResultValue::Varchar("pk1".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Varchar("I am gonna get encrypted!!".into()),
                ResultValue::Int(42),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk2".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Varchar("encrypted2".into()),
                ResultValue::Int(422),
                ResultValue::Boolean(true),
            ],
            &[
                ResultValue::Varchar("pk3".into()),
                ResultValue::Varchar("cluster".into()),
                ResultValue::Varchar("encrypted3".into()),
                ResultValue::Int(423),
                ResultValue::Boolean(false),
            ],
        ],
    );

    // assert that data is encrypted on cassandra side
    let result = execute_query(
        direct_session,
        "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
    );
    assert_eq!(result.len(), 3);
    for row in result {
        assert_eq!(row.len(), 5);

        // regular values are stored unencrypted
        assert_eq!(row[1], ResultValue::Varchar("cluster".into()));

        // protected values are stored encrypted
        if let ResultValue::Varchar(value) = &row[2] {
            assert!(value.starts_with("{\"cipher"), "but was {:?}", value);
        } else {
            panic!("expected 3rd column to be ResultValue::Varchar in {row:?}");
        }
    }
}
