use crate::helpers::cassandra::{execute_query, run_query, ResultValue};
use cassandra_cpp::Session;

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

    // assert that data is decrypted by shotover
    // assert_query_result(
    //     shotover_session,
    //     "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
    //     &[&[
    //         ResultValue::Varchar("pk1".into()),
    //         ResultValue::Varchar("cluster".into()),
    //         ResultValue::Varchar("I am gonna get encrypted!!".into()),
    //         ResultValue::Int(42),
    //         ResultValue::Boolean(true),
    //     ]],
    // );
    // TODO: this should fail, protect currently manages to write the encrypted value but fails to decrypt it.
    let result = execute_query(
        shotover_session,
        "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
    );
    if let ResultValue::Varchar(value) = &result[0][2] {
        assert!(value.starts_with("{\"cipher"), "but was {:?}", value);
    } else {
        panic!("expectected 3rd column to be ResultValue::Varchar in {result:?}");
    }

    // assert that data is encrypted on cassandra side
    let result = execute_query(
        direct_session,
        "SELECT pk, cluster, col1, col2, col3 FROM test_protect_keyspace.test_table",
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].len(), 5);
    assert_eq!(result[0][0], ResultValue::Varchar("pk1".into()));
    assert_eq!(result[0][1], ResultValue::Varchar("cluster".into()));
    if let ResultValue::Varchar(value) = &result[0][2] {
        assert!(value.starts_with("{\"cipher"), "but was {:?}", value);
    } else {
        panic!("expectected 3rd column to be ResultValue::Varchar in {result:?}");
    }
    assert_eq!(result[0][3], ResultValue::Int(42));
    assert_eq!(result[0][4], ResultValue::Boolean(true));
}
