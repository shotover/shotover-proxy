use crate::helpers::cassandra::{
    assert_query_result, assert_query_result_contains_row, run_query, ResultValue,
};
use cassandra_cpp::Session;

fn test_create_keyspace(session: &Session) {
    run_query(session, "CREATE KEYSPACE keyspace_tests_create WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    assert_query_result(
        session,
        "SELECT bootstrapped FROM system.local",
        &[&[ResultValue::Varchar("COMPLETED".into())]],
    );
    assert_query_result_contains_row(
        session,
        "SELECT keyspace_name FROM system_schema.keyspaces;",
        &[ResultValue::Varchar("keyspace_tests_create".into())],
    );
}

fn test_use_keyspace(session: &Session) {
    run_query(session, "USE system");

    assert_query_result(
        session,
        "SELECT bootstrapped FROM local",
        &[&[ResultValue::Varchar("COMPLETED".into())]],
    );
}

fn test_drop_keyspace(session: &Session) {
    run_query(session, "CREATE KEYSPACE keyspace_tests_delete_me WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    assert_query_result(
            session,
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_delete_me';",
            &[&[ResultValue::Varchar("keyspace_tests_delete_me".into())]],
        );
    run_query(session, "DROP KEYSPACE keyspace_tests_delete_me");
    run_query(
            session,
            "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_delete_me';",
        );
}

fn test_alter_keyspace(session: &Session) {
    run_query(session, "CREATE KEYSPACE keyspace_tests_alter_me WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES = false;");
    run_query(
        session,
        "ALTER KEYSPACE keyspace_tests_alter_me WITH DURABLE_WRITES = true;",
    );
    assert_query_result(
            session,
            "SELECT durable_writes FROM system_schema.keyspaces WHERE keyspace_name='keyspace_tests_alter_me'",
            &[&[ResultValue::Boolean(true)]],
        );
}

pub fn test(session: &Session) {
    test_create_keyspace(session);
    test_use_keyspace(session);
    test_drop_keyspace(session);
    test_alter_keyspace(session);
}
