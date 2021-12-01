use crate::helpers::ShotoverManager;
use serial_test::serial;
use test_helpers::docker_compose::DockerCompose;

use crate::cassandra_int_tests::cassandra_connection;

mod keyspace {
    use cassandra_cpp::Session;

    use crate::cassandra_int_tests::{
        assert_query_result, assert_query_result_contains_row, run_query, ResultValue,
    };

    fn test_create_keyspace(session: &Session) {
        run_query(session, "CREATE KEYSPACE keyspace_tests_create WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        assert_query_result(
            session,
            "SELECT release_version FROM system.local",
            &[&[ResultValue::Varchar("3.11.10".into())]],
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
            "SELECT release_version FROM local",
            &[&[ResultValue::Varchar("3.11.10".into())]],
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

    pub fn test(session: Session) {
        test_create_keyspace(&session);
        test_use_keyspace(&session);
        test_drop_keyspace(&session);
        test_alter_keyspace(&session);
    }
}

#[test]
#[serial]
fn test_cluster() {
    let _compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml")
        .wait_for_n_t("Startup complete", 3, 90);

    let _handles: Vec<_> = [
        "examples/cassandra-cluster/topology1.yaml",
        "examples/cassandra-cluster/topology2.yaml",
        "examples/cassandra-cluster/topology3.yaml",
    ]
    .into_iter()
    .map(ShotoverManager::from_topology_file_without_observability)
    .collect();

    keyspace::test(cassandra_connection("127.0.0.1", 9042));
}

#[test]
#[serial]
fn test_passthrough() {
    let _compose = DockerCompose::new("examples/cassandra-passthrough/docker-compose.yml")
        .wait_for_n_t("Startup complete", 1, 90);
    let _shotover_manager =
        ShotoverManager::from_topology_file("examples/cassandra-passthrough/topology.yaml");

    keyspace::test(cassandra_connection("127.0.0.1", 9042));
}
