use crate::helpers::ShotoverManager;
use test_helpers::docker_compose::DockerCompose;

use crate::cassandra_int_tests::cassandra_connection;
use cassandra_cpp::{stmt, Session, Statement, Value, ValueType};
use serial_test::serial;

#[derive(PartialEq, Debug)]
enum ResultValue {
    Text(String),
    Varchar(String),
    Int(i64),
}

impl ResultValue {
    fn new(value: Value) -> ResultValue {
        match value.get_type() {
            ValueType::TEXT => ResultValue::Text(value.get_string().unwrap()),
            ValueType::VARCHAR => ResultValue::Varchar(value.get_string().unwrap()),
            ValueType::INT => ResultValue::Int(value.get_i64().unwrap()),
            ty => todo!(
                "The test infrastructure hasnt implemented the type {} yet, you should add it.",
                ty
            ),
        }
    }
}

fn assert_query_result(session: &Session, query: Statement, expected_rows: &[&[ResultValue]]) {
    let result = session.execute(&query).wait().unwrap();
    let result_rows: Vec<Vec<ResultValue>> = result
        .into_iter()
        .map(|x| x.into_iter().map(ResultValue::new).collect())
        .collect();
    assert_eq!(result_rows, expected_rows);
}

fn test_create_keyspace(session: Session) {
    assert_query_result(
        &session,
        stmt!(
            "CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
        ),
        &[],
    );

    assert_query_result(
        &session,
        stmt!("SELECT release_version FROM system.local"),
        &[&[ResultValue::Varchar("3.11.10".into())]],
    );

    assert_query_result(
        &session,
        stmt!("SELECT keyspace_name FROM system_schema.keyspaces;"),
        &[
            &[ResultValue::Varchar("cycling".into())],
            &[ResultValue::Varchar("system_auth".into())],
            &[ResultValue::Varchar("system_schema".into())],
            &[ResultValue::Varchar("system_distributed".into())],
            &[ResultValue::Varchar("system".into())],
            &[ResultValue::Varchar("system_traces".into())],
        ],
    );
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
    .map(|s| ShotoverManager::from_topology_file_without_observability(s))
    .collect();

    test_create_keyspace(cassandra_connection("127.0.0.1", 9042));
}

#[test]
#[serial]
fn test_passthrough() {
    let _compose = DockerCompose::new("examples/cassandra-passthrough/docker-compose.yml")
        .wait_for_n_t("Startup complete", 1, 90);
    let _shotover_manager =
        ShotoverManager::from_topology_file("examples/cassandra-passthrough/topology.yaml");

    test_create_keyspace(cassandra_connection("127.0.0.1", 9042));
}
