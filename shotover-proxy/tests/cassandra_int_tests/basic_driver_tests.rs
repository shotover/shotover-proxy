use crate::cassandra_int_tests::CassandraTestContext;
use crate::helpers::ShotoverManager;
use test_helpers::docker_compose::DockerCompose;

use cassandra_cpp::*;
use serial_test::serial;
use std::thread;
use tracing::debug;

fn test_create_keyspace(ctx: CassandraTestContext) {
    let mut query = stmt!(
        "CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    );

    let mut result = ctx.session.execute(&query).wait().unwrap();
    debug!("{:?} query result {:?}", thread::current().id(), result);
    assert_eq!(result.row_count(), 0);

    query = stmt!("SELECT release_version FROM system.local");
    result = ctx.session.execute(&query).wait().unwrap();
    debug!("{:?} query result {:?}", thread::current().id(), result);
    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result
            .first_row()
            .unwrap()
            .get_column(0)
            .unwrap()
            .get_str()
            .unwrap(),
        Some("3.11.10")
    );

    query = stmt!("SELECT keyspace_name FROM system_schema.keyspaces;");
    result = ctx.session.execute(&query).wait().unwrap();
    debug!("{:?} query result {:?}", thread::current().id(), result);
    assert_eq!(result.row_count(), 6);
    assert_eq!(
        result
            .first_row()
            .unwrap()
            .get_column(0)
            .unwrap()
            .get_str()
            .unwrap(),
        Some("cycling")
    );
}

#[test]
#[serial]
fn test_basic_connection() {
    let _compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml")
        .wait_for_n_t("Startup complete", 3, 90);
    let _handles: Vec<_> = vec![
        "examples/cassandra-cluster/topology1.yaml",
        "examples/cassandra-cluster/topology2.yaml",
        "examples/cassandra-cluster/topology3.yaml",
    ]
    .iter()
    .map(|s| ShotoverManager::from_topology_file_without_observability(*s))
    .collect();

    test_create_keyspace(CassandraTestContext::new());
}
