use crate::cassandra_int_tests::CassandraTestContext;
use crate::helpers::ShotoverManager;
use test_helpers::docker_compose::DockerCompose;

use anyhow::Result;
use cassandra_cpp::*;
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
            .ok(),
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
            .ok(),
        Some("cycling")
    );
}

#[test]
fn test_basic_connection() -> Result<()> {
    let _compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml")
        .wait_for_n_t("Startup complete", 3, 180);

    let _handles: Vec<_> = vec![
        "examples/cassandra-cluster/topology1.yaml",
        "examples/cassandra-cluster/topology2.yaml",
        "examples/cassandra-cluster/topology3.yaml",
    ]
    .iter()
    .map(|s| ShotoverManager::from_topology_file_without_observability(*s))
    .collect();

    test_create_keyspace(CassandraTestContext::new());

    Ok(())
}

// this test is used for dev testing to ensure that the driver works we directly connect vi the
// test context
// with the Cassandra we are running against.
//#[test] // can not use ignore on test as build builds ignored tests
#[allow(dead_code)] // to make clippy happy
fn test_create_keyspace_direct() {
    let compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml");

    let _handles: Vec<_> = vec![
        "examples/cassandra-cluster/topology1.yaml",
        "examples/cassandra-cluster/topology2.yaml",
        "examples/cassandra-cluster/topology3.yaml",
    ]
    .iter()
    .map(|s| ShotoverManager::from_topology_file_without_observability(*s))
    .collect();

    compose.wait_for_n_t("Startup complete", 3, 120);

    test_create_keyspace(CassandraTestContext::new_with_points("10.5.0.2"));
}

// this test is used for dev testing to ensure that  the cpp driver we are using actually works
// with the Cassandra we are running against.
//#[test] // can not use ignore on test as build builds ignored tests
#[allow(dead_code)] // to make clippy happy
fn test_cpp_driver() {
    let _compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml")
        .wait_for_n_t("Startup complete", 3, 90);

    let mut cluster = Cluster::default();
    cluster.set_contact_points("10.5.0.2").unwrap();
    //cluster.set_port(9043).ok();
    cluster.set_load_balance_round_robin();

    let session = cluster.connect().unwrap();

    test_create_keyspace(CassandraTestContext { session });
}
