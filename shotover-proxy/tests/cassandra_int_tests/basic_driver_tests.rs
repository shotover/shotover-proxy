use crate::cassandra_int_tests::CassandraTestContext;
use crate::helpers::ShotoverManager;
use test_helpers::docker_compose::DockerCompose;

use anyhow::Result;
use cassandra_cpp::*;
use std::{thread, time};
use tracing::info;

fn test_create_keyspace() {
    info!("test_args");
    let query = stmt!(
        "CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    );
    let ctx = CassandraTestContext::new();
    info!("{:?}", ctx.session.execute(&query).wait().unwrap());
}

#[test]
fn test_basic_connection() -> Result<()> {
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

    test_create_keyspace();

    Ok(())
}

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
    info!("test_args");
    let query = stmt!(
        "CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    );
    let ctx = CassandraTestContext::new_with_points_and_port("127.0.0.1", 9043);
    ctx.session.execute(&query).wait().unwrap();
}
