use crate::cassandra_int_tests::CassandraTestContext;
use crate::helpers::ShotoverManager;
use test_helpers::docker_compose::DockerCompose;

use anyhow::Result;
use cassandra_cpp::*;
use tracing::info;
use std::{thread, time};

fn test_create_keyspace() {
    info!("test_args");
    let query = stmt!(
        "CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    );
    let ctx = CassandraTestContext::new();
    ctx.session.execute(&query).wait().unwrap();
}

#[test]
fn test_basic_connection() -> Result<()> {
    let _compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml")
        .wait_for_n( "Startup complete", 3 );;

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

#[test]
fn test_create_keyspace_direct() {
    let _compose = DockerCompose::new("examples/cassandra-cluster/docker-compose.yml")
        .wait_for_n( "Startup complete", 3 );;

    let _handles: Vec<_> = vec![
        "examples/cassandra-cluster/topology1.yaml",
        "examples/cassandra-cluster/topology2.yaml",
        "examples/cassandra-cluster/topology3.yaml",
    ]
        .iter()
        .map(|s| ShotoverManager::from_topology_file_without_observability(*s))
        .collect();

    info!("test_args");
    let query = stmt!(
        "CREATE KEYSPACE IF NOT EXISTS cycling WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    );
    let ctx = CassandraTestContext::new_with_points_and_port( "127.0.0.1" ,9043);
    ctx.session.execute(&query).wait().unwrap();
}
