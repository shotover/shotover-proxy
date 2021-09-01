use serial_test::serial;
use std::any::Any;
use test_helpers::docker_compose::DockerCompose;

use crate::helpers::ShotoverManager;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_runtime_use_existing() {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");
    ShotoverManager::wait_for_socket_to_open(6379);

    // Assert that shotover is using the test runtime
    let handle = tokio::runtime::Handle::current();
    assert_eq!(handle.type_id(), shotover_manager.runtime_handle.type_id());

    // Assert that shotover did not create a runtime for itself
    assert!(shotover_manager.runtime.is_none());
}

#[test]
#[serial]
fn test_runtime_create() {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");

    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");
    ShotoverManager::wait_for_socket_to_open(6379);

    // Assert that shotover created a runtime for itself
    assert!(shotover_manager.runtime.is_some());
}
