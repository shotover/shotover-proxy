use serial_test::serial;
use std::any::Any;

use crate::helpers::ShotoverManager;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_runtime_use_existing() {
    let shotover_manager = ShotoverManager::from_topology_file("examples/null-redis/topology.yaml");

    // Assert that shotover is using the test runtime
    let handle = tokio::runtime::Handle::current();
    assert_eq!(handle.type_id(), shotover_manager.runtime_handle.type_id());

    // Assert that shotover did not create a runtime for itself
    assert!(shotover_manager.runtime.is_none());
}

#[tokio::test(flavor = "current_thread")]
#[ntest::timeout(1000)]
async fn test_shotover_panics_in_single_thread_runtime() {
    let result = std::panic::catch_unwind(|| {
        ShotoverManager::from_topology_file("examples/null-redis/topology.yaml");
    });
    assert!(result.is_err());
}

#[test]
#[serial]
fn test_runtime_create() {
    let shotover_manager = ShotoverManager::from_topology_file("examples/null-redis/topology.yaml");

    // Assert that shotover created a runtime for itself
    assert!(shotover_manager.runtime.is_some());
}

#[test]
#[serial]
fn test_early_shutdown_cassandra_source() {
    ShotoverManager::from_topology_file("examples/null-cassandra/topology.yaml");
}
