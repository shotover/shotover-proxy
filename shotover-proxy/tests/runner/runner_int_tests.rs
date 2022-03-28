use serial_test::serial;
use std::any::Any;

use crate::helpers::ShotoverManager;
use test_helpers::shotover_process::ShotoverProcess;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_runtime_use_existing() {
    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/null-redis/topology.yaml");

    // Assert that shotover is using the test runtime
    let handle = tokio::runtime::Handle::current();
    assert_eq!(handle.type_id(), shotover_manager.runtime_handle.type_id());

    // Assert that shotover did not create a runtime for itself
    assert!(shotover_manager.runtime.is_none());
}

#[test]
#[serial]
fn test_runtime_create() {
    let shotover_manager =
        ShotoverManager::from_topology_file("example-configs/null-redis/topology.yaml");

    // Assert that shotover created a runtime for itself
    assert!(shotover_manager.runtime.is_some());
}

#[test]
#[serial]
fn test_early_shutdown_cassandra_source() {
    std::mem::drop(ShotoverManager::from_topology_file(
        "example-configs/null-cassandra/topology.yaml",
    ));
}

#[test]
#[serial]
fn test_shotover_responds_sigterm() {
    let shotover_process = ShotoverProcess::new("example-configs/null-redis/topology.yaml");
    shotover_process.signal(nix::sys::signal::Signal::SIGTERM);

    let wait_output = shotover_process.wait();
    assert_eq!(wait_output.exit_code, 0);
    if !wait_output.stdout.contains("received SIGTERM") {
        panic!(
            "stdout does not contain 'received SIGTERM'. Instead was: {}",
            wait_output.stdout
        );
    }
}

#[test]
#[serial]
fn test_shotover_responds_sigint() {
    let shotover_process = ShotoverProcess::new("example-configs/null-redis/topology.yaml");
    shotover_process.signal(nix::sys::signal::Signal::SIGINT);

    let wait_output = shotover_process.wait();
    assert_eq!(wait_output.exit_code, 0);
    if !wait_output.stdout.contains("received SIGINT") {
        panic!(
            "stdout does not contain 'received SIGINT'. Instead was: {}",
            wait_output.stdout
        );
    }
}

#[test]
#[should_panic]
#[serial]
fn test_shotover_shutdown_when_invalid_topology_non_terminating_last() {
    let _shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/invalid_non_terminating_last.yaml");
}

#[test]
#[should_panic]
#[serial]
fn test_shotover_shutdown_when_invalid_topology_terminating_not_last() {
    let _shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/invalid_terminating_not_last.yaml");
}

#[test]
#[should_panic]
#[serial]
fn test_shotover_shutdown_when_topology_invalid_topology_subchains() {
    let _shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/invalid_subchains.yaml");
}
