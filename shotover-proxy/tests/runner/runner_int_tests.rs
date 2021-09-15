use rusty_fork::rusty_fork_test;
use serial_test::serial;
use std::any::Any;
use tokio::runtime;

use crate::helpers::{ShotoverManager, ShotoverProcess};

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

rusty_fork_test! {
    #![rusty_fork(timeout_ms = 10000)]
    #[test]
    #[serial]
    fn test_shotover_panics_in_single_thread_runtime() {
        let runtime = runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let result = std::panic::catch_unwind(|| {
                ShotoverManager::from_topology_file("examples/null-redis/topology.yaml");
            });
            assert!(result.is_err());
        });
    }
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

#[test]
#[serial]
fn test_shotover_responds_sigterm() {
    let shotover_process = ShotoverProcess::new("examples/null-redis/topology.yaml");
    shotover_process.signal(nix::sys::signal::Signal::SIGTERM);

    let (code, stdout, _) = shotover_process.wait();
    assert_eq!(code, Some(0));
    assert!(stdout.contains("received SIGTERM"));
}

#[test]
#[serial]
fn test_shotover_responds_sigint() {
    let shotover_process = ShotoverProcess::new("examples/null-redis/topology.yaml");
    shotover_process.signal(nix::sys::signal::Signal::SIGINT);

    let (code, stdout, _) = shotover_process.wait();
    assert_eq!(code, Some(0));
    assert!(stdout.contains("received SIGINT"));
}
