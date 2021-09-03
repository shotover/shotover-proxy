use anyhow::Result;
use redis::{Client, Connection};
use shotover_proxy::runner::{ConfigOpts, Runner};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::runtime::{Handle as RuntimeHandle, Runtime};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub struct ShotoverManager {
    pub runtime: Option<Runtime>,
    pub runtime_handle: RuntimeHandle,
    pub join_handle: Option<JoinHandle<Result<()>>>,
    pub trigger_shutdown_tx: Arc<watch::Sender<bool>>,
    pub trigger_shutdown_rx: watch::Receiver<bool>,
}

impl ShotoverManager {
    pub fn from_topology_file(topology_path: &str) -> ShotoverManager {
        let opts = ConfigOpts {
            topology_file: topology_path.into(),
            config_file: "config/config.yaml".into(),
            ..ConfigOpts::default()
        };
        let spawn = Runner::new(opts)
            .unwrap()
            .with_observability_interface()
            .unwrap()
            .run_spawn();

        // If we allow the tracing_guard to be dropped then the following tests in the same file will not get tracing so we mem::forget it.
        // This is because tracing can only be initialized once in the same execution, secondary attempts to initalize tracing will silently fail.
        std::mem::forget(spawn.tracing_guard);

        ShotoverManager {
            runtime: spawn.runtime,
            runtime_handle: spawn.runtime_handle,
            join_handle: Some(spawn.join_handle),
            trigger_shutdown_tx: spawn.trigger_shutdown_tx,
            trigger_shutdown_rx: spawn.trigger_shutdown_rx,
        }
    }

    fn wait_for_socket_to_open(port: u16) {
        let mut tries = 0;
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            thread::sleep(Duration::from_millis(100));
            assert!(tries < 50, "Ran out of retries to connect to the socket");
            tries += 1;
        }
    }

    #[allow(unused)]
    // false unused warning caused by https://github.com/rust-lang/rust/issues/46379
    pub fn redis_connection(&self, port: u16) -> Connection {
        ShotoverManager::wait_for_socket_to_open(port);
        Client::open(("127.0.0.1", port))
            .unwrap()
            .get_connection()
            .unwrap()
    }
}

impl Drop for ShotoverManager {
    fn drop(&mut self) {
        // Must clear the recorder before skipping a shutdown on panic; if one test panics and the recorder is not cleared,
        // the following tests will panic because they will try to set another recorder
        metrics::clear_recorder();
        if std::thread::panicking() {
            // If already panicking do nothing in order to avoid a double panic.
            // We only shutdown shotover to test the shutdown process not because we need to clean up any resources.
            // So skipping shutdown on panic is fine.
        } else {
            self.trigger_shutdown_tx.send(true).unwrap();
            tokio::task::block_in_place(move || {
                self.join_handle.take().unwrap();
            })
        }
    }
}
