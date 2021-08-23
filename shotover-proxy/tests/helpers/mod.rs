use anyhow::Result;
use redis::{Client, Connection};
use shotover_proxy::runner::{ConfigOpts, Runner};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

pub struct ShotoverManager {
    pub runtime: Runtime,
    pub handle: Option<JoinHandle<Result<()>>>,
    pub trigger_shutdown_tx: broadcast::Sender<()>,
}

impl ShotoverManager {
    pub fn from_topology_file(topology_path: &str) -> ShotoverManager {
        let opts = ConfigOpts {
            topology_file: topology_path.into(),
            config_file: "config/config.yaml".into(),
            ..ConfigOpts::default()
        };
        let spawn = Runner::new(opts).unwrap().run_spawn();

        // If we allow the tracing_guard to be dropped then the following tests in the same file will not get tracing so we mem::forget it.
        // This is because tracing can only be initialized once in the same execution, secondary attempts to initalize tracing will silently fail.
        std::mem::forget(spawn.tracing_guard);

        ShotoverManager {
            runtime: spawn.runtime,
            handle: Some(spawn.handle),
            trigger_shutdown_tx: spawn.trigger_shutdown_tx,
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
        if std::thread::panicking() {
            // If already panicking do nothing in order to avoid a double panic.
            // We only shutdown shotover to test the shutdown process not because we need to clean up any resources.
            // So skipping shutdown on panic is fine.
        } else {
            self.trigger_shutdown_tx.send(()).unwrap();
            self.runtime
                .block_on(self.handle.take().unwrap())
                .unwrap()
                .unwrap();
        }
    }
}
