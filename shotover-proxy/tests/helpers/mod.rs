use anyhow::Result;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use redis::{Client, Connection};
use shotover_proxy::runner::{ConfigOpts, Runner};
use shotover_proxy::tls::{TlsConfig, TlsConnector};
use std::net::TcpStream;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;
use tokio::runtime::{Handle as RuntimeHandle, Runtime};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_io_timeout::TimeoutStream;

pub struct ShotoverManager {
    pub runtime: Option<Runtime>,
    pub runtime_handle: RuntimeHandle,
    pub join_handle: Option<JoinHandle<Result<()>>>,
    pub trigger_shutdown_tx: watch::Sender<bool>,
}

fn wait_for_socket_to_open(port: u16) {
    let mut tries = 0;
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        thread::sleep(Duration::from_millis(100));
        assert!(tries < 50, "Ran out of retries to connect to the socket");
        tries += 1;
    }
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
        }
    }

    #[allow(unused)]
    // false unused warning caused by https://github.com/rust-lang/rust/issues/46379
    pub fn redis_connection(&self, port: u16) -> Connection {
        wait_for_socket_to_open(port);
        let connection = Client::open(("127.0.0.1", port))
            .unwrap()
            .get_connection()
            .unwrap();
        connection.set_read_timeout(Some(Duration::from_secs(10)));
        connection
    }

    #[allow(unused)]
    pub async fn async_redis_connection(&self, port: u16) -> redis::aio::Connection {
        use redis::aio::AsyncStream;
        use tokio::net::TcpStream;

        wait_for_socket_to_open(port);

        let stream = Box::pin(TcpStream::connect(("127.0.0.1", port)).await.unwrap());
        let mut stream_with_timeout = TimeoutStream::new(stream);
        stream_with_timeout.set_read_timeout(Some(Duration::from_secs(10)));

        let connection_info = Default::default();
        redis::aio::Connection::new(
            &connection_info,
            Box::pin(stream_with_timeout) as Pin<Box<dyn AsyncStream + Send + Sync>>,
        )
        .await
        .unwrap()
    }

    #[allow(unused)]
    pub async fn async_tls_redis_connection(
        &self,
        port: u16,
        config: TlsConfig,
    ) -> redis::aio::Connection {
        use redis::aio::AsyncStream;
        use tokio::net::TcpStream;

        wait_for_socket_to_open(port);

        let tcp_stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let connector = TlsConnector::new(config).unwrap();
        let tls_stream = connector.connect(tcp_stream).await.unwrap();

        let connection_info = Default::default();
        redis::aio::Connection::new(
            &connection_info,
            Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>,
        )
        .await
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

            let _enter_guard = self.runtime_handle.enter();
            futures::executor::block_on(self.join_handle.take().unwrap())
                .unwrap()
                .unwrap();
        }
    }
}

pub struct ShotoverProcess {
    pub child: Child,
}

impl ShotoverProcess {
    #[allow(unused)]
    pub fn new(topology_path: &str) -> ShotoverProcess {
        let all_args = ["run", "--", "-t", topology_path];
        let child = Command::new(env!("CARGO"))
            .env("RUST_LOG", "debug,shotover_proxy=debug")
            .args(all_args)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        wait_for_socket_to_open(9001); // Wait for observability metrics port to open

        ShotoverProcess { child }
    }

    #[allow(unused)]
    fn pid(&self) -> Pid {
        Pid::from_raw(self.child.id() as i32)
    }

    #[allow(unused)]
    pub fn signal(&self, signal: Signal) {
        nix::sys::signal::kill(self.pid(), signal).unwrap();
    }

    #[allow(unused)]
    pub fn wait(self) -> (Option<i32>, String, String) {
        let output = self.child.wait_with_output().unwrap();

        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();

        (output.status.code(), stdout, stderr)
    }
}
