use anyhow::Result;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use redis::aio::AsyncStream;
use redis::Client;
use shotover_proxy::runner::{ConfigOpts, Runner};
use shotover_proxy::tls::{TlsConfig, TlsConnector};
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;
use tokio::runtime::{Handle as RuntimeHandle, Runtime};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_io_timeout::TimeoutStream;

#[must_use]
pub struct ShotoverManager {
    pub runtime: Option<Runtime>,
    pub runtime_handle: RuntimeHandle,
    pub join_handle: Option<JoinHandle<Result<()>>>,
    pub trigger_shutdown_tx: watch::Sender<bool>,
}

pub fn wait_for_socket_to_open(address: &str, port: u16) {
    let mut tries = 0;
    while std::net::TcpStream::connect((address, port)).is_err() {
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
            .unwrap_or_else(|x| panic!("{} occurred processing {:?}", x, topology_path))
            .with_observability_interface()
            .unwrap_or_else(|x| panic!("{} occurred processing {:?}", x, topology_path))
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

    #[allow(dead_code)] // to make clippy happy
    pub fn from_topology_file_without_observability(topology_path: &str) -> ShotoverManager {
        let opts = ConfigOpts {
            topology_file: topology_path.into(),
            config_file: "config/config.yaml".into(),
            ..ConfigOpts::default()
        };
        let spawn = Runner::new(opts)
            .unwrap_or_else(|x| panic!("{} occurred processing {:?}", x, topology_path))
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

    // false unused warning caused by https://github.com/rust-lang/rust/issues/46379
    #[allow(unused)]
    pub fn redis_connection(&self, port: u16) -> redis::Connection {
        let address = "127.0.0.1";
        wait_for_socket_to_open(address, port);

        let connection = Client::open((address, port))
            .unwrap()
            .get_connection()
            .unwrap();
        connection.set_read_timeout(Some(Duration::from_secs(10)));
        connection
    }

    #[allow(unused)]
    pub async fn redis_connection_async(&self, port: u16) -> redis::aio::Connection {
        let address = "127.0.0.1";
        wait_for_socket_to_open(address, port);

        let stream = Box::pin(
            tokio::net::TcpStream::connect((address, port))
                .await
                .unwrap(),
        );
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
    pub async fn redis_connection_async_tls(
        &self,
        port: u16,
        config: TlsConfig,
    ) -> redis::aio::Connection {
        let address = "127.0.0.1";
        wait_for_socket_to_open(address, port);

        let tcp_stream = tokio::net::TcpStream::connect((address, port))
            .await
            .unwrap();
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

    fn shutdown_shotover(&mut self) -> Result<()> {
        self.trigger_shutdown_tx.send(true)?;
        let _enter_guard = self.runtime_handle.enter();
        futures::executor::block_on(self.join_handle.take().unwrap())?
    }
}

impl Drop for ShotoverManager {
    fn drop(&mut self) {
        // Must clear the recorder before skipping a shutdown on panic; if one test panics and the recorder is not cleared,
        // the following tests will panic because they will try to set another recorder
        metrics::clear_recorder();

        if std::thread::panicking() {
            // If already panicking do not panic while attempting to shutdown shotover in order to avoid a double panic.
            if let Err(err) = self.shutdown_shotover() {
                println!("Failed to shutdown shotover: {}", err)
            }
        } else {
            self.shutdown_shotover().unwrap();
        }
    }
}

pub struct ShotoverProcess {
    /// Always Some while ShotoverProcess is owned
    pub child: Option<Child>,
}

impl Drop for ShotoverProcess {
    fn drop(&mut self) {
        if let Some(child) = &self.child {
            if let Err(err) =
                nix::sys::signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGKILL)
            {
                println!("Failed to shutdown ShotoverProcess {}", err);
            }
        }
    }
}

impl ShotoverProcess {
    #[allow(unused)]
    pub fn new(topology_path: &str) -> ShotoverProcess {
        // Set in build.rs from PROFILE listed in https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
        let all_args = if env!("PROFILE") == "release" {
            vec!["run", "--release", "--", "-t", topology_path]
        } else {
            vec!["run", "--", "-t", topology_path]
        };
        let child = Some(
            Command::new(env!("CARGO"))
                .env("RUST_LOG", "debug,shotover_proxy=debug")
                .args(&all_args)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );

        wait_for_socket_to_open("127.0.0.1", 9001); // Wait for observability metrics port to open

        ShotoverProcess { child }
    }

    #[allow(unused)]
    fn pid(&self) -> Pid {
        Pid::from_raw(self.child.as_ref().unwrap().id() as i32)
    }

    #[allow(unused)]
    pub fn signal(&self, signal: Signal) {
        nix::sys::signal::kill(self.pid(), signal).unwrap();
    }

    #[allow(unused)]
    pub fn wait(mut self) -> (Option<i32>, String, String) {
        let output = self.child.take().unwrap().wait_with_output().unwrap();

        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();

        (output.status.code(), stdout, stderr)
    }
}
