use anyhow::Result;
use cassandra_cpp::{Cluster, Session, Ssl};
use redis::aio::AsyncStream;
use redis::Client;
use shotover_proxy::runner::{ConfigOpts, Runner};
use shotover_proxy::tls::{TlsConfig, TlsConnector};
use std::fs::read_to_string;
use std::pin::Pin;
use std::time::Duration;
use tokio::runtime::{Handle as RuntimeHandle, Runtime};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_io_timeout::TimeoutStream;

pub mod cassandra;
use cassandra::cassandra_connection;

#[must_use]
pub struct ShotoverManager {
    pub runtime: Option<Runtime>,
    pub runtime_handle: RuntimeHandle,
    pub join_handle: Option<JoinHandle<Result<()>>>,
    pub trigger_shutdown_tx: watch::Sender<bool>,
}

impl ShotoverManager {
    #[allow(dead_code)] // to make clippy happy
    pub fn from_topology_file(topology_path: &str) -> ShotoverManager {
        let opts = ConfigOpts {
            topology_file: topology_path.into(),
            config_file: "tests/helpers/config.yaml".into(),
            ..ConfigOpts::default()
        };
        let spawn = Runner::new(opts)
            .unwrap_or_else(|x| panic!("{x} occurred processing {topology_path:?}"))
            .with_observability_interface()
            .unwrap_or_else(|x| panic!("{x} occurred processing {topology_path:?}"))
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
            config_file: "tests/helpers/config.yaml".into(),
            ..ConfigOpts::default()
        };
        let spawn = Runner::new(opts)
            .unwrap_or_else(|x| panic!("{x} occurred processing {topology_path:?}"))
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
        test_helpers::wait_for_socket_to_open(address, port);

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
        test_helpers::wait_for_socket_to_open(address, port);

        let stream = Box::pin(
            tokio::net::TcpStream::connect((address, port))
                .await
                .unwrap(),
        );
        ShotoverManager::redis_connection_async_inner(
            Box::pin(stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
        )
        .await
    }

    #[allow(unused)]
    pub async fn redis_connection_async_tls(
        &self,
        port: u16,
        config: TlsConfig,
    ) -> redis::aio::Connection {
        let address = "127.0.0.1";
        test_helpers::wait_for_socket_to_open(address, port);

        let tcp_stream = tokio::net::TcpStream::connect((address, port))
            .await
            .unwrap();
        let connector = TlsConnector::new(config).unwrap();
        let tls_stream = connector
            .connect_unverified_hostname(tcp_stream)
            .await
            .unwrap();
        ShotoverManager::redis_connection_async_inner(
            Box::pin(tls_stream) as Pin<Box<dyn AsyncStream + Send + Sync>>
        )
        .await
    }

    async fn redis_connection_async_inner(
        stream: Pin<Box<dyn AsyncStream + Send + Sync>>,
    ) -> redis::aio::Connection {
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
    pub fn cassandra_connection(&self, contact_points: &str, port: u16) -> Session {
        cassandra_connection(contact_points, port)
    }

    #[allow(unused)]
    pub fn cassandra_connection_tls(
        &self,
        contact_points: &str,
        port: u16,
        ca_cert_path: &str,
    ) -> Session {
        let ca_cert = read_to_string(ca_cert_path).unwrap();
        let mut ssl = Ssl::default();
        Ssl::add_trusted_cert(&mut ssl, &ca_cert).unwrap();

        for contact_point in contact_points.split(',') {
            test_helpers::wait_for_socket_to_open(contact_point, port);
        }

        let mut cluster = Cluster::default();
        cluster.set_credentials("cassandra", "cassandra").unwrap();
        cluster.set_contact_points(contact_points).unwrap();
        cluster.set_port(port).ok();
        cluster.set_load_balance_round_robin();
        cluster.set_ssl(&mut ssl);
        cluster.connect().unwrap()
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
                println!("Failed to shutdown shotover: {err}")
            }
        } else {
            self.shutdown_shotover().unwrap();
        }
    }
}
