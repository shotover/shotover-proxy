use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use clap::{crate_version, Clap};
use tracing::{debug, info};
use tokio::runtime::{self, Runtime};
use tokio::task::JoinHandle;
use metrics_runtime::Receiver;
use tracing_appender::non_blocking::WorkerGuard;

use crate::config::topology::Topology;
use crate::config::Config;
use crate::transforms::Transforms;
use crate::transforms::Wrapper;
use crate::admin::httpserver::LogFilterHttpExporter;

#[derive(Clap, Clone)]
#[clap(version = crate_version!(), author = "Instaclustr")]
pub struct ConfigOpts {
    #[clap(short, long, default_value = "config/topology.yaml")]
    pub topology_file: String,

    #[clap(short, long, default_value = "config/config.yaml")]
    pub config_file: String,

    #[clap(long, default_value = "4")]
    pub core_threads: usize,
    // 2,097,152 = 2 * 1024 * 1024 (2MiB)
    #[clap(long, default_value = "2097152")]
    pub stack_size: usize,
}

impl Default for ConfigOpts {
    fn default() -> Self {
        Self {
            topology_file: "config/topology.yaml".into(),
            config_file: "config/config.yaml".into(),
            core_threads: 4,
            stack_size: 2097152
        }
    }
}

pub struct Runner {
    rt: Runtime,
    topology: Topology,
    config: Config,
    /// Once this is dropped tracing logs are ignored
    tracing_guard: Option<WorkerGuard>,
}

impl Runner {
    pub fn new(params: ConfigOpts) -> Result<Self> {
        let config = Config::from_file(params.config_file.clone())?;
        let topology = Topology::from_file(params.topology_file.clone())?;

        let rt = runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("RPProxy-Thread")
            .thread_stack_size(params.stack_size)
            .worker_threads(params.core_threads)
            .build()
            .unwrap();

        let tracing_guard = None;

        Ok(Runner { rt, topology, config, tracing_guard })
    }

    pub fn with_logging(mut self) -> Result<Self> {
        let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());
        self.tracing_guard = Some(guard);

        let builder = tracing_subscriber::fmt()
            .with_writer(non_blocking)
            .with_env_filter(self.config.main_log_level.as_str())
            .with_filter_reloading();
        let handle = builder.reload_handle();
        builder
            .try_init()
            .map_err(|e| anyhow!("couldn't start logging - {}", e))?;

        let receiver = Receiver::builder().build().expect("failed to create receiver");
        let socket: SocketAddr = self.config.observability_interface.parse()?;
        let exporter = LogFilterHttpExporter::new(receiver.controller(), socket, handle);

        receiver.install();
        self.rt.spawn(exporter.async_run());

        Ok(self)
    }

    pub fn run_spawn(self) -> (Runtime, JoinHandle<Result<()>>) {
        let handle = self.rt.spawn(run(self.topology, self.config));
        (self.rt, handle)
    }

    pub fn run_block(self) -> Result<()> {
        self.rt.block_on(run(self.topology, self.config))
    }
}

pub async fn run(topology: Topology, config: Config) -> Result<()> {
    info!("Starting Shotover {}", crate_version!());
    info!(configuration = ?config);
    info!(topology = ?topology);

    debug!(
        "Transform overhead size on stack is {}",
        std::mem::size_of::<Transforms>()
    );

    debug!(
        "Wrapper overhead size on stack is {}",
        std::mem::size_of::<Wrapper<'_>>()
    );

    match topology.run_chains().await {
        Ok((_, mut shutdown_complete_rx)) => {
            let _ = shutdown_complete_rx.recv().await;
            info!("Goodbye!");
            Ok(())
        }
        Err(error) => {
            Err(anyhow!("Failed to run chains: {}", error))
        }
    }
}
