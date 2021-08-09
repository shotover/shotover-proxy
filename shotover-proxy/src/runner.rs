use std::env;
use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use clap::{crate_version, Clap};
use metrics_runtime::Receiver;
use tokio::runtime::{self, Runtime};
use tokio::task::JoinHandle;
use tracing::{debug, info};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::fmt::format::{DefaultFields, Format};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::Layered;
use tracing_subscriber::reload::Handle;
use tracing_subscriber::{EnvFilter, Registry};

use crate::admin::httpserver::LogFilterHttpExporter;
use crate::config::topology::Topology;
use crate::config::Config;
use crate::transforms::Transforms;
use crate::transforms::Wrapper;

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
            stack_size: 2097152,
        }
    }
}

pub struct Runner {
    runtime: Runtime,
    topology: Topology,
    config: Config,
    tracing: TracingState,
}

impl Runner {
    pub fn new(params: ConfigOpts) -> Result<Self> {
        let config = Config::from_file(params.config_file.clone())?;
        let topology = Topology::from_file(params.topology_file.clone())?;

        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("RPProxy-Thread")
            .thread_stack_size(params.stack_size)
            .worker_threads(params.core_threads)
            .build()
            .unwrap();

        let tracing = TracingState::new(config.main_log_level.as_str());

        Ok(Runner {
            runtime,
            topology,
            config,
            tracing,
        })
    }

    pub fn with_observability_interface(self) -> Result<Self> {
        let receiver = Receiver::builder()
            .build()
            .expect("failed to create receiver");
        let socket: SocketAddr = self.config.observability_interface.parse()?;
        let exporter =
            LogFilterHttpExporter::new(receiver.controller(), socket, self.tracing.handle.clone());

        receiver.install();
        self.runtime.spawn(exporter.async_run());

        Ok(self)
    }

    pub fn run_spawn(self) -> RunnerSpawned {
        let handle = self.runtime.spawn(run(self.topology, self.config));

        RunnerSpawned {
            runtime: self.runtime,
            tracing_guard: self.tracing.guard,
            handle,
        }
    }

    pub fn run_block(self) -> Result<()> {
        self.runtime.block_on(run(self.topology, self.config))
    }
}

struct TracingState {
    /// Once this is dropped tracing logs are ignored
    guard: WorkerGuard,
    handle:
        Handle<EnvFilter, Layered<Layer<Registry, DefaultFields, Format, NonBlocking>, Registry>>,
}

impl TracingState {
    fn new(log_level: &str) -> Self {
        let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());

        let builder = tracing_subscriber::fmt()
            .with_writer(non_blocking)
            .with_env_filter({
                // Override directives using RUST_LOG environment variable. Workaround for tokio-rs/tracing#512.
                let overrides = env::var(EnvFilter::DEFAULT_ENV).ok();
                let directives = [Some(log_level), overrides.as_deref()]
                    .iter()
                    .flat_map(Option::as_deref)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
                    .join(",");
                EnvFilter::new(directives)
            })
            .with_filter_reloading();
        let handle = builder.reload_handle();

        // To avoid unit tests that run in the same excutable from blowing up when they try to reinitialize tracing we ignore the result returned by try_init.
        // Currently the implementation of try_init will only fail when it is called multiple times.
        builder.try_init().ok();

        TracingState { guard, handle }
    }
}

pub struct RunnerSpawned {
    pub runtime: Runtime,
    pub handle: JoinHandle<Result<()>>,
    pub tracing_guard: WorkerGuard,
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
        Err(error) => Err(anyhow!("Failed to run chains: {}", error)),
    }
}
