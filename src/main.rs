#![warn(rust_2018_idioms)]
#![recursion_limit = "256"]

use anyhow::{anyhow, Result};
use clap::Clap;
use metrics_runtime::Receiver;
use tracing::{debug, info};
// use tracing_subscriber::{filter::EnvFilter, reload::Handle};

use shotover_proxy::admin::httpserver::LogFilterHttpExporter;
use shotover_proxy::config::topology::Topology;
use shotover_proxy::config::Config;
use shotover_proxy::transforms::Transforms;
use shotover_proxy::transforms::Wrapper;
use std::net::SocketAddr;
use tokio::runtime;

#[derive(Clap)]
#[clap(version = "0.0.14", author = "Instaclustr")]
struct ConfigOpts {
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

#[cfg(not(feature = "no_index"))]
#[cfg(not(feature = "no_object"))]
fn main() -> Result<()> {
    let params = ConfigOpts::parse();
    let config = Config::from_file(params.config_file.clone())?;

    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());

    let builder = tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(config.main_log_level.as_str())
        .with_filter_reloading();

    let handle = builder.reload_handle();

    builder
        .try_init()
        .map_err(|e| anyhow!("couldn't start logging - {}", e))?;

    info!(
        "Loaded the following configuration file: {}",
        params.config_file
    );
    info!(configuration = ?config);

    let receiver = Receiver::builder()
        .build()
        .expect("failed to create receiver");

    let socket: SocketAddr = config.observability_interface.parse()?;

    let exporter = LogFilterHttpExporter::new(receiver.controller(), socket, handle);

    receiver.install();

    debug!(
        "Transform overhead size on stack is {}",
        std::mem::size_of::<Transforms>()
    );

    debug!(
        "Wrapper overhead size on stack is {}",
        std::mem::size_of::<Wrapper<'_>>()
    );

    info!("Starting loaded topology");
    let mut rt = runtime::Builder::new()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .thread_stack_size(params.stack_size)
        .threaded_scheduler()
        .core_threads(params.core_threads)
        .build()
        .unwrap();

    rt.spawn(exporter.async_run());

    rt.block_on(async move {
        let topology = Topology::from_file(params.topology_file.clone())?;
        info!(
            "Loaded the following topology file: {}",
            params.topology_file.clone()
        );
        info!(topology = ?topology);

        if let Ok((_, mut shutdown_complete_rx)) = topology.run_chains().await {
            let _ = shutdown_complete_rx.recv().await;
            info!("Goodbye!");
        }
        Ok(())
    })
}
