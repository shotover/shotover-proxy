use anyhow::Result;
use shotover_proxy::runner::{ConfigOpts, Runner};
use tokio::task::JoinHandle;
use tokio::runtime::Runtime;
use tracing::Level;

pub fn run_shotover_with_topology(topology_path: &str) -> (Runtime, JoinHandle<Result<()>>) {
    tracing_subscriber::fmt().with_max_level(Level::INFO).try_init().ok();

    let opts = ConfigOpts {
        topology_file: topology_path.into(),
        config_file: "config/config.yaml".into(),
        .. ConfigOpts::default()
    };
    Runner::new(opts).unwrap().run_spawn()
}
