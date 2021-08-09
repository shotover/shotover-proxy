use anyhow::Result;
use shotover_proxy::runner::{ConfigOpts, Runner};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

pub fn run_shotover_with_topology(topology_path: &str) -> (Runtime, JoinHandle<Result<()>>) {
    let opts = ConfigOpts {
        topology_file: topology_path.into(),
        config_file: "config/config.yaml".into(),
        ..ConfigOpts::default()
    };
    let spawn = Runner::new(opts).unwrap().run_spawn();

    // If we allow the tracing_guard to be dropped then the following tests in the same file will not get tracing so we mem::forget it.
    // This is because tracing can only be initialized once in the same execution, secondary attempts to initalize tracing will silently fail.
    std::mem::forget(spawn.tracing_guard);
    (spawn.runtime, spawn.handle)
}
