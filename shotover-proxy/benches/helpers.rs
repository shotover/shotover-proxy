use anyhow::Result;
use shotover_proxy::runner::{ConfigOpts, Runner};
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
