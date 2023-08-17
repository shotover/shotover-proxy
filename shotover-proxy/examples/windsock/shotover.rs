use crate::profilers::ProfilerRunner;
use test_helpers::shotover_process::ShotoverProcessBuilder;
use tokio_bin_process::{bin_path, BinProcess};

pub async fn shotover_process(topology_path: &str, profiler: &ProfilerRunner) -> BinProcess {
    ShotoverProcessBuilder::new_with_topology(topology_path)
        .with_bin(bin_path!("shotover-proxy"))
        .with_profile(profiler.shotover_profile())
        .start()
        .await
}
