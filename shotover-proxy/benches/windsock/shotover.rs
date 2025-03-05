use crate::profilers::ProfilerRunner;
use test_helpers::shotover_process::ShotoverProcessBuilder;
use tokio_bin_process::{BinProcess, bin_path};
use uuid::Uuid;

pub async fn shotover_process_custom_topology(
    topology_contents: &str,
    profiler: &ProfilerRunner,
) -> BinProcess {
    let topology_path = std::env::temp_dir().join(Uuid::new_v4().to_string());
    std::fs::write(&topology_path, topology_contents).unwrap();
    ShotoverProcessBuilder::new_with_topology(topology_path.to_str().unwrap())
        .with_config("config/config.yaml")
        .with_bin(bin_path!("shotover-proxy"))
        .with_profile(profiler.shotover_profile())
        .start()
        .await
}
