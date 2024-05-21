#[allow(clippy::single_component_path_imports, unused_imports)]
use rstest_reuse;

use test_helpers::shotover_process::ShotoverProcessBuilder;
use tokio_bin_process::bin_path;

#[cfg(feature = "cassandra")]
mod cassandra_int_tests;
#[cfg(feature = "kafka")]
mod kafka_int_tests;
#[cfg(all(feature = "alpha-transforms", feature = "opensearch"))]
mod opensearch_int_tests;
#[cfg(feature = "redis")]
mod redis_int_tests;
#[cfg(feature = "redis")]
mod runner;
#[cfg(feature = "redis")]
mod transforms;

pub fn shotover_process(topology_path: &str) -> ShotoverProcessBuilder {
    ShotoverProcessBuilder::new_with_topology(topology_path)
        .with_bin(bin_path!("shotover-proxy"))
        .with_config("tests/test-configs/shotover-config/config1.yaml")
}

#[cfg(target_os = "macos")]
#[cfg(feature = "redis")]
const CONNECTION_REFUSED_OS_ERROR: i32 = 61;

#[cfg(not(target_os = "macos"))]
#[cfg(feature = "redis")]
const CONNECTION_REFUSED_OS_ERROR: i32 = 111;
