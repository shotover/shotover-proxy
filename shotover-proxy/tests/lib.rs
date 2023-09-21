#[cfg(test)]
#[allow(clippy::single_component_path_imports)]
use rstest_reuse;

use test_helpers::shotover_process::ShotoverProcessBuilder;
use tokio_bin_process::bin_path;

mod cassandra_int_tests;
mod kafka_int_tests;
#[cfg(feature = "alpha-transforms")]
mod opensearch_int_tests;
mod redis_int_tests;
mod runner;
mod transforms;

pub fn shotover_process(topology_path: &str) -> ShotoverProcessBuilder {
    ShotoverProcessBuilder::new_with_topology(topology_path).with_bin(bin_path!("shotover-proxy"))
}
