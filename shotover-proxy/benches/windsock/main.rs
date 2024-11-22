// Allow dead code if any of the protocol features are disabled
#![cfg_attr(
    any(
        not(feature = "cassandra"),
        not(feature = "redis"),
        not(all(feature = "kafka-cpp-driver-tests", feature = "kafka"))
    ),
    allow(dead_code, unused_imports, unused_variables, unused_mut)
)]

#[cfg(feature = "cassandra")]
mod cassandra;
mod cloud;
mod common;
#[cfg(all(feature = "kafka-cpp-driver-tests", feature = "kafka"))]
mod kafka;
mod profilers;
mod shotover;
#[cfg(feature = "redis")]
mod valkey;

use cloud::CloudResources;
use cloud::CloudResourcesRequired;
use std::path::Path;
use tracing_subscriber::EnvFilter;
use windsock::{Bench, Windsock};

pub type ShotoverBench = Box<
    dyn Bench<CloudResourcesRequired = CloudResourcesRequired, CloudResources = CloudResources>,
>;

fn main() {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(non_blocking)
        .init();

    // The benches and tests automatically set the working directory to CARGO_MANIFEST_DIR.
    // We need to do the same as the DockerCompose + ShotoverProcess types rely on this.
    if Path::new(env!("CARGO_MANIFEST_DIR")).exists() {
        std::env::set_current_dir(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("shotover-proxy"),
        )
        .unwrap();
    }

    let mut benches = vec![];

    #[cfg(feature = "cassandra")]
    benches.extend(cassandra::benches());
    #[cfg(all(feature = "kafka-cpp-driver-tests", feature = "kafka"))]
    benches.extend(kafka::benches());
    #[cfg(feature = "redis")]
    benches.extend(valkey::benches());

    Windsock::new(benches, cloud::AwsCloud::new_boxed(), &["release"]).run();
}
