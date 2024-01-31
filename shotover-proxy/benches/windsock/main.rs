mod cassandra;
mod cloud;
mod common;
#[cfg(feature = "rdkafka-driver-tests")]
mod kafka;
mod profilers;
mod redis;
mod shotover;

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

    benches.extend(cassandra::benches());
    #[cfg(feature = "rdkafka-driver-tests")]
    benches.extend(kafka::benches());
    benches.extend(redis::benches());

    Windsock::new(benches, cloud::AwsCloud::new_boxed(), &["release"]).run();
}
