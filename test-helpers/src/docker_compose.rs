use docker_compose_runner::*;
use std::{env, time::Duration};
use tracing_subscriber::fmt::TestWriter;

pub use docker_compose_runner::DockerCompose;

fn setup_tracing_subscriber_for_test_logic() {
    tracing_subscriber::fmt()
        .with_writer(TestWriter::new())
        .with_env_filter("warn")
        .try_init()
        .ok();
}

pub fn docker_compose(file_path: &str) -> DockerCompose {
    setup_tracing_subscriber_for_test_logic();
    DockerCompose::new(&IMAGE_WAITERS, |_| {}, file_path)
}

/// Creates a new DockerCompose running an instance of moto the AWS mocking server
pub fn new_moto() -> DockerCompose {
    // Overwrite any existing AWS credential env vars belonging to the user with dummy values to be sure that
    // we wont hit their real AWS account in the case of a bug in shotover or the test
    env::set_var("AWS_ACCESS_KEY_ID", "dummy-access-key");
    env::set_var("AWS_SECRET_ACCESS_KEY", "dummy-access-key-secret");

    docker_compose("tests/transforms/docker-compose-moto.yaml")
}

pub static IMAGE_WAITERS: [Image; 10] = [
    Image {
        name: "motoserver/moto",
        log_regex_to_wait_for: r"Press CTRL\+C to quit",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "library/redis:5.0.9",
        log_regex_to_wait_for: r"Ready to accept connections",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "library/redis:6.2.5",
        log_regex_to_wait_for: r"Ready to accept connections",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "bitnami/redis:6.2.13-debian-11-r73",
        log_regex_to_wait_for: r"Ready to accept connections",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "bitnami/redis-cluster:6.2.12-debian-11-r26",
        //`Cluster state changed` is created by the node services
        //`Cluster correctly created` is created by the init service
        log_regex_to_wait_for: r"Cluster state changed|Cluster correctly created",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "bitnami/cassandra:4.0.6",
        log_regex_to_wait_for: r"Startup complete",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "shotover/cassandra-test:4.0.6-r1",
        log_regex_to_wait_for: r"Startup complete",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "shotover/cassandra-test:3.11.13-r1",
        log_regex_to_wait_for: r"Startup complete",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "bitnami/kafka:3.4.0-debian-11-r22",
        log_regex_to_wait_for: r"Kafka Server started",
        timeout: Duration::from_secs(120),
    },
    Image {
        name: "opensearchproject/opensearch:2.9.0",
        log_regex_to_wait_for: r"Node started",
        timeout: Duration::from_secs(120),
    },
];
