use crate::docker_compose_runner::*;
use std::{env, path::Path};
use tracing_subscriber::fmt::TestWriter;

fn setup_tracing_subscriber_for_test_logic() {
    tracing_subscriber::fmt()
        .with_writer(TestWriter::new())
        .with_env_filter("warn")
        .try_init()
        .ok();
}

pub fn docker_compose(file_path: &str) -> DockerCompose {
    setup_tracing_subscriber_for_test_logic();
    DockerCompose::new(get_image_waiters(), build_images, file_path)
}

/// Creates a new DockerCompose running an instance of moto the AWS mocking server
pub fn new_moto() -> DockerCompose {
    // Overwrite any existing AWS credential env vars belonging to the user with dummy values to be sure that
    // we wont hit their real AWS account in the case of a bug in shotover or the test
    env::set_var("AWS_ACCESS_KEY_ID", "dummy-access-key");
    env::set_var("AWS_SECRET_ACCESS_KEY", "dummy-access-key-secret");

    docker_compose("tests/transforms/docker-compose-moto.yaml")
}

fn get_image_waiters() -> &'static [Image] {
    &[
        Image {
            name: "shotover/shotover-proxy",
            log_regex_to_wait_for: r"accepting inbound connections",
        },
        Image {
            name: "motoserver/moto",
            log_regex_to_wait_for: r"Press CTRL\+C to quit",
        },
        Image {
            name: "library/redis:5.0.9",
            log_regex_to_wait_for: r"Ready to accept connections",
        },
        Image {
            name: "library/redis:6.2.5",
            log_regex_to_wait_for: r"Ready to accept connections",
        },
        Image {
            name: "bitnami/redis-cluster:6.0-debian-10",
            //`Cluster state changed` is created by the node services
            //`Cluster correctly created` is created by the init service
            log_regex_to_wait_for: r"Cluster state changed|Cluster correctly created",
        },
        Image {
            name: "bitnami/cassandra:4.0.6",
            log_regex_to_wait_for: r"Startup complete",
        },
        Image {
            name: "shotover-int-tests/cassandra:4.0.6",
            log_regex_to_wait_for: r"Startup complete",
        },
        Image {
            name: "shotover-int-tests/cassandra-tls:4.0.6",
            log_regex_to_wait_for: r"Startup complete",
        },
        Image {
            name: "shotover-int-tests/cassandra:3.11.13",
            log_regex_to_wait_for: r"Startup complete",
        },
        Image {
            name: "bitnami/kafka:3.3.2",
            log_regex_to_wait_for: r"Kafka Server started",
        },
    ]
}

fn build_images(service_to_image: &[&str]) {
    if service_to_image
        .iter()
        .any(|x| *x == "shotover-int-tests/cassandra:4.0.6")
    {
        run_command(
            "docker",
            &[
                "build",
                "example-configs/docker-images/cassandra-4.0.6",
                "--tag",
                "shotover-int-tests/cassandra:4.0.6",
            ],
        )
        .unwrap();
    }
    if service_to_image
        .iter()
        .any(|x| *x == "shotover-int-tests/cassandra:3.11.13")
    {
        run_command(
            "docker",
            &[
                "build",
                "example-configs/docker-images/cassandra-3.11.13",
                "--tag",
                "shotover-int-tests/cassandra:3.11.13",
            ],
        )
        .unwrap();
    }
    if service_to_image
        .iter()
        .any(|x| *x == "shotover-int-tests/cassandra-tls:4.0.6")
        && Path::new("example-configs/docker-images/cassandra-tls-4.0.6/certs/keystore.p12")
            .exists()
    {
        run_command(
            "docker",
            &[
                "build",
                "example-configs/docker-images/cassandra-tls-4.0.6",
                "--tag",
                "shotover-int-tests/cassandra-tls:4.0.6",
            ],
        )
        .unwrap();
    }
}
