use anyhow::{anyhow, Result};
use docker_api::Docker;
use std::io::ErrorKind;
use std::process::Command;
use std::time::{self, Duration};
use std::{env, path::Path};
use subprocess::{Exec, Redirection};
use tracing::trace;

/// Runs a command and returns the output as a string.
///
/// Both stderr and stdout are returned in the result.
///
/// # Arguments
/// * `command` - The system command to run
/// * `args` - An array of command line arguments for the command
///
pub fn run_command(command: &str, args: &[&str]) -> Result<String> {
    trace!("executing {}", command);
    let data = Exec::cmd(command)
        .args(args)
        .stdout(Redirection::Pipe)
        .stderr(Redirection::Merge)
        .capture()?;

    if data.exit_status.success() {
        Ok(data.stdout_str())
    } else {
        Err(anyhow!(
            "command {} {:?} exited with {:?} and output:\n{}",
            command,
            args,
            data.exit_status,
            data.stdout_str()
        ))
    }
}

#[must_use]
pub struct DockerCompose {
    file_path: String,
}

impl DockerCompose {
    /// Creates a new DockerCompose object by submitting a file to the underlying docker-compose
    /// system.  Executes `docker-compose -f [file_path] up -d`
    ///
    /// Will spin until it detects all the containers have started up.
    /// This logic is implemented internally per docker-compose file.
    /// If a docker-compose file is used that hasnt had this logic implemented for it yet
    /// a panic will occur instructing the developer to implement this logic.
    ///
    /// # Arguments
    /// * `file_path` - The path to the docker-compose yaml file.
    ///
    /// # Panics
    /// * Will panic if docker-compose is not installed
    ///
    pub fn new(file_path: &str) -> Self {
        if let Err(ErrorKind::NotFound) = Command::new("docker-compose")
            .output()
            .map_err(|e| e.kind())
        {
            panic!("Could not find docker-compose. Have you installed it?");
        }

        DockerCompose::build_images();

        DockerCompose::clean_up(file_path).unwrap();

        run_command("docker-compose", &["-f", file_path, "up", "-d"]).unwrap();

        let compose = DockerCompose {
            file_path: file_path.to_string(),
        };

        compose.wait_for_containers_to_startup();

        compose
    }

    /// Creates a new DockerCompose running an instance of moto the AWS mocking server
    pub fn new_moto() -> Self {
        // Overwrite any existing AWS credential env vars belonging to the user with dummy values to be sure that
        // we wont hit their real AWS account in the case of a bug in shotover or the test
        env::set_var("AWS_ACCESS_KEY_ID", "dummy-access-key");
        env::set_var("AWS_SECRET_ACCESS_KEY", "dummy-access-key-secret");

        DockerCompose::new("tests/transforms/docker-compose-moto.yaml")
    }

    /// Stops the container with the provided service name
    pub async fn stop_service(&self, service_name: &str) {
        let docker = Docker::new("unix:///var/run/docker.sock").unwrap();
        let containers = docker.containers();
        let mut found = false;
        let mut all_names: Vec<String> = vec![];
        for container in containers.list(&Default::default()).await.unwrap() {
            let compose_service = container
                .labels
                .unwrap()
                .get("com.docker.compose.service")
                .unwrap()
                .to_string();
            if compose_service == service_name {
                found = true;
                let container = containers.get(container.id.unwrap());
                container.stop(None).await.unwrap();
            }
            all_names.push(compose_service);
        }
        assert!(
            found,
            "container was not found with expected docker compose service name, actual names were {:?}",
            all_names
        );
    }

    fn wait_for_containers_to_startup(&self) {
        match self.file_path.as_ref() {
            "tests/transforms/docker-compose-moto.yaml" => {
                self.wait_for_log("Press CTRL+C to quit", 1)
            }
            "example-configs/redis-passthrough/docker-compose.yaml"
            | "example-configs/redis-tls/docker-compose.yaml" => {
                self.wait_for_log("Ready to accept connections", 1)
            }
            "example-configs/redis-multi/docker-compose.yaml" => {
                self.wait_for_log("Ready to accept connections", 3)
            }
            "tests/test-configs/redis-cluster-ports-rewrite/docker-compose.yaml"
            | "tests/test-configs/redis-cluster-auth/docker-compose.yaml"
            | "example-configs/redis-cluster-handling/docker-compose.yaml"
            | "example-configs/redis-cluster-hiding/docker-compose.yaml"
            | "example-configs/redis-cluster-tls/docker-compose.yaml"
            | "example-configs/redis-cluster-tls/docker-compose-with-key.yaml" => {
                self.wait_for_log("Cluster state changed", 6)
            }
            "example-configs/redis-cluster-dr/docker-compose.yaml" => {
                self.wait_for_log("Cluster state changed", 12)
            }
            "example-configs/cassandra-passthrough/docker-compose.yaml"
            | "example-configs/cassandra-tls/docker-compose.yaml"
            | "example-configs/cassandra-redis-cache/docker-compose.yaml"
            | "example-configs/cassandra-protect-local/docker-compose.yaml"
            | "example-configs/cassandra-protect-aws/docker-compose.yaml"
            | "example-configs/cassandra-request-throttling/docker-compose.yaml"
            | "tests/test-configs/cassandra-passthrough-parse-request/docker-compose.yaml"
            | "tests/test-configs/cassandra-passthrough-parse-response/docker-compose.yaml" => {
                self.wait_for_log("Startup complete", 1)
            }
            "tests/test-configs/cassandra-peers-rewrite/docker-compose-4.0-cassandra.yaml"
            | "tests/test-configs/cassandra-peers-rewrite/docker-compose-3.11-cassandra.yaml" => {
                self.wait_for_log("Startup complete", 2)
            }
            "example-configs-docker/cassandra-peers-rewrite/docker-compose.yaml"
            | "example-configs/cassandra-cluster/docker-compose-cassandra-v4.yaml"
            | "example-configs/cassandra-cluster/docker-compose-cassandra-v3.yaml"
            | "example-configs/cassandra-cluster-multi-rack/docker-compose.yaml"
            | "example-configs/cassandra-cluster-tls/docker-compose.yaml" => {
                self.wait_for_log("Startup complete", 3)
            }
            path => unimplemented!(
                "Unknown compose file `{path}` Please implement waiting logic for it here.",
            ),
        }
    }

    /// Waits for a string to appear in the docker-compose log output `count` times within `time` seconds.
    ///
    /// # Arguments
    /// * `log_to_wait_for` - A regular expression defining the text to find in the docker-container log
    /// output.
    /// * `count` - The number of times the regular expression should be found.
    ///
    /// # Panics
    /// * If `count` occurrences of `log_text` is not found in the log within 120 seconds.
    fn wait_for_log(&self, log_to_wait_for: &str, count: usize) {
        let timeout = Duration::from_secs(120);

        // TODO: remove this check once CI docker-compose is updated (probably ubuntu 22.04)
        let can_use_status_flag =
            run_command("docker-compose", &["-f", &self.file_path, "ps", "--help"])
                .unwrap()
                .contains("--status");

        let instant = time::Instant::now();
        loop {
            // check if the service is completely ready
            let full_log = run_command("docker-compose", &["-f", &self.file_path, "logs"]).unwrap();
            let current_count = full_log.matches(log_to_wait_for).count();
            if current_count >= count {
                return;
            }

            // check if the service has failed in some way
            // this allows us to report the failure to the developer a lot sooner than just relying on the timeout
            if can_use_status_flag {
                self.assert_no_containers_in_service_with_status("exited", &full_log);
                self.assert_no_containers_in_service_with_status("dead", &full_log);
                self.assert_no_containers_in_service_with_status("removing", &full_log);
            }

            // if all else fails timeout the wait
            if instant.elapsed() > timeout {
                panic!("wait_for_log {timeout:?} timer expired. Found {current_count}/{count} instances of '{log_to_wait_for}' in the log\n{full_log}");
            }
        }
    }

    fn assert_no_containers_in_service_with_status(&self, status: &str, full_log: &str) {
        let containers = run_command(
            "docker-compose",
            &["-f", &self.file_path, "ps", "--status", status],
        )
        .unwrap();
        // One line for the table heading. If there are more lines then there is some data indicating that containers exist with this status
        if containers.matches('\n').count() > 1 {
            panic!(
                "At least one container failed to initialize\n{containers}\nFull log\n{full_log}"
            );
        }
    }

    fn build_images() {
        // On my machine this only takes 40ms when the image is unchanged.
        // So recreating it for every test is fine, but if we start adding more images maybe we should introduce an atomic flag so we only run it once
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
        if Path::new("example-configs/docker-images/cassandra-tls-4.0.6/certs/keystore.p12")
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

    /// Cleans up the docker-compose by shutting down the running system and removing the images.
    ///
    /// # Arguments
    /// * `file_path` - The path to the docker-compose yaml file that was used to start docker.
    fn clean_up(file_path: &str) -> Result<()> {
        trace!("bringing down docker compose {}", file_path);

        run_command("docker-compose", &["-f", file_path, "kill"])?;
        run_command("docker-compose", &["-f", file_path, "down", "-v"])?;

        Ok(())
    }
}

impl Drop for DockerCompose {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Err(err) = DockerCompose::clean_up(&self.file_path) {
                // We need to use println! here instead of error! because error! does not
                // get output when panicking
                println!(
                    "ERROR: docker compose failed to bring down while already panicking: {err:?}",
                );
            }
        } else {
            DockerCompose::clean_up(&self.file_path).unwrap();
        }
    }
}
