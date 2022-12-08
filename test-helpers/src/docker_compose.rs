use anyhow::{anyhow, Result};
use regex::Regex;
use serde_yaml::Value;
use std::collections::HashMap;
use std::fmt::Write;
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
    /// Will spin until it detects all the containers have started up by inspecting the logs for a magic string.
    /// This logic is implemented internally per docker image name.
    /// If a service uses an image that hasnt had this logic implemented for it yet
    /// a panic will occur instructing the developer to implement this logic.
    ///
    /// # Arguments
    /// * `file_path` - The path to the docker-compose yaml file.
    ///
    /// # Panics
    /// * Will panic if docker-compose is not installed
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
    pub fn stop_service(&self, service_name: &str) {
        run_command(
            "docker-compose",
            &["-f", &self.file_path, "stop", service_name],
        )
        .unwrap();
    }

    /// Kills the container with the provided service name
    pub fn kill_service(&self, service_name: &str) {
        run_command(
            "docker-compose",
            &["-f", &self.file_path, "kill", service_name],
        )
        .unwrap();
    }

    /// Restarts the container with the provided service name
    pub fn start_service(&self, service_name: &str) {
        run_command(
            "docker-compose",
            &["-f", &self.file_path, "start", service_name],
        )
        .unwrap();

        // TODO: call wait_for_containers_to_startup
    }

    fn wait_for_containers_to_startup(&self) {
        let images = [
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
                name: "docker.io/bitnami/redis-cluster:6.0-debian-10",
                log_regex_to_wait_for: r"Cluster state changed|Cluster correctly created",
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
        ];

        let services: Vec<Service> = self
            .get_service_to_image()
            .into_iter()
            .map(
                |(service_name, image_name)| match images.iter().find(|image| image.name == image_name) {
                    Some(image) => Service {
                        name: service_name,
                        log_to_wait_for: Regex::new(image.log_regex_to_wait_for).unwrap(),
                    },
                    None => panic!("DockerCompose does not yet know about the image {image_name}, please add it to the list above."),
                },
            )
            .collect();

        self.wait_for_logs(&services);
    }

    fn get_service_to_image(&self) -> HashMap<String, String> {
        // If we got this far then docker-compose already succesfully parsed the docker-compose.yaml,
        // so our error reporting only has to be good enough to debug issues in our implementation.
        let compose_yaml: Value =
            serde_yaml::from_str(&std::fs::read_to_string(&self.file_path).unwrap()).unwrap();
        let mut result = HashMap::new();
        match compose_yaml {
            Value::Mapping(root) => match root.get("services").unwrap() {
                Value::Mapping(services) => {
                    for (service_name, service) in services {
                        let service_name = match service_name {
                            Value::String(service_name) => service_name,
                            service_name => panic!("Unexpected service_name {service_name:?}"),
                        };
                        match service {
                            Value::Mapping(service) => {
                                let image = match service.get("image").unwrap() {
                                    Value::String(image) => image,
                                    image => panic!("Unexpected image {image:?}"),
                                };
                                result.insert(service_name.clone(), image.clone());
                            }
                            service => panic!("Unexpected service {service:?}"),
                        }
                    }
                }
                services => panic!("Unexpected services {services:?}"),
            },
            root => panic!("Unexpected root {root:?}"),
        }
        result
    }

    /// Wait until the requirements in every Service is met.
    /// Will panic if a timeout occurs.
    fn wait_for_logs(&self, services: &[Service]) {
        let timeout = Duration::from_secs(120);

        // TODO: remove this check once CI docker-compose is updated (probably ubuntu 22.04)
        let can_use_status_flag =
            run_command("docker-compose", &["-f", &self.file_path, "ps", "--help"])
                .unwrap()
                .contains("--status");

        let instant = time::Instant::now();
        loop {
            // check if every service is completely ready
            if services.iter().all(|service| {
                let log = run_command(
                    "docker-compose",
                    &["-f", &self.file_path, "logs", &service.name],
                )
                .unwrap();
                service.log_to_wait_for.is_match(&log)
            }) {
                return;
            }

            let all_logs = run_command("docker-compose", &["-f", &self.file_path, "logs"]).unwrap();

            // check if the service has failed in some way
            // this allows us to report the failure to the developer a lot sooner than just relying on the timeout
            if can_use_status_flag {
                self.assert_no_containers_in_service_with_status("exited", &all_logs);
                self.assert_no_containers_in_service_with_status("dead", &all_logs);
                self.assert_no_containers_in_service_with_status("removing", &all_logs);
            }

            // if all else fails timeout the wait
            if instant.elapsed() > timeout {
                let mut results = "".to_owned();
                for service in services {
                    let log = run_command(
                        "docker-compose",
                        &["-f", &self.file_path, "logs", &service.name],
                    )
                    .unwrap();
                    let found = if service.log_to_wait_for.is_match(&log) {
                        "Found"
                    } else {
                        "Missing"
                    };

                    writeln!(
                        results,
                        "*    Service {}, searched for '{}', was {}",
                        service.name, service.log_to_wait_for, found
                    )
                    .unwrap();
                }

                panic!("wait_for_log {timeout:?} timer expired. Results:\n{results}\nLogs:\n{all_logs}");
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

struct Image<'a> {
    name: &'a str,
    log_regex_to_wait_for: &'a str,
}

struct Service {
    name: String,
    log_to_wait_for: Regex,
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
