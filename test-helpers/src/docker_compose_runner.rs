use anyhow::{anyhow, Result};
use regex::Regex;
use serde_yaml::Value;
use std::collections::HashMap;
use std::fmt::Write;
use std::io::ErrorKind;
use std::process::Command;
use std::time::{self, Duration};
use subprocess::{Exec, Redirection};
use tracing::trace;

/// Runs a command and returns the output as a string.
///
/// Both stderr and stdout are returned in the result.
///
/// # Arguments
/// * `command` - The system command to run
/// * `args` - An array of command line arguments for the command
pub(crate) fn run_command(command: &str, args: &[&str]) -> Result<String> {
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
    /// Runs docker-compose on the provided docker-compose.yaml file.
    /// Dropping the returned object will stop and destroy the launched docker-compose services.
    ///
    /// image_waiters gives DockerCompose a way to know when a container has finished starting up.
    /// Each entry defines an image name and a regex such that if the regex matches on a log line output by a container running that image the container is considered started up.
    ///
    /// image_builder is a callback allowing the user to build a docker image if the docker-compose.yaml depends on it.
    /// The argument is an iterator over all the image names docker-compose is going to use.
    pub fn new(
        image_waiters: &'static [Image],
        image_builder: impl FnOnce(&[&str]),
        yaml_path: &str,
    ) -> Self {
        if let Err(ErrorKind::NotFound) = Command::new("docker-compose")
            .output()
            .map_err(|e| e.kind())
        {
            panic!("Could not find docker-compose. Have you installed it?");
        }

        // It is critical that clean_up is run before everything else as the internal `docker-compose` commands act as validation
        // for the docker-compose.yaml file that we later manually parse with poor error handling
        DockerCompose::clean_up(yaml_path).unwrap();

        let service_to_image = DockerCompose::get_service_to_image(yaml_path);

        let images: Vec<&str> = service_to_image.values().map(|x| x.as_ref()).collect();
        image_builder(&images);

        run_command("docker-compose", &["-f", yaml_path, "up", "-d"]).unwrap();

        DockerCompose::wait_for_containers_to_startup(image_waiters, service_to_image, yaml_path);

        DockerCompose {
            file_path: yaml_path.to_string(),
        }
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

    fn wait_for_containers_to_startup(
        image_waiters: &[Image],
        service_to_image: HashMap<String, String>,
        file_path: &str,
    ) {
        let services: Vec<Service> =
            service_to_image
            .into_iter()
            .map(
                |(service_name, image_name)| match image_waiters.iter().find(|image| image.name == image_name) {
                    Some(image) => Service {
                        name: service_name,
                        log_to_wait_for: Regex::new(image.log_regex_to_wait_for).unwrap(),
                    },
                    None => panic!("The image_waiters list given to DockerCompose::new does not include the image {image_name}, please add it to the list."),
                },
            )
            .collect();

        DockerCompose::wait_for_logs(file_path, &services);
    }

    fn get_service_to_image(file_path: &str) -> HashMap<String, String> {
        let compose_yaml: Value =
            serde_yaml::from_str(&std::fs::read_to_string(file_path).unwrap()).unwrap();
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
    fn wait_for_logs(file_path: &str, services: &[Service]) {
        let timeout = Duration::from_secs(120);

        // TODO: remove this check once CI docker-compose is updated (probably ubuntu 22.04)
        let can_use_status_flag = run_command("docker-compose", &["-f", file_path, "ps", "--help"])
            .unwrap()
            .contains("--status");

        let instant = time::Instant::now();
        loop {
            // check if every service is completely ready
            if services.iter().all(|service| {
                let log = run_command("docker-compose", &["-f", file_path, "logs", &service.name])
                    .unwrap();
                service.log_to_wait_for.is_match(&log)
            }) {
                return;
            }

            let all_logs = run_command("docker-compose", &["-f", file_path, "logs"]).unwrap();

            // check if the service has failed in some way
            // this allows us to report the failure to the developer a lot sooner than just relying on the timeout
            if can_use_status_flag {
                DockerCompose::assert_no_containers_in_service_with_status(
                    file_path, "exited", &all_logs,
                );
                DockerCompose::assert_no_containers_in_service_with_status(
                    file_path, "dead", &all_logs,
                );
                DockerCompose::assert_no_containers_in_service_with_status(
                    file_path, "removing", &all_logs,
                );
            }

            // if all else fails timeout the wait
            if instant.elapsed() > timeout {
                let mut results = "".to_owned();
                for service in services {
                    let log =
                        run_command("docker-compose", &["-f", file_path, "logs", &service.name])
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

    fn assert_no_containers_in_service_with_status(file_path: &str, status: &str, full_log: &str) {
        let containers = run_command(
            "docker-compose",
            &["-f", file_path, "ps", "--status", status],
        )
        .unwrap();
        // One line for the table heading. If there are more lines then there is some data indicating that containers exist with this status
        if containers.matches('\n').count() > 1 {
            panic!(
                "At least one container failed to initialize\n{containers}\nFull log\n{full_log}"
            );
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

pub struct Image {
    pub name: &'static str,
    pub log_regex_to_wait_for: &'static str,
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
