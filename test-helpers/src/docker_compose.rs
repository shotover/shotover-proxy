use anyhow::{anyhow, Result};
use regex::Regex;
use std::io::ErrorKind;
use std::process::Command;
use std::thread;
use std::time;
use subprocess::{Exec, Redirection};
use tracing::{debug, info};

/// Runs a command and returns the output as a string.
///
/// Both stderr and stdout are returned in the result.
///
/// # Arguments
/// * `command` - The system command to run
/// * `args` - An array of command line arguments for the command
///
fn run_command(command: &str, args: &[&str]) -> Result<String> {
    debug!("executing {}", command);
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
    /// # Notes:
    /// * Does not sleep - Calling processes should sleep or use `wait_for()` to delay until the
    /// containers are ready.
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

        DockerCompose::clean_up(file_path).unwrap();

        run_command("docker-compose", &["-f", file_path, "up", "-d"]).unwrap();

        DockerCompose {
            file_path: file_path.to_string(),
        }
    }

    /// Waits for a string to appear in the docker-compose log output.
    ///
    /// This is shorthand for `wait_for_n( log_text, 1 )`
    ///
    /// # Arguments
    /// * `log_text` - A regular expression defining the text to find in the docker-container log
    /// output.
    ///
    /// # Panics
    /// * If `log_text` is not found within 60 seconds.
    ///
    pub fn wait_for(self, log_text: &str) -> Self {
        self.wait_for_n(log_text, 1)
    }

    /// Waits for a string to appear in the docker-compose log output `count` times.
    ///
    /// Counts the number of items returned by `regex.find_iter`.
    ///
    /// # Arguments
    /// * `log_text` - A regular expression defining the text to find in the docker-container log
    /// output.
    /// * `count` - The number of times the regular expression should be found.
    ///
    /// # Panics
    /// * If `count` occurrences of `log_text` is not found in the log within 30 seconds.
    ///
    pub fn wait_for_n(self, log_text: &str, count: usize) -> Self {
        self.wait_for_n_t(log_text, count, 30)
    }

    /// Waits for a string to appear in the docker-compose log output `count` times within `time` seconds.
    ///
    /// Counts the number of items returned by `regex.find_iter`.
    ///
    /// # Arguments
    /// * `log_text` - A regular expression defining the text to find in the docker-container log
    /// output.
    /// * `count` - The number of times the regular expression should be found.
    /// * `time` - The number of seconds to wait for the count to be found.
    ///
    /// # Panics
    /// * If `count` occurrences of `log_text` is not found in the log within `time` seconds.
    ///
    pub fn wait_for_n_t(self, log_text: &str, count: usize, time: u64) -> Self {
        info!("wait_for_n_t: '{}' {} {}", log_text, count, time);
        let args = ["-f", &self.file_path, "logs"];
        let re = Regex::new(log_text).unwrap();
        let sys_time = time::Instant::now();
        let mut result = run_command("docker-compose", &args).unwrap();
        let mut my_count = re.find_iter(&result).count();
        while my_count < count {
            if sys_time.elapsed().as_secs() > time {
                panic!(
                    "wait_for: {} second timer expired. Found {}  instances of '{}' in the log",
                    time,
                    re.find_iter(&result).count(),
                    log_text
                );
            }
            debug!("wait_for_n: {:?} looping {}/{}", log_text, my_count, count);
            result = run_command("docker-compose", &args).unwrap();
            my_count = re.find_iter(&result).count();
        }
        debug!(
            "wait_for_n_t: found '{}' {} times in {:?} seconds",
            log_text,
            count,
            sys_time.elapsed()
        );
        self
    }

    /// Cleans up the docker-compose by shutting down the running system and removing the images.
    ///
    /// # Arguments
    /// * `file_path` - The path to the docker-compose yaml file that was used to start docker.
    fn clean_up(file_path: &str) -> Result<()> {
        debug!("bringing down docker compose {}", file_path);

        run_command("docker-compose", &["-f", file_path, "down", "-v"])?;
        run_command("docker-compose", &["-f", file_path, "rm", "-f", "-s", "-v"])?;

        thread::sleep(time::Duration::from_secs(1));

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
                    "ERROR: docker compose failed to bring down while already panicking: {:?}",
                    err
                );
            }
        } else {
            DockerCompose::clean_up(&self.file_path).unwrap();
        }
    }
}
