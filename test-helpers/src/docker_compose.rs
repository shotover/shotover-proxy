use anyhow::{anyhow, Result};
use std::thread;
use std::time;
use subprocess::{Exec, Redirection};
use tracing::info;
use tracing::field::debug;

fn run_command(command: &str, args: &[&str]) -> Result<&str> {
    let data = Exec::cmd(command)
        .args(args)
        .stdout(Redirection::Pipe)
        .stderr(Redirection::Merge)
        .capture()?;

    if data.exit_status.success() {
        Ok(data.stdout_str().as_str())
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

pub struct DockerCompose {
    file_path: String,
}

impl DockerCompose {
    pub fn new(file_path: &str) -> Self {
        DockerCompose::clean_up(file_path).unwrap();

        info!("bringing up docker compose {}", file_path);

        run_command("docker-compose", &["-f", file_path, "up", "-d"]).unwrap();

        thread::sleep(time::Duration::from_secs(4));

        DockerCompose {
            file_path: file_path.to_string(),
        }
    }

    pub fn wait_for(&self, log_text : &str) -> Result<()> {
        info!("waiting for {}", log_text );
        let args = ["-f", self.file_path, "logs"];
        let re = Regex::new( log_text ).unwrap();

        let mut result = run_command( "docker-compose", &args ).unwrap();
        while ! result.contains(&re) {
            debug!( &result );
            result = run_command( "docker-compose", &args ).unwrap();
        }
        Ok(())
    }

    fn clean_up(file_path: &str) -> Result<()> {
        info!("bringing down docker compose {}", file_path);

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
