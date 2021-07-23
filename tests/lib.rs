use anyhow::Result;
use shotover_proxy::config::topology::Topology;
use std::thread;
use std::time;
use tokio::task::JoinHandle;
use tracing::info;
use subprocess::{Exec, Redirection};

pub mod redis_int_tests;
pub mod codec;

fn run_command(command: &str, args: &[&str]) {
    let data = Exec::cmd(command)
        .args(args)
        .stdout(Redirection::Pipe)
        .stderr(Redirection::Merge)
        .capture()
        .unwrap();
    if !data.exit_status.success() {
        panic!("command {} {:?} exited with {:?} and output:\n{}", command, args, data.exit_status, data.stdout_str())
    }
}

struct DockerCompose {
    file_path: String
}

impl DockerCompose {
    pub fn new(file_path: &str) -> Self {
        DockerCompose::clean_up(file_path);

        info!("bringing up docker compose {}", file_path);

        run_command("docker-compose", &["-f", file_path, "up", "-d"]);

        thread::sleep(time::Duration::from_secs(4));

        DockerCompose {
            file_path: file_path.to_string()
        }
    }

    fn clean_up(file_path: &str) {
        info!("bringing down docker compose {}", file_path);

        run_command("docker-compose", &["-f", file_path, "down", "-v"]);
        run_command("docker-compose", &["-f", file_path, "rm", "-f", "-s", "-v"]);

        thread::sleep(time::Duration::from_secs(1));
    }
}

impl Drop for DockerCompose {
    fn drop(&mut self) {
        DockerCompose::clean_up(&self.file_path);
    }
}

pub fn start_proxy(config: String) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) = Topology::from_file(config)?.run_chains().await {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok(())
    })
}
