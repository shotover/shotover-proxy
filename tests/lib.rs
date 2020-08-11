use anyhow::{anyhow, Result};
use instaproxy::config::topology::Topology;
use std::process::Command;
use std::thread;
use std::time;
use tokio::task::JoinHandle;
use tracing::info;

pub mod redis_int_tests;

pub fn start_proxy(config: String) -> JoinHandle<Result<()>> {
    return tokio::spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) = Topology::from_file(config)?.run_chains().await {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok(())
    });
}

pub fn load_docker_compose(file_path: String) -> Result<()> {
    // stop_docker_compose(file_path.clone())?;
    thread::sleep(time::Duration::from_secs(1));
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(format!("docker-compose -f {} up -d", file_path.as_str()));

    info!("running {:#?}", command);

    let output = command
        .status()
        .expect("could not exec process docker-compose");
    if output.success() {
        return Ok(());
    }
    Err(anyhow!(
        "couldn't start docker compose {}",
        output.to_string()
    ))
}

pub fn stop_docker_compose(file_path: String) -> Result<()> {
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(format!("docker-compose -f {} down", file_path.as_str()));

    info!("running {:#?}", command);

    let output = command
        .status()
        .expect("could not exec process docker-compose");

    let mut command2 = Command::new("sh");
    command2.arg("-c").arg(format!(
        "docker-compose -f {} rm -f -s -v",
        file_path.as_str()
    ));

    info!("running {:#?}", command2);

    let output2 = command2
        .status()
        .expect("could not exec process docker-compose");

    thread::sleep(time::Duration::from_secs(1));

    let mut command3 = Command::new("sh");
    command3
        .arg("-c")
        .arg(format!("yes | docker network prune"));

    info!("running {:#?}", command3);

    let output3 = command3
        .status()
        .expect("could not exec process docker-compose");

    output3.success();

    if output.success() || output2.success() {
        return Ok(());
    }
    Err(anyhow!(
        "couldn't start docker compose {}",
        output.to_string()
    ))
}
