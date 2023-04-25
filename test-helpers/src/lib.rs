pub mod bench;
pub mod cert;
pub mod connection;
pub mod docker_compose;
pub mod flamegraph;
pub mod kafka_producer_perf_test;
pub mod latte;
pub mod lazy;
pub mod metrics;
pub mod mock_cassandra;
pub mod shotover_process;

use anyhow::{anyhow, bail, Result};
use subprocess::{Exec, Redirection};

pub fn wait_for_socket_to_open(address: &str, port: u16) {
    try_wait_for_socket_to_open(address, port).unwrap();
}

pub fn try_wait_for_socket_to_open(address: &str, port: u16) -> Result<()> {
    let mut tries = 0;
    while std::net::TcpStream::connect((address, port)).is_err() {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if tries > 50 {
            bail!("Ran out of retries to connect to the socket");
        }
        tries += 1;
    }
    Ok(())
}

fn run_command_to_stdout(command: &str, args: &[&str]) {
    assert!(
        std::process::Command::new(command)
            .args(args)
            .status()
            .unwrap()
            .success(),
        "Failed to run: {command} {args:?}"
    );
}

/// Runs a command and returns the output as a string.
///
/// Both stderr and stdout are returned in the result.
///
/// # Arguments
/// * `command` - The system command to run
/// * `args` - An array of command line arguments for the command
pub(crate) fn run_command(command: &str, args: &[&str]) -> Result<String> {
    tracing::trace!("executing {}", command);
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
