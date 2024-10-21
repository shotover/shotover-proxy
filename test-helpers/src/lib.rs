pub mod cert;
pub mod connection;
pub mod docker_compose;
pub mod metrics;
pub mod mock_cassandra;
pub mod shotover_process;
mod test_tracing;

use anyhow::{anyhow, Result};
use std::path::Path;
use subprocess::{Exec, Redirection};

/// Runs a command and returns the output as a string.
///
/// Both stderr and stdout are returned in the result.
///
/// # Arguments
/// * `command` - The system command to run
/// * `args` - An array of command line arguments for the command
pub fn run_command(command: &str, args: &[&str]) -> Result<String> {
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

pub async fn run_command_async(current_dir: &Path, command: &str, args: &[&str]) {
    let output = tokio::process::Command::new(command)
        .args(args)
        .current_dir(current_dir)
        .status()
        .await
        .unwrap();

    if !output.success() {
        panic!("command {command} {args:?} failed. See above output.")
    }
}
