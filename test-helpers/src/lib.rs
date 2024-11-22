pub mod cert;
pub mod connection;
pub mod docker_compose;
pub mod metrics;
pub mod mock_cassandra;
pub mod shotover_process;
mod test_tracing;

use anyhow::{anyhow, Error, Result};
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

fn command_not_found_error(command: &str, args: &[&str]) -> Error {
    // Maps a command to its associated dependency; however, if the name of the command and dependency is the same, we just use the command name.
    // Currently, the only use case is mapping `npm` to its dependency `nodejs`.
    let dependency = match command {
        "npm" => "nodejs",
        _ => command,
    };

    let args_part = if !args.is_empty() {
        format!(" {}", args.join(" "))
    } else {
        String::new()
    };

    anyhow!(
        "Attempted to run the command `{}{}` but {} does not exist. Have you installed {}?",
        command,
        args_part,
        command,
        dependency
    )
}

pub async fn run_command_async(current_dir: &Path, command: &str, args: &[&str]) {
    let output = tokio::process::Command::new(command)
        .args(args)
        .current_dir(current_dir)
        .kill_on_drop(true)
        .status()
        .await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return command_not_found_error(command, args);
            }

            anyhow!(e)
        })
        .unwrap();

    if !output.success() {
        panic!("command {command} {args:?} failed. See above output.")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn test_run_command_async_not_found_message() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let command = "shotover_non_existent_command";
        let args = &["arg1", "arg2"];
        let result = std::panic::AssertUnwindSafe(run_command_async(dir, command, args))
            .catch_unwind()
            .await;

        assert!(result.is_err(), "Expected a panic but none occurred");

        if let Err(panic_info) = result {
            if let Some(error_message) = panic_info.downcast_ref::<String>() {
                assert!(
                    error_message.contains("Attempted to run the command `shotover_non_existent_command arg1 arg2` but shotover_non_existent_command does not exist. Have you installed shotover_non_existent_command?"),
                    "Error message did not contain the expected NotFound error got: {}",
                    error_message
                );
            } else {
                panic!("Panic payload was not a string: {:?}", panic_info);
            }
        }
    }
}
