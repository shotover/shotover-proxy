use std::path::PathBuf;
use tokio::task::JoinHandle;

pub struct Samply(JoinHandle<()>);

impl Samply {
    pub async fn run(results_path: PathBuf, pid: u32) -> Samply {
        run_command(
            "cargo",
            &[
                "install",
                "samply",
                "--git",
                "https://github.com/mstange/samply",
                "--rev",
                "4c8d5eb164e44c4eda1b29de116f5ea546d64c65",
            ],
        )
        .await;
        let output_file = results_path.join("samply.json");
        let home = std::env::var("HOME").unwrap();

        // Run `sudo ls` so that we can get sudo to stop asking for password for a while.
        // It also lets us block until the user has entered the password, otherwise the rest of the test would continue before `perf` has started.
        // TODO: It would be more robust to get a single root prompt and then keep feeding commands into it.
        //       But that would require a bunch of work so its out of scope for now.
        run_command("sudo", &["ls"]).await;

        Samply(tokio::spawn(async move {
            run_command(
                "sudo",
                &[
                    // specify the full path as CI has trouble finding it for some reason.
                    &format!("{home}/.cargo/bin/samply"),
                    "record",
                    "-p",
                    &pid.to_string(),
                    "--save-only",
                    "--profile-name",
                    results_path.file_name().unwrap().to_str().unwrap(),
                    "--output",
                    output_file.as_os_str().to_str().unwrap(),
                ],
            )
            .await;
        }))
    }

    pub fn wait(self) {
        futures::executor::block_on(self.0).unwrap();
    }
}

async fn run_command(command: &str, args: &[&str]) -> String {
    let output = tokio::process::Command::new(command)
        .args(args)
        .output()
        .await
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    if !output.status.success() {
        panic!("command {command} {args:?} failed:\nstdout:\n{stdout}\nstderr:\n{stderr}")
    }
    stdout
}
