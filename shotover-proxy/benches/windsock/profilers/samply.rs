use std::path::PathBuf;
use tokio::task::JoinHandle;
use windsock::ReportArchive;

pub struct Samply {
    bench_name: String,
    handle: JoinHandle<()>,
    output_file: String,
}

impl Samply {
    pub async fn run(bench_name: String, results_path: PathBuf, pid: i32) -> Samply {
        run_command(
            "cargo",
            &[
                "install",
                "samply",
                "--git",
                "https://github.com/mstange/samply",
                "--rev",
                "75a8971b4291db8091a346431a1727ffcec4f038",
                "--locked",
            ],
        )
        .await;
        let output_file = results_path
            .join("samply.json")
            .as_os_str()
            .to_str()
            .unwrap()
            .to_owned();
        let home = std::env::var("HOME").unwrap();

        // Run `sudo ls` so that we can get sudo to stop asking for password for a while.
        // It also lets us block until the user has entered the password, otherwise the rest of the test would continue before `perf` has started.
        // TODO: It would be more robust to get a single root prompt and then keep feeding commands into it.
        //       But that would require a bunch of work so its out of scope for now.
        run_command("sudo", &["ls"]).await;

        Samply {
            bench_name,
            output_file: output_file.clone(),
            handle: tokio::spawn(async move {
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
                        &output_file,
                    ],
                )
                .await;
            }),
        }
    }

    pub fn wait(self) {
        futures::executor::block_on(self.handle).unwrap();
        let mut report = ReportArchive::load(&self.bench_name).unwrap();
        report.info_messages.push(format!(
            "To open profiler results run: samply load {}",
            self.output_file
        ));
        report.save();
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
