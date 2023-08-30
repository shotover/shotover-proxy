use crate::aws::Ec2InstanceWithShotover;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle};

pub struct Samply(JoinHandle<()>);

impl Samply {
    pub async fn run(results_path: PathBuf, pid: u32) -> Samply {
        install_samply().await;
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

pub struct SamplyCloud {
    instance: Arc<Ec2InstanceWithShotover>,
    receiver: UnboundedReceiver<String>,
    results_path: PathBuf,
}

impl SamplyCloud {
    /// Copy the samply binary to the passed instance and run it over ssh.
    /// Each line of output is returned via the `UnboundedReceiver`
    pub async fn run(
        bench_name: &str,
        instance: Arc<Ec2InstanceWithShotover>,
        results_path: PathBuf,
    ) -> Self {
        let home = PathBuf::from(std::env::var("HOME").unwrap());
        install_samply().await;
        println!("{}", instance.instance.ssh_instructions());
        instance
            .instance
            .ssh()
            .push_file(
                &home.join(".cargo").join("bin").join("samply"),
                Path::new("samply"),
            )
            .await;

        let receiver = instance.instance
                .ssh()
                .shell_stdout_lines(
                    &format!("
echo '1' | sudo tee /proc/sys/kernel/perf_event_paranoid > /dev/null;
sudo ./samply record -p `pgrep shotover-bin` --save-only --profile-name {bench_name} --output samply.json && sudo cat samply.json;
echo \"\" # workaround aws-throwaway bug that ignores lines without a newline")
                )
                .await;

        SamplyCloud {
            instance,
            receiver,
            results_path,
        }
    }

    pub async fn download_results(mut self) {
        // TODO: we should be able to expand aws-throwaway to allow sending a sigint directly to the running samply
        self.instance
            .instance
            .ssh()
            .shell("sudo killall -2 samply")
            .await;
        let mut output = String::new();
        while let Some(line) = tokio::time::timeout(Duration::from_secs(30), self.receiver.recv())
            .await
            .unwrap()
        {
            output.push_str(&line);
            output.push('\n');
        }
        tokio::fs::write(self.results_path.join("samply.json"), output.as_bytes())
            .await
            .unwrap();
    }
}

async fn install_samply() {
    run_command(
        "cargo",
        &[
            "install",
            "samply",
            "--git",
            "https://github.com/rukai/samply",
            "--rev",
            "af14e56ac9e809a445e4f0bd80fcd4fefc45b552",
        ],
    )
    .await;
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
