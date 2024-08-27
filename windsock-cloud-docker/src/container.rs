pub struct Container(());

impl Container {
    pub async fn new() -> Container {
        // ensure container is setup
        let container_status = docker(&[
            "container",
            "ls",
            "-a",
            "--filter",
            "Name=windsock-cloud",
            "--format",
            "{{.Status}}",
        ])
        .await;
        if container_status.starts_with("Exited") {
            docker(&["start", "windsock-cloud"]).await;
        } else if !container_status.starts_with("Up") {
            docker(&[
                "run",
                "-d",
                "--name",
                "windsock-cloud",
                "ubuntu:20.04",
                "sleep",
                "infinity",
            ])
            .await;
            container_bash("apt-get update").await;
            container_bash(
            "DEBIAN_FRONTEND=noninteractive apt-get install -y curl git cmake pkg-config g++ libssl-dev librdkafka-dev psmisc unzip",
        ).await;
            container_bash(
                "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y",
            )
            .await;
            container_bash(
                r#"
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install
                "#,
            )
            .await;
        } else {
            cleanup().await;
        }

        Container(())
    }

    pub async fn run_windsock(&self) {
        // copy in shotover project
        let root = std::env::current_dir().unwrap();
        container_bash("rm -rf /shotover-proxy").await;
        // TODO: This copy will be very expensive if the user doesnt have their target directory setup as a symlink
        // Maybe we should do something like:
        // 1. rsync to target/shotover-copy-for-docker with the target directory filtered out
        // 2. `docker cp target/shotover-copy-for-docker windsock-cloud:/shotover-proxy`
        docker(&[
            "cp",
            root.to_str().unwrap(),
            "windsock-cloud:/shotover-proxy",
        ])
        .await;
        container_bash("rm -rf /shotover-proxy/target").await;

        // run windsock
        let mut args = std::env::args();
        args.next(); // skip binary name
        let features = args
            .next()
            .expect("The first argument must be a list of features to compile shotover with");
        let args: Vec<String> = args
            .map(|x| {
                if x.is_empty() {
                    String::from("''")
                } else {
                    String::from_utf8(shell_quote::Bash::quote_vec(&x)).unwrap()
                }
            })
            .collect();
        let args = args.join(" ");
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        let rust_log = std::env::var("RUST_LOG").unwrap_or_default();
        container_bash(&format!(
        r#"cd shotover-proxy;
source "$HOME/.cargo/env";

export RUST_LOG={rust_log}
export AWS_ACCESS_KEY_ID={access_key_id}
export AWS_SECRET_ACCESS_KEY={secret_access_key}
export CARGO_TERM_COLOR=always
cargo test --target-dir /target --release --bench windsock --no-default-features --features alpha-transforms,kafka-cpp-driver-tests,{features} -- {args}"#
    )).await;

        // extract windsock results
        let local_windsock_data = root.join("target").join("windsock_data");
        std::fs::remove_dir_all(&local_windsock_data).ok();
        docker(&[
            "cp",
            "windsock-cloud:/target/windsock_data",
            local_windsock_data.to_str().unwrap(),
        ])
        .await;
    }
}

pub async fn cleanup() {
    // killing a `docker exec` command will not propogate the kill onto the process within the container: https://github.com/moby/moby/issues/9098
    // so instead we need to manually kill the containers cargo + rustc to avoid leaving a process running.
    container_bash("killall cargo 2> /dev/null || true").await;
    container_bash("killall rustc 2> /dev/null || true").await;
}

async fn docker(args: &[&str]) -> String {
    run_command("docker", args).await
}

async fn container_bash(command: &str) {
    run_command_to_stdout("docker", &["exec", "windsock-cloud", "bash", "-c", command]).await
}

async fn run_command_to_stdout(command: &str, args: &[&str]) {
    let status = tokio::process::Command::new("docker")
        .args(args)
        .status()
        .await
        .unwrap();

    if !status.success() {
        println!(
            "Failed to run windsock, command {} {:?} exited with {:?}",
            command, args, status
        );
        std::process::exit(status.code().unwrap_or(1))
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
