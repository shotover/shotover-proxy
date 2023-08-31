// A helper to run `windsock --cloud` within docker to workaround libc issues
// It is not possible to use this helper to run windsock locally as that would involve running docker within docker

use test_helpers::run_command;

fn main() {
    let mut args = std::env::args();
    args.next(); // skip binary name
    let args: Vec<String> = args.collect();
    let args = args.join(" ");

    // ensure container is setup
    let container_status = docker(&[
        "container",
        "ls",
        "-a",
        "--filter",
        "Name=windsock-cloud",
        "--format",
        "{{.Status}}",
    ]);
    if container_status.starts_with("Exited") {
        docker(&["start", "windsock-cloud"]);
    } else if !container_status.starts_with("Up") {
        docker(&[
            "run",
            "-d",
            "--name",
            "windsock-cloud",
            "ubuntu:20.04",
            "sleep",
            "infinity",
        ]);
        container_bash("apt-get update");
        container_bash(
            "DEBIAN_FRONTEND=noninteractive apt-get install -y curl git cmake pkg-config g++ libssl-dev librdkafka-dev uidmap",
        );
        container_bash("curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y");
    }

    // copy in shotover project
    let root = std::env::current_dir().unwrap();
    container_bash("rm -r /shotover-proxy");
    // TODO: This copy will be very expensive if the user doesnt have their target directory setup as a symlink
    // Maybe we should do something like:
    // 1. rsync to target/shotover-copy-for-docker with the target directory filtered out
    // 2. `docker cp target/shotover-copy-for-docker windsock-cloud:/shotover-proxy`
    docker(&[
        "cp",
        root.to_str().unwrap(),
        "windsock-cloud:/shotover-proxy",
    ]);
    container_bash("rm -r /shotover-proxy/target");

    // run windsock
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();
    container_bash(&format!(
        r#"cd shotover-proxy;
source "$HOME/.cargo/env";
AWS_ACCESS_KEY_ID={access_key_id} AWS_SECRET_ACCESS_KEY={secret_access_key} CARGO_TERM_COLOR=always cargo test --target-dir /target --release --bench windsock --features alpha-transforms -- {args}"#
    ));
}

pub fn docker(args: &[&str]) -> String {
    run_command("docker", args).unwrap()
}

pub fn container_bash(command: &str) {
    run_command_to_stdout("docker", &["exec", "windsock-cloud", "bash", "-c", command])
}

pub fn run_command_to_stdout(command: &str, args: &[&str]) {
    let status = std::process::Command::new("docker")
        .args(args)
        .status()
        .unwrap();

    if !status.success() {
        println!(
            "Failed to run windsock, command {} {:?} exited with {:?}",
            command, args, status
        );
        std::process::exit(status.code().unwrap_or(1))
    }
}
