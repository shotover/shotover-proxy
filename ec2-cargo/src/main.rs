use aws_throwaway::{
    Aws, CleanupResources, Ec2Instance, Ec2InstanceDefinition, IngressRestriction, InstanceType,
};
use cargo_metadata::{Metadata, MetadataCommand};
use clap::Parser;
use shellfish::{async_fn, handler::DefaultAsyncHandler, rustyline::DefaultEditor, Command, Shell};
use std::error::Error;
use std::fs::Permissions;
use std::os::unix::prelude::PermissionsExt;
use tracing_subscriber::EnvFilter;

/// Spins up an EC2 instance and then presents a shell from which you can run `cargo test` on the ec2 instance.
///
/// Every time the shell runs a cargo command, any local changes to the repo are reuploaded to the ec2 instance.
///
/// When the shell is exited all created EC2 instances are destroyed.
#[derive(Parser, Clone)]
#[clap()]
pub struct Args {
    /// Specify the instance type to use for building and running tests.
    #[clap(long, default_value = "c7g.2xlarge")]
    pub instance_type: String,

    /// Cleanup resources and then immediately terminate without opening the shell
    #[clap(long)]
    pub cleanup: bool,
}

#[tokio::main]
async fn main() {
    // Needed to disable anyhow stacktraces by default
    if std::env::var("RUST_LIB_BACKTRACE").is_err() {
        std::env::set_var("RUST_LIB_BACKTRACE", "0");
    }

    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(non_blocking)
        .init();

    let cargo_meta = MetadataCommand::new().exec().unwrap();
    let args = Args::parse();
    if args.cleanup {
        Aws::cleanup_resources_static(CleanupResources::AllResources).await;
        println!("All AWS throwaway resources have been deleted");
        return;
    }

    let aws = Aws::builder(CleanupResources::AllResources)
        .use_az(Some("us-east-1b".into()))
        .use_ingress_restriction(IngressRestriction::LocalPublicAddress)
        .build()
        .await;
    let instance_type = InstanceType::from(args.instance_type.as_str());
    let instance = aws
        .create_ec2_instance(Ec2InstanceDefinition::new(instance_type).volume_size_gigabytes(40))
        .await;

    println!(
        "If something goes wrong with setup you can ssh into the machine by:\n{}",
        instance.ssh_instructions()
    );

    let mut receiver = instance
        .ssh()
        .shell_stdout_lines(r#"
set -e
set -u
# Need to retry until succeeds as apt-get update may fail if run really quickly after starting the instance
until sudo apt-get update -qq
do
  sleep 1
done
sudo apt-get install -y cmake pkg-config g++ libssl-dev librdkafka-dev unzip default-jre-headless npm
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

curl -sSL https://get.docker.com/ | sudo sh

# silly hack to let us run root docker without having to relogin for a group change to take affect
sudo mv /bin/docker /bin/docker1
echo '#!/bin/bash
sudo /bin/docker1 "$@"
' | sudo dd of=/bin/docker
sudo chmod +x /bin/docker

echo "export RUST_BACKTRACE=1" >> .profile
echo "export CARGO_TERM_COLOR=always" >> .profile
echo 'source "$HOME/.cargo/env"' >> .profile

source .profile
if [ "$(uname -m)" = "aarch64" ]; then
    curl -LsSf https://get.nexte.st/latest/linux-arm | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin

    curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install
else
    curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C ${CARGO_HOME:-~/.cargo}/bin

    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install
fi
"#).await;
    while let Some(line) = receiver.recv().await {
        match line {
            Ok(line) => println!("{line}"),
            Err(err) => panic!("Failed to setup instance: {err:?}"),
        }
    }

    println!("Finished creating instance.");

    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();

    let mut shell = Shell::new_with_async_handler(
        State {
            cargo_meta,
            instance,
            access_key_id,
            secret_access_key,
        },
        "ec2-cargo$ ",
        DefaultAsyncHandler::default(),
        DefaultEditor::new().unwrap(),
    );
    shell.commands.insert(
        "test",
        Command::new_async(
            "Uploads changes and runs `cargo nextest run $args`".to_owned(),
            async_fn!(State, test),
        ),
    );
    shell.commands.insert(
        "windsock",
        Command::new_async(
            "Uploads changes and runs `cargo windsock $args`. Windsock results are downloaded to target/windsock_data".to_owned(),
            async_fn!(State, windsock),
        ),
    );
    shell.commands.insert(
        "windsock-kafka",
        Command::new_async(
            "Uploads changes and runs `cargo windsock-kafka $args`. Windsock results are downloaded to target/windsock_data".to_owned(),
            async_fn!(State, windsock_kafka),
        ),
    );
    shell.commands.insert(
        "windsock-cassandra",
        Command::new_async(
            "Uploads changes and runs `cargo windsock-cassandra $args`. Windsock results are downloaded to target/windsock_data".to_owned(),
            async_fn!(State, windsock_cassandra),
        ),
    );
    shell.commands.insert(
        "windsock-redis",
        Command::new_async(
            "Uploads changes and runs `cargo windsock-redis $args`. Windsock results are downloaded to target/windsock_data".to_owned(),
            async_fn!(State, windsock_redis),
        ),
    );
    shell.commands.insert(
        "ssh-instructions",
        Command::new(
            "Print a bash snippet that can be used to ssh into the machine".to_owned(),
            ssh_instructions,
        ),
    );

    shell.run_async().await.unwrap();

    aws.cleanup_resources().await;
    println!("All AWS throwaway resources have been deleted")
}

async fn test(state: &mut State, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    rsync_push_shotover(state).await;
    let args = process_args(args);
    let mut receiver = state
        .instance
        .ssh()
        .shell_stdout_lines(&format!(
            r#"
source .profile
cd shotover
cargo nextest run {} 2>&1
"#,
            args
        ))
        .await;
    while let Some(line) = receiver.recv().await {
        match line {
            Ok(line) => println!("{line}"),
            Err(err) => println!("{err:?}"),
        }
    }

    Ok(())
}

async fn windsock(state: &mut State, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    windsock_inner("windsock", state, args).await
}

async fn windsock_kafka(state: &mut State, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    windsock_inner("windsock-kafka", state, args).await
}

async fn windsock_cassandra(state: &mut State, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    windsock_inner("windsock-cassandra", state, args).await
}

async fn windsock_redis(state: &mut State, args: Vec<String>) -> Result<(), Box<dyn Error>> {
    windsock_inner("windsock-redis", state, args).await
}

async fn windsock_inner(
    command: &str,
    state: &mut State,
    args: Vec<String>,
) -> Result<(), Box<dyn Error>> {
    rsync_push_shotover(state).await;
    let args = process_args(args);
    let mut receiver = state
        .instance
        .ssh()
        .shell_stdout_lines(&format!(
            r#"
source .profile
cd shotover
# ensure windsock_data exists so that fetching results still succeeds even if windsock never created the directory
mkdir -p target/windsock_data
AWS_ACCESS_KEY_ID={} AWS_SECRET_ACCESS_KEY={} cargo {command} {args} 2>&1
"#,
            state.access_key_id,
            state.secret_access_key
        ))
        .await;
    while let Some(line) = receiver.recv().await {
        match line {
            Ok(line) => println!("{line}"),
            Err(err) => println!("{err:?}"),
        }
    }

    rsync_fetch_windsock_results(state).await;

    Ok(())
}

fn process_args(mut args: Vec<String>) -> String {
    args.remove(0);
    args.iter()
        .map(|x| String::from_utf8(shell_quote::Bash::quote_vec(x)).unwrap())
        .collect::<Vec<_>>()
        .join(" ")
}

async fn rsync_push_shotover(state: &State) {
    let instance = &state.instance;
    let project_root_dir = &state.cargo_meta.workspace_root;
    let address = instance.public_ip().unwrap().to_string();

    rsync(
        state,
        vec![
            "--exclude".to_owned(),
            "target".to_owned(),
            format!("{}/", project_root_dir), // trailing slash means copy the contents of the directory instead of the directory itself
            format!("ubuntu@{address}:/home/ubuntu/shotover"),
        ],
    )
    .await
}

async fn rsync_fetch_windsock_results(state: &State) {
    let instance = &state.instance;
    let windsock_dir = &state.cargo_meta.target_directory.join("windsock_data");
    let address = instance.public_ip().unwrap().to_string();

    rsync(
        state,
        vec![
            format!("ubuntu@{address}:/home/ubuntu/shotover/target/windsock_data/"), // trailing slash means copy the contents of the directory instead of the directory itself
            windsock_dir.to_string(),
        ],
    )
    .await
}

async fn rsync(state: &State, append_args: Vec<String>) {
    let instance = &state.instance;
    let target_dir = &state.cargo_meta.target_directory;

    let key_path = target_dir.join("ec2-cargo-privatekey");
    tokio::fs::remove_file(&key_path).await.ok();
    tokio::fs::write(&key_path, instance.client_private_key())
        .await
        .unwrap();
    std::fs::set_permissions(&key_path, Permissions::from_mode(0o400)).unwrap();

    let known_hosts_path = target_dir.join("ec2-cargo-known_hosts");
    tokio::fs::write(&known_hosts_path, instance.openssh_known_hosts_line())
        .await
        .unwrap();

    let mut args = vec![
        "--delete".to_owned(),
        "-e".to_owned(),
        format!(
            "ssh -i {} -o 'UserKnownHostsFile {}'",
            key_path, known_hosts_path
        ),
        "-ra".to_owned(),
    ];
    args.extend(append_args);

    let output = tokio::process::Command::new("rsync")
        .args(args)
        .output()
        .await
        .unwrap();
    if !output.status.success() {
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        panic!("rsync failed:\nstdout:\n{stdout}\nstderr:\n{stderr}")
    }
}

fn ssh_instructions(state: &mut State, mut _args: Vec<String>) -> Result<(), Box<dyn Error>> {
    println!(
        "Run the following to ssh into the EC2 instance:\n{}",
        state.instance.ssh_instructions()
    );

    Ok(())
}

struct State {
    cargo_meta: Metadata,
    instance: Ec2Instance,
    access_key_id: String,
    secret_access_key: String,
}
