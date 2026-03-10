mod cli;
mod instance;

use aws_throwaway::{
    Aws, CleanupResources, Ec2Instance, Ec2InstanceDefinition, IngressRestriction, InstanceType,
};
use cargo_metadata::{Metadata, MetadataCommand};
use clap::Parser;
use cli::{Args, Cmd};
use instance::Instance;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

fn state_file_path(cargo_meta: &Metadata) -> PathBuf {
    cargo_meta
        .target_directory
        .join("ec2-cargo-state.json")
        .into()
}

async fn load_instance(cargo_meta: &Metadata) -> Instance {
    let path = state_file_path(cargo_meta);
    let data = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "No instance state found at {}. Run `create` first.",
            path.display()
        )
    });
    let mut ec2: Ec2Instance =
        serde_json::from_str(&data).expect("Failed to deserialize instance state");
    ec2.init()
        .await
        .expect("Failed to initialize SSH connection to instance");
    Instance(ec2)
}

#[tokio::main]
async fn main() {
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

    match args.command {
        Cmd::Create { instance_type } => create(cargo_meta, &instance_type).await,
        Cmd::Test { args } => test(cargo_meta, &args).await,
        Cmd::Windsock { args } => windsock("windsock", cargo_meta, &args).await,
        Cmd::WindsockKafka { args } => windsock("windsock-kafka", cargo_meta, &args).await,
        Cmd::WindsockCassandra { args } => windsock("windsock-cassandra", cargo_meta, &args).await,
        Cmd::WindsockValkey { args } => windsock("windsock-valkey", cargo_meta, &args).await,
        Cmd::SshInstructions => ssh_instructions(cargo_meta).await,
        Cmd::Cleanup => cleanup(cargo_meta).await,
    }
}

async fn create(cargo_meta: Metadata, instance_type: &str) {
    let aws = Aws::builder(CleanupResources::AllResources)
        .use_az(Some("us-east-1b".into()))
        .use_ingress_restriction(IngressRestriction::LocalPublicAddress)
        .build()
        .await;
    let instance_type = InstanceType::from(instance_type);
    let instance = aws
        .create_ec2_instance(Ec2InstanceDefinition::new(instance_type).volume_size_gigabytes(40))
        .await;

    let instance = Instance(instance);

    println!(
        "If something goes wrong with setup you can ssh into the machine by:\n{}",
        instance.0.ssh_instructions()
    );

    instance.run_command(r#"
set -e
set -u
# Need to retry until succeeds as apt-get update may fail if run really quickly after starting the instance
until sudo apt-get update -qq
do
  sleep 1
done
sudo apt-get install -y cmake pkg-config g++ libssl-dev librdkafka-dev unzip default-jre-headless npm libcurl4-openssl-dev
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
"#).await.expect("Failed to setup instance");

    println!("Finished creating instance.");

    let path = state_file_path(&cargo_meta);
    let data = serde_json::to_string(&instance.0).unwrap();
    std::fs::write(&path, data).unwrap();
    println!("Instance state saved to {}", path.display());
}

async fn test(cargo_meta: Metadata, args: &[String]) {
    let instance = load_instance(&cargo_meta).await;
    println!("Uploading shotover project");
    instance.rsync_push_shotover(&cargo_meta).await;
    println!("Upload complete");
    let args = quote_args(args);
    if let Err(err) = instance
        .run_command(&format!(
            r#"
source .profile
cd shotover
cargo nextest run {args} 2>&1
"#
        ))
        .await
    {
        println!("{err:?}");
    }
}

async fn windsock(command: &str, cargo_meta: Metadata, args: &[String]) {
    let instance = load_instance(&cargo_meta).await;
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").unwrap();
    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap();

    println!("Uploading shotover project");
    instance.rsync_push_shotover(&cargo_meta).await;
    println!("Upload complete");
    let args = quote_args(args);
    if let Err(err) = instance
        .run_command(&format!(
            r#"
source .profile
cd shotover
# ensure windsock_data exists so that fetching results still succeeds even if windsock never created the directory
mkdir -p target/windsock_data
RUST_LOG=info AWS_ACCESS_KEY_ID={access_key_id} AWS_SECRET_ACCESS_KEY={secret_access_key} cargo {command} {args} 2>&1
"#
        ))
        .await
    {
        println!("{err:?}");
    }

    instance.rsync_fetch_windsock_results(&cargo_meta).await;
}

async fn ssh_instructions(cargo_meta: Metadata) {
    let instance = load_instance(&cargo_meta).await;
    println!(
        "Run the following to ssh into the EC2 instance:\n{}",
        instance.0.ssh_instructions()
    );
}

async fn cleanup(cargo_meta: Metadata) {
    Aws::cleanup_resources_static(CleanupResources::AllResources).await;
    let path = state_file_path(&cargo_meta);
    let _ = std::fs::remove_file(&path);
    println!("All AWS throwaway resources have been deleted");
}

fn quote_args(args: &[String]) -> String {
    args.iter()
        .map(|x| String::from_utf8(shell_quote::Bash::quote_vec(x)).unwrap())
        .collect::<Vec<_>>()
        .join(" ")
}
