use aws_throwaway::{Aws, InstanceType};
use clap::Parser;
use std::str::FromStr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(non_blocking)
        .init();

    let args = Args::parse();
    if args.cleanup {
        Aws::cleanup_resources_static().await;
        println!("All AWS throwaway resources have been deleted")
    } else if let Some(instance_type) = args.instance_type {
        println!("Creating instance of type {instance_type}");

        let aws = Aws::new().await;
        let instance_type = InstanceType::from_str(&instance_type).unwrap();
        let instance = aws.create_ec2_instance(instance_type).await;

        let result = instance.ssh().shell("lsb_release -a").await;
        println!("Created instance running:\n{}", result.stdout);

        println!(
            "Run the following to ssh into it:
```
chmod 700 key 2> /dev/null || true
echo '{}' > key
chmod 400 key
ssh -i key ubuntu@{}
```",
            instance.client_private_key(),
            instance.public_ip()
        );
    } else {
        println!("Need to specify either --cleanup or --instance-type")
    }
}

#[derive(Parser, Clone)]
#[clap()]
pub struct Args {
    /// e.g. --instance-type t2.micro
    #[clap(long)]
    pub instance_type: Option<String>,

    #[clap(long)]
    pub cleanup: bool,
}
