use aws_throwaway::{Aws, InstanceType};
use std::path::Path;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(non_blocking)
        .init();

    let aws = Aws::new().await;
    let instance = aws.create_ec2_instance(InstanceType::T2Micro, 8).await;

    instance
        .ssh()
        .shell("echo 'some string' > some_remote_file")
        .await;

    let result = instance.ssh().shell("xxd some_remote_file").await;
    println!("The bytes of the remote file:\n{}", result.stdout);
    instance
        .ssh()
        .pull_file(Path::new("some_remote_file"), Path::new("some_local_file"))
        .await;
    println!("Remote file copied locally to some_local_file");

    instance
        .ssh()
        .push_file(Path::new("README.md"), Path::new("readme.md"))
        .await;
    let mut receiver = instance.ssh().shell_stdout_lines("cat readme.md").await;
    println!();
    while let Some(line) = receiver.recv().await {
        println!("Received: {line}");
    }

    aws.cleanup_resources().await;
    println!("\nAll AWS throwaway resources have been deleted")
}
