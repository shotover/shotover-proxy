use clap::Parser;
use test_helpers::docker_compose::DockerCompose;
use test_helpers::latte::Latte;
use test_helpers::shotover_process::ShotoverProcessBuilder;

/// e.g.
/// cargo run --release --example cassandra_bench -- --config-dir example-configs/cassandra-passthrough -r 1000
/// or
/// cargo run --release --example cassandra_bench -- --config-dir tests/test-configs/cassandra-passthrough-parse-request -r 1000
#[derive(Parser, Clone)]
#[clap(name = "cassandra_bench", arg_required_else_help = true)]
pub struct Args {
    #[clap(short, long)]
    pub config_dir: String,

    #[clap(short, long)]
    pub rate: u64,
}

#[tokio::main]
async fn main() {
    test_helpers::bench::init();
    let args = Args::parse();

    let latte = Latte::new(args.rate);
    let bench = "read";
    {
        let _compose = DockerCompose::new(&format!("{}/docker-compose.yaml", args.config_dir));

        let shotover = ShotoverProcessBuilder::new_with_topology(&format!(
            "{}/topology.yaml",
            args.config_dir
        ))
        .start()
        .await;

        println!("Benching Shotover ...");
        latte.init(bench, "localhost:9043");
        latte.bench(bench, "localhost:9042");
        println!("Benching Direct Cassandra ...");
        latte.init(bench, "localhost:9043");
        latte.bench(bench, "localhost:9043");

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    println!("Direct Cassandra (A) vs Shotover (B)");
    latte.compare("read-localhost:9043.json", "read-localhost:9042.json");
}
