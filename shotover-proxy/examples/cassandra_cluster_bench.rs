use test_helpers::docker_compose::DockerCompose;
use test_helpers::latte::Latte;
use test_helpers::shotover_process::ShotoverProcessBuilder;

#[tokio::main]
async fn main() {
    test_helpers::bench::init();

    let latte = Latte::new(10000000);
    let config_dir = "example-configs/cassandra-cluster-v4";
    let bench = "read";
    {
        let _compose = DockerCompose::new(&format!("{}/docker-compose.yaml", config_dir));
        let shotover =
            ShotoverProcessBuilder::new_with_topology(&format!("{}/topology.yaml", config_dir))
                .start()
                .await;

        println!("Benching Shotover ...");
        latte.init(bench, "172.16.1.2:9044");
        latte.bench(bench, "localhost:9042");

        shotover.shutdown_and_then_consume_events(&[]).await;

        println!("Benching Direct Cassandra ...");
        latte.init(bench, "172.16.1.2:9044");
        latte.bench(bench, "172.16.1.2:9044");
    }

    println!("Direct Cassandra (A) vs Shotover (B)");
    latte.compare("read-172.16.1.2:9044.json", "read-localhost:9042.json");
}
