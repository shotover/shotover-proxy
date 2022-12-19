use test_helpers::docker_compose::DockerCompose;
use test_helpers::latte::Latte;
use test_helpers::shotover_process::shotover_from_topology_file;

#[tokio::main]
async fn main() {
    test_helpers::bench::init();

    let latte = Latte::new(10000000);
    let config_dir = "example-configs/cassandra-cluster-v4";
    let bench = "read";
    {
        let _compose = DockerCompose::new(&format!("{}/docker-compose.yaml", config_dir));
        latte.init(bench, "172.16.1.2:9044");

        let shotover = shotover_from_topology_file(&format!("{}/topology.yaml", config_dir)).await;

        println!("Benching Shotover ...");
        latte.bench(bench, "localhost:9042");

        shotover.shutdown_and_then_consume_events(&[]).await;
    }
}
