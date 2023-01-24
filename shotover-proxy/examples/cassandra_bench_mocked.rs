use test_helpers::latte::Latte;
use test_helpers::shotover_process::shotover_from_topology_file;

#[tokio::main]
async fn main() {
    test_helpers::bench::init();

    let latte = Latte::new(10000000);
    let config_dir = "example-configs/cassandra-passthrough";
    let bench = "read";
    {
        test_helpers::mock_cassandra::start(9043);
        let shotover = shotover_from_topology_file(&format!("{}/topology.yaml", config_dir)).await;

        println!("Benching Shotover ...");
        // no need to initialize as cassandra is completely mocked out, just directly start benching.
        latte.bench(bench, "localhost:9042");

        shotover.shutdown_and_then_consume_events(&[]).await;

        println!("Benching Direct Mocked Cassandra ...");
        latte.bench(bench, "localhost:9043");
    }

    println!("Direct Mocked Cassandra (A) vs Shotover (B)");
    latte.compare("read-localhost:9043.json", "read-localhost:9042.json");
}
