use std::num::NonZeroUsize;

use test_helpers::latte::Latte;
use test_helpers::shotover_process::ShotoverProcessBuilder;

// This benchmark does not reflect realistic use as we are using a mocked out version of cassandra.
// The purpose of this bench is to create a scenario where shotover is the bottleneck between the client and cassandra.
// This will allow us to observe optimizations that might be too small to reliably show up in a realistic bench. e.g. improvements to parsing speed
// On the other hand, for optimizations that could be dependent on cassandra having such unrealistically low latency, they need to be confirmed in more realistic scenarios. e.g. changes to the way we batch messages in tcp

#[tokio::main]
async fn main() {
    test_helpers::bench::init();

    let (shotover_cores, client_cores, db_cores) =
        // Assigning multiple cores to the db and client will help ensure that shotover acts as the bottleneck.
        // So take advantage of that when we have the cores available to do so.
        if std::thread::available_parallelism().unwrap() >= NonZeroUsize::new(8).unwrap() {
            (1, 3, 3)
        } else {
            (1, 1, 1)
        };

    let latte = Latte::new(10000000, client_cores);
    let config_dir = "example-configs/cassandra-passthrough";
    let bench = "read";
    {
        test_helpers::mock_cassandra::start(db_cores, 9043);
        let shotover =
            ShotoverProcessBuilder::new_with_topology(&format!("{}/topology.yaml", config_dir))
                .with_cores(shotover_cores)
                .start()
                .await;

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
