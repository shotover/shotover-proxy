use test_helpers::docker_compose::DockerCompose;
use test_helpers::flamegraph::Perf;
use test_helpers::latte::Latte;
use test_helpers::shotover_process::ShotoverProcessBuilder;

// To get useful results you will need to modify the Cargo.toml like:
// [profile.release]
// #lto = "fat"
// codegen-units = 1
// debug = true

#[tokio::main]
async fn main() {
    test_helpers::bench::init();

    let latte = Latte::new(10000000);
    let config_dir = "example-configs/cassandra-cluster-v4";
    let bench = "read";
    {
        let _compose = DockerCompose::new(&format!("{}/docker-compose.yaml", config_dir));
        latte.init(bench, "172.16.1.2:9044");

        let shotover =
            ShotoverProcessBuilder::new_with_topology(&format!("{}/topology.yaml", config_dir))
                .start()
                .await;

        let perf = Perf::new(shotover.child.as_ref().unwrap().id().unwrap());

        println!("Benching Shotover ...");
        latte.bench(bench, "localhost:9042");

        shotover.shutdown_and_then_consume_events(&[]).await;
        perf.flamegraph();
    }
}
