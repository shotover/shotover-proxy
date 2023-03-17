use test_helpers::docker_compose::DockerCompose;
use test_helpers::kafka_producer_perf_test::run_producer_bench;
use test_helpers::shotover_process::ShotoverProcessBuilder;

#[tokio::main]
async fn main() {
    test_helpers::bench::init();

    let config_dir = "tests/test-configs/kafka/passthrough";
    {
        let _compose = DockerCompose::new(&format!("{}/docker-compose.yaml", config_dir));
        let shotover =
            ShotoverProcessBuilder::new_with_topology(&format!("{}/topology.yaml", config_dir))
                .start()
                .await;

        println!("Benching Shotover ...");
        run_producer_bench("[localhost]:9192");

        shotover.shutdown_and_then_consume_events(&[]).await;
    }

    // restart the docker container to avoid running out of disk space
    let _compose = DockerCompose::new(&format!("{}/docker-compose.yaml", config_dir));
    println!("\nBenching Direct Kafka ...");
    run_producer_bench("[localhost]:9092");
}
