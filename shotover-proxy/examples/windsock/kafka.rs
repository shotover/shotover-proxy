use async_trait::async_trait;
use std::collections::HashMap;
use test_helpers::{
    docker_compose::docker_compose, flamegraph::Perf, kafka_producer_perf_test::run_producer_bench,
    shotover_process::ShotoverProcessBuilder,
};
use tokio::sync::mpsc::UnboundedSender;
use windsock::{Bench, Report};

pub struct KafkaBench {}

impl KafkaBench {
    pub fn new() -> Self {
        KafkaBench {}
    }
}

#[async_trait]
impl Bench for KafkaBench {
    fn tags(&self) -> HashMap<String, String> {
        [
            ("name".to_owned(), "kafka".to_owned()),
            ("topology".to_owned(), "single".to_owned()),
            // TODO: split with and without shotover into seperate cases
            // variants: None, Standard, ForcedMessageParsed
            ("shotover".to_owned(), "with_and_without".to_owned()),
            // TODO: run with different OPS values
            ("OPS".to_owned(), "10000000".to_owned()),
            // TODO: run with different connection count
            ("connections".to_owned(), "TODO".to_owned()),
            // TODO: run with different message types
            ("message_type".to_owned(), "write1000bytes".to_owned()),
        ]
        .into_iter()
        .collect()
    }

    async fn run(
        &self,
        flamegraph: bool,
        _local: bool,
        _runtime_seconds: u32,
        reporter: UnboundedSender<Report>,
    ) {
        let config_dir = "tests/test-configs/kafka/passthrough";
        {
            let _compose = docker_compose(&format!("{}/docker-compose.yaml", config_dir));
            let shotover =
                ShotoverProcessBuilder::new_with_topology(&format!("{}/topology.yaml", config_dir))
                    .start()
                    .await;

            println!("Benching Shotover ...");
            // flamegraph fails on CI atm due to other var("CI") gate causing test to end instantly so perf cant find pid
            let perf = if flamegraph && std::env::var("CI").is_err() {
                Some(Perf::new(shotover.child.as_ref().unwrap().id().unwrap()))
            } else {
                None
            };

            // run_producer_bench does not work on CI
            if std::env::var("CI").is_err() {
                run_producer_bench("[localhost]:9192");
            }

            shotover.shutdown_and_then_consume_events(&[]).await;
            if let Some(perf) = perf {
                perf.flamegraph();
            }
        }

        // restart the docker container to avoid running out of disk space
        let _compose = docker_compose(&format!("{}/docker-compose.yaml", config_dir));
        println!("\nBenching Direct Kafka ...");
        if std::env::var("CI").is_err() {
            run_producer_bench("[localhost]:9092");
        }

        // just pretend to do windsock things for now
        reporter.send(Report::Start).unwrap();
        reporter
            .send(Report::FinishedIn(std::time::Duration::from_secs(1)))
            .unwrap();
    }
}
