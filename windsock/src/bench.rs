use crate::cli::Args;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Write;
use tokio::sync::mpsc::UnboundedSender;

pub struct BenchState {
    bench: Box<dyn Bench>,
    tags: Tags,
}

impl BenchState {
    pub fn new(bench: Box<dyn Bench>) -> Self {
        let tags = bench.tags();
        BenchState { bench, tags }
    }

    pub async fn run(&mut self, args: &Args) {
        println!("Running {:?}", self.get_name());

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        self.bench.run(args.flamegraph, true, tx).await;

        println!("Success!");
    }

    pub fn get_name(&self) -> String {
        let mut result = if let Some(name) = self.tags.get("name") {
            name.clone()
        } else {
            panic!("Needs name");
        };

        let mut tags: Vec<(&String, &String)> = self.tags.iter().collect();
        tags.sort_by_key(|x| x.0);
        for (key, value) in tags {
            if key != "name" {
                write!(result, ",{key}={value}").unwrap();
            }
        }
        result
    }
}

#[async_trait]
pub trait Bench {
    /// Returns tags that are used for forming comparisons, graphs and naming the benchmark
    fn tags(&self) -> Tags;
    /// Runs the benchmark.
    /// Setup, benching and teardown all take place in here.
    async fn run(&self, flamegraph: bool, local: bool, reporter: UnboundedSender<Report>);
}

pub type Tags = HashMap<String, String>;

#[derive(Debug)]
pub enum Report {
    ThingHappen,
}
