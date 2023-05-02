use crate::cli::Args;
use crate::report::{report_builder, Report};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use tokio::sync::mpsc::UnboundedSender;

pub struct BenchState {
    bench: Box<dyn Bench>,
    pub(crate) tags: Tags,
}

impl BenchState {
    pub fn new(bench: Box<dyn Bench>) -> Self {
        let tags = Tags(bench.tags());
        BenchState { bench, tags }
    }

    pub async fn run(&mut self, args: &Args) {
        println!("Running {:?}", self.tags.get_name());

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let process = tokio::spawn(report_builder(self.tags.clone(), rx));
        self.bench.run(args.flamegraph, true, tx).await;
        let report = process.await.unwrap();
        crate::tables::display_results_table(&[report]);
    }
}

#[async_trait]
pub trait Bench {
    /// Returns tags that are used for forming comparisons, graphs and naming the benchmark
    fn tags(&self) -> HashMap<String, String>;
    /// Runs the benchmark.
    /// Setup, benching and teardown all take place in here.
    async fn run(&self, flamegraph: bool, local: bool, reporter: UnboundedSender<Report>);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Tags(pub HashMap<String, String>);

impl Tags {
    pub fn get_name(&self) -> String {
        let mut result = if let Some(name) = self.0.get("name") {
            name.clone()
        } else {
            "".to_string()
        };

        let mut tags: Vec<(&String, &String)> = self.0.iter().collect();
        tags.sort_by_key(|x| x.0);
        for (key, value) in tags {
            if key != "name" {
                if !result.is_empty() {
                    write!(result, ",").unwrap();
                }
                write!(result, "{key}={value}").unwrap();
            }
        }
        result
    }

    /// returns the set wise intersection of two `Tags`s
    pub(crate) fn intersection(&self, other: &Tags) -> Self {
        let mut intersection = HashMap::new();
        for (key, value) in &self.0 {
            if other.0.get(key).map(|x| x == value).unwrap_or(false) {
                intersection.insert(key.clone(), value.clone());
            }
        }
        Tags(intersection)
    }

    pub(crate) fn keys(&self) -> HashSet<String> {
        self.0.keys().cloned().collect()
    }
}
