use crate::cli::Args;
use crate::report::{report_builder, Report, ReportArchive};
use crate::tables::ReportColumn;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

pub struct BenchState {
    bench: Box<dyn Bench>,
    pub(crate) tags: Tags,
    pub(crate) supported_profilers: Vec<String>,
}

impl BenchState {
    pub fn new(bench: Box<dyn Bench>) -> Self {
        let tags = Tags(bench.tags());
        let supported_profilers = bench.supported_profilers();
        BenchState {
            bench,
            tags,
            supported_profilers,
        }
    }

    pub async fn run(&mut self, args: &Args, running_in_release: bool) {
        let name = self.tags.get_name();
        println!("Running {:?}", name);

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let process = tokio::spawn(report_builder(
            self.tags.clone(),
            rx,
            args.operations_per_second,
            running_in_release,
        ));

        let profilers_to_use = args.profilers.clone();
        let results_path = if !profilers_to_use.is_empty() {
            let path = crate::data::windsock_path()
                .join("profiler_results")
                .join(&name);
            std::fs::create_dir_all(&path).unwrap();
            path
        } else {
            PathBuf::new()
        };

        self.bench
            .run(
                Profiling {
                    results_path,
                    profilers_to_use,
                },
                true,
                args.bench_length_seconds.unwrap_or(15),
                args.operations_per_second,
                tx,
            )
            .await;
        let report = process.await.unwrap();

        crate::tables::display_results_table(&[ReportColumn {
            baseline: ReportArchive::load_baseline(&name).unwrap(),
            current: report,
        }]);
    }

    // TODO: will return None when running in non-local setup
    pub fn cores_required(&self) -> Option<usize> {
        Some(self.bench.cores_required())
    }
}

/// Implement this to define your benchmarks
/// A single implementation of `Bench` can represent multiple benchmarks by initializing it multiple times with different state that returns unique tags.
#[async_trait]
pub trait Bench {
    /// Returns tags that are used for forming comparisons, graphs and naming the benchmark
    fn tags(&self) -> HashMap<String, String>;

    /// Returns the names of profilers that this bench can be run with
    fn supported_profilers(&self) -> Vec<String> {
        vec![]
    }

    /// How many cores to assign the async runtime in which the bench runs.
    fn cores_required(&self) -> usize {
        1
    }

    /// Runs the benchmark.
    /// Setup, benching and teardown all take place in here.
    async fn run(
        &self,
        profiling: Profiling,
        local: bool,
        runtime_seconds: u32,
        operations_per_second: Option<u64>,
        reporter: UnboundedSender<Report>,
    );
}

pub struct Profiling {
    pub results_path: PathBuf,
    pub profilers_to_use: Vec<String>,
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

    /// Does not handle invalid names, only use on internally generated names
    pub fn from_name(name: &str) -> Self {
        let mut map = HashMap::new();
        for tag in name.split(',') {
            if tag.contains('=') {
                let mut pair = tag.split('=');
                let key = pair.next().unwrap().to_owned();
                let value = pair.next().unwrap().to_owned();
                map.insert(key, value);
            } else if map.contains_key("name") {
                panic!("The name tag was already set and a tag without an '=' was found")
            } else {
                map.insert("name".to_owned(), tag.to_owned());
            }
        }
        Tags(map)
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

/// An optional helper trait for defining benchmarks.
/// Usually you have an async rust DB driver that you need to call across multiple tokio tasks
/// This helper will spawn these tasks and send the required `Report::QueryCompletedIn`.
///
/// To use this helper:
///  1. implement `BenchTask` for a struct that contains the required db resources
///  2. have run_one_operation use those resources to perform a single operation
///  3. call spawn_tasks on an instance of BenchTask, it will clone your BenchTask instance once for each task it generates
#[async_trait]
pub trait BenchTask: Clone + Send + Sync + 'static {
    async fn run_one_operation(&self);

    async fn spawn_tasks(
        &self,
        reporter: UnboundedSender<Report>,
        operations_per_second: Option<u64>,
    ) -> Vec<JoinHandle<()>> {
        let mut tasks = vec![];
        // 100 is a generally nice amount of tasks to have, but if we have more tasks than OPS the throughput is very unstable
        let task_count = operations_per_second.map(|x| x.min(100)).unwrap_or(100);

        let allocated_time_per_op = operations_per_second
            .map(|ops| (Duration::from_secs(1) * task_count as u32) / ops as u32);
        for i in 0..task_count {
            let task = self.clone();
            let reporter = reporter.clone();
            tasks.push(tokio::spawn(async move {
                // spread load out over a second
                tokio::time::sleep(Duration::from_nanos((1_000_000_000 / task_count) * i)).await;

                let mut interval = allocated_time_per_op.map(tokio::time::interval);

                loop {
                    if let Some(interval) = &mut interval {
                        interval.tick().await;
                    }

                    let operation_start = Instant::now();
                    task.run_one_operation().await;
                    let report = Report::QueryCompletedIn(operation_start.elapsed());
                    if reporter.send(report).is_err() {
                        // The benchmark has completed and the reporter no longer wants to receive reports so just shutdown
                        return;
                    }
                }
            }));
        }

        // sleep until all tasks have started running
        tokio::time::sleep(Duration::from_secs(1)).await;

        tasks
    }
}
