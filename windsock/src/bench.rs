use crate::cli::RunArgs;
use crate::report::{report_builder, Report, ReportArchive};
use crate::tables::ReportColumn;
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

pub struct BenchState<ResourcesRequired, Resources> {
    bench: Box<dyn Bench<CloudResourcesRequired = ResourcesRequired, CloudResources = Resources>>,
    pub(crate) tags: Tags,
    pub(crate) supported_profilers: Vec<String>,
}

impl<ResourcesRequired, Resources> BenchState<ResourcesRequired, Resources> {
    pub fn new(
        bench: Box<
            dyn Bench<CloudResourcesRequired = ResourcesRequired, CloudResources = Resources>,
        >,
    ) -> Self {
        let tags = Tags(bench.tags());
        let supported_profilers = bench.supported_profilers();
        BenchState {
            bench,
            tags,
            supported_profilers,
        }
    }

    pub async fn orchestrate(
        &mut self,
        args: &RunArgs,
        running_in_release: bool,
        cloud_resources: Option<Resources>,
    ) {
        let name = self.tags.get_name();
        println!("Running {:?}", name);

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

        if let Some(cloud_resources) = cloud_resources {
            self.bench
                .orchestrate_cloud(
                    cloud_resources,
                    running_in_release,
                    Profiling {
                        results_path,
                        profilers_to_use,
                    },
                    BenchParameters::from_args(args),
                )
                .await
                .unwrap();
        } else {
            self.bench
                .orchestrate_local(
                    running_in_release,
                    Profiling {
                        results_path,
                        profilers_to_use,
                    },
                    BenchParameters::from_args(args),
                )
                .await
                .unwrap();
        }

        crate::tables::display_results_table(&[ReportColumn {
            baseline: ReportArchive::load_baseline(&name).unwrap(),
            current: ReportArchive::load(&name).unwrap(),
        }]);
    }

    pub async fn run(&mut self, args: &RunArgs, running_in_release: bool, resources: &str) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let process = tokio::spawn(report_builder(
            self.tags.clone(),
            rx,
            args.operations_per_second,
            running_in_release,
        ));

        self.bench
            .run_bencher(resources, BenchParameters::from_args(args), tx)
            .await;

        process.await.unwrap();
    }

    // TODO: will return None when running in non-local setup
    pub fn cores_required(&self) -> Option<usize> {
        Some(self.bench.cores_required())
    }

    pub fn required_cloud_resources(&self) -> ResourcesRequired {
        self.bench.required_cloud_resources()
    }
}

/// Implement this to define your benchmarks
/// A single implementation of `Bench` can represent multiple benchmarks by initializing it multiple times with different state that returns unique tags.
#[async_trait]
pub trait Bench {
    type CloudResourcesRequired;
    type CloudResources;

    /// Returns tags that are used for forming comparisons, graphs and naming the benchmark
    fn tags(&self) -> HashMap<String, String>;

    /// Returns the names of profilers that this bench can be run with
    fn supported_profilers(&self) -> Vec<String> {
        vec![]
    }

    /// Specifies the cloud resources that should be provided to this bench
    fn required_cloud_resources(&self) -> Self::CloudResourcesRequired {
        unimplemented!("To support running in cloud this bench needs to implement `Bench::required_cloud_resources`");
    }

    /// How many cores to assign the async runtime in which the bench runs.
    fn cores_required(&self) -> usize {
        1
    }

    /// Windsock will call this method to orchestrate the bench in cloud mode.
    /// It must setup cloud resources to run the bench in a cloud and then start the bench returning the results on conclusion
    async fn orchestrate_cloud(
        &self,
        cloud: Self::CloudResources,
        running_in_release: bool,
        profiling: Profiling,
        bench_parameters: BenchParameters,
    ) -> Result<()>;

    /// Windsock will call this method to orchestrate the bench in local mode.
    /// It must setup local resources to run the bench locally and then start the bench returning the results on conclusion
    async fn orchestrate_local(
        &self,
        running_in_release: bool,
        profiling: Profiling,
        bench_parameters: BenchParameters,
    ) -> Result<()>;

    /// Windsock will call this method to run the bencher.
    /// But the implementation of `orchestrate_local` or `orchestrate_cloud` must run the bencher through windsock in some way.
    /// This will be:
    /// * In the case of `orchestrate_cloud`, the windsock binary must be uploaded to a cloud VM and `windsock --internal-run` executed there.
    /// * In the case of `orchestrate_local`, call `Bench::execute_run` to indirectly call this method by executing another instance of the windsock executable.
    ///
    /// The `resources` arg is a string that is passed in from the argument to `--internal-run` or at a higher level the argument to `Bench::execute_run`.
    /// Use this string to instruct the bencher where to find the resources it needs. e.g. the IP address of the DB to benchmark.
    /// To pass in multiple resources it is recommended to use a serialization method such as `serde-json`.
    async fn run_bencher(
        &self,
        resources: &str,
        bench_parameters: BenchParameters,
        reporter: UnboundedSender<Report>,
    );

    /// Call within `Bench::orchestrate_local` to call `Bench::run`
    async fn execute_run(&self, resources: &str, bench_parameters: &BenchParameters) {
        let name_and_resources = format!("{} {}", self.name(), resources);
        let output = tokio::process::Command::new(std::env::current_exe().unwrap().as_os_str())
            .args(run_args_vec(name_and_resources, bench_parameters))
            .output()
            .await
            .unwrap();
        if !output.status.success() {
            let stdout = String::from_utf8(output.stdout).unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();
            panic!("Bench run failed:\nstdout:\n{stdout}\nstderr:\n{stderr}")
        }
    }

    /// Call within `Bench::orchestrate_cloud` to determine how to invoke the uploaded windsock executable
    fn run_args(&self, resources: &str, bench_parameters: &BenchParameters) -> String {
        let name_and_resources = format!("\"{} {}\"", self.name(), resources);
        run_args_vec(name_and_resources, bench_parameters).join(" ")
    }

    fn name(&self) -> String {
        Tags(self.tags()).get_name()
    }
}

fn run_args_vec(name_and_resources: String, bench_parameters: &BenchParameters) -> Vec<String> {
    let mut args = vec![];
    args.push("internal-run".to_owned());
    args.push("--bench-length-seconds".to_owned());
    args.push(bench_parameters.runtime_seconds.to_string());

    if let Some(ops) = bench_parameters.operations_per_second {
        args.push("--operations-per-second".to_owned());
        args.push(ops.to_string());
    };

    args.push(name_and_resources);

    args
}

pub struct BenchParameters {
    pub runtime_seconds: u32,
    pub operations_per_second: Option<u64>,
}

impl BenchParameters {
    fn from_args(args: &RunArgs) -> Self {
        BenchParameters {
            runtime_seconds: args.bench_length_seconds.unwrap_or(15),
            operations_per_second: args.operations_per_second,
        }
    }
}

pub struct Profiling {
    pub results_path: PathBuf,
    pub profilers_to_use: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Tags(pub HashMap<String, String>);

impl Tags {
    pub fn get_name(&self) -> String {
        let mut result = String::new();

        let mut tags: Vec<(&String, &String)> = self.0.iter().collect();
        tags.sort_by_key(|x| x.0);
        for (key, value) in tags {
            if !result.is_empty() {
                write!(result, ",").unwrap();
            }
            write!(result, "{key}={value}").unwrap();
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
            } else {
                panic!("tag without an '=' was found")
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
    async fn run_one_operation(&self) -> Result<(), String>;

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
                    let report = match task.run_one_operation().await {
                        Ok(()) => Report::QueryCompletedIn(operation_start.elapsed()),
                        Err(message) => Report::QueryErrored {
                            completed_in: operation_start.elapsed(),
                            message,
                        },
                    };
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
