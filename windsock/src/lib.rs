mod bench;
mod cli;
pub mod cloud;
mod data;
mod filter;
mod list;
mod report;
mod tables;

pub use bench::{Bench, BenchParameters, BenchTask, Profiling};
use data::cloud_resources_path;
pub use report::{
    ExternalReport, LatencyPercentile, Metric, OperationsReport, PubSubReport, Report,
    ReportArchive,
};
pub use tables::Goal;

use anyhow::{anyhow, Result};
use bench::BenchState;
use clap::Parser;
use cli::Args;
use cloud::{BenchInfo, Cloud};
use filter::Filter;
use std::{path::Path, process::exit};
use tokio::runtime::Runtime;

pub struct Windsock<ResourcesRequired, Resources> {
    benches: Vec<BenchState<ResourcesRequired, Resources>>,
    cloud: Box<dyn Cloud<CloudResourcesRequired = ResourcesRequired, CloudResources = Resources>>,
    running_in_release: bool,
}

impl<ResourcesRequired: Clone, Resources: Clone> Windsock<ResourcesRequired, Resources> {
    /// The benches will be run and filtered out according to the CLI arguments
    ///
    /// Run order:
    /// * Locally: The benches that are run will always be done so in the order they are listed, this allows tricks to avoid recreating DB's for every bench.
    ///      e.g. the database handle can be put behind a mutex and only resetup when actually neccessary
    /// * Cloud: The benches will be run in an order optimized according to its required cloud resources.
    ///
    /// `release_profiles` specifies which cargo profiles Windsock will run under, if a different profile is used windsock will refuse to run.
    pub fn new(
        benches: Vec<
            Box<dyn Bench<CloudResourcesRequired = ResourcesRequired, CloudResources = Resources>>,
        >,
        cloud: Box<
            dyn Cloud<CloudResourcesRequired = ResourcesRequired, CloudResources = Resources>,
        >,
        release_profiles: &[&str],
    ) -> Self {
        let running_in_release = release_profiles.contains(&env!("PROFILE"));

        Windsock {
            benches: benches.into_iter().map(BenchState::new).collect(),
            cloud,
            running_in_release,
        }
    }

    // Hands control of the process over to windsock, this method will never return
    // Windsock processes CLI arguments and then runs benchmarks as instructed by the user.
    pub fn run(self) -> ! {
        match self.run_inner() {
            Ok(()) => exit(0),
            Err(err) => {
                eprintln!("{:?}", err);
                exit(1);
            }
        }
    }

    fn run_inner(mut self) -> Result<()> {
        let args = cli::Args::parse();

        let running_in_release = self.running_in_release;
        if args.cleanup_cloud_resources {
            let rt = create_runtime(None);
            rt.block_on(self.cloud.cleanup_resources());
        } else if let Some(compare_by_name) = &args.compare_by_name {
            tables::compare_by_name(compare_by_name)?;
        } else if let Some(compare_by_name) = &args.results_by_name {
            tables::results_by_name(compare_by_name)?;
        } else if let Some(compare_by_tags) = &args.compare_by_tags {
            tables::compare_by_tags(compare_by_tags)?;
        } else if let Some(results_by_tags) = &args.results_by_tags {
            tables::results_by_tags(results_by_tags)?;
        } else if let Some(filter) = &args.baseline_compare_by_tags {
            tables::baseline_compare_by_tags(filter)?;
        } else if args.set_baseline {
            ReportArchive::set_baseline();
            println!("Baseline set");
        } else if args.clear_baseline {
            ReportArchive::clear_baseline();
            println!("Baseline cleared");
        } else if args.list {
            list::list(&args, &self.benches);
        } else if args.nextest_run_by_name() {
            create_runtime(None).block_on(self.run_nextest(args, running_in_release))?;
        } else if let Some(err) = args.nextest_invalid_args() {
            return Err(err);
        } else if let Some(internal_run) = &args.internal_run {
            self.internal_run(&args, internal_run, running_in_release)?;
        } else if let Some(name) = args.name.clone() {
            create_runtime(None).block_on(self.run_named_bench(args, name, running_in_release))?;
        } else if args.cloud {
            create_runtime(None)
                .block_on(self.run_filtered_benches_cloud(args, running_in_release))?;
        } else {
            create_runtime(None)
                .block_on(self.run_filtered_benches_local(args, running_in_release))?;
        }

        Ok(())
    }

    fn internal_run(
        &mut self,
        args: &Args,
        internal_run: &str,
        running_in_release: bool,
    ) -> Result<()> {
        let (name, resources) = internal_run.split_at(internal_run.find(' ').unwrap() + 1);
        let name = name.trim();
        match self.benches.iter_mut().find(|x| x.tags.get_name() == name) {
            Some(bench) => {
                if args
                    .profilers
                    .iter()
                    .all(|x| bench.supported_profilers.contains(x))
                {
                    create_runtime(bench.cores_required()).block_on(async {
                        bench.run(args, running_in_release, resources).await;
                    });
                    Ok(())
                } else {
                    Err(anyhow!("Specified bench {name:?} was requested to run with the profilers {:?} but it only supports the profilers {:?}", args.profilers, bench.supported_profilers))
                }
            }
            None => Err(anyhow!("Specified bench {name:?} does not exist.")),
        }
    }

    async fn run_nextest(&mut self, mut args: Args, running_in_release: bool) -> Result<()> {
        // This is not a real bench we are just testing that it works,
        // so set some really minimal runtime values
        args.bench_length_seconds = Some(2);
        args.operations_per_second = Some(100);

        let name = args.filter.as_ref().unwrap().clone();
        self.run_named_bench(args, name, running_in_release).await
    }

    async fn run_named_bench(
        &mut self,
        args: Args,
        name: String,
        running_in_release: bool,
    ) -> Result<()> {
        ReportArchive::clear_last_run();
        let resources_path = cloud_resources_path();

        let bench = match self.benches.iter_mut().find(|x| x.tags.get_name() == name) {
            Some(bench) => {
                if args
                    .profilers
                    .iter()
                    .all(|x| bench.supported_profilers.contains(x))
                {
                    bench
                } else {
                    return Err(anyhow!("Specified bench {name:?} was requested to run with the profilers {:?} but it only supports the profilers {:?}", args.profilers, bench.supported_profilers));
                }
            }
            None => return Err(anyhow!("Specified bench {name:?} does not exist.")),
        };

        let resources = if args.cloud {
            let resources = vec![bench.required_cloud_resources()];
            Some(if args.load_cloud_resources_file {
                self.cloud
                    .load_resources_file(&resources_path, resources)
                    .await
            } else {
                self.cloud
                    .create_resources(resources, !args.store_cloud_resources_file)
                    .await
            })
        } else {
            None
        };

        if args.store_cloud_resources_file {
            println!(
                "Cloud resources have been created in preparation for running the bench:\n  {name}"
            );
            println!("Make sure to use `--cleanup-cloud-resources` when you are finished with these resources.");
        } else {
            bench
                .orchestrate(&args, running_in_release, resources.clone())
                .await;
        }

        if args.cloud {
            self.cleanup_cloud_resources(resources, &args, &resources_path)
                .await;
        }
        Ok(())
    }

    async fn run_filtered_benches_cloud(
        &mut self,
        args: Args,
        running_in_release: bool,
    ) -> Result<()> {
        ReportArchive::clear_last_run();
        let filter = parse_filter(&args)?;
        let resources_path = cloud_resources_path();

        let mut bench_infos = vec![];
        for bench in &mut self.benches {
            if filter
                .as_ref()
                .map(|x| {
                    x.matches(&bench.tags)
                        && args
                            .profilers
                            .iter()
                            .all(|x| bench.supported_profilers.contains(x))
                })
                .unwrap_or(true)
            {
                bench_infos.push(BenchInfo {
                    resources: bench.required_cloud_resources(),
                    name: bench.tags.get_name(),
                });
            }
        }
        bench_infos = self.cloud.order_benches(bench_infos);

        let mut resources = if !bench_infos.is_empty() {
            let resources = bench_infos.iter().map(|x| x.resources.clone()).collect();
            Some(if args.load_cloud_resources_file {
                self.cloud
                    .load_resources_file(&resources_path, resources)
                    .await
            } else {
                self.cloud
                    .create_resources(resources, !args.store_cloud_resources_file)
                    .await
            })
        } else {
            None
        };

        if args.store_cloud_resources_file {
            println!("Cloud resources have been created in preparation for running the following benches:");
            for bench in bench_infos {
                println!("  {}", bench.name);
            }
            println!("Make sure to use `--cleanup-cloud-resources` when you are finished with these resources");
        } else {
            for (i, bench_info) in bench_infos.iter().enumerate() {
                for bench in &mut self.benches {
                    if bench.tags.get_name() == bench_info.name {
                        if let Some(resources) = &mut resources {
                            self.cloud
                                .adjust_resources(&bench_infos, i, resources)
                                .await;
                        }
                        bench
                            .orchestrate(&args, running_in_release, resources.clone())
                            .await;
                        break;
                    }
                }
            }
        }

        self.cleanup_cloud_resources(resources, &args, &resources_path)
            .await;

        Ok(())
    }

    async fn cleanup_cloud_resources(
        &mut self,
        resources: Option<Resources>,
        args: &Args,
        resources_path: &Path,
    ) {
        if args.store_cloud_resources_file {
            if let Some(resources) = resources {
                self.cloud
                    .store_resources_file(resources_path, resources)
                    .await;
            }
        } else if args.load_cloud_resources_file {
            println!("Cloud resources have not been cleaned up.");
            println!(
                "Make sure to use `--cleanup-cloud-resources` when you are finished with them."
            );
        } else {
            std::fs::remove_file(resources_path).ok();
            self.cloud.cleanup_resources().await;
        }
    }

    async fn run_filtered_benches_local(
        &mut self,
        args: Args,
        running_in_release: bool,
    ) -> Result<()> {
        ReportArchive::clear_last_run();
        let filter = parse_filter(&args)?;
        for bench in &mut self.benches {
            if filter
                .as_ref()
                .map(|x| {
                    x.matches(&bench.tags)
                        && args
                            .profilers
                            .iter()
                            .all(|x| bench.supported_profilers.contains(x))
                })
                .unwrap_or(true)
            {
                bench.orchestrate(&args, running_in_release, None).await;
            }
        }
        Ok(())
    }
}

fn parse_filter(args: &Args) -> Result<Option<Filter>> {
    args.filter
        .as_ref()
        .map(|filter| {
            Filter::from_query(filter.as_ref())
                .map_err(|err| anyhow!("Failed to parse FILTER {filter:?}\n{err}"))
        })
        .transpose()
}

fn create_runtime(worker_threads: Option<usize>) -> Runtime {
    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all().thread_name("Windsock-Thread");
    if let Some(worker_threads) = worker_threads {
        runtime_builder.worker_threads(worker_threads);
    }
    runtime_builder.build().unwrap()
}
