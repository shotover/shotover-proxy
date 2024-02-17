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
use clap::{CommandFactory, Parser};
use cli::{Command, RunArgs, WindsockArgs};
use cloud::{BenchInfo, Cloud};
use filter::Filter;
use std::process::exit;
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
        let args = WindsockArgs::parse();

        let running_in_release = self.running_in_release;
        if let Some(command) = args.command {
            match command {
                Command::List => list::list(&self.benches),
                Command::BaselineSet => {
                    ReportArchive::set_baseline();
                    println!("Baseline set");
                }
                Command::BaselineClear => {
                    ReportArchive::clear_baseline();
                    println!("Baseline cleared");
                }
                Command::GenerateWebpage => {
                    println!("Webpage generation is not implemented yet!")
                }
                Command::Results {
                    ignore_baseline,
                    filter,
                } => tables::results(
                    ignore_baseline,
                    &filter.unwrap_or_default().replace(',', " "),
                )?,
                Command::CompareByName { filter } => tables::compare_by_name(&filter)?,
                Command::CompareByTags { filter } => tables::compare_by_tags(&filter)?,
                Command::CloudSetup { filter } => {
                    create_runtime(None).block_on(self.cloud_setup(filter))?
                }
                Command::CloudRun(args) => {
                    create_runtime(None).block_on(self.cloud_run(args, running_in_release))?;
                }
                Command::CloudCleanup => {
                    create_runtime(None).block_on(self.cloud_cleanup());
                }
                Command::CloudSetupRunCleanup(args) => {
                    create_runtime(None)
                        .block_on(self.cloud_setup_run_cleanup(args, running_in_release))?;
                }
                Command::LocalRun(args) => {
                    create_runtime(None).block_on(self.local_run(args, running_in_release))?;
                }
                Command::InternalRun(args) => self.internal_run(&args, running_in_release)?,
            }
        } else if args.nextest_list() {
            list::nextest_list(&args, &self.benches);
        } else if let Some(name) = args.nextest_run_by_name() {
            create_runtime(None).block_on(self.run_nextest(name, running_in_release))?;
        } else if let Some(err) = args.nextest_invalid_args() {
            return Err(err);
        } else {
            WindsockArgs::command().print_help().unwrap();
        }

        Ok(())
    }

    async fn cloud_run(&mut self, args: RunArgs, running_in_release: bool) -> Result<()> {
        let bench_infos = self.bench_infos(&args.filter(), &args.profilers)?;
        let resources = self.load_cloud_from_disk(&bench_infos).await?;
        self.run_filtered_benches_cloud(args, running_in_release, bench_infos, resources)
            .await?;
        println!("Cloud resources have not been cleaned up.");
        println!("Make sure to use `cloud-cleanup` when you are finished with them.");
        Ok(())
    }

    async fn cloud_setup_run_cleanup(
        &mut self,
        args: RunArgs,
        running_in_release: bool,
    ) -> Result<()> {
        let bench_infos = self.bench_infos(&args.filter(), &args.profilers)?;
        let resources = self.temp_setup_cloud(&bench_infos).await?;
        self.run_filtered_benches_cloud(args, running_in_release, bench_infos, resources)
            .await?;
        self.cloud_cleanup().await;
        Ok(())
    }

    fn internal_run(&mut self, args: &RunArgs, running_in_release: bool) -> Result<()> {
        let name_and_resources = args
            .filter
            .as_ref()
            .expect("Filter arg must be provided for internal-run");
        let (name, resources) =
            name_and_resources.split_at(name_and_resources.find(' ').unwrap() + 1);
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

    async fn run_nextest(&mut self, name: &str, running_in_release: bool) -> Result<()> {
        let args = RunArgs {
            profilers: vec![],
            // This is not a real bench we are just testing that it works,
            // so set some really minimal runtime values
            bench_length_seconds: Some(2),
            operations_per_second: Some(100),
            filter: Some(name.to_string()),
        };

        self.local_run(args, running_in_release).await
    }

    fn bench_infos(
        &mut self,
        filter: &str,
        profilers_enabled: &[String],
    ) -> Result<Vec<BenchInfo<ResourcesRequired>>> {
        let filter = Filter::from_query(filter)
            .map_err(|err| anyhow!("Failed to parse FILTER {filter:?}\n{err}"))?;
        let mut bench_infos = vec![];
        for bench in &mut self.benches {
            if filter.matches(&bench.tags)
                && profilers_enabled
                    .iter()
                    .all(|x| bench.supported_profilers.contains(x))
            {
                bench_infos.push(BenchInfo {
                    resources: bench.required_cloud_resources(),
                    name: bench.tags.get_name(),
                });
            }
        }
        Ok(self.cloud.order_benches(bench_infos))
    }

    async fn load_cloud_from_disk(
        &mut self,
        bench_infos: &[BenchInfo<ResourcesRequired>],
    ) -> Result<Resources> {
        if !bench_infos.is_empty() {
            let resources = bench_infos.iter().map(|x| x.resources.clone()).collect();
            Ok(self
                .cloud
                .load_resources_file(&cloud_resources_path(), resources)
                .await)
        } else {
            Err(anyhow!("No benches found with the specified filter"))
        }
    }

    async fn cloud_setup(&mut self, filter: String) -> Result<()> {
        let bench_infos = self.bench_infos(&filter, &[])?;

        let resources = if !bench_infos.is_empty() {
            let resources = bench_infos.iter().map(|x| x.resources.clone()).collect();
            self.cloud.create_resources(resources, false).await
        } else {
            return Err(anyhow!("No benches found with the specified filter"));
        };

        self.cloud
            .store_resources_file(&cloud_resources_path(), resources)
            .await;

        println!(
            "Cloud resources have been created in preparation for running the following benches:"
        );
        for bench in bench_infos {
            println!("  {}", bench.name);
        }
        println!("Make sure to use `cloud-cleanup` when you are finished with these resources");

        Ok(())
    }

    async fn temp_setup_cloud(
        &mut self,
        bench_infos: &[BenchInfo<ResourcesRequired>],
    ) -> Result<Resources> {
        let resources = if !bench_infos.is_empty() {
            let resources = bench_infos.iter().map(|x| x.resources.clone()).collect();
            self.cloud.create_resources(resources, true).await
        } else {
            return Err(anyhow!("No benches found with the specified filter"));
        };

        Ok(resources)
    }

    async fn run_filtered_benches_cloud(
        &mut self,
        args: RunArgs,
        running_in_release: bool,
        bench_infos: Vec<BenchInfo<ResourcesRequired>>,
        mut resources: Resources,
    ) -> Result<()> {
        ReportArchive::clear_last_run();

        for (i, bench_info) in bench_infos.iter().enumerate() {
            for bench in &mut self.benches {
                if bench.tags.get_name() == bench_info.name {
                    self.cloud
                        .adjust_resources(&bench_infos, i, &mut resources)
                        .await;
                    bench
                        .orchestrate(&args, running_in_release, Some(resources.clone()))
                        .await;
                    break;
                }
            }
        }

        Ok(())
    }

    async fn cloud_cleanup(&mut self) {
        std::fs::remove_file(cloud_resources_path()).ok();
        self.cloud.cleanup_resources().await;
    }

    async fn local_run(&mut self, args: RunArgs, running_in_release: bool) -> Result<()> {
        ReportArchive::clear_last_run();
        let filter = args.filter();
        let filter = Filter::from_query(&filter)
            .map_err(|err| anyhow!("Failed to parse FILTER {:?}\n{err}", filter))?;

        for bench in &mut self.benches {
            if filter.matches(&bench.tags)
                && args
                    .profilers
                    .iter()
                    .all(|x| bench.supported_profilers.contains(x))
            {
                bench.orchestrate(&args, running_in_release, None).await;
            }
        }
        Ok(())
    }
}

fn create_runtime(worker_threads: Option<usize>) -> Runtime {
    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all().thread_name("Windsock-Thread");
    if let Some(worker_threads) = worker_threads {
        runtime_builder.worker_threads(worker_threads);
    }
    runtime_builder.build().unwrap()
}
