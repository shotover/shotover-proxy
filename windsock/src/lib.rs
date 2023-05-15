mod bench;
mod cli;
mod filter;
mod report;
mod tables;
pub use bench::{Bench, BenchTask};
pub use report::Report;

use anyhow::{anyhow, Result};
use bench::BenchState;
use clap::Parser;
use cli::Args;
use filter::Filter;
use report::ReportArchive;
use std::process::exit;
use tokio::runtime::Runtime;

pub struct Windsock {
    benches: Vec<BenchState>,
    running_in_release: bool,
}

impl Windsock {
    /// The benches will be run and filtered out according to the CLI arguments
    /// The benches that are run will always be done so in the order they are listed, this allows tricks to avoid recreating DB's for every bench.
    /// e.g. the database handle can be put behind a mutex and only resetup when actually neccessary
    ///
    /// `release_profiles` specifies which cargo profiles Windsock will run under, if a different profile is used windsock will refuse to run.
    pub fn new(benches: Vec<Box<dyn Bench>>, release_profiles: &[&str]) -> Self {
        let running_in_release = release_profiles.contains(&env!("PROFILE"));

        Windsock {
            benches: benches
                .into_iter()
                .map(|bench| BenchState::new(bench))
                .collect(),
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
        if !args.disable_release_safety_check && !running_in_release {
            panic!("Windsock was not run with a configured release profile, maybe try running with the `--release` flag. Failing that check the release profiles provided in `Windsock::new(..)`.");
        }

        if let Some(compare_by_name) = &args.compare_by_name {
            tables::compare_by_name(compare_by_name)?;
        } else if let Some(compare_by_name) = &args.results_by_name {
            tables::results_by_name(compare_by_name)?;
        } else if let Some(compare_by_tags) = &args.compare_by_tags {
            tables::compare_by_tags(compare_by_tags)?;
        } else if let Some(results_by_tags) = &args.results_by_tags {
            tables::results_by_tags(results_by_tags)?;
        } else if args.list {
            println!("Benches:");
            for bench in &self.benches {
                println!("{}", bench.tags.get_name());
            }
        } else if let Some(name) = &args.name {
            ReportArchive::clear_last_run();
            match self.benches.iter_mut().find(|x| &x.tags.get_name() == name) {
                Some(bench) => run_bench(bench, &args, running_in_release),
                None => {
                    return Err(anyhow!("Specified bench {name:?} does not exist."));
                }
            }
        } else {
            ReportArchive::clear_last_run();
            let filter = match args
                .filter
                .as_ref()
                .map(|x| Filter::from_query(x.as_ref()))
                .transpose()
            {
                Ok(filter) => filter,
                Err(err) => {
                    return Err(anyhow!(
                        "Failed to parse FILTER {:?}\n{}",
                        args.filter.unwrap(),
                        err
                    ))
                }
            };
            for bench in &mut self.benches {
                if filter
                    .as_ref()
                    .map(|x| x.matches(&bench.tags))
                    .unwrap_or(true)
                {
                    run_bench(bench, &args, running_in_release)
                }
            }
        }

        Ok(())
    }
}
fn run_bench(bench: &mut BenchState, args: &Args, running_in_release: bool) {
    let runtime = create_runtime(Some(1));
    runtime.block_on(async {
        bench.run(args, running_in_release).await;
    });
}

fn create_runtime(worker_threads: Option<usize>) -> Runtime {
    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all().thread_name("Windsock-Thread");
    if let Some(worker_threads) = worker_threads {
        runtime_builder.worker_threads(worker_threads);
    }
    runtime_builder.build().unwrap()
}
