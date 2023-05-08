mod bench;
mod cli;
pub use bench::{Bench, Report, Tags};

use bench::BenchState;
use clap::Parser;
use std::process::exit;
use tokio::runtime::Runtime;

pub struct Windsock {
    benches: Vec<BenchState>,
}

impl Windsock {
    /// The benches will be run and filtered out according to the CLI arguments
    /// The benches that are run will always be done so in the order they are listed, this allows tricks to avoid recreating DB's for every bench.
    /// e.g. the database handle can be put behind a mutex and only resetup when actually neccessary
    ///
    /// `release_profiles` specifies which cargo profiles Windsock will run under, if a different profile is used windsock will refuse to run.
    pub fn new(benches: Vec<Box<dyn Bench>>, release_profiles: &[&str]) -> Self {
        if !release_profiles.contains(&env!("PROFILE")) {
            panic!("Windsock was not run with a configured release profile, maybe try running with the `--release` flag. Failing that check the release profiles provided in `Windsock::new(..)`.");
        }

        Windsock {
            benches: benches
                .into_iter()
                .map(|bench| BenchState::new(bench))
                .collect(),
        }
    }

    // Hands control of the process over to windsock, this method will never return
    // Windsock processes CLI arguments and then runs benchmarks as instructed by the user.
    pub fn run(mut self) -> ! {
        let args = cli::Args::parse();
        let runtime = create_runtime(None);
        runtime.block_on(async {
            if args.list {
                println!("Benches:");
                for bench in &self.benches {
                    println!("{}", bench.get_name());
                }
            } else {
                for bench in &mut self.benches {
                    if filter(&args, bench) {
                        bench.run(&args).await;
                    }
                }
            }
        });
        exit(0)
    }
}

fn filter(args: &cli::Args, bench: &BenchState) -> bool {
    if args.filter.is_some() {
        todo!("Filter not yet implemented");
    }

    if let Some(filter_name) = &args.name {
        &bench.get_name() == filter_name
    } else {
        true
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
