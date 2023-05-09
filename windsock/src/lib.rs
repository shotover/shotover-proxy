mod bench;
mod cli;
mod report;
mod tables;
pub use bench::Bench;
pub use report::Report;

use bench::BenchState;
use clap::Parser;
use cli::Args;
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
        // TODO: maybe we should create a new runtime per bench for consistency reasons
        if let Some(compare_by_name) = &args.compare_by_name {
            tables::compare_by_name(compare_by_name);
        } else if let Some(compare_by_name) = &args.results_by_name {
            tables::results_by_name(compare_by_name);
        } else if let Some(compare_by_tags) = &args.compare_by_tags {
            todo!("compare_by_tags: {compare_by_tags}")
        } else if let Some(results_by_tags) = &args.results_by_tags {
            todo!("results_by_tags: {results_by_tags}")
        } else if args.list {
            println!("Benches:");
            for bench in &self.benches {
                println!("{}", bench.tags.get_name());
            }
        } else if let Some(name) = &args.name {
            match self.benches.iter_mut().find(|x| &x.tags.get_name() == name) {
                Some(bench) => run_bench(bench, &args),
                None => {
                    eprintln!("Specified bench {name:?} does not exist.");
                    exit(1)
                }
            }
        } else {
            for bench in &mut self.benches {
                if filter(&args, bench) {
                    run_bench(bench, &args)
                }
            }
        }
        exit(0)
    }
}
fn run_bench(bench: &mut BenchState, args: &Args) {
    let runtime = create_runtime(Some(1));
    runtime.block_on(async {
        bench.run(args).await;
    });
}

fn filter(args: &cli::Args, _bench: &BenchState) -> bool {
    if args.filter.is_some() {
        todo!("Filter not yet implemented");
    }

    true
}

fn create_runtime(worker_threads: Option<usize>) -> Runtime {
    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all().thread_name("Windsock-Thread");
    if let Some(worker_threads) = worker_threads {
        runtime_builder.worker_threads(worker_threads);
    }
    runtime_builder.build().unwrap()
}
