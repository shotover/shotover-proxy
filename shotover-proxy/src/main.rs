#![warn(rust_2018_idioms)]
#![recursion_limit = "256"]

use anyhow::{Context, Result};
use clap::Parser;
use shotover::runner::{ConfigOpts, Runner, TracingState};
use tokio::runtime::Runtime;

fn main() -> Result<()> {
    let opts = ConfigOpts::parse();
    let log_format = opts.log_format.clone();
    match run(opts) {
        Ok(()) => Ok(()),
        Err(err) => {
            let rt = Runtime::new()
                .context("Failed to create runtime while trying to report {err:?}")
                .unwrap();
            let _guard = rt.enter();
            let _tracing_state = TracingState::new("error", log_format)
                .context("Failed to create TracingState while trying to report {err:?}")
                .unwrap();

            tracing::error!("{:?}", err.context("Failed to start shotover"));
            std::process::exit(1);
        }
    }
}

fn run(opts: ConfigOpts) -> Result<()> {
    Runner::new(opts)?
        .with_observability_interface()?
        .run_block()
}
