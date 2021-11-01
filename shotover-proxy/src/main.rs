#![warn(rust_2018_idioms)]
#![recursion_limit = "256"]

use anyhow::Result;
use clap::Parser;

use shotover_proxy::runner::{ConfigOpts, Runner};

fn main() -> Result<()> {
    Runner::new(ConfigOpts::parse())?
        .with_observability_interface()?
        .run_block()
}
