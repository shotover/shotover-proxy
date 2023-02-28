use anyhow::Result;
use clap::Parser;

use shotover_proxy::runner::{ConfigOpts, Runner};

mod redis_get_rewrite;

fn main() -> Result<()> {
    Runner::new(ConfigOpts::parse())?
        .with_observability_interface()?
        .run_block()
}
