use anyhow::Result;
use clap::Parser;

use shotover::runner::{ConfigOpts, Runner};

mod redis_get_rewrite;

fn main() -> Result<()> {
    // TODO: this API should become just `Runner::new().run_block()`
    // * User shouldnt have to provide ConfigOpts
    // * The user shouldnt have to manually set `with_observability_interface`
    // * Runner::new() should internally perform the backup tracing reporter logic that is currently in `shotover-proxy/src/main.rs`
    // * run_block should also return `!`
    // * Using a basic builder gives us room to non-breakingly expand the API in the future
    Runner::new(ConfigOpts::parse())?
        .with_observability_interface()?
        .run_block()
}
