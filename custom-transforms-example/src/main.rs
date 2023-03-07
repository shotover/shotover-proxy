use anyhow::Result;
use clap::Parser;
// We need to import this to get the typetag to register.
// TODO: I suspect hiding it behind a `shotover_proxy::import_transform!(redis_get_rewrite::RedisGetRewriteConfig)` would look nicer.
// We could also have the macro check that it implements the TransformConfig trait
#[allow(unused_imports)]
use redis_get_rewrite::RedisGetRewriteConfig;
use shotover_proxy::runner::{ConfigOpts, Runner};

fn main() -> Result<()> {
    Runner::new(ConfigOpts::parse())?
        .with_observability_interface()?
        .run_block()
}
