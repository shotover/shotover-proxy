#![warn(rust_2018_idioms)]
#![recursion_limit = "256"]

use std::error::Error;

use clap::Clap;

use rust_practice::config::topology::Topology;
use rust_practice::sources::Sources;
use slog::info;
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::Severity;
use sloggers::Build;

#[derive(Clap)]
#[clap(version = "0.1", author = "Ben B. <ben.bromhead@gmail.com>")]
struct ConfigOpts {
    #[clap(short, long, default_value = "config/config.yaml")]
    pub config_file: String,
}

//TODO: manually build the tokio engine so users can configure thread count and scheduling properties
#[cfg(not(feature = "no_index"))]
#[cfg(not(feature = "no_object"))]
#[tokio::main(core_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);

    let logger = builder.build().unwrap();
    info!(logger, "Loading configuration");

    let configuration = ConfigOpts::parse();
    info!(logger, "Starting loaded topology");

    if let Ok(sources) = Topology::from_file(configuration.config_file)?
        .run_chains(&logger)
        .await
    {
        //TODO: probably a better way to handle various join handles / threads
        for s in sources {
            let _ = match s {
                Sources::Cassandra(c) => tokio::join!(c.join_handle),
                Sources::Mpsc(m) => tokio::join!(m.rx_handle),
                Sources::Redis(r) => tokio::join!(r.join_handle)
            };
        }
        info!(logger, "Goodbye!");
    }
    Ok(())
}
