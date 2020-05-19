#![warn(rust_2018_idioms)]
#![recursion_limit="256"]

use std::error::Error;

use rust_practice::sources::{Sources};
use rust_practice::config::topology::Topology;


#[tokio::main(core_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    if let Ok(sources) = Topology::get_demo_config().run_chains().await {
        //TODO: probably a better way to handle various join handles / threads
        for s in sources {
            let _ = match s {
                Sources::Cassandra(c) => {tokio::join!(c.join_handle)},
                Sources::Mpsc(m) => {tokio::join!(m.rx_handle)},
            };
        }
    }
    Ok(())
}