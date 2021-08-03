use anyhow::Result;
use shotover_proxy::config::topology::Topology;
use tokio::task::JoinHandle;

pub mod redis_int_tests;
pub mod codec;

pub fn start_proxy(config: String) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) = Topology::from_file(config)?.run_chains().await {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok(())
    })
}
