use std::time::Duration;

pub use tokio_bin_process::event::{Event, Level};
pub use tokio_bin_process::event_matcher::{Count, EventMatcher, Events};
pub use tokio_bin_process::BinProcess;

pub struct ShotoverProcessBuilder {
    topology_path: String,
    log_name: Option<String>,
    cores: Option<String>,
}

impl ShotoverProcessBuilder {
    pub fn new_with_topology(topology_path: &str) -> Self {
        Self {
            topology_path: topology_path.to_owned(),
            log_name: None,
            cores: None,
        }
    }

    pub fn with_log_name(mut self, log_name: &str) -> Self {
        self.log_name = Some(log_name.to_owned());
        self
    }

    pub fn with_cores(mut self, cores: u32) -> Self {
        self.cores = Some(cores.to_string());
        self
    }

    pub async fn start(&self) -> BinProcess {
        let mut args = vec!["-t", &self.topology_path, "--log-format", "json"];
        if let Some(cores) = &self.cores {
            args.extend(["--core-threads", cores]);
        }
        let mut shotover = BinProcess::start_with_args(
            "shotover-proxy",
            &args,
            self.log_name.as_deref().unwrap_or("shotover"),
        )
        .await;

        tokio::time::timeout(
            Duration::from_secs(30),
            shotover.wait_for(
                &EventMatcher::new()
                    .with_level(Level::Info)
                    .with_target("shotover_proxy::server")
                    .with_message("accepting inbound connections"),
            ),
        )
        .await
        .unwrap();

        shotover
    }

    pub async fn assert_fails_to_start(
        &self,
        expected_errors_and_warnings: &[EventMatcher],
    ) -> Events {
        BinProcess::start_with_args(
            "shotover-proxy",
            &["-t", &self.topology_path, "--log-format", "json"],
            "Shotover",
        )
        .await
        .consume_remaining_events_expect_failure(expected_errors_and_warnings)
        .await
    }
}
