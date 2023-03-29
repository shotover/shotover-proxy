use std::time::Duration;
use uuid::Uuid;

pub use tokio_bin_process::event::{Event, Level};
pub use tokio_bin_process::event_matcher::{Count, EventMatcher, Events};
pub use tokio_bin_process::BinProcess;

pub struct ShotoverProcessBuilder {
    topology_path: String,
    log_name: Option<String>,
    cores: Option<String>,
    observability_port: Option<u16>,
}

impl ShotoverProcessBuilder {
    pub fn new_with_topology(topology_path: &str) -> Self {
        Self {
            topology_path: topology_path.to_owned(),
            log_name: None,
            cores: None,
            observability_port: None,
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

    pub fn with_observability_port(mut self, port: u16) -> Self {
        self.observability_port = Some(port);
        self
    }

    pub async fn start(&self) -> BinProcess {
        let mut args = vec!["-t", &self.topology_path, "--log-format", "json"];
        if let Some(cores) = &self.cores {
            args.extend(["--core-threads", cores]);
        }
        let config_path = if let Some(observability_port) = self.observability_port {
            let config_path = std::env::temp_dir().join(Uuid::new_v4().to_string());
            let config_contents = format!(
                r#"
---
main_log_level: "info,shotover_proxy=info"
observability_interface: "127.0.0.1:{observability_port}"
"#
            );
            std::fs::write(&config_path, config_contents).unwrap();
            config_path.into_os_string().into_string().unwrap()
        } else {
            "config/config.yaml".to_owned()
        };
        args.extend(["-c", &config_path]);

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
                    .with_target("shotover::server")
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
