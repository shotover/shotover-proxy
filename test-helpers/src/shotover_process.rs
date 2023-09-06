use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

pub use tokio_bin_process::bin_path;
pub use tokio_bin_process::event::{Event, Level};
pub use tokio_bin_process::event_matcher::{Count, EventMatcher, Events};
pub use tokio_bin_process::BinProcess;

pub struct ShotoverProcessBuilder {
    topology_path: String,
    bin_path: Option<PathBuf>,
    log_name: Option<String>,
    cores: Option<String>,
    profile: Option<String>,
    observability_port: Option<u16>,
    event_matchers: Vec<EventMatcher>,
}

impl ShotoverProcessBuilder {
    pub fn new_with_topology(topology_path: &str) -> Self {
        Self {
            topology_path: topology_path.to_owned(),
            bin_path: None,
            log_name: None,
            cores: None,
            profile: None,
            observability_port: None,
            event_matchers: vec![],
        }
    }

    /// Hint that there is a precompiled shotover binary available.
    /// This binary will be used unless a profile is specified.
    pub fn with_bin(mut self, bin_path: &Path) -> Self {
        self.bin_path = Some(bin_path.to_owned());
        self
    }

    /// Prefix forwarded shotover logs with the provided string.
    /// Use this when there are multiple shotover instances to give them a unique name in the logs.
    pub fn with_log_name(mut self, log_name: &str) -> Self {
        self.log_name = Some(log_name.to_owned());
        self
    }

    /// Run shotover with the specified number of cores
    pub fn with_cores(mut self, cores: u32) -> Self {
        self.cores = Some(cores.to_string());
        self
    }

    /// Force shotover to be compiled with the specified profile
    pub fn with_profile(mut self, profile: Option<&str>) -> Self {
        if let Some(profile) = profile {
            self.profile = Some(profile.to_string());
        }
        self
    }

    pub fn with_observability_port(mut self, port: u16) -> Self {
        self.observability_port = Some(port);
        self
    }

    pub fn expect_startup_events(mut self, matchers: Vec<EventMatcher>) -> Self {
        self.event_matchers = matchers;
        self
    }

    pub async fn start(&self) -> BinProcess {
        let mut shotover = self.start_inner().await;

        tokio::time::timeout(
            Duration::from_secs(30),
            shotover.wait_for(
                &EventMatcher::new()
                    .with_level(Level::Info)
                    .with_message("Shotover is now accepting inbound connections"),
                &self.event_matchers,
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
        self.start_inner()
            .await
            .consume_remaining_events_expect_failure(expected_errors_and_warnings)
            .await
    }

    async fn start_inner(&self) -> BinProcess {
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

        let log_name = self.log_name.as_deref().unwrap_or("shotover");

        match (&self.profile, &self.bin_path) {
            (Some(profile), _) => {
                BinProcess::start_binary_name("shotover-proxy", log_name, &args, Some(profile))
                    .await
            }
            (None, Some(bin_path)) => BinProcess::start_binary(bin_path, log_name, &args).await,
            (None, None) => {
                BinProcess::start_binary_name("shotover-proxy", log_name, &args, None).await
            }
        }
    }
}
