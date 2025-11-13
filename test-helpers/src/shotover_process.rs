use std::path::PathBuf;
use std::time::Duration;

pub use tokio_bin_process::BinProcess;
pub use tokio_bin_process::BinProcessBuilder;
pub use tokio_bin_process::bin_path;
pub use tokio_bin_process::event::{Event, Level};
pub use tokio_bin_process::event_matcher::{Count, EventMatcher, Events};

pub struct ShotoverProcessBuilder {
    topology_path: String,
    config_path: Option<String>,
    bin_path: Option<PathBuf>,
    log_name: Option<String>,
    cores: Option<String>,
    profile: Option<String>,
    event_matchers: Vec<EventMatcher>,
    additional_args: Vec<String>,
}

impl ShotoverProcessBuilder {
    pub fn new_with_topology(topology_path: &str) -> Self {
        // Run setup here to ensure any test that calls this gets tracing
        crate::test_tracing::setup_tracing_subscriber_for_test();

        Self {
            topology_path: topology_path.to_owned(),
            config_path: None,
            bin_path: None,
            log_name: None,
            cores: None,
            profile: None,
            event_matchers: vec![],
            additional_args: vec![],
        }
    }

    /// Specify the config file path, if none specified will use `config/config.yaml`
    pub fn with_config(mut self, path: &str) -> Self {
        self.config_path = Some(path.to_owned());
        self
    }

    /// Hint that there is a precompiled shotover binary available.
    /// This binary will be used unless a profile is specified.
    pub fn with_bin(mut self, bin_path: PathBuf) -> Self {
        self.bin_path = Some(bin_path);
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

    /// Enable hot reload at the specified socket path.
    /// If the socket exists, shotover will connect for hot reload.
    /// If the socket doesn't exist, shotover will create it and enable hot reload server.
    pub fn with_hotreload_socket(mut self, socket_path: &str) -> Self {
        self.additional_args
            .push("--hotreload-socket".to_string());
        self.additional_args.push(socket_path.to_string());
        self
    }

    pub fn expect_startup_events(mut self, matchers: Vec<EventMatcher>) -> Self {
        self.event_matchers = matchers;
        self
    }

    pub async fn start(self) -> BinProcess {
        let (mut shotover, event_matchers) = self.start_inner().await;

        tokio::time::timeout(
            Duration::from_secs(30),
            shotover.wait_for(
                &EventMatcher::new()
                    .with_level(Level::Info)
                    .with_message("Shotover is now accepting inbound connections"),
                &event_matchers,
            ),
        )
        .await
        .unwrap();

        shotover
    }

    pub async fn assert_fails_to_start(
        self,
        expected_errors_and_warnings: &[EventMatcher],
    ) -> Events {
        self.start_inner()
            .await
            .0
            .consume_remaining_events_expect_failure(expected_errors_and_warnings)
            .await
    }

    async fn start_inner(self) -> (BinProcess, Vec<EventMatcher>) {
        let mut args = vec![
            "-t".to_owned(),
            self.topology_path,
            "--log-format".to_owned(),
            "json".to_owned(),
        ];
        if let Some(cores) = self.cores {
            args.extend(["--core-threads".to_owned(), cores]);
        }
        let config_path = self
            .config_path
            .clone()
            .unwrap_or_else(|| "config/config.yaml".to_owned());
        args.extend(["-c".to_owned(), config_path]);

        args.extend(self.additional_args);

        let log_name = self.log_name.unwrap_or_else(|| "shotover".to_owned());

        let builder = match (self.profile, self.bin_path) {
            (Some(profile), _) => {
                BinProcessBuilder::from_cargo_name("shotover-proxy".to_owned(), Some(profile))
            }
            (None, Some(bin_path)) => BinProcessBuilder::from_path(bin_path),
            (None, None) => BinProcessBuilder::from_cargo_name("shotover-proxy".to_owned(), None),
        };
        let process = builder
            .with_log_name(Some(log_name))
            .with_args(args)
            // Overwrite any existing AWS credential env vars belonging to the user with dummy values to be sure that
            // shotover wont run with their real AWS account
            //
            // This also enables tests to run against moto mock AWS service as shotover's AWS client will give up
            // if it cant find a key even though moto will accept any key.
            .with_env_vars(vec![
                (
                    "AWS_ACCESS_KEY_ID".to_owned(),
                    "dummy-access-key".to_owned(),
                ),
                (
                    "AWS_SECRET_ACCESS_KEY".to_owned(),
                    "dummy-access-key-secret".to_owned(),
                ),
            ])
            .start()
            .await;

        (process, self.event_matchers)
    }
}
