use crate::config::topology::Topology;
use crate::config::Config;
use crate::observability::LogFilterHttpExporter;
use crate::transforms::Transforms;
use crate::transforms::Wrapper;
use anyhow::{anyhow, Result};
use clap::{crate_version, Parser};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::env;
use std::net::SocketAddr;
use tokio::runtime::{self, Handle as RuntimeHandle, Runtime};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::filter::Directive;
use tracing_subscriber::fmt::format::{DefaultFields, Format};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::Layered;
use tracing_subscriber::reload::Handle;
use tracing_subscriber::{EnvFilter, Registry};

#[derive(Parser, Clone)]
#[clap(version = crate_version!(), author = "Instaclustr")]
pub struct ConfigOpts {
    #[clap(short, long, default_value = "config/topology.yaml")]
    pub topology_file: String,

    #[clap(short, long, default_value = "config/config.yaml")]
    pub config_file: String,

    #[clap(long, default_value = "4")]
    pub core_threads: usize,
    // 2,097,152 = 2 * 1024 * 1024 (2MiB)
    #[clap(long, default_value = "2097152")]
    pub stack_size: usize,
}

impl Default for ConfigOpts {
    fn default() -> Self {
        Self {
            topology_file: "config/topology.yaml".into(),
            config_file: "config/config.yaml".into(),
            core_threads: 4,
            stack_size: 2097152,
        }
    }
}

pub struct Runner {
    runtime: Option<Runtime>,
    runtime_handle: RuntimeHandle,
    topology: Topology,
    config: Config,
    tracing: TracingState,
}

impl Runner {
    pub fn new(params: ConfigOpts) -> Result<Self> {
        let config = Config::from_file(params.config_file)?;
        let topology = Topology::from_file(params.topology_file)?;

        let tracing = TracingState::new(config.main_log_level.as_str())?;

        let (runtime_handle, runtime) = Runner::get_runtime(params.stack_size, params.core_threads);

        Ok(Runner {
            runtime,
            runtime_handle,
            topology,
            config,
            tracing,
        })
    }

    pub fn with_observability_interface(self) -> Result<Self> {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::set_boxed_recorder(Box::new(recorder))?;

        let socket: SocketAddr = self.config.observability_interface.parse()?;
        let exporter = LogFilterHttpExporter::new(handle, socket, self.tracing.handle.clone());

        self.runtime_handle.spawn(exporter.async_run());

        Ok(self)
    }

    pub fn run_spawn(self) -> RunnerSpawned {
        let (trigger_shutdown_tx, trigger_shutdown_rx) = watch::channel(false);

        let join_handle =
            self.runtime_handle
                .spawn(run(self.topology, self.config, trigger_shutdown_rx));

        RunnerSpawned {
            runtime_handle: self.runtime_handle,
            runtime: self.runtime,
            tracing_guard: self.tracing.guard,
            trigger_shutdown_tx,
            join_handle,
        }
    }

    pub fn run_block(self) -> Result<()> {
        let (trigger_shutdown_tx, trigger_shutdown_rx) = watch::channel(false);

        self.runtime_handle.spawn(async move {
            let mut interrupt = signal(SignalKind::interrupt()).unwrap();
            let mut terminate = signal(SignalKind::terminate()).unwrap();

            tokio::select! {
                _ = interrupt.recv() => {
                    info!("received SIGINT");
                },
                _ = terminate.recv() => {
                    info!("received SIGTERM");
                },
            };

            trigger_shutdown_tx.send(true).unwrap();
        });

        self.runtime_handle
            .block_on(run(self.topology, self.config, trigger_shutdown_rx))
    }

    /// Get handle for an existing runtime or create one
    fn get_runtime(stack_size: usize, core_threads: usize) -> (RuntimeHandle, Option<Runtime>) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Using block_in_place to trigger a panic in case the runtime is set up in single-threaded mode.
            // Shotover does not function correctly in single threaded mode (currently hangs)
            // and block_in_place gives an error message explaining to setup the runtime in multi-threaded mode.
            // This does not protect us when calling Runtime::enter() or when no runtime is set up at all.
            tokio::task::block_in_place(|| {});

            (handle, None)
        } else {
            let runtime = runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("Shotover-Proxy-Thread")
                .thread_stack_size(stack_size)
                .worker_threads(core_threads)
                .build()
                .unwrap();

            (runtime.handle().clone(), Some(runtime))
        }
    }
}

type TracingStateHandle =
    Handle<EnvFilter, Layered<Layer<Registry, DefaultFields, Format, NonBlocking>, Registry>>;

struct TracingState {
    /// Once this is dropped tracing logs are ignored
    guard: WorkerGuard,
    handle: TracingStateHandle,
}

/// Returns a new `EnvFilter` by parsing each directive string, or an error if any directive is invalid.
/// The parsing is robust to formatting, but will reject the first invalid directive (e.g. bad log level).
fn try_parse_log_directives(directives: &[Option<&str>]) -> Result<EnvFilter> {
    let directives: Vec<Directive> = directives
        .iter()
        .flat_map(Option::as_deref)
        .flat_map(|s| s.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().map_err(|e| anyhow!("{}: {}", e, s)))
        .collect::<Result<_>>()?;

    let filter = directives
        .into_iter()
        .fold(EnvFilter::default(), |filter, directive| {
            filter.add_directive(directive)
        });

    Ok(filter)
}

impl TracingState {
    fn new(log_level: &str) -> Result<Self> {
        let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());

        let builder = tracing_subscriber::fmt()
            .with_writer(non_blocking)
            .with_env_filter({
                // Load log directives from shotover config and then from the RUST_LOG env var, with the latter taking priority.
                // In the future we might be able to simplify the implementation if work is done on tokio-rs/tracing#1466.
                let overrides = env::var(EnvFilter::DEFAULT_ENV).ok();
                try_parse_log_directives(&[Some(log_level), overrides.as_deref()])?
            })
            .with_filter_reloading();
        let handle = builder.reload_handle();

        // To avoid unit tests that run in the same excutable from blowing up when they try to reinitialize tracing we ignore the result returned by try_init.
        // Currently the implementation of try_init will only fail when it is called multiple times.
        builder.try_init().ok();

        Ok(TracingState { guard, handle })
    }
}

pub struct RunnerSpawned {
    pub runtime: Option<Runtime>,
    pub runtime_handle: RuntimeHandle,
    pub join_handle: JoinHandle<Result<()>>,
    pub tracing_guard: WorkerGuard,
    pub trigger_shutdown_tx: watch::Sender<bool>,
}

pub async fn run(
    topology: Topology,
    config: Config,
    trigger_shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    info!("Starting Shotover {}", crate_version!());
    info!(configuration = ?config);
    info!(topology = ?topology);

    debug!(
        "Transform overhead size on stack is {}",
        std::mem::size_of::<Transforms>()
    );

    debug!(
        "Wrapper overhead size on stack is {}",
        std::mem::size_of::<Wrapper<'_>>()
    );

    match topology.run_chains(trigger_shutdown_rx).await {
        Ok(sources) => {
            futures::future::join_all(sources.into_iter().map(|x| x.into_join_handle())).await;
            info!("Shotover was shutdown cleanly.");
            Ok(())
        }
        Err(error) => {
            error!("{:?}", error);
            Err(anyhow!(
                "Shotover failed to initialize, the fatal error was logged."
            ))
        }
    }
}

#[test]
fn test_try_parse_log_directives() {
    assert_eq!(
        try_parse_log_directives(&[
            Some("info,short=warn,error"),
            None,
            Some("debug"),
            Some("alongname=trace")
        ])
        .unwrap()
        .to_string(),
        // Ordered by descending specificity.
        "alongname=trace,short=warn,debug"
    );
    match try_parse_log_directives(&[Some("good=info,bad=blah,warn")]) {
        Ok(_) => panic!(),
        Err(e) => assert_eq!(e.to_string(), "invalid filter directive: bad=blah"),
    }
}
