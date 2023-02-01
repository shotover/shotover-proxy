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
use tokio::runtime::{self, Runtime};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;
use tracing::{debug, error, info};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::filter::Directive;
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::fmt::format::Full;
use tracing_subscriber::fmt::format::Json;
use tracing_subscriber::fmt::format::JsonFields;
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

    // Number of tokio worker threads.
    // By default uses the number of cores on the system.
    #[clap(long)]
    pub core_threads: Option<usize>,

    // 2,097,152 = 2 * 1024 * 1024 (2MiB)
    #[clap(long, default_value = "2097152")]
    pub stack_size: usize,

    #[arg(long, value_enum, default_value = "human")]
    pub log_format: LogFormat,
}

#[derive(clap::ValueEnum, Clone)]
pub enum LogFormat {
    Human,
    Json,
}

impl Default for ConfigOpts {
    fn default() -> Self {
        Self {
            topology_file: "config/topology.yaml".into(),
            config_file: "config/config.yaml".into(),
            core_threads: None,
            stack_size: 2097152,
            log_format: LogFormat::Human,
        }
    }
}

pub struct Runner {
    runtime: Runtime,
    topology: Topology,
    config: Config,
    tracing: TracingState,
}

impl Runner {
    pub fn new(params: ConfigOpts) -> Result<Self> {
        let config = Config::from_file(params.config_file)?;
        let topology = Topology::from_file(params.topology_file)?;

        let tracing = TracingState::new(config.main_log_level.as_str(), params.log_format)?;

        let runtime = Runner::create_runtime(params.stack_size, params.core_threads);

        Ok(Runner {
            runtime,
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

        self.runtime.spawn(exporter.async_run());

        Ok(self)
    }

    pub fn run_block(self) -> Result<()> {
        let (trigger_shutdown_tx, trigger_shutdown_rx) = watch::channel(false);

        // We need to block on this part to ensure that we immediately register these signals.
        // Otherwise if we included signal creation in the below spawned task we would be at the mercy of whenever tokio decides to start running the task.
        let (mut interrupt, mut terminate) = self.runtime.block_on(async {
            (
                signal(SignalKind::interrupt()).unwrap(),
                signal(SignalKind::terminate()).unwrap(),
            )
        });
        self.runtime.spawn(async move {
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

        self.runtime
            .block_on(run(self.topology, self.config, trigger_shutdown_rx))
    }

    fn create_runtime(stack_size: usize, worker_threads: Option<usize>) -> Runtime {
        let mut runtime_builder = runtime::Builder::new_multi_thread();
        runtime_builder
            .enable_all()
            .thread_name("Shotover-Proxy-Thread")
            .thread_stack_size(stack_size);
        if let Some(worker_threads) = worker_threads {
            runtime_builder.worker_threads(worker_threads);
        }
        runtime_builder.build().unwrap()
    }
}

struct TracingState {
    /// Once this is dropped tracing logs are ignored
    _guard: WorkerGuard,
    handle: ReloadHandle,
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
    fn new(log_level: &str, format: LogFormat) -> Result<Self> {
        let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stdout());

        // Load log directives from shotover config and then from the RUST_LOG env var, with the latter taking priority.
        // In the future we might be able to simplify the implementation if work is done on tokio-rs/tracing#1466.
        let overrides = env::var(EnvFilter::DEFAULT_ENV).ok();
        let env_filter = try_parse_log_directives(&[Some(log_level), overrides.as_deref()])?;

        let handle = match format {
            LogFormat::Json => {
                let builder = tracing_subscriber::fmt()
                    .json()
                    .with_writer(non_blocking)
                    .with_env_filter(env_filter)
                    .with_filter_reloading();
                let handle = ReloadHandle::Json(builder.reload_handle());
                // To avoid unit tests that run in the same excutable from blowing up when they try to reinitialize tracing we ignore the result returned by try_init.
                // Currently the implementation of try_init will only fail when it is called multiple times.
                builder.try_init().ok();
                handle
            }
            LogFormat::Human => {
                let builder = tracing_subscriber::fmt()
                    .with_writer(non_blocking)
                    .with_env_filter(env_filter)
                    .with_filter_reloading();
                let handle = ReloadHandle::Human(builder.reload_handle());
                builder.try_init().ok();
                handle
            }
        };
        if let LogFormat::Json = format {
            crate::tracing_panic_handler::setup();
        }

        // When in json mode we need to process panics as events instead of printing directly to stdout.
        // This is so that:
        // * We dont include invalid json in stdout
        // * panics can be received by whatever is processing the json events
        //
        // We dont do this for LogFormat::Human because the default panic messages are more readable for humans
        if let LogFormat::Json = format {
            crate::tracing_panic_handler::setup();
        }

        Ok(TracingState {
            _guard: guard,
            handle,
        })
    }
}

type Formatter<A, B> = Layered<Layer<Registry, A, Format<B>, NonBlocking>, Registry>;

// TODO: We will be able to remove this and just directly use the handle once tracing 0.2 is released. See:
// * https://github.com/tokio-rs/tracing/pull/1035
// * https://github.com/linkerd/linkerd2-proxy/blob/6c484f6dcdeebda18b68c800b4494263bf98fcdc/linkerd/app/core/src/trace.rs#L19-L36
#[derive(Clone)]
pub enum ReloadHandle {
    Json(Handle<EnvFilter, Formatter<JsonFields, Json>>),
    Human(Handle<EnvFilter, Formatter<DefaultFields, Full>>),
}

impl ReloadHandle {
    pub fn reload(&self, filter: EnvFilter) -> Result<()> {
        match self {
            ReloadHandle::Json(handle) => handle.reload(filter).map_err(|e| anyhow!(e)),
            ReloadHandle::Human(handle) => handle.reload(filter).map_err(|e| anyhow!(e)),
        }
    }
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
