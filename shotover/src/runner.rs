//! Tools for initializing shotover in the final binary.
use crate::config::Config;
use crate::config::topology::Topology;
use crate::observability::LogFilterHttpExporter;
use anyhow::Context;
use anyhow::{Result, anyhow};
use clap::{Parser, crate_version};
use metrics_exporter_prometheus::PrometheusBuilder;
use rustls::crypto::aws_lc_rs::default_provider;
use std::env;
use std::net::SocketAddr;
use tokio::runtime::{self, Runtime};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::{error, info};
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::filter::Directive;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::fmt::format::Format;
use tracing_subscriber::fmt::format::Full;
use tracing_subscriber::fmt::format::Json;
use tracing_subscriber::fmt::format::JsonFields;
use tracing_subscriber::layer::Layered;
use tracing_subscriber::reload::Handle;
use tracing_subscriber::{EnvFilter, Registry};

#[derive(Parser, Clone)]
#[clap(version = crate_version!(), author = "Instaclustr")]
struct ConfigOpts {
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

    /// Enable hot reloading functionality
    #[clap(long)]
    pub hotreload: bool,

    /// Path for Unix socket used in hot reload communication
    #[clap(long, default_value = "/tmp/shotover-hotreload.sock")]
    pub hotreload_socket_path: String,
}

#[derive(clap::ValueEnum, Clone, Copy)]
enum LogFormat {
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
            hotreload: false,
            hotreload_socket_path: "/tmp/shotover-hotreload.sock".to_string(),
        }
    }
}

pub struct Shotover {
    runtime: Runtime,
    topology: Topology,
    config: Config,
    tracing: TracingState,
    hotreload_enabled: bool,
    hotreload_socket_path: String,
}

impl Shotover {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        if std::env::var("RUST_LIB_BACKTRACE").is_err() {
            std::env::set_var("RUST_LIB_BACKTRACE", "0");
        }

        default_provider().install_default().unwrap();

        let opts = ConfigOpts::parse();
        let log_format = opts.log_format;

        match Shotover::new_inner(opts) {
            Ok(x) => x,
            Err(err) => {
                // If initialization failed then we have no tokio runtime or tracing to use.
                // Create the simplest runtime + tracing so we can write out an `error!`, if even that fails then just panic.
                // Put it all in its own scope so we drop it (and therefore perform tracing log flushing) before we exit
                {
                    let rt = Runtime::new()
                        .context("Failed to create runtime while trying to report {err:?}")
                        .unwrap();
                    let _guard = rt.enter();
                    let _tracing_state = TracingState::new("error", log_format)
                        .context("Failed to create TracingState while trying to report {err:?}")
                        .unwrap();

                    tracing::error!("{:?}", err.context("Failed to start shotover"));
                }
                std::process::exit(1);
            }
        }
    }

    fn new_inner(params: ConfigOpts) -> Result<Self> {
        let config = Config::from_file(params.config_file)?;
        let topology = Topology::from_file(&params.topology_file)?;
        let tracing = TracingState::new(config.main_log_level.as_str(), params.log_format)?;
        let runtime = Shotover::create_runtime(params.stack_size, params.core_threads);

        // Log hot reload status
        if params.hotreload {
            tracing::info!(
                "Hot reloading is ENABLED - shotover will support hot reload operations"
            );
        } else {
            tracing::debug!("Hot reloading is disabled");
        }

        Shotover::start_observability_interface(&runtime, &config, &tracing)?;

        Ok(Shotover {
            runtime,
            topology,
            config,
            tracing,
            hotreload_enabled: params.hotreload,
            hotreload_socket_path: params.hotreload_socket_path,
        })
    }

    fn start_observability_interface(
        runtime: &Runtime,
        config: &Config,
        tracing: &TracingState,
    ) -> Result<()> {
        if let Some(observability_interface) = &config.observability_interface {
            let recorder = PrometheusBuilder::new()
                .set_quantiles(&[0.0, 0.1, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0])
                .unwrap()
                .build_recorder();
            let handle = recorder.handle();
            metrics::set_global_recorder(recorder)?;

            let socket: SocketAddr = observability_interface.parse()?;
            let exporter = LogFilterHttpExporter::new(handle, socket, tracing.handle.clone());

            runtime.spawn(exporter.async_run());
        }
        Ok(())
    }

    /// Begins running shotover, permanently handing control of the appplication over to shotover.
    /// As such this method never returns.
    pub fn run_block(self) -> ! {
        let (trigger_shutdown_tx, trigger_shutdown_rx) = watch::channel(false);

        // Setup Unix socket server for hot reload if enabled
        if self.hotreload_enabled {
            info!("Starting shotover with hot reloading enabled");

            let socket_path = self.hotreload_socket_path.clone();
            self.runtime.spawn(async move {
                let mut server = crate::unix_socket_server::UnixSocketServer::new(socket_path);
                match server.start().await {
                    Ok(()) => {
                        info!("Unix socket server started for hot reload communication");
                        if let Err(e) = server.run().await {
                            error!("Unix socket server error: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Failed to start Unix socket server: {:?}", e);
                    }
                }
            });
        }

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

        let code = match self
            .runtime
            .block_on(run(self.topology, self.config, trigger_shutdown_rx))
        {
            Ok(()) => {
                info!("Shotover was shutdown cleanly.");
                0
            }
            Err(err) => {
                error!("{:?}", err.context("Failed to start shotover"));
                1
            }
        };
        // Ensure tracing is flushed by dropping before exiting
        std::mem::drop(self.tracing);
        std::mem::drop(self.runtime);
        std::process::exit(code);
    }

    fn create_runtime(stack_size: usize, worker_threads: Option<usize>) -> Runtime {
        let mut runtime_builder = runtime::Builder::new_multi_thread();
        runtime_builder
            .enable_all()
            .thread_name("shotover-worker")
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
    pub fn new(log_level: &str, format: LogFormat) -> Result<Self> {
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
                builder.init();
                handle
            }
            LogFormat::Human => {
                let builder = tracing_subscriber::fmt()
                    .with_writer(non_blocking)
                    .with_env_filter(env_filter)
                    .with_filter_reloading();
                let handle = ReloadHandle::Human(builder.reload_handle());
                builder.init();
                handle
            }
        };

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
pub(crate) enum ReloadHandle {
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

async fn run(
    topology: Topology,
    config: Config,
    trigger_shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    info!("Starting Shotover {}", crate_version!());
    info!(configuration = ?config);
    info!(topology = ?topology);

    match topology.run_chains(trigger_shutdown_rx).await {
        Ok(sources) => {
            futures::future::join_all(sources.into_iter().map(|x| x.into_join_handle())).await;
            Ok(())
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

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
}
