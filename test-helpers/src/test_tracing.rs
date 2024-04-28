use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

pub fn setup_tracing_subscriber_for_test() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .unwrap(),
        )
        .try_init()
        .ok();
}
