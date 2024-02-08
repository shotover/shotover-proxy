use criterion::criterion_main;
use metrics_exporter_prometheus::PrometheusBuilder;

mod chain;
mod codec;

fn init() {
    std::env::set_var("RUST_BACKTRACE", "1");
    std::env::set_var("RUST_LIB_BACKTRACE", "0");

    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::set_boxed_recorder(Box::new(recorder)).ok();
}

criterion_main!(
    chain::benches,
    codec::kafka::benches,
    codec::cassandra::benches
);
