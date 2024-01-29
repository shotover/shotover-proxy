use criterion::criterion_main;

mod chain;
mod codec;

fn init() {
    std::env::set_var("RUST_BACKTRACE", "1");
    std::env::set_var("RUST_LIB_BACKTRACE", "0");
}

criterion_main!(
    chain::benches,
    codec::kafka::benches,
    codec::cassandra::benches
);
