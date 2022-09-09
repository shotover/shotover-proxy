use criterion::criterion_main;

mod cassandra;
mod chain;
mod codec;
#[path = "../../tests/helpers/mod.rs"]
mod helpers;
mod redis;

criterion_main!(
    cassandra::benches,
    redis::benches,
    chain::benches,
    codec::benches,
);
