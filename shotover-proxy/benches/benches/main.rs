use criterion::criterion_main;

mod cassandra;
mod chain;
mod codec;
mod redis;

criterion_main!(
    cassandra::benches,
    redis::benches,
    chain::benches,
    codec::benches,
);
