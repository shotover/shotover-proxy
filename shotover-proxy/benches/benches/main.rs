use criterion::criterion_main;

#[cfg(feature = "cassandra-cpp-driver-tests")]
mod cassandra;
#[cfg(not(feature = "cassandra-cpp-driver-tests"))]
mod cassandra {
    use criterion::{criterion_group, Criterion};
    fn cassandra(_: &mut Criterion) {}
    criterion_group!(benches, cassandra);
}

mod chain;
mod codec;
mod redis;

criterion_main!(
    cassandra::benches,
    redis::benches,
    chain::benches,
    codec::benches,
);
