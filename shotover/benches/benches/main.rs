use criterion::criterion_main;

mod chain;
mod codec;

criterion_main!(chain::benches, codec::benches);
