use shotover::runner::Shotover;

mod redis_get_rewrite;

fn main() {
    Shotover::new().run_block();
}
