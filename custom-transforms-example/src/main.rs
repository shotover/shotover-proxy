use shotover::runner::Shotover;

mod redis_get_rewrite;
shotover::import_transform!(redis_get_rewrite::RedisGetRewriteConfig);

fn main() {
    Shotover::new().run_block();
}
