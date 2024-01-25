use shotover::runner::Shotover;

#[cfg(feature = "redis")]
mod redis_get_rewrite;
#[cfg(feature = "redis")]
shotover::import_transform!(redis_get_rewrite::RedisGetRewriteConfig);

fn main() {
    Shotover::new().run_block();
}
