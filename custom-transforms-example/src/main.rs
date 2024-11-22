use shotover::runner::Shotover;

#[cfg(feature = "redis")]
mod valkey_get_rewrite;
#[cfg(feature = "redis")]
shotover::import_transform!(valkey_get_rewrite::ValkeyGetRewriteConfig);

fn main() {
    Shotover::new().run_block();
}
