use shotover::runner::Shotover;

#[cfg(feature = "valkey")]
mod valkey_get_rewrite;
#[cfg(feature = "valkey")]
shotover::import_transform!(valkey_get_rewrite::ValkeyGetRewriteConfig);

fn main() {
    Shotover::new().run_block();
}
