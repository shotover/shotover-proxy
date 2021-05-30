pub mod cassandra;

use serde::{Deserialize, Serialize};

use shotover_redis_transforms::redis_cache::RedisConfig;
use shotover_redis_transforms::redis_cluster::RedisClusterConfig;
use shotover_redis_transforms::redis_codec_destination::RedisCodecConfiguration;
use shotover_transforms::TransformsFromConfig;
use shotover_util_transforms::mpsc::{BufferConfig, TeeConfig};
use shotover_util_transforms::PrinterConfig;

// This is a hack to work around https://github.com/rust-lang/rust/issues/47384
// Which causes submodules with typetag implementations not to get registered if nothing else calls them
pub fn hack() {
    let _: Box<dyn TransformsFromConfig> = Box::new(RedisConfig::default());
    let _: Box<dyn TransformsFromConfig> = Box::new(RedisClusterConfig::default());
    let _: Box<dyn TransformsFromConfig> = Box::new(RedisCodecConfiguration::default());
    let _: Box<dyn TransformsFromConfig> = Box::new(PrinterConfig::default());
    let _: Box<dyn TransformsFromConfig> = Box::new(TeeConfig::default());
    let _: Box<dyn TransformsFromConfig> = Box::new(BufferConfig::default());
}
