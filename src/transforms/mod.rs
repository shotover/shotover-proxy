use serde::{Deserialize, Serialize};

use shotover_redis_transforms::redis_cache::RedisConfig;
use shotover_redis_transforms::redis_cluster::RedisClusterConfig;
use shotover_redis_transforms::redis_codec_destination::RedisCodecConfiguration;
use shotover_transforms::TransformsFromConfig;
use shotover_util_transforms::PrinterConfig;

// This is a hack to work around https://github.com/rust-lang/rust/issues/47384
// Which causes submodules with typetag implementations not to get registered if nothing else calls them
pub fn hack() {
    let _: Box<dyn TransformsFromConfig> = Box::new(PrinterConfig {});
    let _: Box<dyn TransformsFromConfig> = Box::new(RedisConfig {
        uri: "".to_string(),
    });
    let _: Box<dyn TransformsFromConfig> = Box::new(RedisClusterConfig {
        first_contact_points: vec![],
        strict_close_mode: None,
        connection_count: None,
    });
    let _: Box<dyn TransformsFromConfig> = Box::new(RedisCodecConfiguration {
        address: "".to_string(),
    });
    let _: Box<dyn TransformsFromConfig> = Box::new(PrinterConfig {});
}
