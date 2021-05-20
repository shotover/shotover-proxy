pub mod redis_cache;
pub mod redis_cluster;
pub mod redis_codec_destination;
pub mod timestamp_tagging;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
