use async_trait::async_trait;
use aws_throwaway::Aws;
use windsock::cloud::Cloud;

use super::AWS;

pub struct AwsCloud;

impl AwsCloud {
    pub fn new_boxed() -> Box<dyn Cloud> {
        Box::new(AwsCloud)
    }
}

#[async_trait]
impl Cloud for AwsCloud {
    async fn cleanup_resources(&self) {
        match AWS.get() {
            // AWS is initialized, it'll be faster to cleanup resources making use of the initialization
            Some(aws) => aws.cleanup_resources().await,
            // AWS is not initialized, it'll be faster to cleanup resources skipping initialization
            None => Aws::cleanup_resources_static().await,
        }
    }
}
