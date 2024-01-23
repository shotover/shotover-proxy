use super::{
    Ec2InstanceWithBencher, Ec2InstanceWithDocker, Ec2InstanceWithShotover, AWS, AWS_THROWAWAY_TAG,
};
use async_trait::async_trait;
use aws_throwaway::{Aws, CleanupResources};
use std::sync::Arc;
use windsock::cloud::{BenchInfo, Cloud};

pub struct AwsCloud;

impl AwsCloud {
    pub fn new_boxed() -> Box<
        dyn Cloud<CloudResourcesRequired = CloudResourcesRequired, CloudResources = CloudResources>,
    > {
        Box::new(AwsCloud)
    }
}

#[async_trait(?Send)]
impl Cloud for AwsCloud {
    type CloudResourcesRequired = CloudResourcesRequired;
    type CloudResources = CloudResources;
    async fn cleanup_resources(&self) {
        match AWS.get() {
            // AWS is initialized, it'll be faster to cleanup resources making use of the initialization
            Some(aws) => aws.cleanup_resources().await,
            // AWS is not initialized, it'll be faster to cleanup resources skipping initialization
            None => {
                Aws::cleanup_resources_static(CleanupResources::WithAppTag(
                    AWS_THROWAWAY_TAG.to_owned(),
                ))
                .await
            }
        }
    }

    async fn create_resources(&self, required: Vec<CloudResourcesRequired>) -> CloudResources {
        let required = required.into_iter().fold(
            CloudResourcesRequired::default(),
            CloudResourcesRequired::combine,
        );
        println!("Creating AWS resources: {required:#?}");

        // TODO: make Option<WindsockAws> field of AwsCloud
        let aws = crate::aws::WindsockAws::get().await;

        let (docker, mut bencher, shotover) = futures::join!(
            aws.create_docker_instances(
                required.include_shotover_in_docker_instance,
                required.docker_instance_count
            ),
            aws.create_bencher_instances(1),
            aws.create_shotover_instances(required.shotover_instance_count)
        );
        let bencher = bencher.pop();
        CloudResources {
            shotover,
            docker,
            bencher,
        }
    }

    fn order_benches(
        &self,
        benches: Vec<BenchInfo<CloudResourcesRequired>>,
    ) -> Vec<BenchInfo<CloudResourcesRequired>> {
        // TODO: put benches with most resources first
        benches
    }

    async fn adjust_resources(
        &self,
        _benches: &[BenchInfo<CloudResourcesRequired>],
        _bench_index: usize,
        resources: &mut CloudResources,
    ) {
        for instance in &resources.docker {
            if Arc::strong_count(instance) != 1 {
                panic!("A reference to a docker instance has been held past the end of the benchmark. Ensure the benchmark destroys all instance references before ending.")
            }
        }
        for instance in &resources.shotover {
            if Arc::strong_count(instance) != 1 {
                panic!("A reference to a shotover instance has been held past the end of the benchmark. Ensure the benchmark destroys all instance references before ending.")
            }
        }
        if let Some(instance) = &resources.bencher {
            if Arc::strong_count(instance) != 1 {
                panic!("A reference to a bencher instance has been held past the end of the benchmark. Ensure the benchmark destroys all instance references before ending.")
            }
        }

        // TODO: spin up background tokio task to delete unneeded EC2 instances once we add the functionality to aws_throwaway
    }
}

#[derive(Clone, Debug, Default)]
pub struct CloudResourcesRequired {
    pub shotover_instance_count: usize,
    pub docker_instance_count: usize,
    /// When set to true all docker instances are also capable of hosting shotover
    pub include_shotover_in_docker_instance: bool,
}

impl CloudResourcesRequired {
    fn combine(self, other: Self) -> Self {
        CloudResourcesRequired {
            shotover_instance_count: self
                .shotover_instance_count
                .max(other.shotover_instance_count),
            docker_instance_count: self.docker_instance_count.max(other.docker_instance_count),
            include_shotover_in_docker_instance: self.include_shotover_in_docker_instance
                || other.include_shotover_in_docker_instance,
        }
    }
}

#[derive(Clone)]
pub struct CloudResources {
    pub shotover: Vec<Arc<Ec2InstanceWithShotover>>,
    pub docker: Vec<Arc<Ec2InstanceWithDocker>>,
    pub bencher: Option<Arc<Ec2InstanceWithBencher>>,
}
