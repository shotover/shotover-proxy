//! Windsock specific logic built on top of aws_throwaway

mod aws;

pub use aws::{
    Ec2InstanceWithBencher, Ec2InstanceWithDocker, Ec2InstanceWithShotover, RunningShotover,
};

use anyhow::Result;
use async_trait::async_trait;
use aws::AwsInstances;
use futures::StreamExt;
use futures::{Future, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::{path::Path, sync::Arc};
use windsock::cloud::{BenchInfo, Cloud};

pub struct AwsCloud {
    aws: Option<AwsInstances>,
}

impl AwsCloud {
    pub fn new_boxed() -> Box<
        dyn Cloud<CloudResourcesRequired = CloudResourcesRequired, CloudResources = CloudResources>,
    > {
        Box::new(AwsCloud { aws: None })
    }
}

#[async_trait(?Send)]
impl Cloud for AwsCloud {
    type CloudResourcesRequired = CloudResourcesRequired;
    type CloudResources = CloudResources;
    async fn cleanup_resources(&mut self) {
        match &self.aws {
            // AWS is initialized, it'll be faster to cleanup resources making use of the initialization
            Some(aws) => aws.cleanup_resources().await,
            // AWS is not initialized, it'll be faster to cleanup resources skipping initialization
            None => AwsInstances::cleanup().await,
        }
        println!("All AWS throwaway resources have been deleted");
    }

    async fn create_resources(
        &mut self,
        required: Vec<CloudResourcesRequired>,
        benches_will_run: bool,
    ) -> CloudResources {
        let required = required.into_iter().fold(
            CloudResourcesRequired::default(),
            CloudResourcesRequired::combine,
        );
        println!("Creating AWS resources: {required:#?}");

        if self.aws.is_none() {
            self.aws = Some(crate::cloud::AwsInstances::new().await);
        }

        let aws = self.aws.as_ref().unwrap();
        let (docker, mut bencher, shotover) = futures::join!(
            aws.create_docker_instances(
                benches_will_run,
                required.include_shotover_in_docker_instance,
                required.docker_instance_count
            ),
            aws.create_bencher_instances(benches_will_run, 1),
            aws.create_shotover_instances(benches_will_run, required.shotover_instance_count)
        );

        println!();
        println!("In order to investigate bench failures you may want to ssh into the instances:");
        for (i, docker) in docker.iter().enumerate() {
            println!("Docker instance #{i}");
            println!("{}\n", docker.instance.ssh_instructions());
        }
        for (i, shotover) in shotover.iter().enumerate() {
            println!("Shotover instance #{i}");
            println!("{}\n", shotover.instance.ssh_instructions());
        }
        for (i, bencher) in bencher.iter().enumerate() {
            println!("Bencher instance #{i}");
            println!("{}\n", bencher.instance.ssh_instructions());
        }

        let bencher = bencher.pop();
        CloudResources {
            shotover,
            docker,
            bencher,
            required,
        }
    }

    fn order_benches(
        &mut self,
        benches: Vec<BenchInfo<CloudResourcesRequired>>,
    ) -> Vec<BenchInfo<CloudResourcesRequired>> {
        // TODO: put benches with most resources first
        benches
    }

    async fn adjust_resources(
        &mut self,
        _benches: &[BenchInfo<CloudResourcesRequired>],
        _bench_index: usize,
        resources: &mut CloudResources,
    ) {
        for instance in &resources.docker {
            if Arc::strong_count(instance) != 1 {
                panic!(
                    "A reference to a docker instance has been held past the end of the benchmark. Ensure the benchmark destroys all instance references before ending."
                )
            }
        }
        for instance in &resources.shotover {
            if Arc::strong_count(instance) != 1 {
                panic!(
                    "A reference to a shotover instance has been held past the end of the benchmark. Ensure the benchmark destroys all instance references before ending."
                )
            }
        }
        if let Some(instance) = &resources.bencher
            && Arc::strong_count(instance) != 1
        {
            panic!(
                "A reference to a bencher instance has been held past the end of the benchmark. Ensure the benchmark destroys all instance references before ending."
            )
        }

        // TODO: spin up background tokio task to delete unneeded EC2 instances once we add the functionality to aws_throwaway
    }

    async fn store_resources_file(&mut self, path: &Path, resources: CloudResources) {
        let resources = CloudResourcesDisk {
            shotover: resources
                .shotover
                .into_iter()
                .map(|x| {
                    Arc::into_inner(x)
                        .expect("A bench is still referencing an Ec2InstanceWithShotover")
                })
                .collect(),
            docker: resources
                .docker
                .into_iter()
                .map(|x| {
                    Arc::into_inner(x)
                        .expect("A bench is still referencing an Ec2InstanceWithDocker")
                })
                .collect(),
            bencher: resources.bencher.map(|x| {
                Arc::into_inner(x).expect("A bench is still referencing an Ec2InstanceWithBencher")
            }),
            required: resources.required,
        };
        std::fs::write(path, serde_json::to_string(&resources).unwrap()).unwrap();
    }

    async fn load_resources_file(
        &mut self,
        path: &Path,
        required_resources: Vec<CloudResourcesRequired>,
    ) -> CloudResources {
        let required_resources = required_resources.into_iter().fold(
            CloudResourcesRequired::default(),
            CloudResourcesRequired::combine,
        );
        let mut resources: CloudResourcesDisk =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();

        if resources.required != required_resources {
            panic!(
                "Stored resources do not meet the requirements of this bench run. Maybe try rerunning --store-cloud-resources-file\nloaded:\n{:?}\nrequired:\n{:?}",
                resources.required, required_resources
            );
        }

        {
            let mut futures =
                FuturesUnordered::<Pin<Box<dyn Future<Output = Result<()>> + Send>>>::new();
            if let Some(instance) = &mut resources.bencher {
                futures.push(Box::pin(instance.reinit()));
            }
            for instance in &mut resources.docker {
                futures.push(Box::pin(instance.reinit()));
            }

            for instance in &mut resources.shotover {
                futures.push(Box::pin(instance.reinit()));
            }

            while let Some(result) = futures.next().await {
                let _: () = result.unwrap();
            }
        }

        CloudResources {
            shotover: resources.shotover.into_iter().map(Arc::new).collect(),
            docker: resources.docker.into_iter().map(Arc::new).collect(),
            bencher: resources.bencher.map(Arc::new),
            required: resources.required,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
    pub required: CloudResourcesRequired,
}

#[derive(Serialize, Deserialize)]
pub struct CloudResourcesDisk {
    pub shotover: Vec<Ec2InstanceWithShotover>,
    pub docker: Vec<Ec2InstanceWithDocker>,
    pub bencher: Option<Ec2InstanceWithBencher>,
    pub required: CloudResourcesRequired,
}
