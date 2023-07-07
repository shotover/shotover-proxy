pub mod ec2_instance;
mod iam;
mod ssh;
pub use aws_sdk_ec2::types::InstanceType;

use aws_config::meta::region::RegionProviderChain;
use aws_config::SdkConfig;
use aws_sdk_ec2::types::{
    BlockDeviceMapping, EbsBlockDevice, KeyType, ResourceType, Tag, TagSpecification, VolumeType,
};
use aws_sdk_ec2::{config::Region, types::Filter};
use base64::Engine;
use ec2_instance::Ec2Instance;
use ssh_key::rand_core::OsRng;
use ssh_key::PrivateKey;
use uuid::Uuid;

pub async fn config() -> SdkConfig {
    let region_provider = RegionProviderChain::first_try(Region::new("us-east-1"));
    aws_config::from_env().region(region_provider).load().await
}

pub struct Aws {
    client: aws_sdk_ec2::Client,
    user_name: String,
    keyname: String,
    client_private_key: String,
    host_public_key: String,
    host_public_key_bytes: Vec<u8>,
    host_private_key: String,
    security_group: String,
}

// include a magic number in the keyname to avoid collisions
// This can never change or we may fail to cleanup resources.
const USER_TAG_NAME: &str = "aws-throwaway-23c2d22c-d929-43fc-b2a4-c1c72f0b733f:user";

impl Aws {
    pub async fn new() -> Self {
        let config = config().await;
        let user_name = iam::user_name(&config).await;
        let keyname = format!("aws-throwaway-{user_name}-{}", Uuid::new_v4());
        let client = aws_sdk_ec2::Client::new(&config);

        // Cleanup any resources that were previously failed to cleanup
        Self::cleanup_resources_inner(&client, &user_name).await;

        let keypair = client
            .create_key_pair()
            .key_name(&keyname)
            .key_type(KeyType::Ed25519)
            .tag_specifications(
                TagSpecification::builder()
                    .resource_type(ResourceType::KeyPair)
                    .tags(Tag::builder().key(USER_TAG_NAME).value(&user_name).build())
                    .build(),
            )
            .send()
            .await
            .map_err(|e| e.into_service_error())
            .unwrap();
        let client_private_key = keypair.key_material().unwrap().to_string();
        tracing::info!("client_private_key:\n{}", client_private_key);

        let security_group = format!("aws-throwaway-{user_name}-{}", Uuid::new_v4());
        client
            .create_security_group()
            .group_name(&security_group)
            .description("aws-throwaway security group")
            .tag_specifications(
                TagSpecification::builder()
                    .resource_type(ResourceType::SecurityGroup)
                    .tags(Tag::builder().key("Name").value("aws-throwaway").build())
                    .tags(Tag::builder().key(USER_TAG_NAME).value(&user_name).build())
                    .build(),
            )
            .send()
            .await
            .map_err(|e| e.into_service_error())
            .unwrap();
        tracing::info!("created security group");
        assert!(client
            .authorize_security_group_ingress()
            .group_name(&security_group)
            .source_security_group_name(&security_group)
            .tag_specifications(
                TagSpecification::builder()
                    .resource_type(ResourceType::SecurityGroupRule)
                    .tags(
                        Tag::builder()
                            .key("Name")
                            .value("within aws-throwaway SG")
                            .build()
                    )
                    .tags(Tag::builder().key(USER_TAG_NAME).value(&user_name).build())
                    .build(),
            )
            .send()
            .await
            .map_err(|e| e.into_service_error())
            .unwrap()
            .r#return()
            .unwrap());
        tracing::info!("created security group rule");
        assert!(client
            .authorize_security_group_ingress()
            .group_name(&security_group)
            .ip_protocol("tcp")
            .from_port(22)
            .to_port(22)
            .cidr_ip("0.0.0.0/0")
            .tag_specifications(
                TagSpecification::builder()
                    .resource_type(ResourceType::SecurityGroupRule)
                    .tags(Tag::builder().key("Name").value("ssh").build())
                    .tags(Tag::builder().key(USER_TAG_NAME).value(&user_name).build())
                    .build(),
            )
            .send()
            .await
            .map_err(|e| e.into_service_error())
            .unwrap()
            .r#return()
            .unwrap());
        tracing::info!("created security group rule");

        let key = PrivateKey::random(OsRng {}, ssh_key::Algorithm::Ed25519).unwrap();
        let host_public_key_bytes = key.public_key().to_bytes().unwrap();
        let host_public_key = key.public_key().to_openssh().unwrap();
        let host_private_key = key.to_openssh(ssh_key::LineEnding::LF).unwrap().to_string();

        Aws {
            client,
            user_name,
            keyname,
            client_private_key,
            host_public_key_bytes,
            host_public_key,
            host_private_key,
            security_group,
        }
    }

    /// Call before dropping [`Aws`]
    pub async fn cleanup_resources(&self) {
        Self::cleanup_resources_inner(&self.client, &self.user_name).await
    }

    /// Call to cleanup without constructing an [`Aws`]
    pub async fn cleanup_resources_static() {
        let config = config().await;
        let user_name = iam::user_name(&config).await;
        let client = aws_sdk_ec2::Client::new(&config);
        Aws::cleanup_resources_inner(&client, &user_name).await;
    }

    async fn get_all_throwaway_tags(
        client: &aws_sdk_ec2::Client,
        user_name: &str,
        resource_type: &str,
    ) -> Vec<String> {
        let user_filter_name = format!("tag:{}", USER_TAG_NAME);

        let mut ids = vec![];
        for tag in client
            .describe_tags()
            .set_filters(Some(vec![
                Filter::builder()
                    .name(&user_filter_name)
                    .values(user_name)
                    .build(),
                Filter::builder()
                    .name("resource-type")
                    .values(resource_type)
                    .build(),
            ]))
            .send()
            .await
            .map_err(|e| e.into_service_error())
            .unwrap()
            .tags()
            .unwrap()
        {
            if let Some(id) = tag.resource_id() {
                ids.push(id.to_owned());
            }
        }
        ids
    }

    pub async fn cleanup_resources_inner(client: &aws_sdk_ec2::Client, user_name: &str) {
        // delete instances
        tracing::info!("Terminating instances");
        let instance_ids = Self::get_all_throwaway_tags(client, user_name, "instance").await;
        if !instance_ids.is_empty() {
            for result in client
                .terminate_instances()
                .set_instance_ids(Some(instance_ids))
                .send()
                .await
                .map_err(|e| e.into_service_error())
                .unwrap()
                .terminating_instances()
                .unwrap()
            {
                tracing::info!(
                    "Instance {:?} {:?} -> {:?}",
                    result.instance_id.as_ref().unwrap(),
                    result.previous_state().unwrap().name().unwrap(),
                    result.current_state().unwrap().name().unwrap()
                );
            }
        }

        // delete security groups
        for id in Self::get_all_throwaway_tags(client, user_name, "security-group").await {
            if let Err(err) = client.delete_security_group().group_id(&id).send().await {
                tracing::info!(
                    "security group {id:?} could not be deleted, this will get cleaned up eventually on a future aws-throwaway cleanup: {:?}",
                    err.into_service_error().meta().message()
                )
            } else {
                tracing::info!("security group {id:?} was succesfully deleted",)
            }
        }

        // delete keypairs
        for id in Self::get_all_throwaway_tags(client, user_name, "key-pair").await {
            client
                .delete_key_pair()
                .key_pair_id(&id)
                .send()
                .await
                .map_err(|e| {
                    anyhow::anyhow!(e.into_service_error())
                        .context(format!("Failed to delete keypair {id:?}"))
                })
                .unwrap();
            tracing::info!("keypair {id:?} was succesfully deleted");
        }
    }

    pub async fn create_ec2_instance(
        &self,
        instance_type: InstanceType,
        storage_gb: u32,
    ) -> Ec2Instance {
        let result = self
            .client
            .run_instances()
            .instance_type(instance_type.clone())
            .min_count(1)
            .max_count(1)
            .block_device_mappings(
                BlockDeviceMapping::builder().device_name("/dev/sda1").ebs(
                    EbsBlockDevice::builder()
                        .delete_on_termination(true)
                        .volume_size(storage_gb as i32)
                        .volume_type(VolumeType::Gp2)
                        .build()
                ).build()
            )
            .security_groups(&self.security_group)
            .key_name(&self.keyname)
            .user_data(base64::engine::general_purpose::STANDARD.encode(format!(
                r#"#!/bin/bash
sudo systemctl stop ssh
echo "{}" > /etc/ssh/ssh_host_ed25519_key.pub
echo "{}" > /etc/ssh/ssh_host_ed25519_key

echo "ClientAliveInterval 30" >> /etc/ssh/sshd_config
sudo systemctl start ssh
            "#,
                self.host_public_key, self.host_private_key
            )))
            .tag_specifications(
                TagSpecification::builder()
                    .resource_type(ResourceType::Instance)
                    .set_tags(Some(vec![
                        Tag::builder().key("Name").value("aws-throwaway").build(),
                        Tag::builder()
                            .key(USER_TAG_NAME)
                            .value(&self.user_name)
                            .build(),
                    ]))
                    .build(),
            )
            .image_id(format!(
                "resolve:ssm:/aws/service/canonical/ubuntu/server/22.04/stable/current/{}/hvm/ebs-gp2/ami-id",
                get_arch_of_instance_type(instance_type).get_ubuntu_arch_identifier()
            ))
            .send()
            .await
            .map_err(|e| e.into_service_error())
            .unwrap();
        let instance_id = result
            .instances()
            .unwrap()
            .iter()
            .next()
            .unwrap()
            .instance_id()
            .unwrap()
            .to_owned();

        let mut public_ip = None;
        let mut private_ip = None;

        while public_ip.is_none() || private_ip.is_none() {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            for reservation in self
                .client
                .describe_instances()
                .instance_ids(&instance_id)
                .send()
                .await
                .map_err(|e| e.into_service_error())
                .unwrap()
                .reservations()
                .unwrap()
            {
                for instance in reservation.instances().unwrap() {
                    public_ip = instance.public_ip_address().map(|x| x.parse().unwrap());
                    private_ip = instance.private_ip_address().map(|x| x.parse().unwrap());
                }
            }
        }
        let public_ip = public_ip.unwrap();
        let private_ip = private_ip.unwrap();
        tracing::info!("created EC2 instance at: {public_ip}");

        Ec2Instance::new(
            public_ip,
            private_ip,
            self.host_public_key_bytes.clone(),
            &self.client_private_key,
        )
        .await
    }
}

enum CpuArch {
    X86_64,
    Aarch64,
}

impl CpuArch {
    fn get_ubuntu_arch_identifier(&self) -> &'static str {
        match self {
            CpuArch::X86_64 => "amd64",
            CpuArch::Aarch64 => "arm64",
        }
    }
}

fn get_arch_of_instance_type(instance_type: InstanceType) -> CpuArch {
    // Instance names looke like something like:
    // type + revision_number + subtypes + '.' + size
    // So say for example `Im4gn.large` would be split into:
    // type = "Im"
    // revision_number = 4
    // subtypes = "gn"
    // size = "large"
    //
    // The 'g' character existing in subtypes indicates that the instance type is a gravitron aka arm instance.
    // We can check for the existence of 'g' to determine if we are aarch64 or x86_64
    // This is a bit hacky because this format is not explicitly documented anywhere but the instance type naming does consistently follow this pattern.
    let mut reached_revision_number = false;
    for c in instance_type.as_str().chars() {
        if !reached_revision_number {
            if c.is_ascii_digit() {
                reached_revision_number = true;
            }
        } else if c == '.' {
            return CpuArch::X86_64;
        } else if c == 'g' {
            return CpuArch::Aarch64;
        }
    }
    unreachable!("Cannot parse instance type: {instance_type:?}")
}
