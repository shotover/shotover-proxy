//! Windsock specific logic built on top of aws_throwaway

pub mod cloud;

use async_once_cell::OnceCell;
use aws_throwaway::{Aws, Ec2Instance, InstanceType};
use aws_throwaway::{CleanupResources, Ec2InstanceDefinition};
use regex::Regex;
use std::fmt::Write;
use std::time::Duration;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use test_helpers::docker_compose::get_image_waiters;
use tokio::sync::RwLock;
use tokio_bin_process::bin_path;
use tokio_bin_process::event::{Event, Level};
use windsock::ReportArchive;

static AWS_THROWAWAY_TAG: &str = "windsock";

static AWS: OnceCell<WindsockAws> = OnceCell::new();

pub struct WindsockAws {
    shotover_instance: RwLock<Option<Arc<Ec2InstanceWithShotover>>>,
    bencher_instance: RwLock<Option<Arc<Ec2InstanceWithBencher>>>,
    docker_instances: RwLock<Vec<Arc<Ec2InstanceWithDocker>>>,
    aws: Aws,
}

impl WindsockAws {
    pub async fn get() -> &'static Self {
        AWS.get_or_init(async move {
            WindsockAws {
                shotover_instance: RwLock::new(None),
                bencher_instance: RwLock::new(None),
                docker_instances: RwLock::new(vec![]),
                aws: Aws::new(CleanupResources::WithAppTag(AWS_THROWAWAY_TAG.to_owned())).await,
            }
        })
        .await
    }

    pub async fn create_bencher_instance(&self) -> Arc<Ec2InstanceWithBencher> {
        if let Some(instance) = &*self.bencher_instance.read().await {
            if Arc::strong_count(instance) != 1 {
                panic!("Only one bencher instance can be held at once, make sure you drop the previous instance before calling this method again.")
            }
            return instance.clone();
        }

        let instance = Arc::new(Ec2InstanceWithBencher {
            instance: self
                .aws
                .create_ec2_instance(
                    Ec2InstanceDefinition::new(get_compatible_instance_type())
                        .volume_size_gigabytes(8),
                )
                .await,
        });
        instance
            .instance
            .ssh()
            .shell(
                r#"
sudo apt-get update
sudo apt-get install -y sysstat"#,
            )
            .await;
        instance
            .instance
            .ssh()
            .push_file(
                std::env::current_exe().unwrap().as_ref(),
                Path::new("windsock"),
            )
            .await;
        *self.bencher_instance.write().await = Some(instance.clone());

        instance
    }

    pub async fn create_docker_instance(&self) -> Arc<Ec2InstanceWithDocker> {
        for instance in &*self.docker_instances.read().await {
            if Arc::strong_count(instance) == 1 {
                return instance.clone();
            }
        }
        let instance = self
            .aws
            .create_ec2_instance(
                Ec2InstanceDefinition::new(get_compatible_instance_type())
                    // databases will need more storage than the shotover or bencher instances
                    .volume_size_gigabytes(40),
            )
            .await;
        instance
        .ssh()
            .shell(
                r#"
# Need to retry until succeeds as apt-get update may fail if run really quickly after starting the instance
until sudo apt-get update -qq
do
  sleep 1
done
sudo apt-get install -y sysstat
curl -sSL https://get.docker.com/ | sudo sh"#,
            )
            .await;

        let instance = Arc::new(Ec2InstanceWithDocker { instance });
        (*self.docker_instances.write().await).push(instance.clone());
        instance
    }

    pub async fn create_shotover_instance(&self) -> Arc<Ec2InstanceWithShotover> {
        if let Some(instance) = &*self.shotover_instance.read().await {
            if Arc::strong_count(instance) != 1 {
                panic!("Only one shotover instance can be held at once, make sure you drop the previous instance before calling this method again.")
            }
            return instance.clone();
        }

        let instance = Arc::new(Ec2InstanceWithShotover {
            instance: self
                .aws
                .create_ec2_instance(
                    Ec2InstanceDefinition::new(get_compatible_instance_type())
                        .volume_size_gigabytes(8),
                )
                .await,
        });
        instance
            .instance
            .ssh()
            .shell(
                r#"
sudo apt-get update
sudo apt-get install -y sysstat"#,
            )
            .await;

        let local_shotover_path = bin_path!("shotover-proxy");
        instance
            .instance
            .ssh()
            .push_file(local_shotover_path, Path::new("shotover-bin"))
            .await;
        instance
            .instance
            .ssh()
            .push_file(Path::new("config/config.yaml"), Path::new("config.yaml"))
            .await;
        *self.shotover_instance.write().await = Some(instance.clone());

        instance
    }

    pub async fn cleanup_resources(&self) {
        self.aws.cleanup_resources().await;
    }
}

pub struct Ec2InstanceWithDocker {
    pub instance: Ec2Instance,
}

impl Ec2InstanceWithDocker {
    pub async fn run_container(&self, image: &str, envs: &[(String, String)]) {
        // cleanup old resources
        self.instance
            .ssh()
            .shell(
                r#"
CONTAINERS=$(sudo docker ps -a -q)
if [ -n "$CONTAINERS" ]
then
    sudo docker stop $CONTAINERS
    sudo docker rm $CONTAINERS
fi
sudo docker system prune -af"#,
            )
            .await;

        // start container
        let mut env_args = String::new();
        for (key, value) in envs {
            // TODO: shell escape key and value
            env_args.push_str(&format!(" -e \"{key}={value}\""))
        }
        let output = self
            .instance
            .ssh()
            .shell(&format!(
                "sudo docker run -qd --network host {env_args} {image}"
            ))
            .await;
        let container_id = output.stdout;

        // wait for container to finish starting
        let mut receiver = self
            .instance
            .ssh()
            .shell_stdout_lines(&format!("sudo docker logs -f {container_id} 2>&1"))
            .await;
        let image_waiter = get_image_waiters()
            .iter()
            .find(|x| x.name == image)
            .unwrap_or_else(|| {
                panic!("The image {image:?} is not configured in get_image_waiters")
            });
        let mut logs = String::new();
        let regex = Regex::new(image_waiter.log_regex_to_wait_for).unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(120), receiver.recv()).await {
                Ok(Some(line)) => {
                    writeln!(logs, "{}", line).unwrap();
                    if regex.is_match(&line) {
                        return;
                    }
                }
                Ok(None) => panic!(
                    "Docker container of image {:?} shutdown before {:?} occurred in the logs.\nDocker logs:\n{logs}",
                    image,
                    image_waiter.log_regex_to_wait_for
                ),
                Err(_) => panic!(
                    "Docker container of image {:?} timed out after 2mins waiting for {:?} in the logs.\nDocker logs:\n{logs}",
                    image,
                    image_waiter.log_regex_to_wait_for
                )
            }
        }
    }
}

fn get_compatible_instance_type() -> InstanceType {
    if cfg!(target_arch = "x86_64") {
        InstanceType::M6aLarge
    } else if cfg!(target_arch = "aarch64") {
        InstanceType::M6gLarge
    } else {
        panic!("target_arch not supported by AWS");
    }
}

pub struct Ec2InstanceWithBencher {
    pub instance: Ec2Instance,
}

impl Ec2InstanceWithBencher {
    pub async fn run_bencher(&self, args: &str, name: &str) {
        self.instance
            .ssh()
            .shell(&format!("RUST_BACKTRACE=1 ./windsock {args}"))
            .await;

        let source = PathBuf::from("windsock_data").join("last_run").join(name);
        let dest = ReportArchive::last_run_path().join(name);
        self.instance.ssh().pull_file(&source, &dest).await;
    }
}

pub struct Ec2InstanceWithShotover {
    pub instance: Ec2Instance,
}

impl Ec2InstanceWithShotover {
    pub async fn run_shotover(self: Arc<Self>, topology: &str) -> RunningShotover {
        self.instance
            .ssh()
            .push_file_from_bytes(topology.as_bytes(), Path::new("topology.yaml"))
            .await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::task::spawn(async move {
            let mut receiver = self
            .instance
            .ssh()
            // TODO: invoke current_exe --harnessed-startup shotover --name $benchname --profilers $profilers
            .shell_stdout_lines(r#"
killall -w shotover-bin > /dev/null || true
RUST_BACKTRACE=1 ./shotover-bin --config-file config.yaml --topology-file topology.yaml --log-format json"#)
            .await;
            loop {
                tokio::select! {
                    line = receiver.recv() => {
                        match line {
                            Some(line) => {
                                let event = Event::from_json_str(&line).unwrap();
                                if let Level::Warn = event.level {
                                    tracing::error!("shotover warn:\n    {event}");
                                }
                                if let Level::Error = event.level {
                                    tracing::error!("shotover error:\n    {event}");
                                }
                                if tx.send(event).is_err() {
                                    return
                                }
                            }
                            None => return,
                        }
                    },
                    _ = tx.closed() => {
                        return;
                    },
                };
            }
        });

        // wait for shotover to startup
        loop {
            let event = rx
                .recv()
                .await
                .expect("Shotover shutdown before indicating that it had started");
            if let Level::Warn | Level::Error = event.level {
                panic!("Received error/warn event from shotover:\n     {event}")
            }
            if event.fields.message == "Shotover is now accepting inbound connections" {
                break;
            }
        }
        RunningShotover { rx }
    }
}

pub struct RunningShotover {
    rx: tokio::sync::mpsc::UnboundedReceiver<Event>,
}

impl RunningShotover {
    pub async fn shutdown(mut self) {
        while let Ok(event) = self.rx.try_recv() {
            if let Level::Warn | Level::Error = event.level {
                panic!("Received error/warn event from shotover:\n     {event}")
            }
        }

        // dropping rx instructs the task to shutdown causing shotover to be terminated
        std::mem::drop(self.rx)
    }
}
