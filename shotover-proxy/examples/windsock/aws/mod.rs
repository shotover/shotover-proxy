//! Windsock specific logic built on top of aws_throwaway

pub mod cloud;

use async_once_cell::OnceCell;
use aws_throwaway::{ec2_instance::Ec2Instance, Aws, InstanceType};
use regex::Regex;
use std::fmt::Write;
use std::time::Duration;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use test_helpers::docker_compose::get_image_waiters;
use tokio::sync::RwLock;
use windsock::{Profiling, ReportArchive};

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
                aws: Aws::new().await,
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
                .create_ec2_instance(InstanceType::M6aLarge, 8)
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
            // databases will need more storage than the shotover or bencher instances
            .create_ec2_instance(InstanceType::M6aLarge, 40)
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

    pub async fn create_shotover_instance(&self, profile: &str) -> Arc<Ec2InstanceWithShotover> {
        if let Some(instance) = &*self.shotover_instance.read().await {
            if Arc::strong_count(instance) != 1 {
                panic!("Only one shotover instance can be held at once, make sure you drop the previous instance before calling this method again.")
            }
            return instance.clone();
        }

        let instance = Arc::new(Ec2InstanceWithShotover {
            instance: self
                .aws
                .create_ec2_instance(InstanceType::M6aLarge, 8)
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

        let output = tokio::process::Command::new(env!("CARGO"))
            .args(["build", "--all-features", "--profile", profile])
            .output()
            .await
            .unwrap();
        if !output.status.success() {
            let stdout = String::from_utf8(output.stdout).unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();
            panic!("Bench run failed:\nstdout:\n{stdout}\nstderr:\n{stderr}")
        }
        instance
            .instance
            .ssh()
            .push_file(
                &std::env::current_exe()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .parent()
                    .unwrap()
                    .join(profile)
                    .join("shotover-proxy"),
                Path::new("shotover-bin"),
            )
            .await;
        instance
            .instance
            .ssh()
            .push_file(Path::new("config/config.yaml"), Path::new("config.yaml"))
            .await;
        *self.shotover_instance.write().await = Some(instance.clone());

        instance
            .instance
            .ssh()
            .push_file(&std::env::current_exe().unwrap(), Path::new("windsock"))
            .await;

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
            .shell_stdout_lines(&format!("sudo docker logs -f {container_id}"))
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
    pub async fn run_shotover(
        self: Arc<Self>,
        bench_name: String,
        profiling: Profiling,
        topology: &str,
    ) -> RunningShotover {
        self.instance
            .ssh()
            .push_file_from_bytes(topology.as_bytes(), Path::new("topology.yaml"))
            .await;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let cloned_self = self.clone();
        tokio::task::spawn(async move {
            let profilers = profiling.profile_arg();
            let mut receiver = cloned_self.instance.ssh().shell_stdout_lines(&format!(
                r#"
killall -w windsock 2> /dev/null || true
killall -w shotover-bin 2> /dev/null || true
#RUST_BACKTRACE=1 ./shotover-bin --config-file config.yaml --topology-file topology.yaml --log-format json
./windsock --internal-run-service "{bench_name} ." --profilers "{profilers}"
"#
                )).await;
            loop {
                tokio::select! {
                    line = receiver.recv() => {
                        match line {
                            Some(line) => {
                                println!("{line}");
                                tx.send(line).ok();
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
            let line = rx
                .recv()
                .await
                .expect("Shotover shutdown before indicating that it had started");
            if line.contains("Shotover is now accepting inbound connections") {
                break;
            }
        }
        RunningShotover { rx, shotover: self }
    }
}

pub struct RunningShotover {
    shotover: Arc<Ec2InstanceWithShotover>,
    rx: tokio::sync::mpsc::UnboundedReceiver<String>,
}

impl RunningShotover {
    pub async fn shutdown(mut self, bench_name: String) {
        // TODO: instead bubble up failure exit code from windsock as error
        while let Ok(event) = self.rx.try_recv() {
            if event.contains("ERROR") || event.contains("WARN") {
                panic!("Received error/warn event from shotover:\n     {event}")
            }
        }

        // dropping rx instructs the task to shutdown causing shotover to be terminated
        self.rx.close();

        // wait for shotover to shutdown
        while self.rx.recv().await.is_some() {}

        // TODO: This should pull the entire contents of the folder,
        // but aws-throwaway does not support that yet,
        // so for now we just hardcode it to the flamegraph.svg as that is the only profiler we currently support
        let source = Path::new("profiler_results")
            .join(&bench_name)
            .join("flamegraph.svg");
        let dest = windsock::data::windsock_path()
            .join("profiler_results")
            .join(bench_name)
            .join("flamegraph.svg");
        self.shotover.instance.ssh().pull_file(&source, &dest).await;
    }
}
