use self::{samply::Samply, shotover_metrics::ShotoverMetrics};
use crate::common::Shotover;
use anyhow::Result;
use aws_throwaway::Ec2Instance;
use std::{collections::HashMap, path::PathBuf};
use test_helpers::shotover_process::BinProcess;
use tokio::sync::mpsc::UnboundedReceiver;
use windsock::Profiling;

mod samply;
mod sar;
mod shotover_metrics;

pub struct ProfilerRunner {
    bench_name: String,
    run_samply: bool,
    run_shotover_metrics: bool,
    run_sys_monitor: bool,
    results_path: PathBuf,
    shotover_metrics: Option<ShotoverMetrics>,
    samply: Option<Samply>,
    sys_monitor: Option<UnboundedReceiver<Result<String>>>,
}

impl ProfilerRunner {
    pub fn new(bench_name: String, profiling: Profiling) -> Self {
        let run_samply = profiling.profilers_to_use.contains(&"samply".to_owned());
        let run_sys_monitor = profiling
            .profilers_to_use
            .contains(&"sys_monitor".to_owned());
        let run_shotover_metrics = profiling
            .profilers_to_use
            .contains(&"shotover_metrics".to_owned());

        ProfilerRunner {
            bench_name,
            run_sys_monitor,
            run_samply,
            run_shotover_metrics,
            results_path: profiling.results_path,
            shotover_metrics: None,
            samply: None,
            sys_monitor: None,
        }
    }

    pub async fn run(&mut self, shotover: &Option<BinProcess>) {
        self.shotover_metrics = if self.run_shotover_metrics {
            if shotover.is_some() {
                Some(ShotoverMetrics::new(self.bench_name.clone(), "localhost"))
            } else {
                panic!("shotover_metrics not supported when benching without shotover")
            }
        } else {
            None
        };
        self.samply = if self.run_samply {
            if let Some(shotover) = &shotover {
                Some(Samply::run(self.results_path.clone(), shotover.child().id().unwrap()).await)
            } else {
                panic!("samply not supported when benching without shotover")
            }
        } else {
            None
        };
        self.sys_monitor = if self.run_sys_monitor {
            Some(sar::run_sar_local())
        } else {
            None
        };
    }

    pub fn shotover_profile(&self) -> Option<&'static str> {
        if self.run_samply {
            Some("profiling")
        } else {
            None
        }
    }
}

impl Drop for ProfilerRunner {
    fn drop(&mut self) {
        if let Some(samply) = self.samply.take() {
            samply.wait();
        }
        if let Some(shotover_metrics) = self.shotover_metrics.take() {
            shotover_metrics.insert_results_to_bench_archive();
        }
        if let Some(mut rx) = self.sys_monitor.take() {
            sar::insert_sar_results_to_bench_archive(&self.bench_name, "", sar::parse_sar(&mut rx));
        }
    }
}

pub struct CloudProfilerRunner {
    bench_name: String,
    monitor_instances: HashMap<String, UnboundedReceiver<Result<String>>>,
    shotover_metrics: Option<ShotoverMetrics>,
}

impl CloudProfilerRunner {
    pub async fn new(
        bench_name: String,
        profiling: Profiling,
        instances: HashMap<String, &Ec2Instance>,
        shotover_ip: &Option<String>,
    ) -> Self {
        let run_sys_monitor = profiling
            .profilers_to_use
            .contains(&"sys_monitor".to_owned());

        let run_shotover_metrics = profiling
            .profilers_to_use
            .contains(&"shotover_metrics".to_owned());

        let mut monitor_instances = HashMap::new();
        if run_sys_monitor {
            for (name, instance) in instances {
                monitor_instances.insert(name, sar::run_sar_remote(instance).await);
            }
        }

        let shotover_metrics = if run_shotover_metrics {
            Some(ShotoverMetrics::new(
                bench_name.clone(),
                shotover_ip.as_ref().unwrap(),
            ))
        } else {
            None
        };

        CloudProfilerRunner {
            bench_name,
            monitor_instances,
            shotover_metrics,
        }
    }

    pub fn finish(&mut self) {
        for (name, instance_rx) in &mut self.monitor_instances {
            sar::insert_sar_results_to_bench_archive(
                &self.bench_name,
                name,
                sar::parse_sar(instance_rx),
            );
        }
        if let Some(shotover_metrics) = self.shotover_metrics.take() {
            shotover_metrics.insert_results_to_bench_archive();
        }
    }
}

pub fn supported_profilers(shotover: Shotover) -> Vec<String> {
    if let Shotover::None = shotover {
        vec!["sys_monitor".to_owned()]
    } else {
        vec![
            "flamegraph".to_owned(),
            "samply".to_owned(),
            "sys_monitor".to_owned(),
            "shotover_metrics".to_owned(),
        ]
    }
}
