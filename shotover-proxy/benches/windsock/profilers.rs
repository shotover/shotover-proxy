use self::samply::Samply;
use crate::common::Shotover;
use anyhow::Result;
use aws_throwaway::Ec2Instance;
use perf_flamegraph::Perf;
use std::{collections::HashMap, path::PathBuf};
use test_helpers::shotover_process::BinProcess;
use tokio::sync::mpsc::UnboundedReceiver;
use windsock::Profiling;

mod perf_flamegraph;
mod samply;
mod sar;

pub struct ProfilerRunner {
    bench_name: String,
    run_flamegraph: bool,
    run_samply: bool,
    run_sys_monitor: bool,
    results_path: PathBuf,
    perf: Option<Perf>,
    samply: Option<Samply>,
    sys_monitor: Option<UnboundedReceiver<Result<String>>>,
}

impl ProfilerRunner {
    pub fn new(bench_name: String, profiling: Profiling) -> Self {
        let run_flamegraph = profiling
            .profilers_to_use
            .contains(&"flamegraph".to_owned());
        let run_samply = profiling.profilers_to_use.contains(&"samply".to_owned());
        let run_sys_monitor = profiling
            .profilers_to_use
            .contains(&"sys_monitor".to_owned());

        ProfilerRunner {
            bench_name,
            run_flamegraph,
            run_sys_monitor,
            run_samply,
            results_path: profiling.results_path,
            perf: None,
            samply: None,
            sys_monitor: None,
        }
    }

    pub async fn run(&mut self, shotover: &Option<BinProcess>) {
        self.perf = if self.run_flamegraph {
            if let Some(shotover) = &shotover {
                Some(Perf::new(
                    self.results_path.clone(),
                    shotover.child().id().unwrap(),
                ))
            } else {
                panic!("flamegraph not supported when benching without shotover")
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
        if self.run_flamegraph || self.run_samply {
            Some("profiling")
        } else {
            None
        }
    }
}

impl Drop for ProfilerRunner {
    fn drop(&mut self) {
        if let Some(perf) = self.perf.take() {
            perf.flamegraph();
        }
        if let Some(samply) = self.samply.take() {
            samply.wait();
        }
        if let Some(mut rx) = self.sys_monitor.take() {
            sar::insert_sar_results_to_bench_archive(&self.bench_name, "", sar::parse_sar(&mut rx));
        }
    }
}

pub struct CloudProfilerRunner {
    bench_name: String,
    monitor_instances: HashMap<String, UnboundedReceiver<Result<String>>>,
}

impl CloudProfilerRunner {
    pub async fn new(
        bench_name: String,
        profiling: Profiling,
        instances: HashMap<String, &Ec2Instance>,
    ) -> Self {
        let run_sys_monitor = profiling
            .profilers_to_use
            .contains(&"sys_monitor".to_owned());

        let mut monitor_instances = HashMap::new();
        if run_sys_monitor {
            for (name, instance) in instances {
                monitor_instances.insert(name, sar::run_sar_remote(instance).await);
            }
        }

        CloudProfilerRunner {
            bench_name,
            monitor_instances,
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
        ]
    }
}
