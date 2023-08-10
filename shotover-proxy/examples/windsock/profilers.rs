use crate::common::Shotover;
use std::path::PathBuf;
use test_helpers::{flamegraph::Perf, shotover_process::BinProcess};
use windsock::Profiling;

pub struct ProfilerRunner {
    run_flamegraph: bool,
    results_path: PathBuf,
    perf: Option<Perf>,
}

impl ProfilerRunner {
    pub fn new(profiling: Profiling) -> Self {
        let run_flamegraph = profiling
            .profilers_to_use
            .contains(&"flamegraph".to_owned());
        ProfilerRunner {
            run_flamegraph,
            results_path: profiling.results_path,
            perf: None,
        }
    }

    pub fn run(&mut self, shotover: &Option<BinProcess>) {
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
    }

    pub fn shotover_profile(&self) -> Option<&'static str> {
        if self.run_flamegraph {
            Some("profiling")
        } else {
            None
        }
    }

    pub fn supported_profilers(shotover: Shotover) -> Vec<String> {
        if let Shotover::None = shotover {
            vec![]
        } else {
            vec!["flamegraph".to_owned()]
        }
    }
}

impl Drop for ProfilerRunner {
    fn drop(&mut self) {
        if let Some(perf) = self.perf.take() {
            perf.flamegraph();
        }
    }
}
