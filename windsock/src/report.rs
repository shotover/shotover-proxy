use crate::bench::Tags;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::{io::ErrorKind, path::PathBuf, time::Duration};
use strum::{EnumCount, EnumIter, IntoEnumIterator};
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Debug, Serialize, Deserialize)]
pub enum Report {
    Start,
    QueryCompletedIn(Duration),
    SecondPassed(Duration),
    /// contains the time that the test ran for
    FinishedIn(Duration),
}

#[derive(EnumIter, EnumCount)]
pub enum Percentile {
    Min = 0,
    P1,
    P2,
    P5,
    P10,
    P25,
    P50,
    P75,
    P90,
    P95,
    P98,
    P99,
    P99_9,
    P99_99,
    Max,
}

impl Percentile {
    pub fn value(&self) -> f64 {
        match self {
            Percentile::Min => 0.0,
            Percentile::P1 => 0.01,
            Percentile::P2 => 0.02,
            Percentile::P5 => 0.05,
            Percentile::P10 => 0.10,
            Percentile::P25 => 0.25,
            Percentile::P50 => 0.50,
            Percentile::P75 => 0.75,
            Percentile::P90 => 0.90,
            Percentile::P95 => 0.95,
            Percentile::P98 => 0.98,
            Percentile::P99 => 0.99,
            Percentile::P99_9 => 0.999,
            Percentile::P99_99 => 0.9999,
            Percentile::Max => 1.0,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Percentile::Min => "  Min   ",
            Percentile::P1 => "    1   ",
            Percentile::P2 => "    2   ",
            Percentile::P5 => "    5   ",
            Percentile::P10 => "   10   ",
            Percentile::P25 => "   25   ",
            Percentile::P50 => "   50   ",
            Percentile::P75 => "   75   ",
            Percentile::P90 => "   90   ",
            Percentile::P95 => "   95   ",
            Percentile::P98 => "   98   ",
            Percentile::P99 => "   99   ",
            Percentile::P99_9 => "   99.9 ",
            Percentile::P99_99 => "  99.99",
            Percentile::Max => "  Max   ",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReportArchive {
    pub(crate) running_in_release: bool,
    pub(crate) tags: Tags,
    pub(crate) operations_total: u64,
    pub(crate) requested_ops: Option<u64>,
    pub(crate) actual_ops: u64,
    pub(crate) mean_response_time: Duration,
    pub(crate) response_time_percentiles: [Duration; Percentile::COUNT],
    pub(crate) operations_each_second: Vec<u64>,
}

impl ReportArchive {
    fn path(&self) -> PathBuf {
        windsock_path().join(self.tags.get_name())
    }

    pub fn load(path: &str) -> Result<Self> {
        match std::fs::read(windsock_path().join(path)) {
            Ok(bytes) => bincode::deserialize(&bytes).map_err(|e|
                anyhow!(e).context("The bench archive from the previous run is not valid archive, maybe the format changed since the last run")
            ),
            Err(err) if err.kind() == ErrorKind::NotFound => Err(anyhow!("The bench {path:?} does not exist or was not run in the previous run")),
            Err(err) => Err(anyhow!("The bench {path:?} encountered a file read error {err:?}"))
        }
    }

    pub fn reports_in_last_run() -> Vec<String> {
        let report_dir = windsock_path();
        std::fs::create_dir_all(&report_dir).unwrap();

        let mut reports: Vec<String> = std::fs::read_dir(report_dir)
            .unwrap()
            .map(|x| {
                x.unwrap()
                    .path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        reports.sort();
        reports
    }

    fn save(&self) {
        let path = self.path();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bincode::serialize(self).unwrap())
            .map_err(|e| panic!("Failed to write to {path:?} {e}"))
            .unwrap()
    }

    pub(crate) fn clear_last_run() {
        let path = windsock_path();
        if path.exists() {
            // Just an extra sanity check that we truly are deleting a windsock_data directory
            assert_eq!(path.file_name().unwrap(), "windsock_data");
            std::fs::remove_dir_all(windsock_path()).unwrap();
        }
    }
}

pub fn windsock_path() -> PathBuf {
    // If we are run via cargo (we are in a target directory) use the target directory for storage.
    // Otherwise just fallback to the current working directory.
    let mut path = std::env::current_exe().unwrap();
    while path.pop() {
        if path.file_name().map(|x| x == "target").unwrap_or(false) {
            return path.join("windsock_data");
        }
    }

    PathBuf::from("windsock_data")
}

pub(crate) async fn report_builder(
    tags: Tags,
    mut rx: UnboundedReceiver<Report>,
    requested_ops: Option<u64>,
    running_in_release: bool,
) -> ReportArchive {
    let mut operations_total = 0;
    let mut response_times = vec![];
    let mut total_response_time = Duration::from_secs(0);
    let mut finished_in = None;
    let mut started = false;
    let mut operations_each_second = vec![0];

    while let Some(report) = rx.recv().await {
        match report {
            Report::Start => {
                started = true;
            }
            Report::QueryCompletedIn(duration) => {
                if started {
                    operations_total += 1;
                    total_response_time += duration;
                    response_times.push(duration);
                    *operations_each_second.last_mut().unwrap() += 1;
                }
            }
            Report::SecondPassed(duration) => {
                assert!(
                    duration >= Duration::from_secs(1) && duration < Duration::from_millis(1050),
                    "Expected duration to be within 50ms of a second but was {duration:?}"
                );
                operations_each_second.push(0);
            }
            Report::FinishedIn(duration) => {
                // This is not a complete result so discard it.
                operations_each_second.pop();

                if !started {
                    panic!("The bench never returned Report::Start")
                }
                finished_in = Some(duration);
                // immediately drop rx so the benchmarks tasks stop trying to bench, logic doesnt rely on this it just saves resources
                std::mem::drop(rx);
                break;
            }
        }
    }
    let finished_in = match finished_in {
        Some(x) => x,
        None => panic!("The bench never returned Report::FinishedIn(..)"),
    };

    let mean_response_time = if !response_times.is_empty() {
        total_response_time / response_times.len() as u32
    } else {
        Duration::from_secs(0)
    };
    let actual_ops = (operations_total as u128 / (finished_in.as_nanos() / 1_000_000_000)) as u64;

    let mut response_time_percentiles = [Duration::new(0, 0); Percentile::COUNT];
    response_times.sort();
    if !response_times.is_empty() {
        for (i, p) in Percentile::iter().enumerate() {
            let percentile_index = (p.value() * response_times.len() as f64) as usize;
            // Need to cap at last index, otherwise the MAX percentile will overflow by 1
            let index = percentile_index.min(response_times.len() - 1);
            response_time_percentiles[i] = response_times[index];
        }
    }

    let archive = ReportArchive {
        running_in_release,
        tags,
        requested_ops,
        actual_ops,
        operations_total,
        mean_response_time,
        response_time_percentiles,
        operations_each_second,
    };
    archive.save();
    archive
}
