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
    ProduceCompletedIn(Duration),
    ConsumeCompleted,
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
            Percentile::Min => "Min   ",
            Percentile::P1 => "1   ",
            Percentile::P2 => "2   ",
            Percentile::P5 => "5   ",
            Percentile::P10 => "10   ",
            Percentile::P25 => "25   ",
            Percentile::P50 => "50   ",
            Percentile::P75 => "75   ",
            Percentile::P90 => "90   ",
            Percentile::P95 => "95   ",
            Percentile::P98 => "98   ",
            Percentile::P99 => "99   ",
            Percentile::P99_9 => "99.9 ",
            Percentile::P99_99 => "99.99",
            Percentile::Max => "Max   ",
        }
    }
}

type Percentiles = [Duration; Percentile::COUNT];

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReportArchive {
    pub(crate) running_in_release: bool,
    pub(crate) tags: Tags,
    pub(crate) operations_report: Option<OperationsReport>,
    pub(crate) pubsub_report: Option<PubSubReport>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct OperationsReport {
    pub(crate) total: u64,
    pub(crate) requested_ops: Option<u64>,
    pub(crate) total_ops: u32,
    pub(crate) mean_time: Duration,
    pub(crate) time_percentiles: Percentiles,
    pub(crate) total_each_second: Vec<u64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct PubSubReport {
    pub(crate) total_produce: u64,
    pub(crate) total_consume: u64,
    pub(crate) total_backlog: i64,
    pub(crate) requested_produce_per_second: Option<u64>,
    pub(crate) produce_per_second: u32,
    pub(crate) consume_per_second: u32,
    pub(crate) produce_mean_time: Duration,
    pub(crate) produce_time_percentiles: Percentiles,
    pub(crate) produce_each_second: Vec<u64>,
    pub(crate) consume_each_second: Vec<u64>,
    pub(crate) backlog_each_second: Vec<i64>,
}

impl ReportArchive {
    fn path(&self) -> PathBuf {
        windsock_path().join(self.tags.get_name())
    }

    pub fn load(path: &str) -> Result<Self> {
        match std::fs::read(windsock_path().join(path)) {
            Ok(bytes) => bincode::deserialize(&bytes).map_err(|e|
                anyhow!(e).context("The bench archive from the previous run is not a valid archive, maybe the format changed since the last run")
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
    let mut finished_in = None;
    let mut started = false;
    let mut pubsub_report = None;
    let mut operations_report = None;
    let mut operation_times = vec![];
    let mut produce_times = vec![];
    let mut total_operation_time = Duration::from_secs(0);
    let mut total_produce_time = Duration::from_secs(0);

    while let Some(report) = rx.recv().await {
        match report {
            Report::Start => {
                started = true;
            }
            Report::QueryCompletedIn(duration) => {
                let report = operations_report.get_or_insert_with(OperationsReport::default);
                if started {
                    report.total += 1;
                    total_operation_time += duration;
                    operation_times.push(duration);
                    match report.total_each_second.last_mut() {
                        Some(last) => *last += 1,
                        None => report.total_each_second.push(0),
                    }
                }
            }
            Report::ProduceCompletedIn(duration) => {
                let report = pubsub_report.get_or_insert_with(PubSubReport::default);
                if started {
                    report.total_backlog += 1;
                    report.total_produce += 1;
                    total_produce_time += duration;
                    produce_times.push(duration);
                    match report.produce_each_second.last_mut() {
                        Some(last) => *last += 1,
                        None => report.produce_each_second.push(0),
                    }
                }
            }
            Report::ConsumeCompleted => {
                let report = pubsub_report.get_or_insert_with(PubSubReport::default);
                if started {
                    report.total_backlog -= 1;
                    report.total_consume += 1;
                    match report.consume_each_second.last_mut() {
                        Some(last) => *last += 1,
                        None => report.consume_each_second.push(0),
                    }
                }
            }
            Report::SecondPassed(duration) => {
                assert!(
                    duration >= Duration::from_secs(1) && duration < Duration::from_millis(1050),
                    "Expected duration to be within 50ms of a second but was {duration:?}"
                );
                if let Some(report) = operations_report.as_mut() {
                    report.total_each_second.push(0);
                }
                if let Some(report) = pubsub_report.as_mut() {
                    report.produce_each_second.push(0);
                    report.consume_each_second.push(0);
                    report.backlog_each_second.push(report.total_backlog);
                }
            }
            Report::FinishedIn(duration) => {
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

    if let Some(report) = operations_report.as_mut() {
        report.requested_ops = requested_ops;
        report.mean_time = mean_time(&operation_times, total_operation_time);
        report.total_ops = calculate_ops(report.total, finished_in);
        report.time_percentiles = calculate_percentiles(operation_times);

        // This is not a complete result so discard it.
        report.total_each_second.pop();
    }

    if let Some(report) = pubsub_report.as_mut() {
        report.requested_produce_per_second = requested_ops;
        report.produce_mean_time = mean_time(&produce_times, total_produce_time);
        report.produce_per_second = calculate_ops(report.total_produce, finished_in);
        report.consume_per_second = calculate_ops(report.total_consume, finished_in);
        report.produce_time_percentiles = calculate_percentiles(produce_times);

        // This is not a complete result so discard it.
        report.produce_each_second.pop();
        report.consume_each_second.pop();
    }

    let archive = ReportArchive {
        running_in_release,
        tags,
        pubsub_report,
        operations_report,
    };
    archive.save();
    archive
}

fn mean_time(times: &[Duration], total_time: Duration) -> Duration {
    if !times.is_empty() {
        total_time / times.len() as u32
    } else {
        Duration::from_secs(0)
    }
}

fn calculate_ops(total: u64, finished_in: Duration) -> u32 {
    (total as u128 / (finished_in.as_nanos() / 1_000_000_000)) as u32
}

fn calculate_percentiles(mut times: Vec<Duration>) -> Percentiles {
    let mut percentiles = [Duration::ZERO; Percentile::COUNT];
    times.sort();
    if !times.is_empty() {
        for (i, p) in Percentile::iter().enumerate() {
            let percentile_index = (p.value() * times.len() as f64) as usize;
            // Need to cap at last index, otherwise the MAX percentile will overflow by 1
            let index = percentile_index.min(times.len() - 1);
            percentiles[i] = times[index];
        }
    }
    percentiles
}
