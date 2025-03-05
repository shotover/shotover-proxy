//! This module provides abstractions for getting system usage from the unix command `sar`, on ubuntu it is contained within the package `sysstat`.

use anyhow::Result;
use aws_throwaway::Ec2Instance;
use std::{collections::HashMap, process::Stdio};
use time::OffsetDateTime;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
    sync::mpsc::{UnboundedReceiver, unbounded_channel},
};
use windsock::{Goal, Metric, ReportArchive};

/// Reads the bench archive for `bench_name` from disk.
/// Inserts the passed sar metrics for `instance_name`.
/// Then writes the resulting archive back to disk
pub fn insert_sar_results_to_bench_archive(
    bench_name: &str,
    instance_name: &str,
    mut sar: ParsedSar,
) {
    let mut report = ReportArchive::load(bench_name).unwrap();

    // The bench will start after sar has started so we need to throw away all sar metrics that were recorded before the bench started.
    let time_diff = report.bench_started_at - sar.started_at;
    let inital_values_to_discard = time_diff.as_seconds_f32().round() as usize;
    for values in sar.named_values.values_mut() {
        values.drain(0..inital_values_to_discard);
    }

    // use short names so we can keep each call on one line.
    let p = instance_name;
    let s = &sar;
    report.metrics.extend([
        metric(s, p, "CPU User", "%", "%user", Goal::SmallerIsBetter),
        metric(s, p, "CPU System", "%", "%system", Goal::SmallerIsBetter),
        metric(s, p, "CPU Nice", "%", "%nice", Goal::SmallerIsBetter),
        metric(s, p, "CPU IO Wait", "%", "%iowait", Goal::SmallerIsBetter),
        metric(s, p, "CPU Steal", "%", "%steal", Goal::SmallerIsBetter),
        metric(s, p, "CPU Idle", "%", "%idle", Goal::BiggerIsBetter),
        metric_with_formatter(
            s,
            p,
            "Memory Used",
            |value| {
                // sar calls this a KB (kilobyte) but its actually a KiB (kibibyte)
                let value_kib: f32 = value.parse().unwrap();
                let value_mib = value_kib / 1024.0;
                format!("{} MiB", value_mib)
            },
            "kbmemused",
            Goal::SmallerIsBetter,
        ),
    ]);

    report.save();
}

/// Shortcut for common metric formatting case
fn metric(
    sar: &ParsedSar,
    prefix: &str,
    name: &str,
    unit: &str,
    sar_name: &str,
    goal: Goal,
) -> Metric {
    metric_with_formatter(sar, prefix, name, |x| format!("{x}{unit}"), sar_name, goal)
}

/// Take a sars metric and transform it into a metric that can be stored in a bench archive
fn metric_with_formatter<F: Fn(&str) -> String>(
    sar: &ParsedSar,
    prefix: &str,
    name: &str,
    value_formatter: F,
    sar_name: &str,
    goal: Goal,
) -> Metric {
    let name = if prefix.is_empty() {
        name.to_owned()
    } else {
        format!("{prefix} - {name}")
    };
    Metric::EachSecond {
        name,
        values: sar
            .named_values
            .get(sar_name)
            .ok_or_else(|| format!("No key {} in {:?}", sar_name, sar.named_values))
            .unwrap()
            .iter()
            .map(|x| (x.parse().unwrap(), value_formatter(x), goal))
            .collect(),
    }
}

/// parse lines of output from the sar command which looks like:
/// ```text
/// Linux 6.4.8-arch1-1 (memes) 09/08/23 _x86_64_ (24 CPU)
///
/// 12:19:51        CPU     %user     %nice   %system   %iowait    %steal     %idle
/// 12:19:52        all      4.39      0.00      0.17      0.00      0.00     95.44
///
/// 12:19:51    kbmemfree   kbavail kbmemused  %memused kbbuffers  kbcached  kbcommit   %commit  kbactive   kbinact   kbdirty
/// 12:19:52     10848136  17675452  14406672     43.91    482580   6566224  20441872     62.30  13626936   7304044        76
///
/// 12:19:52        CPU     %user     %nice   %system   %iowait    %steal     %idle
/// 12:19:53        all      4.45      0.00      0.50      0.12      0.00     94.92
///
/// 12:19:52    kbmemfree   kbavail kbmemused  %memused kbbuffers  kbcached  kbcommit   %commit  kbactive   kbinact   kbdirty
/// 12:19:53     10827924  17655248  14426872     43.97    482592   6566224  20441924     62.30  13649508   7304056       148
/// ```
pub fn parse_sar(rx: &mut UnboundedReceiver<Result<String>>) -> ParsedSar {
    let mut named_values = HashMap::new();

    // read date command
    let Ok(started_at) = rx.try_recv() else {
        return ParsedSar {
            started_at: OffsetDateTime::UNIX_EPOCH,
            named_values: HashMap::new(),
        };
    };
    let started_at =
        OffsetDateTime::from_unix_timestamp_nanos(started_at.unwrap().parse().unwrap()).unwrap();

    // skip header
    if rx.try_recv().is_err() {
        return ParsedSar {
            started_at: OffsetDateTime::UNIX_EPOCH,
            named_values: HashMap::new(),
        };
    }

    // keep reading until we exhaust the receiver
    loop {
        let Ok(_blank_line) = rx.try_recv() else {
            return ParsedSar {
                started_at,
                named_values,
            };
        };
        let Ok(header) = rx.try_recv() else {
            return ParsedSar {
                started_at,
                named_values,
            };
        };
        let Ok(data) = rx.try_recv() else {
            return ParsedSar {
                started_at,
                named_values,
            };
        };
        for (head, data) in header
            .unwrap()
            .split_whitespace()
            .zip(data.unwrap().split_whitespace())
            .skip(1)
        {
            named_values
                .entry(head.to_owned())
                .or_default()
                .push(data.to_owned());
        }
    }
}

pub struct ParsedSar {
    /// The time in UTC at which the sar command was started
    started_at: OffsetDateTime,
    /// The key contains the name of the metric
    /// The value contains a list of values recorded for that metric over the runtime of sar
    named_values: HashMap<String, Vec<String>>,
}

const SAR_COMMAND: &str = "date +%s%N; sar -r -u 1";

/// Run the sar command on the local machine.
/// Each line of output is returned via the `UnboundedReceiver`
pub fn run_sar_local() -> UnboundedReceiver<Result<String>> {
    let (tx, rx) = unbounded_channel();
    tokio::spawn(async move {
        let mut child = Command::new("bash")
            .args(["-c", SAR_COMMAND])
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut reader = BufReader::new(child.stdout.take().unwrap()).lines();
        while let Some(line) = reader.next_line().await.unwrap() {
            if tx.send(Ok(line)).is_err() {
                child.kill().await.unwrap();
                return;
            }
        }
    });

    rx
}

/// Run the sar command over ssh on the passed instance.
/// Each line of output is returned via the `UnboundedReceiver`
pub async fn run_sar_remote(instance: &Ec2Instance) -> UnboundedReceiver<Result<String>> {
    instance.ssh().shell_stdout_lines(SAR_COMMAND).await
}
