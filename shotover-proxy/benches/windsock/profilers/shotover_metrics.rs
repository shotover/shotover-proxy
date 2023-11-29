use prometheus_parse::Value;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use windsock::{Goal, Metric, ReportArchive};

pub struct ShotoverMetrics {
    shutdown_tx: mpsc::Sender<()>,
    task: JoinHandle<()>,
}
type ParsedMetrics = HashMap<String, Vec<Value>>;

impl ShotoverMetrics {
    pub fn new(bench_name: String, shotover_ip: &str) -> Self {
        let url = format!("http://{shotover_ip}:9001/metrics");
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let mut results: ParsedMetrics = HashMap::new();
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                    _ = Self::get_metrics(&mut results, &url) => {}
                }
            }

            // The bench will start after sar has started so we need to throw away all sar metrics that were recorded before the bench started.
            // let time_diff = report.bench_started_at - sar.started_at;
            // let inital_values_to_discard = time_diff.as_seconds_f32().round() as usize;
            // for values in sar.named_values.values_mut() {
            //     values.drain(0..inital_values_to_discard);
            // }

            let mut new_metrics = vec![];
            for (name, value) in results {
                match value[0] {
                    Value::Gauge(_) => {
                        new_metrics.push(Metric::EachSecond {
                            name,
                            values: value
                                .iter()
                                .map(|x| {
                                    let Value::Gauge(x) = x else {
                                        panic!("metric type changed during bench run")
                                    };
                                    (*x, x.to_string(), Goal::SmallerIsBetter) // TODO: Goal::None
                                })
                                .collect(),
                        });
                    }
                    Value::Counter(_) => {
                        let mut prev = 0.0;
                        new_metrics.push(Metric::EachSecond {
                            name,
                            values: value
                                .iter()
                                .map(|x| {
                                    let Value::Counter(x) = x else {
                                        panic!("metric type changed during bench run")
                                    };
                                    let diff = x - prev;
                                    prev = *x;
                                    (diff, diff.to_string(), Goal::SmallerIsBetter)
                                    // TODO: Goal::None
                                })
                                .collect(),
                        });
                    }
                    Value::Summary(_) => {
                        let last = value.last().unwrap();
                        let Value::Summary(summary) = last else {
                            panic!("metric type changed during bench run")
                        };
                        let values = summary
                            .iter()
                            .map(|x| {
                                (
                                    x.count,
                                    format!("{} - {:.4}ms", x.quantile, x.count * 1000.0),
                                    Goal::SmallerIsBetter,
                                )
                            })
                            .collect();
                        // TODO: add a Metric::QuantileLatency and use instead
                        new_metrics.push(Metric::EachSecond { name, values });
                    }
                    _ => {
                        tracing::warn!("Unused shotover metric: {name}")
                    }
                }
            }
            new_metrics.sort_by_key(|x| {
                let name = x.name();
                // move latency metrics to the top
                if name.starts_with("chain_latency") || name.starts_with("transform_latency") {
                    format!("aaa_{name}")
                }
                // move failure metrics to the bottom.
                else if name.starts_with("transform_failures")
                    || name.starts_with("failed_requests")
                    || name.starts_with("chain_failures")
                {
                    format!("zzz_{name}")
                } else {
                    name.to_owned()
                }
            });

            let mut report = ReportArchive::load(&bench_name).unwrap();
            report.metrics.extend(new_metrics);
            report.save();
        });
        ShotoverMetrics { task, shutdown_tx }
    }

    async fn get_metrics(results: &mut ParsedMetrics, url: &str) {
        let metrics = tokio::time::timeout(Duration::from_secs(3), reqwest::get(url))
            .await
            .unwrap()
            .unwrap()
            .text()
            .await
            .unwrap();

        let metrics =
            prometheus_parse::Scrape::parse(metrics.lines().map(|x| Ok(x.to_owned()))).unwrap();
        for sample in metrics.samples {
            let key = format!(
                "{}{{{}}}",
                sample
                    .metric
                    .strip_prefix("shotover_")
                    .unwrap_or(&sample.metric),
                sample.labels
            );

            results.entry(key).or_default().push(sample.value);
        }
        tokio::time::sleep(Duration::from_secs(1)).await
    }

    pub fn insert_results_to_bench_archive(self) {
        std::mem::drop(self.shutdown_tx);
        // TODO: asyncify it all or something
        futures::executor::block_on(async { self.task.await.unwrap() })
    }
}
