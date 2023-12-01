use prometheus_parse::Value;
use std::{collections::HashMap, time::Duration};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio::{sync::mpsc, time::MissedTickBehavior};
use windsock::{Goal, Metric, ReportArchive};

pub struct ShotoverMetrics {
    shutdown_tx: mpsc::Sender<()>,
    task: JoinHandle<()>,
}

pub struct RawPrometheusExposition {
    timestamp: OffsetDateTime,
    content: String,
}
type ParsedMetrics = HashMap<String, Vec<Value>>;

impl ShotoverMetrics {
    pub fn new(bench_name: String, shotover_ip: &str) -> Self {
        let url = format!("http://{shotover_ip}:9001/metrics");
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            // collect metrics until shutdown
            let raw_metrics = Self::collect_metrics(shutdown_rx, &url).await;

            // we are now shutting down and need to process and save all collected metrics
            let mut report = ReportArchive::load(&bench_name).unwrap();
            let parsed = Self::parse_metrics(raw_metrics, &report);
            report.metrics.extend(Self::windsock_metrics(parsed));
            report.save();
        });
        ShotoverMetrics { task, shutdown_tx }
    }

    pub fn parse_metrics(
        raw_metrics: Vec<RawPrometheusExposition>,
        report: &ReportArchive,
    ) -> ParsedMetrics {
        let mut parsed_metrics: ParsedMetrics = HashMap::new();
        for raw_metric in raw_metrics {
            if raw_metric.timestamp > report.bench_started_at {
                let metrics = prometheus_parse::Scrape::parse(
                    raw_metric.content.lines().map(|x| Ok(x.to_owned())),
                )
                .unwrap();
                for sample in metrics.samples {
                    let key = format!(
                        "{}{{{}}}",
                        sample
                            .metric
                            .strip_prefix("shotover_")
                            .unwrap_or(&sample.metric),
                        sample.labels
                    );

                    parsed_metrics.entry(key).or_default().push(sample.value);
                }
            }
        }
        parsed_metrics
    }
    pub fn windsock_metrics(parsed_metrics: ParsedMetrics) -> Vec<Metric> {
        let mut new_metrics = vec![];
        for (name, value) in parsed_metrics {
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
                                (*x, x.to_string(), Goal::None)
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
                                (diff, diff.to_string(), Goal::None)
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
        new_metrics
    }

    async fn collect_metrics(
        mut shutdown_rx: mpsc::Receiver<()>,
        url: &str,
    ) -> Vec<RawPrometheusExposition> {
        let mut results = vec![];

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                _ = interval.tick() => {
                    match tokio::time::timeout(Duration::from_secs(3), reqwest::get(url)).await.unwrap() {
                        Ok(response) => {
                            results.push(RawPrometheusExposition {
                                timestamp: OffsetDateTime::now_utc(),
                                content: response.text().await.unwrap(),
                            });
                        }
                        Err(err) => tracing::debug!("Failed to request from metrics endpoint, probably not up yet, error was {err:?}")
                    }
                }
            }
        }
        results
    }

    pub fn insert_results_to_bench_archive(self) {
        std::mem::drop(self.shutdown_tx);
        // TODO: asyncify it all or something
        futures::executor::block_on(async { self.task.await.unwrap() })
    }
}
