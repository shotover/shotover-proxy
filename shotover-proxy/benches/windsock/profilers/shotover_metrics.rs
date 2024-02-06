use prometheus_parse::Value;
use std::{collections::HashMap, time::Duration};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio::{sync::mpsc, time::MissedTickBehavior};
use windsock::{Goal, LatencyPercentile, Metric, ReportArchive};

pub struct ShotoverMetrics {
    shutdown_tx: mpsc::Sender<()>,
    task: JoinHandle<()>,
}

pub struct RawPrometheusExposition {
    timestamp: OffsetDateTime,
    content: String,
}

#[derive(Eq, PartialEq, Hash)]
struct MetricLabel {
    label: String,
    unit: Unit,
}
type ParsedMetrics = HashMap<MetricLabel, Vec<Value>>;

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

    fn parse_metrics(
        raw_metrics: Vec<RawPrometheusExposition>,
        report: &ReportArchive,
    ) -> ParsedMetrics {
        let mut result: ParsedMetrics = HashMap::new();
        for raw_metric in raw_metrics {
            if raw_metric.timestamp > report.bench_started_at {
                let metrics = prometheus_parse::Scrape::parse(
                    raw_metric.content.lines().map(|x| Ok(x.to_owned())),
                )
                .unwrap();
                for sample in metrics.samples {
                    let (name, unit) = parse_name(sample.metric);
                    let label = format!("{name}{{{}}}", sample.labels);
                    let metric_name = MetricLabel { label, unit };

                    result.entry(metric_name).or_default().push(sample.value);
                }
            }
        }
        result
    }

    fn windsock_metrics(parsed_metrics: ParsedMetrics) -> Vec<Metric> {
        let mut result = vec![];
        for (MetricLabel { label: name, unit }, value) in parsed_metrics {
            match value[0] {
                Value::Gauge(_) => {
                    result.push(Metric::EachSecond {
                        name,
                        values: value
                            .iter()
                            .map(|x| {
                                let Value::Gauge(x) = x else {
                                    panic!("metric type changed during bench run")
                                };
                                (*x, unit.format(*x), Goal::None)
                            })
                            .collect(),
                    });
                }
                Value::Counter(_) => {
                    let mut prev = 0.0;
                    result.push(Metric::EachSecond {
                        name,
                        values: value
                            .iter()
                            .map(|x| {
                                let Value::Counter(x) = x else {
                                    panic!("metric type changed during bench run")
                                };
                                let diff = x - prev;
                                prev = *x;
                                (diff, unit.format(diff), Goal::None)
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
                        .map(|x| LatencyPercentile {
                            value: x.count,
                            value_display: unit.format(x.count),
                            quantile: x.quantile.to_string(),
                        })
                        .collect();
                    result.push(Metric::LatencyPercentiles { name, values });
                }
                _ => {
                    tracing::warn!("Unused shotover metric: {name}")
                }
            }
        }
        result.sort_by_key(|x| {
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
        result
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
                    match tokio::time::timeout(Duration::from_secs(3), reqwest::get(url)).await {
                        Ok(Ok(response)) => {
                            results.push(RawPrometheusExposition {
                                timestamp: OffsetDateTime::now_utc(),
                                content: response.text().await.unwrap(),
                            });
                        }
                        Ok(Err(err)) => tracing::debug!("Failed to request from metrics endpoint {url}, probably not up yet, error was {err:?}"),
                        Err(_) => panic!("Timed out request from metrics endpoint {url}")
                    }
                }
            }
        }
        results
    }

    pub fn insert_results_to_bench_archive(self) {
        std::mem::drop(self.shutdown_tx);
        // TODO: make this function + caller async, lets do this in a follow up PR to avoid making this PR even more complex.
        futures::executor::block_on(async { self.task.await.unwrap() })
    }
}

fn parse_name(name: String) -> (String, Unit) {
    let name = name.strip_prefix("shotover_").unwrap_or(&name);

    if let Some(name) = name.strip_suffix("_seconds") {
        (name.to_owned(), Unit::Seconds)
    } else if let Some(name) = name.strip_suffix("_count") {
        (name.to_owned(), Unit::Count)
    } else {
        (name.to_owned(), Unit::Undefined)
    }
}

#[derive(Eq, PartialEq, Hash)]
enum Unit {
    Seconds,
    Count,
    Undefined,
}

impl Unit {
    fn format(&self, value: f64) -> String {
        match self {
            Unit::Seconds => format!("{:.4}ms", value * 1000.0),
            // Rounding is needed to get reasonable values out of histograms
            Unit::Count => value.round().to_string(),
            Unit::Undefined => value.to_string(),
        }
    }
}
