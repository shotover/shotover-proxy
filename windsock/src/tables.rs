use crate::{
    bench::Tags,
    filter::Filter,
    report::{MetricIdentifier, Percentile, ReportArchive},
    Metric,
};
use anyhow::{Context, Result};
use console::{pad_str, pad_str_with, style, Alignment};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, time::Duration};
use strum::IntoEnumIterator;

pub(crate) struct ReportColumn {
    pub(crate) baseline: Option<ReportArchive>,
    pub(crate) current: ReportArchive,
}

impl ReportColumn {
    pub fn load(name: &str) -> Result<Self> {
        Ok(ReportColumn {
            baseline: None,
            current: ReportArchive::load(name)?,
        })
    }

    pub fn load_with_baseline(name: &str) -> Result<Self> {
        Ok(ReportColumn {
            baseline: ReportArchive::load_baseline(name)?,
            current: ReportArchive::load(name)?,
        })
    }
}

pub fn compare_by_name(names: &str) -> Result<()> {
    let columns: Result<Vec<ReportColumn>> =
        names.split_whitespace().map(ReportColumn::load).collect();
    let mut columns = columns?;

    let baseline = columns.get(0).map(|x| x.current.clone());
    for column in &mut columns.iter_mut().skip(1) {
        column.baseline = baseline.clone();
    }

    display_compare_table(&columns);
    Ok(())
}

pub fn results_by_name(names: &str) -> Result<()> {
    let archives: Result<Vec<ReportColumn>> =
        names.split_whitespace().map(ReportColumn::load).collect();
    display_results_table(&archives?);
    Ok(())
}

pub fn baseline_compare_by_tags(arg: &str) -> Result<()> {
    let filter = Filter::from_query(arg)
        .with_context(|| format!("Failed to parse tag filter from {:?}", arg))?;
    let archives: Result<Vec<ReportColumn>> = ReportArchive::reports_in_last_run()
        .iter()
        .filter(|name| filter.matches(&Tags::from_name(name)))
        .map(|x| ReportColumn::load_with_baseline(x))
        .collect();
    display_baseline_compare_table(&archives?);

    Ok(())
}

pub fn compare_by_tags(arg: &str) -> Result<()> {
    let mut split = arg.split_whitespace();
    let base_name = split.next().unwrap().to_owned();
    let base = ReportArchive::load(&base_name)?;

    let tag_args: Vec<_> = split.collect();
    let tag_args = tag_args.join(" ");

    let filter = Filter::from_query(&tag_args)
        .with_context(|| format!("Failed to parse tag filter from {:?}", tag_args))?;
    let archives: Result<Vec<ReportColumn>> = ReportArchive::reports_in_last_run()
        .iter()
        .filter(|name| **name != base_name && filter.matches(&Tags::from_name(name)))
        .map(|x| {
            Ok(ReportColumn {
                baseline: Some(base.clone()),
                current: ReportArchive::load(x)?,
            })
        })
        .collect();
    let mut archives = archives?;

    archives.insert(
        0,
        ReportColumn {
            baseline: None,
            current: base,
        },
    );

    display_compare_table(&archives);

    Ok(())
}

pub fn results_by_tags(arg: &str) -> Result<()> {
    let filter = Filter::from_query(arg)
        .with_context(|| format!("Failed to parse tag filter from {:?}", arg))?;
    let archives: Result<Vec<ReportColumn>> = ReportArchive::reports_in_last_run()
        .iter()
        .filter(|name| filter.matches(&Tags::from_name(name)))
        .map(|x| ReportColumn::load(x))
        .collect();
    display_results_table(&archives?);

    Ok(())
}

pub(crate) fn display_baseline_compare_table(reports: &[ReportColumn]) {
    if reports.is_empty() {
        println!("Need at least one report to display baseline comparison");
        return;
    }

    base(reports, "Comparison against baseline");
}

pub(crate) fn display_compare_table(reports: &[ReportColumn]) {
    if reports.len() < 2 {
        println!("Need at least two reports to display a comparison against first column");
        return;
    }

    base(reports, "Comparison against first column");
}

pub(crate) fn display_results_table(reports: &[ReportColumn]) {
    if reports.is_empty() {
        println!("Need at least one report to display results");
        return;
    }

    base(reports, "Results");
}

fn base(reports: &[ReportColumn], table_type: &str) {
    // if the user has set CARGO_TERM_COLOR to force cargo to use colors then they probably want us to use colors too
    if std::env::var("CARGO_TERM_COLOR")
        .map(|x| x.to_lowercase() == "always")
        .unwrap_or(false)
    {
        console::set_colors_enabled(true);
    }

    let mut intersection = reports[0].current.tags.clone();
    for report in reports {
        intersection = intersection.intersection(&report.current.tags);
    }

    let mut rows = vec![];
    rows.push(Row::Heading(format!(
        "{} for {}",
        table_type,
        intersection.get_name()
    )));

    let intersection_keys = intersection.keys();
    let mut nonintersecting_keys: Vec<String> = reports
        .iter()
        .fold(HashSet::new(), |acc, x| {
            acc.union(
                &x.current
                    .tags
                    .keys()
                    .difference(&intersection_keys)
                    .cloned()
                    .collect(),
            )
            .cloned()
            .collect()
        })
        .into_iter()
        .collect();
    nonintersecting_keys.sort();
    if !nonintersecting_keys.is_empty() {
        rows.push(Row::Heading("Unique Tags".to_owned()));
    }
    for key in nonintersecting_keys {
        rows.push(Row::ColumnNames {
            names: reports
                .iter()
                .map(|x| x.current.tags.0.get(&key).cloned().unwrap_or("".to_owned()))
                .collect(),
            legend: key,
        });
    }

    if reports
        .iter()
        .any(|x| x.current.operations_report.is_some())
    {
        rows.push(Row::Heading("(Opns) Operations".to_owned()));
        rows.push(Row::measurements(reports, "Total Opns", |report| {
            report.operations_report.as_ref().map(|report| {
                (
                    report.total as f64,
                    report.total.to_string(),
                    Goal::BiggerIsBetter,
                )
            })
        }));
        rows.push(Row::measurements(reports, "Total Errors", |report| {
            report.operations_report.as_ref().map(|report| {
                (
                    report.total_errors as f64,
                    report.total_errors.to_string(),
                    Goal::SmallerIsBetter,
                )
            })
        }));
        rows.push(Row::measurements(
            reports,
            "Target Opns Per Sec",
            |report| {
                report.operations_report.as_ref().map(|report| {
                    (
                        report
                            .requested_operations_per_second
                            .map(|x| x as f64)
                            .unwrap_or(f64::INFINITY),
                        report
                            .requested_operations_per_second
                            .map(|x| x.to_string())
                            .unwrap_or("MAX".to_owned()),
                        Goal::BiggerIsBetter,
                    )
                })
            },
        ));
        rows.push(Row::measurements(reports, "Opns Per Sec", |report| {
            report.operations_report.as_ref().map(|report| {
                (
                    report.total_operations_per_second as f64,
                    format!("{:.0}", report.total_operations_per_second),
                    Goal::BiggerIsBetter,
                )
            })
        }));
        rows.push(Row::measurements(reports, "Errors Per Sec", |report| {
            report.operations_report.as_ref().map(|report| {
                (
                    report.total_errors_per_second as f64,
                    format!("{:.0}", report.total_errors_per_second),
                    Goal::SmallerIsBetter,
                )
            })
        }));

        rows.push(Row::measurements(reports, "Opn Time Mean", |report| {
            report.operations_report.as_ref().map(|report| {
                (
                    report.mean_time.as_secs_f64(),
                    duration_ms(report.mean_time),
                    Goal::SmallerIsBetter,
                )
            })
        }));

        rows.push(Row::Heading("Opn Time Percentiles".to_owned()));
        for (i, p) in Percentile::iter().enumerate() {
            rows.push(Row::measurements(reports, p.name(), |report| {
                report.operations_report.as_ref().map(|report| {
                    (
                        report.time_percentiles[i].as_secs_f64(),
                        duration_ms(report.time_percentiles[i]),
                        Goal::SmallerIsBetter,
                    )
                })
            }));
        }

        rows.push(Row::Heading("Opns Each Second".to_owned()));
        for i in 0..reports
            .iter()
            .map(|x| {
                x.current
                    .operations_report
                    .as_ref()
                    .map(|report| report.total_each_second.len())
                    .unwrap_or(0)
            })
            .max()
            .unwrap()
        {
            rows.push(Row::measurements(reports, &i.to_string(), |report| {
                report.operations_report.as_ref().and_then(|report| {
                    report
                        .total_each_second
                        .get(i)
                        .map(|value| (*value as f64, value.to_string(), Goal::BiggerIsBetter))
                })
            }));
        }
    }

    if reports.iter().any(|x| x.current.pubsub_report.is_some()) {
        rows.push(Row::Heading("Produce/Consume".to_owned()));
        rows.push(Row::measurements(reports, "Total Produce", |report| {
            report.pubsub_report.as_ref().map(|report| {
                (
                    report.total_produce as f64,
                    report.total_produce.to_string(),
                    Goal::BiggerIsBetter,
                )
            })
        }));
        rows.push(Row::measurements(
            reports,
            "Errors Total Produce",
            |report| {
                report.pubsub_report.as_ref().map(|report| {
                    (
                        report.total_produce_error as f64,
                        report.total_produce_error.to_string(),
                        Goal::SmallerIsBetter,
                    )
                })
            },
        ));
        rows.push(Row::measurements(reports, "Total Consume", |report| {
            report.pubsub_report.as_ref().map(|report| {
                (
                    report.total_consume as f64,
                    report.total_consume.to_string(),
                    Goal::BiggerIsBetter,
                )
            })
        }));
        rows.push(Row::measurements(
            reports,
            "Errors Total Consume",
            |report| {
                report.pubsub_report.as_ref().map(|report| {
                    (
                        report.total_consume_error as f64,
                        report.total_consume_error.to_string(),
                        Goal::SmallerIsBetter,
                    )
                })
            },
        ));
        rows.push(Row::measurements(reports, "Total Backlog", |report| {
            report.pubsub_report.as_ref().map(|report| {
                (
                    report.total_backlog as f64,
                    report.total_backlog.to_string(),
                    Goal::SmallerIsBetter,
                )
            })
        }));

        rows.push(Row::measurements(
            reports,
            "Target Produce Per Sec",
            |report| {
                report.pubsub_report.as_ref().map(|report| {
                    (
                        report
                            .requested_produce_per_second
                            .map(|x| x as f64)
                            .unwrap_or(f64::INFINITY),
                        report
                            .requested_produce_per_second
                            .map(|x| x.to_string())
                            .unwrap_or("MAX".to_owned()),
                        Goal::BiggerIsBetter,
                    )
                })
            },
        ));
        rows.push(Row::measurements(reports, "Produce Per Sec", |report| {
            report.pubsub_report.as_ref().map(|report| {
                (
                    report.produce_per_second as f64,
                    format!("{:.0}", report.produce_per_second),
                    Goal::BiggerIsBetter,
                )
            })
        }));
        rows.push(Row::measurements(
            reports,
            "Errors Produce Per Sec",
            |report| {
                report.pubsub_report.as_ref().map(|report| {
                    (
                        report.produce_errors_per_second as f64,
                        format!("{:.0}", report.produce_errors_per_second),
                        Goal::SmallerIsBetter,
                    )
                })
            },
        ));
        rows.push(Row::measurements(reports, "Consume Per Sec", |report| {
            report.pubsub_report.as_ref().map(|report| {
                (
                    report.consume_per_second as f64,
                    format!("{:.0}", report.consume_per_second),
                    Goal::BiggerIsBetter,
                )
            })
        }));
        rows.push(Row::measurements(
            reports,
            "Errors Consume Per Sec",
            |report| {
                report.pubsub_report.as_ref().map(|report| {
                    (
                        report.consume_errors_per_second as f64,
                        format!("{:.0}", report.consume_errors_per_second),
                        Goal::SmallerIsBetter,
                    )
                })
            },
        ));

        rows.push(Row::measurements(reports, "Produce Time Mean", |report| {
            report.pubsub_report.as_ref().map(|report| {
                (
                    report.produce_mean_time.as_secs_f64(),
                    duration_ms(report.produce_mean_time),
                    Goal::SmallerIsBetter,
                )
            })
        }));

        rows.push(Row::Heading("Produce Time Percentiles".to_owned()));
        for (i, p) in Percentile::iter().enumerate() {
            rows.push(Row::measurements(reports, p.name(), |report| {
                report.pubsub_report.as_ref().map(|report| {
                    (
                        report.produce_time_percentiles[i].as_secs_f64(),
                        duration_ms(report.produce_time_percentiles[i]),
                        Goal::SmallerIsBetter,
                    )
                })
            }));
        }

        rows.push(Row::Heading("Produce Each Second".to_owned()));
        for i in 0..reports
            .iter()
            .map(|x| {
                x.current
                    .pubsub_report
                    .as_ref()
                    .map(|report| report.produce_each_second.len())
                    .unwrap_or(0)
            })
            .max()
            .unwrap()
        {
            rows.push(Row::measurements(reports, &i.to_string(), |report| {
                report.pubsub_report.as_ref().and_then(|report| {
                    report
                        .produce_each_second
                        .get(i)
                        .map(|value| (*value as f64, value.to_string(), Goal::BiggerIsBetter))
                })
            }));
        }

        rows.push(Row::Heading("Consume Each Second".to_owned()));
        for i in 0..reports
            .iter()
            .map(|x| {
                x.current
                    .pubsub_report
                    .as_ref()
                    .map(|report| report.consume_each_second.len())
                    .unwrap_or(0)
            })
            .max()
            .unwrap()
        {
            rows.push(Row::measurements(reports, &i.to_string(), |report| {
                report.pubsub_report.as_ref().and_then(|report| {
                    report
                        .consume_each_second
                        .get(i)
                        .map(|value| (*value as f64, value.to_string(), Goal::BiggerIsBetter))
                })
            }));
        }

        rows.push(Row::Heading("Total Backlog Each Second".to_owned()));
        for i in 0..reports
            .iter()
            .map(|x| {
                x.current
                    .pubsub_report
                    .as_ref()
                    .map(|report| report.backlog_each_second.len())
                    .unwrap_or(0)
            })
            .max()
            .unwrap()
        {
            rows.push(Row::measurements(reports, &i.to_string(), |report| {
                report.pubsub_report.as_ref().and_then(|report| {
                    report
                        .backlog_each_second
                        .get(i)
                        .map(|value| (*value as f64, value.to_string(), Goal::SmallerIsBetter))
                })
            }));
        }
    }

    let mut metrics_to_display = vec![];
    for report in reports {
        for metric in &report.current.metrics {
            if !metrics_to_display.contains(&metric.identifier()) {
                metrics_to_display.push(metric.identifier())
            }
        }
    }
    for metric_identifier in metrics_to_display {
        match &metric_identifier {
            MetricIdentifier::Total { name } => {
                rows.push(Row::measurements(reports, name, |report| {
                    report
                        .metrics
                        .iter()
                        .find(|metric| metric.identifier() == metric_identifier)
                        .map(|metric| match metric {
                            Metric::Total {
                                compare,
                                value,
                                goal,
                                ..
                            } => (*compare, value.to_owned(), *goal),
                            _ => unreachable!(),
                        })
                }));
            }
            MetricIdentifier::EachSecond { name } => {
                rows.push(Row::Heading(format!("{name} Each Second")));
                for i in 0..reports
                    .iter()
                    .map(|x| {
                        x.current
                            .metrics
                            .iter()
                            .find(|x| x.identifier() == metric_identifier)
                            .map(|metric| metric.len())
                            .unwrap_or(0)
                    })
                    .max()
                    .unwrap()
                {
                    rows.push(Row::measurements(reports, &i.to_string(), |report| {
                        report
                            .metrics
                            .iter()
                            .find(|x| x.identifier() == metric_identifier)
                            .and_then(|metric| match metric {
                                Metric::EachSecond { values, .. } => values.get(i).cloned(),
                                _ => unreachable!(),
                            })
                    }));
                }
            }
            MetricIdentifier::LatencyPercentiles { name } => {
                rows.push(Row::Heading(format!("{name} Percentiles")));
                for (i, largest_col) in reports
                    .iter()
                    .map(|x| {
                        x.current
                            .metrics
                            .iter()
                            .find(|x| x.identifier() == metric_identifier)
                            .map(|metric| match metric {
                                Metric::LatencyPercentiles { values, .. } => values.clone(),
                                _ => unreachable!(),
                            })
                            .unwrap_or(vec![])
                    })
                    .max_by_key(|x| x.len())
                    .unwrap()
                    .into_iter()
                    .enumerate()
                {
                    rows.push(Row::measurements(
                        reports,
                        &largest_col.quantile,
                        |report| {
                            report
                                .metrics
                                .iter()
                                .find(|x| x.identifier() == metric_identifier)
                                .and_then(|metric| match metric {
                                    Metric::LatencyPercentiles { values, .. } => {
                                        values.get(i).map(|x| x.to_measurement())
                                    }
                                    _ => unreachable!(),
                                })
                        },
                    ));
                }
            }
        }
    }

    // the width of the legend column
    let legend_width: usize = rows
        .iter()
        .skip(1) // skip the main heading because its big and its alignment doesnt matter
        .map(|x| match x {
            Row::Heading(heading) => heading.len(),
            Row::ColumnNames { legend, .. } => legend.len(),
            Row::Measurements { legend, .. } => legend.len(),
        })
        .max()
        .unwrap_or(10);
    // the width of the comparison component of each column
    let comparison_widths: Vec<usize> = reports
        .iter()
        .enumerate()
        .map(|(i, _)| {
            rows.iter()
                .map(|x| match x {
                    Row::Heading(_) => 0,
                    Row::ColumnNames { .. } => 0,
                    Row::Measurements { measurements, .. } => measurements[i].comparison.len() + 1, // + 1 ensures we get separation from the previous column
                })
                .max()
                .unwrap()
        })
        .collect();
    // the width of each entire column
    let column_widths: Vec<usize> = reports
        .iter()
        .enumerate()
        .map(|(i, _)| {
            rows.iter()
                .map(|x| match x {
                    Row::Heading(_) => 0,                                 // Ignore these
                    Row::ColumnNames { names, .. } => names[i].len() + 1, // + 1 ensures we get separation from the previous column
                    Row::Measurements { measurements, .. } => {
                        measurements[i].value.len() + 1 // ensures we get seperation from the previous column
                        + comparison_widths[i]
                    }
                })
                .max()
                .unwrap()
        })
        .collect();
    let total_width = legend_width + column_widths.iter().sum::<usize>();

    for row in rows {
        match row {
            Row::Heading(heading) => {
                println!(
                    "{}",
                    style(pad_str_with(
                        &format!("{} ", heading),
                        total_width,
                        Alignment::Left,
                        None,
                        '═'
                    ))
                    .yellow()
                    .bold()
                )
            }
            Row::ColumnNames { legend, names } => {
                print!(
                    "{}",
                    style(pad_str(&legend, legend_width, Alignment::Right, None))
                        .yellow()
                        .bold()
                );
                for (i, name) in names.into_iter().enumerate() {
                    print!(
                        " {}",
                        style(pad_str_with(
                            &name,
                            column_widths[i] - 1,
                            Alignment::Center,
                            None,
                            '─',
                        ))
                    )
                }
                println!()
            }
            Row::Measurements {
                legend,
                measurements,
            } => {
                print!(
                    "{}",
                    style(pad_str(&legend, legend_width, Alignment::Right, None))
                        .yellow()
                        .bold()
                );
                for (i, measurement) in measurements.into_iter().enumerate() {
                    let colorer = match measurement.color {
                        Color::Good => |x| style(x).green(),
                        Color::Bad => |x| style(x).red(),
                        Color::Neutral => |x| style(x).dim(),
                    };
                    let contents = format!(
                        "{}{}",
                        measurement.value,
                        colorer(pad_str(
                            &measurement.comparison,
                            comparison_widths[i],
                            Alignment::Right,
                            None
                        )),
                    );
                    print!(
                        "{}",
                        pad_str(&contents, column_widths[i], Alignment::Right, None),
                    );
                }
                println!()
            }
        }
    }

    for report in reports {
        if !report.current.error_messages.is_empty() {
            let error = format!(
                "Bench encountered errors: {}",
                report.current.tags.get_name()
            );
            println!("{}", style(error).red().bold());
            for (i, message) in report.current.error_messages.iter().enumerate() {
                let line = format!("    {i}.  {message}");
                println!("{}", line);
            }
        }

        if let Some(baseline) = &report.baseline {
            if !baseline.error_messages.is_empty() {
                let error = format!(
                    "Bench baseline encountered errors: {}",
                    report.current.tags.get_name()
                );
                println!("{}", style(error).red().bold());
                for (i, message) in report.current.error_messages.iter().enumerate() {
                    let line = format!("    {i}.  {message}");
                    println!("{}", line);
                }
            }
        }
    }

    let errors_found = reports.iter().any(|x| {
        !x.current.error_messages.is_empty()
            || x.baseline
                .as_ref()
                .map(|x| !x.error_messages.is_empty())
                .unwrap_or(false)
    });
    let not_running_in_release_found = reports.iter().any(|x| {
        !x.current.running_in_release
            || x.baseline
                .as_ref()
                .map(|x| !x.error_messages.is_empty())
                .unwrap_or(false)
    });
    if errors_found && not_running_in_release_found {
        // ensure these two sections are kept apart
        println!();
    }

    for report in reports {
        if !report.current.running_in_release {
            let error = format!(
                "Bench results invalid! Bench compiled with non-release profile: {}",
                report.current.tags.get_name()
            );
            println!("{}", style(error).red().bold());
        }

        if let Some(baseline) = &report.baseline {
            if !baseline.running_in_release {
                let error = format!(
                    "Baseline bench results invalid! Baseline bench compiled with non-release profile: {}",
                    baseline.tags.get_name()
                );
                println!("{}", style(error).red().bold());
            }
        }
    }
}

fn duration_ms(duration: Duration) -> String {
    format!("{:.3}ms", duration.as_micros() as f32 / 1000.0)
}

enum Row {
    Heading(String),
    ColumnNames {
        legend: String,
        names: Vec<String>,
    },
    Measurements {
        legend: String,
        measurements: Vec<Measurement>,
    },
}

struct Measurement {
    value: String,
    comparison: String,
    color: Color,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Goal {
    BiggerIsBetter,
    SmallerIsBetter,
    None,
}

enum Color {
    Good,
    Bad,
    Neutral,
}

impl Row {
    fn measurements<F: Fn(&ReportArchive) -> Option<(f64, String, Goal)>>(
        reports: &[ReportColumn],
        legend: &str,
        f: F,
    ) -> Row {
        let legend = legend.to_owned();
        let measurements = reports
            .iter()
            .map(|x| {
                let (value, comparison, comparison_raw, goal) =
                    if let Some((compare, value, goal)) = f(&x.current) {
                        if let Some((base, _, _)) = x.baseline.as_ref().and_then(&f) {
                            let comparison_raw: f64 = (compare - base) / base * 100.0;
                            let comparison = if comparison_raw.is_nan() {
                                "-".into()
                            } else {
                                format!("{:+.1}%", comparison_raw)
                            };

                            (value, comparison, comparison_raw, goal)
                        } else {
                            (value, "".to_owned(), 0.0, Goal::BiggerIsBetter)
                        }
                    } else {
                        ("".to_owned(), "".to_owned(), 0.0, Goal::BiggerIsBetter)
                    };

                let color = if comparison_raw > 5.0 {
                    if let Goal::BiggerIsBetter = goal {
                        Color::Good
                    } else {
                        Color::Bad
                    }
                } else if comparison_raw < -5.0 {
                    if let Goal::SmallerIsBetter = goal {
                        Color::Good
                    } else {
                        Color::Bad
                    }
                } else {
                    Color::Neutral
                };
                Measurement {
                    value,
                    comparison,
                    color,
                }
            })
            .collect();
        Row::Measurements {
            legend,
            measurements,
        }
    }
}
