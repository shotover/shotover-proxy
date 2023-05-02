use std::{collections::HashSet, time::Duration};

use console::{pad_str, pad_str_with, style, Alignment};
use strum::IntoEnumIterator;

use crate::report::{Percentile, ReportArchive};

pub fn compare_by_name(names: &str) {
    let archives: Vec<ReportArchive> = names.split_whitespace().map(ReportArchive::load).collect();
    display_compare_table(&archives);
}

pub fn results_by_name(names: &str) {
    let archives: Vec<ReportArchive> = names.split_whitespace().map(ReportArchive::load).collect();
    display_results_table(&archives);
}

fn display_compare_table(reports: &[ReportArchive]) {
    if reports.len() < 2 {
        println!("Need at least two reports to display a comparison");
        return;
    }

    base(reports, "Comparison", true);
}

pub(crate) fn display_results_table(reports: &[ReportArchive]) {
    if reports.is_empty() {
        println!("Need at least one report to display results");
        return;
    }

    base(reports, "Results", false);
}

fn base(reports: &[ReportArchive], table_type: &str, comparison: bool) {
    let mut intersection = reports[0].tags.clone();
    for report in reports {
        intersection = intersection.intersection(&report.tags);
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
                &x.tags
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
    for key in nonintersecting_keys {
        rows.push(Row::ColumnNames {
            names: reports.iter().map(|x| x.tags.0[&key].clone()).collect(),
            legend: key,
        });
    }

    rows.push(Row::Heading("Measurements".to_owned()));

    rows.push(Row::measurements(reports, "Operations Total", |report| {
        (
            report.operations_total as f64,
            report.operations_total.to_string(),
            Goal::BiggerIsBetter,
        )
    }));
    rows.push(Row::measurements(reports, "Operations Per Sec", |report| {
        (
            report.ops as f64,
            format!("{:.0}", report.ops),
            Goal::BiggerIsBetter,
        )
    }));

    rows.push(Row::measurements(
        reports,
        "Operation Time Mean",
        |report| {
            (
                report.mean_response_time.as_secs_f64(),
                duration_ms(report.mean_response_time),
                Goal::SmallerIsBetter,
            )
        },
    ));

    rows.push(Row::Heading("Operation Time Percentiles".to_owned()));
    for (i, p) in Percentile::iter().enumerate() {
        rows.push(Row::measurements(reports, p.name(), |report| {
            (
                report.response_time_percentiles[i].as_secs_f64(),
                duration_ms(report.response_time_percentiles[i]),
                Goal::SmallerIsBetter,
            )
        }));
    }

    // the width of the legend column
    let legend_width: usize = rows
        .iter()
        .map(|x| match x {
            Row::Heading(_) => 0, // Ignore these
            Row::ColumnNames { legend, .. } => legend.len(),
            Row::Measurements { legend, .. } => legend.len(),
        })
        .max()
        .unwrap();
    // the width of the comparison compoenent of each column
    let comparison_widths: Vec<usize> = reports
        .iter()
        .enumerate()
        .map(|(i, _)| {
            rows.iter()
                .map(|x| match x {
                    Row::Heading(_) => 0,
                    Row::ColumnNames { .. } => 0,
                    Row::Measurements { measurements, .. } => measurements[i].comparison.len() + 1, // + 1 ensures we get seperation from the previous column
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
                        measurements[i].value.len()
                            + 1 // ensures we get seperation from the previous column
                            + if comparison { comparison_widths[i] } else { 0 }
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
                        .yellow()
                        .bold()
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
                    let contents = if comparison && i != 0 {
                        let colorer = match measurement.color {
                            Color::Good => |x| style(x).green(),
                            Color::Bad => |x| style(x).red(),
                            Color::Neutral => |x| style(x).dim(),
                        };
                        format!(
                            "{}{}",
                            measurement.value,
                            colorer(pad_str(
                                &measurement.comparison,
                                comparison_widths[i],
                                Alignment::Right,
                                None
                            )),
                        )
                    } else {
                        measurement.value
                    };
                    print!(
                        "{}",
                        pad_str(&contents, column_widths[i], Alignment::Right, None),
                    );
                }
                println!()
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

enum Goal {
    BiggerIsBetter,
    SmallerIsBetter,
}

enum Color {
    Good,
    Bad,
    Neutral,
}

impl Row {
    fn measurements<F: Fn(&ReportArchive) -> (f64, String, Goal)>(
        reports: &[ReportArchive],
        legend: &str,
        f: F,
    ) -> Row {
        let legend = legend.to_owned();
        let mut base = None;
        let measurements = reports
            .iter()
            .map(|x| {
                let (compare, value, goal) = f(x);
                let (comparison, comparison_raw) = if let Some(base) = base {
                    let comparison_raw = (compare - base) / base * 100.0;
                    (format!("{:+.1}%", comparison_raw), comparison_raw)
                } else {
                    base = Some(compare);
                    ("".to_owned(), 0.0)
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
