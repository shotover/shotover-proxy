use anyhow::{anyhow, Error};
use clap::{Args, Parser, Subcommand};

const ABOUT: &str = r#"Bench Names:
    Each benchmark has a unique name, this name is used by many options listed below.
    The name is derived from an alphabetical sorting of its tags so you wont find it directly in the bench
    implementation but it will be listed in the `list` command.

Tag Filters:
    Many options below take tag filters that specify which benches to include.
    Tag filters specify which benches to include and the filter results are unioned.

    So:
    * The filter "foo=some_value" will include only benches with the tag key `foo` and the tag value `some_value`
    * The filter "foo=some_value bar=another_value" will include only benches that match "foo=some_value" and "bar=another_value"
    * The filter "" will include all benches

    A filters tags can also be separated by commas allowing names to function as filters.
    So: foo=some_value,bar=another_value is a name but it can also be used where a filter is accepted."#;

#[derive(Subcommand, Clone)]
pub enum Command {
    /// List the name of every bench.
    #[clap(verbatim_doc_comment)]
    List,

    /// Create cloud resources for running benches
    #[clap(verbatim_doc_comment)]
    CloudSetup {
        /// e.g. "db=kafka connection_count=100"
        #[clap(verbatim_doc_comment)]
        filter: String,
    },

    /// Run benches in the cloud using the resources created by cloud-setup
    #[clap(verbatim_doc_comment)]
    CloudRun(RunArgs),

    /// cleanup cloud resources created by cloud-setup
    /// Make sure to call this when your benchmarking session is finished!
    #[clap(verbatim_doc_comment)]
    CloudCleanup,

    /// cloud-setup, cloud-run and cloud-cleanup combined into a single command.
    /// Convenient for getting a quick understanding of performance.
    /// However, if you are performing optimization work prefer the individual commands as you will get:
    /// * more stable results (same cloud instance)
    /// * faster results (skip recreating and destroying cloud resources)
    #[clap(verbatim_doc_comment)]
    CloudSetupRunCleanup(RunArgs),

    /// Run benches entirely on your local machine
    #[clap(verbatim_doc_comment)]
    LocalRun(RunArgs),

    /// The results of the last benchmarks run becomes the new baseline from which future benchmark runs will be compared.
    #[clap(verbatim_doc_comment)]
    BaselineSet,

    /// Removes the stored baseline. Following runs will no longer compare against a baseline.
    #[clap(verbatim_doc_comment)]
    BaselineClear,

    /// Generate graphs webpage from the last benchmarks run.
    #[clap(verbatim_doc_comment)]
    GenerateWebpage,

    /// Display results from the last benchmark run by:
    ///     Listing bench results matching tag filters.
    ///
    /// Usage: Provide tag filters
    #[clap(verbatim_doc_comment)]
    // TODO: get trailing_var_arg(true) working so user can avoid having to wrap in ""
    Results {
        /// Do not compare against the set baseline.
        #[clap(long, verbatim_doc_comment)]
        ignore_baseline: bool,

        /// e.g. "db=kafka connection_count=100"
        #[clap(verbatim_doc_comment)]
        filter: Option<String>,
    },

    /// Display results from the last benchmark run by:
    ///     Comparing various benches against a specific base bench.
    ///
    /// Usage: First provide the base benchmark name then provide benchmark names to compare against the base.
    ///     "base_name other_name1 other_name2"
    #[clap(verbatim_doc_comment)]
    CompareByName { filter: String },

    /// Display results from the last benchmark run by:
    ///     Comparing benches matching tag filters against a specific base bench.
    ///
    /// Usage: First provide the base benchmark name then provide tag filters
    ///     "base_name db=kafka connection_count=10"
    #[clap(verbatim_doc_comment)]
    CompareByTags { filter: String },

    /// Not for human use. Call this from your bench orchestration method to launch your bencher.
    #[clap(verbatim_doc_comment)]
    InternalRun(RunArgs),
}

#[derive(Args, Clone)]
pub struct RunArgs {
    /// Instruct benches to profile the application under test with the specified profilers.
    /// Benches that do not support the specified profilers will be skipped.
    #[clap(long, verbatim_doc_comment, value_delimiter = ',')]
    pub profilers: Vec<String>,

    /// How long in seconds to run each bench for.
    /// By default benches will run for 15 seconds.
    #[clap(long, verbatim_doc_comment)]
    pub bench_length_seconds: Option<u32>,

    /// Instruct the benches to cap their operations per second to the specified amount.
    /// By default the benches will run with unlimited operations per second.
    #[clap(long, verbatim_doc_comment)]
    pub operations_per_second: Option<u64>,

    /// Run all benches that match the specified tag key/values.
    /// `tag_key=tag_value foo=bar`
    #[clap(verbatim_doc_comment)]
    pub filter: Option<String>,
}

impl RunArgs {
    pub fn filter(&self) -> String {
        match &self.filter {
            // convert a name into a filter by swapping commas for spaces
            Some(filter) => filter.replace(',', " "),
            // If not provided use the empty filter
            None => String::new(),
        }
    }
}

#[derive(Parser)]
#[clap(about=ABOUT)]
pub struct WindsockArgs {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[clap(long, hide(true))]
    list: bool,

    #[clap(long, hide(true))]
    format: Option<NextestFormat>,

    #[clap(long, hide(true))]
    ignored: bool,

    #[clap(long, hide(true))]
    pub exact: Option<String>,

    #[clap(long, hide(true))]
    nocapture: bool,
}

#[derive(clap::ValueEnum, Clone, Copy)]
enum NextestFormat {
    Terse,
}

impl WindsockArgs {
    pub fn nextest_list(&self) -> bool {
        self.list
    }

    pub fn nextest_list_all(&self) -> bool {
        self.list && matches!(&self.format, Some(NextestFormat::Terse)) && !self.ignored
    }

    pub fn nextest_list_ignored(&self) -> bool {
        self.list && matches!(&self.format, Some(NextestFormat::Terse)) && self.ignored
    }

    pub fn nextest_run_by_name(&self) -> Option<&str> {
        if self.nocapture {
            self.exact.as_deref()
        } else {
            None
        }
    }

    pub fn nextest_invalid_args(&self) -> Option<Error> {
        if self.format.is_some() && self.list {
            Some(anyhow!("`--format` only exists for nextest compatibility and is not supported without `--list`"))
        } else if self.nocapture && self.exact.is_none() {
            Some(anyhow!("`--nocapture` only exists for nextest compatibility and is not supported without `--exact`"))
        } else if self.exact.is_some() && !self.nocapture {
            Some(anyhow!("`--exact` only exists for nextest compatibility and is not supported without `--nocapture`"))
        } else {
            None
        }
    }
}
