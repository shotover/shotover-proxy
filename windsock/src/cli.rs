use anyhow::{anyhow, Error};
use clap::{Parser, Subcommand};

#[derive(Subcommand, Clone)]
pub enum Command {
    /// List the name of every bench.
    #[clap(verbatim_doc_comment)]
    List,

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

        /// e.g. "db=kafka OPS=10000"
        #[clap(verbatim_doc_comment)]
        filter: Option<String>,
    },

    /// Display results from the last benchmark run by:
    ///     Comparing various benches against a specific base bench.
    ///
    /// Usage: First provide the base benchmark name then provide benchmark names to compare against the base.
    ///     --compare_by_name "base_name other_name1 other_name2"
    #[clap(verbatim_doc_comment)]
    CompareByName { filter: String },

    /// Display results from the last benchmark run by:
    ///     Comparing benches matching tag filters against a specific base bench.
    ///
    /// Usage: First provide the base benchmark name then provide tag filters
    ///     --compare_by_tags "base_name db=kafka OPS=10000"
    #[clap(verbatim_doc_comment)]
    CompareByTags { filter: String },
}

const ABOUT: &str = r#"Bench Names:
    Each benchmark has a unique name, this name is used by many options listed below.
    The name is derived from its tags so you wont find it directly in the bench implementation but it will be listed in `--list`.

Tag Filters:
    Many options below take tag filters that specify which benches to include.
    Tag filters specify which benches to include and the filter results are unioned.

    So:
    * The filter "foo=some_value" will include only benches with the tag key `foo` and the tag value `some_value`
    * The filter "foo=some_value bar=another_value" will include only benches that match "foo=some_value" and "bar=another_value"
    * The filter "" will include all benches"#;

#[derive(Parser, Clone)]
#[clap(about=ABOUT)]
pub struct Args {
    /// Run all benches that match the specified tag key/values.
    /// `tag_key=tag_value foo=bar`
    #[clap(verbatim_doc_comment)]
    pub filter: Option<String>,

    #[command(subcommand)]
    pub command: Option<Command>,

    /// Run a specific bench with the name produced via `--list`.
    #[clap(long, verbatim_doc_comment)]
    pub name: Option<String>,

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

    /// By default windsock will run benches on your local machine.
    /// Set this flag to have windsock run the benches in your configured cloud.
    #[clap(long, verbatim_doc_comment)]
    pub cloud: bool,

    /// Windsock will automatically cleanup cloud resources after benches have been run.
    /// However this command exists to force cleanup in case windsock panicked before automatic cleanup could occur.
    #[clap(long, verbatim_doc_comment)]
    pub cleanup_cloud_resources: bool,

    /// Skip running of benches.
    /// Skip automatic deletion of cloud resources on bench run completion.
    /// Instead, just create cloud resources and write details of the resources to disk so they may be restored via `--load-cloud-resources-file`
    #[clap(long, verbatim_doc_comment)]
    pub store_cloud_resources_file: bool,

    /// Skip automatic creation of cloud resources on bench run completion.
    /// Skip automatic deletion of cloud resources on bench run completion.
    /// Instead, details of the resources are loaded from disk as saved via a previous run using `--store-cloud-resources-file`
    #[clap(long, verbatim_doc_comment)]
    pub load_cloud_resources_file: bool,

    /// Not for human use. Call this from your bench orchestration method to launch your bencher.
    #[clap(long, verbatim_doc_comment)]
    pub internal_run: Option<String>,

    #[clap(long, hide(true))]
    list: bool,

    #[clap(long, hide(true))]
    format: Option<NextestFormat>,

    #[clap(long, hide(true))]
    ignored: bool,

    #[clap(long, hide(true))]
    exact: bool,

    #[clap(long, hide(true))]
    nocapture: bool,
}

#[derive(clap::ValueEnum, Clone, Copy)]
enum NextestFormat {
    Terse,
}

impl Args {
    pub fn nextest_list(&self) -> bool {
        self.list
    }

    pub fn nextest_list_all(&self) -> bool {
        self.list && matches!(&self.format, Some(NextestFormat::Terse)) && !self.ignored
    }

    pub fn nextest_list_ignored(&self) -> bool {
        self.list && matches!(&self.format, Some(NextestFormat::Terse)) && self.ignored
    }

    pub fn nextest_run_by_name(&self) -> bool {
        self.nocapture && self.exact
    }

    pub fn nextest_invalid_args(&self) -> Option<Error> {
        if self.format.is_some() && self.list {
            Some(anyhow!("`--format` only exists for nextest compatibility and is not supported without `--list`"))
        } else if self.nocapture && !self.exact {
            Some(anyhow!("`--nocapture` only exists for nextest compatibility and is not supported without `--exact`"))
        } else if self.exact && !self.nocapture {
            Some(anyhow!("`--nocapture` only exists for nextest compatibility and is not supported without `--nocapture`"))
        } else {
            None
        }
    }
}
