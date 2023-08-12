use clap::Parser;

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

    /// List the name of every bench.
    #[clap(long, verbatim_doc_comment)]
    pub list: bool,

    /// Run a specific bench with the name produced via `--list`.
    #[clap(long, verbatim_doc_comment)]
    pub name: Option<String>,

    /// Instruct benches to profile the application under test with the specified profilers.
    /// Benches that do not support the specified profilers will be skipped.
    #[clap(long, verbatim_doc_comment)]
    pub profilers: Vec<String>,

    /// How long in seconds to run each bench for.
    /// By default benches will run for 15 seconds.
    #[clap(long, verbatim_doc_comment)]
    pub bench_length_seconds: Option<u32>,

    /// Instruct the benches to cap their operations per second to the specified amount.
    /// By default the benches will run with unlimited operations per second.
    #[clap(long, verbatim_doc_comment)]
    pub operations_per_second: Option<u64>,

    /// The results of the last benchmarks run becomes the new baseline from which following benchmark runs will be compared.
    /// Baseline bench results are merged with the results of following results under a `baseline=true` tag.
    #[clap(long, verbatim_doc_comment)]
    pub set_baseline: bool,

    /// Removes the stored baseline. Following runs will no longer compare against a baseline.
    #[clap(long, verbatim_doc_comment)]
    pub clear_baseline: bool,

    /// Generate graphs webpage from the last benchmarks run.
    #[clap(long, verbatim_doc_comment)]
    pub generate_webpage: bool,

    /// By default windsock will run benches on your local machine.
    /// Set this flag to have windsock run the benches in your configured cloud.
    #[clap(long, verbatim_doc_comment)]
    pub cloud: bool,

    /// Windsock will automatically cleanup cloud resources after benches have been run.
    /// However this command exists to force cleanup in case windsock panicked before automatic cleanup could occur.
    #[clap(long, verbatim_doc_comment)]
    pub cleanup_cloud_resources: bool,

    /// Display results from the last benchmark run by:
    ///     Comparing various benches against a specific base bench.
    ///
    /// Usage: First provide the base benchmark name then provide benchmark names to compare against the base.
    ///     --compare_by_name "base_name other_name1 other_name2"
    #[clap(long, verbatim_doc_comment)]
    pub compare_by_name: Option<String>,

    /// Display results from the last benchmark run by:
    ///     Comparing benches matching tag filters against a specific base bench.
    ///
    /// Usage: First provide the base benchmark name then provide tag filters
    ///     --compare_by_tags "base_name db=kafka OPS=10000"
    #[clap(long, verbatim_doc_comment)]
    pub compare_by_tags: Option<String>,

    /// Display results from the last benchmark run by:
    ///     Comparing benches with specified bench names.
    ///
    /// Usage: Provide benchmark names to include
    ///     --results-by-name "name1 name2 name3"
    #[clap(long, verbatim_doc_comment)]
    pub results_by_name: Option<String>,

    /// Display results from the last benchmark run by:
    ///     Listing bench results matching tag filters.
    ///
    /// Usage: Provide tag filters
    ///     --results-by-tags "db=kafka OPS=10000"
    #[clap(long, verbatim_doc_comment)]
    pub results_by_tags: Option<String>,

    /// Display results from the last benchmark run by:
    ///     Comparing all benches matching tag filters against their results in the stored baseline from `--set-baseline`.
    ///
    /// Usage: Provide tag filters
    ///     --baseline-compare-by-tags "db=kafka OPS=10000"
    #[clap(long, verbatim_doc_comment)]
    pub baseline_compare_by_tags: Option<String>,

    /// Not for human use. Call this from your bench orchestration method to launch your bencher.
    #[clap(long, verbatim_doc_comment)]
    pub internal_run: Option<String>,
}
