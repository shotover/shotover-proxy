use clap::Parser;

#[derive(Parser, Clone)]
#[clap()]
pub struct Args {
    /// Run all benches that match the specified tag key/values
    /// `tag_key=tag_value foo=bar`
    pub filter: Option<String>,
    /// List the name of every bench
    #[clap(long)]
    pub list: bool,
    /// Run a specific bench with the name produced via `--list`
    #[clap(long)]
    pub name: Option<String>,
    /// Instruct benches to profile the application under test with the specified profilers
    /// Benches that do not support the specified profilers will be skipped.
    #[clap(long)]
    pub profilers: Vec<String>,
    /// How long in seconds to run each bench for
    #[clap(long)]
    pub bench_length_seconds: Option<u32>,
    /// Max operations per second
    #[clap(long)]
    pub operations_per_second: Option<u64>,

    /// The results of the last benchmarks run becomes the new baseline from which following benchmark runs will be compared.
    /// Baseline bench results are merged with the results of following results under a `baseline=true` tag.
    #[clap(long)]
    pub set_baseline: bool,

    /// Removes the stored baseline. Following runs will no longer compare against a baseline.
    #[clap(long)]
    pub clear_baseline: bool,

    /// Generate graphs webpage from the last benchmarks run
    #[clap(long)]
    pub generate_webpage: bool,

    /// Display results from the last benchmark run by: comparing various benches against a specific base bench
    /// The first bench name listed becomes the base bench
    /// --compare_by_name "base_name other_name1 other_name2"
    #[clap(long)]
    pub compare_by_name: Option<String>,

    /// Display results from the last benchmark run by: comparing benches matching tag filters against a base named bench
    /// First list the base_name then provide tag filters
    /// --compare_by_tags "base_name db=kafka OPS=10000"
    #[clap(long)]
    pub compare_by_tags: Option<String>,

    /// Display results from the last benchmark run by: comparing benches with specified bench names
    /// --results-by-name "name1 name2 name3"
    #[clap(long)]
    pub results_by_name: Option<String>,

    /// Display results from the last benchmark run by: listing bench results matching tag filters
    /// --results-by-tags "db=kafka OPS=10000"
    #[clap(long)]
    pub results_by_tags: Option<String>,

    /// Display results from the last benchmark run by: comparing all benches matching tag filters against their results in the stored baseline from `--set-baseline`
    /// --baseline-compare_by_tags "db=kafka OPS=10000"
    #[clap(long)]
    pub baseline_compare_by_tags: Option<String>,

    /// Prevent release mode safety check from triggering to allow running in debug mode
    #[clap(long)]
    pub disable_release_safety_check: bool,

    /// Not for human use. Call this from your bench orchestration method to launch your bencher.
    #[clap(long)]
    pub internal_run: Option<String>,
}
