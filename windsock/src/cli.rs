use clap::Parser;

#[derive(Parser, Clone)]
#[clap()]
pub struct Args {
    // TODO: parse `filter` from `tag_name=tag_value tag_name2=bar`
    /// Run all benches that match the specified tag key/values
    pub filter: Option<String>,
    /// Run a specific bench with the name produced via `--list`
    #[clap(long)]
    pub name: Option<String>,
    /// List the name of every bench
    #[clap(long)]
    pub list: bool,
    /// Instruct benches to profile the application under test and produce a flamegraph
    #[clap(long)]
    pub flamegraph: bool,
}
