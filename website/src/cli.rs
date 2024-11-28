use clap::Parser;

/// Generates the shotover website.
#[derive(Parser, Clone)]
#[clap()]
pub struct Args {
    /// As well as generating the site, serve the contents of the site over http.
    #[clap(long)]
    pub serve: bool,
}
