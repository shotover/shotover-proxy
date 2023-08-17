use crate::{bench::BenchState, cli::Args};

pub fn list(args: &Args, benches: &[BenchState]) {
    if args.nextest_list_all() {
        // list all windsock benches in nextest format
        for bench in benches {
            println!("{}: benchmark", bench.tags.get_name());
        }
    } else if args.nextest_list_ignored() {
        // windsock does not support ignored tests
    } else {
        // regular usage
        println!("Benches:");
        for bench in benches {
            println!("{}", bench.tags.get_name());
        }
    }
}
