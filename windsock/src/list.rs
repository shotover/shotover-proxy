use crate::{bench::BenchState, cli::WindsockArgs};

pub fn list<ResourcesRequired, Resources>(benches: &[BenchState<ResourcesRequired, Resources>]) {
    // regular usage
    println!("Benches:");
    for bench in benches {
        println!("{}", bench.tags.get_name());
    }
}

pub fn nextest_list<ResourcesRequired, Resources>(
    args: &WindsockArgs,
    benches: &[BenchState<ResourcesRequired, Resources>],
) {
    if args.nextest_list_all() {
        // list all windsock benches in nextest format
        for bench in benches {
            println!("{}: benchmark", bench.tags.get_name());
        }
    } else if args.nextest_list_ignored() {
        // windsock does not support ignored tests
    } else {
        // in case the user accidentally runs `--list` just give them the regular `list` output.
        list(benches);
    }
}
