use clap::Parser;
use test_helpers::docker_compose::DockerCompose;
use test_helpers::shotover_process::ShotoverProcess;

/// e.g.
/// cargo run --release --example cassandra_bench -- --example-dir cassandra-passthrough -r 1000
#[derive(Parser, Clone)]
#[clap(name = "cassandra_bench", arg_required_else_help = true)]
pub struct Args {
    #[clap(short, long)]
    pub example_dir: String,

    #[clap(short, long)]
    pub rate: u64,
}

fn main() {
    if env!("PROFILE") != "release" {
        println!("Need to run with --release flag");
        return;
    }

    let args = Args::parse();

    // The benches and tests automatically set the working directory to CARGO_MANIFEST_DIR.
    // We need to do the same as the DockerCompose + ShotoverManager types rely on this.
    std::env::set_current_dir(env!("CARGO_MANIFEST_DIR")).unwrap();

    let latte = Latte::new(args.rate);
    {
        let _compose = DockerCompose::new(&format!(
            "example-configs/{}/docker-compose.yml",
            args.example_dir
        ));

        // Uses ShotoverProcess instead of ShotoverManager for a more accurate benchmark
        let _shotover_manager = ShotoverProcess::new(&format!(
            "example-configs/{}/topology.yaml",
            args.example_dir
        ));

        println!("Benching Shotover ...");
        bench_read(&latte, "localhost:9043", "localhost:9042");
        println!("Benching Direct Cassandra ...");
        bench_read(&latte, "localhost:9043", "localhost:9043");
    }

    println!("Direct Cassandra (A) vs Shotover (B)");
    latte.compare("read-localhost:9042.json", "read-localhost:9043.json");
}

fn bench_read(latte: &Latte, address_load: &str, address_bench: &str) {
    latte.bench("read", address_load, address_bench)
}

// TODO: Shelling out directly like this is just for experimenting.
// Either:
// * get access to latte as a crate
// * write our own benchmark logic
struct Latte {
    rate: u64,
}

impl Latte {
    fn new(rate: u64) -> Latte {
        test_helpers::docker_compose::run_command(
            "cargo",
            &[
                "install",
                "--git",
                "https://github.com/pkolaczk/latte",
                "--rev",
                "3294afdb56ddea77f9f56bc795f325cb734b352c",
            ],
        )
        .unwrap();
        Latte { rate }
    }

    fn bench(&self, name: &str, address_load: &str, address_bench: &str) {
        test_helpers::docker_compose::run_command(
            "latte",
            &[
                "schema",
                "--user",
                "cassandra",
                "--password",
                "cassandra",
                &format!("examples/{name}.rn"),
                "--",
                address_load,
            ],
        )
        .unwrap();
        test_helpers::docker_compose::run_command(
            "latte",
            &[
                "load",
                "--user",
                "cassandra",
                "--password",
                "cassandra",
                &format!("examples/{name}.rn"),
                "--",
                address_load,
            ],
        )
        .unwrap();
        test_helpers::docker_compose::run_command(
            "latte",
            &[
                "run",
                "--user",
                "cassandra",
                "--password",
                "cassandra",
                "--rate",
                &self.rate.to_string(),
                "--duration",
                "15s", // default is 60s but 15 seems fine
                "--connections",
                "128", // Shotover performs extremely poorly with 1 connection and this is not currently an intended usecase
                "--output",
                &format!("{name}-{address_bench}.json"),
                &format!("examples/{name}.rn"),
                "--",
                address_bench,
            ],
        )
        .unwrap();
    }

    fn compare(&self, first: &str, second: &str) {
        run_command_to_stdout("latte", &["show", first, "-b", second]);
    }
}

/// unlike test_helpers::docker_compose::run_command stdout of the command is sent to the stdout of the application
fn run_command_to_stdout(command: &str, args: &[&str]) {
    assert!(
        std::process::Command::new(command)
            .args(args)
            .status()
            .unwrap()
            .success(),
        "Failed to run: {command} {args:?}"
    );
}
