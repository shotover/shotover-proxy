use crate::run_command_to_stdout;

// TODO: Shelling out directly like this is just for experimenting.
// Either:
// * get access to latte as a crate
// * write our own benchmark logic
pub struct Latte {
    rate: u64,
    threads: u64,
}

impl Latte {
    pub fn new(rate: u64, threads: u64) -> Latte {
        crate::docker_compose_runner::run_command(
            "cargo",
            &[
                "install",
                "--git",
                "https://github.com/pkolaczk/latte",
                "--rev",
                "571e9ed2456e85668890cb4599686c8ccd43adad",
            ],
        )
        .unwrap();
        Latte { rate, threads }
    }

    pub fn init(&self, name: &str, address_load: &str) {
        crate::docker_compose_runner::run_command(
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
        crate::docker_compose_runner::run_command(
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
    }

    pub fn bench(&self, name: &str, address_bench: &str) {
        crate::docker_compose_runner::run_command(
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
                "--threads",
                &self.threads.to_string(),
                "--output",
                &format!("{name}-{address_bench}.json"),
                &format!("examples/{name}.rn"),
                "--",
                address_bench,
            ],
        )
        .unwrap();
    }

    pub fn compare(&self, file_a: &str, file_b: &str) {
        run_command_to_stdout("latte", &["show", file_b, "-b", file_a]);
    }
}
