use crate::run_command_to_stdout;

pub fn run_producer_bench(address_bench: &str) {
    run_command_to_stdout(
        "kafka-producer-perf-test.sh",
        &[
            "--producer-props",
            &format!("bootstrap.servers={address_bench}"),
            "--record-size",
            "1000",
            "--throughput",
            "-1",
            "--num-records",
            "5000000",
            "--topic",
            "foo",
        ],
    );
}
