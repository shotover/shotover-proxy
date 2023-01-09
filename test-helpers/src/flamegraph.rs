use inferno::collapse::perf::{Folder, Options as CollapseOptions};
use inferno::collapse::Collapse;
use inferno::flamegraph::{from_reader, Options};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::process::{Child, Command};

use crate::docker_compose::run_command;

pub struct Perf(Child);

impl Perf {
    /// Begin recording perf data to perf.data
    pub fn new(pid: u32) -> Perf {
        // remove perf.data if it exists to avoid generating perf.data.old clutter
        std::fs::remove_file("perf.data").ok();

        // Run `sudo ls` so that we can get sudo to stop asking for password for a while.
        // It also lets us block until the user has entered the password, otherwise the rest of the test would continue before `perf` has started.
        // TODO: It would be more robust to get a single root prompt and then keep feeding commands into it.
        //       But that would require a bunch of work so its out of scope for now.
        run_command("sudo", &["ls"]).unwrap();

        let mut command = Command::new("sudo");
        command.args([
            "perf",
            "record",
            "-F",
            "997",
            "--call-graph",
            "dwarf,16384",
            "-g",
            "-o",
            "perf.data",
            "-p",
            &pid.to_string(),
        ]);

        Perf(command.spawn().unwrap())
    }

    /// Blocks until the recorded process has terminated
    /// Generates a flamegraph at flamegraph.svg
    pub fn flamegraph(mut self) {
        self.0.wait().unwrap();

        run_command(
            "sudo",
            &["chown", &std::env::var("USER").unwrap(), "perf.data"],
        )
        .unwrap();

        let output = Command::new("perf").args(["script"]).output().unwrap();
        if !output.status.success() {
            panic!(
                "unable to run 'perf script': ({}) {}",
                output.status,
                std::str::from_utf8(&output.stderr).unwrap()
            );
        }
        let output = output.stdout;

        let perf_reader = BufReader::new(&*output);
        let mut collapsed = vec![];
        let collapsed_writer = BufWriter::new(&mut collapsed);

        Folder::from(CollapseOptions::default())
            .collapse(perf_reader, collapsed_writer)
            .unwrap();

        let collapsed_reader = BufReader::new(&*collapsed);

        let filename = "flamegraph.svg";
        println!("writing flamegraph to {filename:?}");
        let flamegraph_writer = BufWriter::new(File::create(filename).unwrap());

        from_reader(&mut Options::default(), collapsed_reader, flamegraph_writer).unwrap();
    }
}
