use inferno::collapse::perf::{Folder, Options as CollapseOptions};
use inferno::collapse::Collapse;
use inferno::flamegraph::{from_reader, Options};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::process::{Child, Command};

pub struct Perf {
    child: Child,
    path: PathBuf,
    perf_file: PathBuf,
}

impl Perf {
    /// Begin recording perf data to perf.data
    pub fn new(path: PathBuf, pid: u32) -> Perf {
        let perf_file = path.join("perf.data");
        // remove perf.data if it exists to avoid generating perf.data.old clutter
        std::fs::remove_file(&perf_file).ok();

        // Run `sudo ls` so that we can get sudo to stop asking for password for a while.
        // It also lets us block until the user has entered the password, otherwise the rest of the test would continue before `perf` has started.
        // TODO: It would be more robust to get a single root prompt and then keep feeding commands into it.
        //       But that would require a bunch of work so its out of scope for now.
        test_helpers::run_command("sudo", &["ls"]).unwrap();

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
            perf_file.to_str().unwrap(),
            "-p",
            &pid.to_string(),
        ]);

        Perf {
            child: command.spawn().unwrap(),
            path,
            perf_file,
        }
    }

    /// Blocks until the recorded process has terminated
    /// Generates a flamegraph at flamegraph.svg
    pub fn flamegraph(mut self) {
        self.child.wait().unwrap();

        test_helpers::run_command(
            "sudo",
            &[
                "chown",
                &std::env::var("USER").unwrap(),
                self.perf_file.to_str().unwrap(),
            ],
        )
        .unwrap();

        let output = Command::new("perf")
            .args([
                "script",
                "-i",
                self.perf_file.to_str().unwrap(),
                // --inline (the default) is so slow on CI that it times out after 5h 😭
                //https://bugzilla.kernel.org/show_bug.cgi?id=202677
                if std::env::var("CI").is_ok() {
                    "--no-inline"
                } else {
                    "--inline"
                },
            ])
            .output()
            .unwrap();
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

        let flamegraph_file = self.path.join("flamegraph.svg");
        println!("writing flamegraph to {flamegraph_file:?}");
        let flamegraph_writer = BufWriter::new(File::create(flamegraph_file).unwrap());

        let mut options = Options::default();
        options.title = self.path.file_name().unwrap().to_str().unwrap().to_owned();
        from_reader(&mut options, collapsed_reader, flamegraph_writer).unwrap();
    }
}
