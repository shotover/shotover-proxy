use nix::sys::signal::Signal;
use nix::unistd::Pid;
use std::process::{Child, Command, Stdio};

pub struct ShotoverProcess {
    /// Always Some while ShotoverProcess is owned
    pub child: Option<Child>,
}

impl Drop for ShotoverProcess {
    fn drop(&mut self) {
        if let Some(child) = &self.child {
            if let Err(err) =
                nix::sys::signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGKILL)
            {
                println!("Failed to shutdown ShotoverProcess {err}");
            }
        }
    }
}

impl ShotoverProcess {
    #[allow(unused)]
    pub fn new(topology_path: &str) -> ShotoverProcess {
        // First ensure shotover is fully built so that the potentially lengthy build time is not included in the wait_for_socket_to_open timeout
        // PROFILE is set in build.rs from PROFILE listed in https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
        let all_args = if env!("PROFILE") == "release" {
            vec!["build", "--all-features", "--release"]
        } else {
            vec!["build", "--all-features"]
        };
        assert!(Command::new(env!("CARGO"))
            .args(&all_args)
            .stdout(Stdio::piped())
            .status()
            .unwrap()
            .success());

        // Now actually run shotover and keep hold of the child process
        let all_args = if env!("PROFILE") == "release" {
            vec![
                "run",
                "--all-features",
                "--release",
                "--",
                "-t",
                topology_path,
            ]
        } else {
            vec!["run", "--all-features", "--", "-t", topology_path]
        };
        let child = Some(
            Command::new(env!("CARGO"))
                .env("RUST_LOG", "debug,shotover_proxy=debug")
                .args(&all_args)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );

        crate::wait_for_socket_to_open("127.0.0.1", 9001); // Wait for observability metrics port to open

        ShotoverProcess { child }
    }

    #[allow(unused)]
    fn pid(&self) -> Pid {
        Pid::from_raw(self.child.as_ref().unwrap().id() as i32)
    }

    #[allow(unused)]
    pub fn signal(&self, signal: Signal) {
        nix::sys::signal::kill(self.pid(), signal).unwrap();
    }

    #[allow(unused)]
    pub fn wait(mut self) -> (Option<i32>, String, String) {
        let output = self.child.take().unwrap().wait_with_output().unwrap();

        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();

        (output.status.code(), stdout, stderr)
    }
}
