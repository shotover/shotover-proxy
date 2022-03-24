use crate::docker_compose::run_command;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use std::process::{Child, Command, Stdio};

pub struct ShotoverProcess {
    /// Always Some while ShotoverProcess is owned
    pub child: Option<Child>,
    assert_exit: AssertExitsWith,
}

impl Drop for ShotoverProcess {
    fn drop(&mut self) {
        if let Some(child) = self.child.take() {
            if let Err(err) =
                nix::sys::signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGKILL)
            {
                println!("Failed to shutdown ShotoverProcess {err}");
            }

            if !std::thread::panicking() {
                let output = child.wait_with_output().unwrap();

                let stdout = String::from_utf8(output.stdout).unwrap();
                let stderr = String::from_utf8(output.stderr).unwrap();
                let exit_code = output.status.code().unwrap();
                match self.assert_exit {
                    AssertExitsWith::Success => {
                        if exit_code != 0 {
                            panic!(
                            "Shotover exited with {} but expected 0 exit code (Success).\nstdout: {}\nstderr: {}",
                            exit_code, stdout, stderr
                        );
                        }
                    }
                    AssertExitsWith::Failure => {
                        if exit_code == 0 {
                            panic!(
                            "Shotover exited with {} but expected non 0 exit code (Failure).\nstdout: {}\nstderr: {}",
                            exit_code, stdout, stderr
                        );
                        }
                    }
                };
            }
        }
    }
}

pub enum AssertExitsWith {
    Success,
    #[allow(unused)]
    Failure,
}

impl ShotoverProcess {
    #[allow(unused)]
    pub fn new(topology_path: &str, assert_exit: AssertExitsWith) -> ShotoverProcess {
        // First ensure shotover is fully built so that the potentially lengthy build time is not included in the wait_for_socket_to_open timeout
        // PROFILE is set in build.rs from PROFILE listed in https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
        let all_args = if env!("PROFILE") == "release" {
            vec!["build", "--all-features", "--release"]
        } else {
            vec!["build", "--all-features"]
        };
        run_command(env!("CARGO"), &all_args).unwrap();

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
                .args(&all_args)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );

        crate::wait_for_socket_to_open("127.0.0.1", 9001); // Wait for observability metrics port to open

        ShotoverProcess { child, assert_exit }
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
    pub fn wait(mut self) -> WaitOutput {
        let output = self.child.take().unwrap().wait_with_output().unwrap();

        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();

        WaitOutput {
            exit_code: output.status.code().unwrap(),
            stdout,
            stderr,
        }
    }
}

pub struct WaitOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}
