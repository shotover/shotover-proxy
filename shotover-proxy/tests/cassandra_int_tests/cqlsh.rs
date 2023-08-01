use std::path::Path;
use tokio::process::Command;

const CQLSH_VERSION: &str = "6.1.2";

fn venv_path() -> &'static Path {
    Path::new("../target/cqlsh_venv")
}

pub async fn check_basic_cqlsh(address: &str, port: u16, ca_cert: Option<&str>) {
    setup_cqlsh().await;

    let mut args = vec!["-e", "SELECT rack FROM system.local"];

    if ca_cert.is_some() {
        args.push("--ssl");
    }

    args.push(address);
    let port = port.to_string();
    args.push(&port);

    let output = run(Command::new(venv_path().join("bin").join("cqlsh"))
        .args(args)
        .env("SSL_CERTFILE", ca_cert.unwrap_or("")))
    .await;
    let expected = r#"
 rack
-------
 rack1

(1 rows)
"#;
    assert_eq!(expected, output);
}

async fn setup_cqlsh() {
    if need_to_reinstall().await {
        std::fs::remove_dir_all(venv_path()).ok();
        run(Command::new("python3").args(["-m", "venv", venv_path().to_str().unwrap()])).await;
        run(Command::new(venv_path().join("bin").join("pip"))
            .args(["install", &format!("cqlsh=={CQLSH_VERSION}")]))
        .await;
    }
}

async fn need_to_reinstall() -> bool {
    if !venv_path().exists() {
        tracing::warn!("Installing cqlsh because it is not installed");
        return true;
    }
    match Command::new(venv_path().join("bin").join("pip"))
        .args(["show", "cqlsh"])
        .output()
        .await
    {
        Err(err) => {
            tracing::warn!("Reinstalling cqlsh because venv is broken, failed to run pip {err}");
            true
        }
        Ok(output) => {
            let stdout = String::from_utf8(output.stdout).unwrap();
            let stderr = String::from_utf8(output.stderr).unwrap();
            if !output.status.success() {
                tracing::warn!("Reinstalling cqlsh because `pip show cqlsh` failed\nstdout: {stdout}\nstderr: {stderr}");
                return true;
            }

            if stdout.contains(&format!("Version: {CQLSH_VERSION}")) {
                //the correct version of cqlsh is installed
                false
            } else {
                tracing::warn!("Reinstalling cqlsh because it is not the correct version");
                true
            }
        }
    }
}

async fn run(command: &mut Command) -> String {
    let output = command.output().await.unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    if !output.status.success() {
        panic!("command failed:\n{command:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",);
    }
    stdout
}
