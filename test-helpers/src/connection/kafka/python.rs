use tokio::process::Command;

use crate::run_command_async;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

pub async fn run_python_smoke_test(address: &str) {
    ensure_uv_is_installed().await;

    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connection/kafka/python");
    let uv_binary = uv_binary_path();
    let config = format!(
        r#"{{
    'bootstrap_servers': ["{address}"],
}}"#
    );
    tokio::time::timeout(
        Duration::from_secs(60),
        run_command_async(
            &project_dir,
            uv_binary.to_str().unwrap(),
            &["run", "main.py", &config],
        ),
    )
    .await
    .unwrap();
}

pub async fn run_python_smoke_test_sasl_plain(address: &str, user: &str, password: &str) {
    ensure_uv_is_installed().await;

    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connection/kafka/python");
    let uv_binary = uv_binary_path();
    let config = format!(
        r#"{{
    'bootstrap_servers': ["{address}"],
    'security_protocol': "SASL_PLAINTEXT",
    'sasl_mechanism': "PLAIN",
    'sasl_plain_username': "{user}",
    'sasl_plain_password': "{password}",
}}"#
    );
    tokio::time::timeout(
        Duration::from_secs(60),
        run_command_async(
            &project_dir,
            uv_binary.to_str().unwrap(),
            &["run", "main.py", &config],
        ),
    )
    .await
    .unwrap();
}

pub async fn run_python_smoke_test_sasl_scram(address: &str, user: &str, password: &str) {
    ensure_uv_is_installed().await;

    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connection/kafka/python");
    let uv_binary = uv_binary_path();
    let config = format!(
        r#"{{
    'bootstrap_servers': ["{address}"],
    'security_protocol': "SASL_PLAINTEXT",
    'sasl_mechanism': "SCRAM-SHA-256",
    'sasl_plain_username': "{user}",
    'sasl_plain_password': "{password}",
}}"#
    );
    tokio::time::timeout(
        Duration::from_secs(60),
        run_command_async(
            &project_dir,
            uv_binary.to_str().unwrap(),
            &["run", "main.py", &config],
        ),
    )
    .await
    .unwrap();
}

pub async fn run_python_bad_auth_sasl_scram(address: &str, user: &str, password: &str) {
    ensure_uv_is_installed().await;

    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connection/kafka/python");
    let uv_binary = uv_binary_path();
    let config = format!(
        r#"{{
    'bootstrap_servers': ["{address}"],
    'security_protocol': "SASL_PLAINTEXT",
    'sasl_mechanism': "SCRAM-SHA-256",
    'sasl_plain_username': "{user}",
    'sasl_plain_password': "{password}",
}}"#
    );
    tokio::time::timeout(
        Duration::from_secs(60),
        run_command_async(
            &project_dir,
            uv_binary.to_str().unwrap(),
            &["run", "auth_fail.py", &config],
        ),
    )
    .await
    .unwrap();
}

/// Install a specific version of UV to:
/// * avoid developers having to manually install an external tool
/// * avoid issues due to a different version being installed
pub async fn ensure_uv_is_installed() {
    let uv_binary = uv_binary_path();

    if let Ok(output) = Command::new(uv_binary).arg("--help").output().await
        && output.status.success()
    {
        // already correctly installed
        return;
    }

    run_command_async(
        Path::new("."),
        "bash",
        &[
            "-c",
            // Install to a custom path to avoid overwriting any UV already installed by the user.
            // Specifically uses `..` instead of absolute path to avoid spaces messing up the bash script.
            "curl -LsSf https://astral.sh/uv/0.4.6/install.sh | env INSTALLER_NO_MODIFY_PATH=1 UV_INSTALL_DIR=../target/uv sh",
        ],
    )
    .await
}

/// The path the uv binary is installed to
fn uv_binary_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/uv/bin/uv")
}
