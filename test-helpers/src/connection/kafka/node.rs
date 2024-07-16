use std::path::Path;

pub async fn run_node_smoke_test(address: &str) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connection/kafka/node");
    let config = format!(
        r#"({{
    clientId: 'nodejs-client',
    brokers: ["{address}"],
}})"#
    );
    run_command(&dir, "npm", &["install"]).await;
    run_command(&dir, "npm", &["start", &config]).await;
}

pub async fn run_node_smoke_test_scram(address: &str, user: &str, password: &str) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connection/kafka/node");
    let config = format!(
        r#"({{
    clientId: 'nodejs-client',
    brokers: ["{address}"],
    sasl: {{
       mechanism: 'scram-sha-256',
       username: '{user}',
       password: '{password}'
    }}
}})"#
    );
    run_command(&dir, "npm", &["install"]).await;
    run_command(&dir, "npm", &["start", &config]).await;
}

async fn run_command(current_dir: &Path, command: &str, args: &[&str]) -> String {
    let output = tokio::process::Command::new(command)
        .args(args)
        .current_dir(current_dir)
        .output()
        .await
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    if !output.status.success() {
        panic!("command {command} {args:?} failed:\nstdout:\n{stdout}\nstderr:\n{stderr}")
    }
    stdout
}
