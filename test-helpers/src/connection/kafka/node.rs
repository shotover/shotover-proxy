use std::path::Path;

use crate::run_command_async;

pub async fn run_node_smoke_test(address: &str) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connection/kafka/node");
    let config = format!(
        r#"({{
    clientId: 'nodejs-client',
    brokers: ["{address}"],
}})"#
    );
    run_command_async(&dir, "npm", &["install"]).await;
    run_command_async(&dir, "npm", &["start", &config]).await;
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
    run_command_async(&dir, "npm", &["install"]).await;
    run_command_async(&dir, "npm", &["start", &config]).await;
}
