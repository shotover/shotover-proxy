use crate::run_command_async;
use std::{path::Path, time::Duration};

pub async fn run_go_smoke_test() {
    let project_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/connection/cassandra/go");
    tokio::time::timeout(
        Duration::from_secs(60),
        run_command_async(&project_dir, "go", &["run", "basic.go"]),
    )
    .await
    .unwrap();
}
