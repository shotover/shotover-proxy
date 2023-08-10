use crate::message::{Encodable, Message};
use crate::transforms::{Transform, TransformBuilder, Transforms, Wrapper};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{error, info};

#[derive(Deserialize, Debug)]
pub struct DebugLogToFileConfig;

#[cfg(feature = "alpha-transforms")]
#[typetag::deserialize(name = "DebugLogToFile")]
#[async_trait(?Send)]
impl crate::transforms::TransformConfig for DebugLogToFileConfig {
    async fn get_builder(&self, _chain_name: String) -> Result<Box<dyn TransformBuilder>> {
        // This transform is used for debugging a specific run, so we clean out any logs left over from the previous run
        std::fs::remove_dir_all("message-log").ok();

        Ok(Box::new(DebugLogToFileBuilder {
            connection_counter: Arc::new(AtomicU64::new(0)),
        }))
    }
}

pub struct DebugLogToFileBuilder {
    connection_counter: Arc<AtomicU64>,
}

impl TransformBuilder for DebugLogToFileBuilder {
    fn build(&self) -> Transforms {
        self.connection_counter.fetch_add(1, Ordering::Relaxed);
        let connection_current = self.connection_counter.load(Ordering::Relaxed);

        let connection_dir = connection_current.to_string();
        let requests = Path::new("message-log")
            .join(&connection_dir)
            .join("requests");
        let responses = Path::new("message-log")
            .join(&connection_dir)
            .join("responses");

        std::fs::create_dir_all(&requests)
            .context("failed to create directory for logging requests")
            .unwrap();
        std::fs::create_dir_all(&responses)
            .context("failed to create directory for logging responses")
            .unwrap();

        Transforms::DebugLogToFile(DebugLogToFile {
            request_counter: 0,
            response_counter: 0,
            requests,
            responses,
        })
    }

    fn get_name(&self) -> &'static str {
        "DebugLogToFile"
    }
}

pub struct DebugLogToFile {
    request_counter: u64,
    response_counter: u64,
    requests: PathBuf,
    responses: PathBuf,
}

#[async_trait]
impl Transform for DebugLogToFile {
    async fn transform<'a>(&'a mut self, requests_wrapper: Wrapper<'a>) -> Result<Vec<Message>> {
        for message in &requests_wrapper.requests {
            self.request_counter += 1;
            let path = self
                .requests
                .join(format!("message{}.bin", self.request_counter));
            log_message(message, path.as_path()).await?;
        }

        let response = requests_wrapper.call_next_transform().await?;

        for message in &response {
            self.response_counter += 1;
            let path = self
                .responses
                .join(format!("message{}.bin", self.response_counter));
            log_message(message, path.as_path()).await?;
        }
        Ok(response)
    }
}

async fn log_message(message: &Message, path: &Path) -> Result<()> {
    info!("Logged message to {:?}", path);
    match message.clone().into_encodable() {
        Encodable::Bytes(bytes) => {
            tokio::fs::write(path, bytes).await.map_err(|e| {
                anyhow!(e).context(format!("failed to write message to disk at {path:?}"))
            })?;
        }
        Encodable::Frame(_) => {
            error!("Failed to log message because it was a frame. Ensure this Transform is the first transform in the main chain to ensure it only receives unmodified messages.")
        }
    }
    Ok(())
}
