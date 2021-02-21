use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use shotover_transforms::{ChainResponse, TopicHolder, Transform, TransformsFromConfig, Wrapper};
use std::pin::Pin;
use tracing::info;

#[derive(Debug, Clone)]
pub struct Printer {
    name: &'static str,
    counter: i32,
}

impl Default for Printer {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct PrinterConfig {}

#[typetag::serde]
#[async_trait]
impl TransformsFromConfig for PrinterConfig {
    async fn get_source(&self, _topics: &TopicHolder) -> Result<Box<dyn Transform + Send + Sync>> {
        Ok(Box::new(Printer::new()))
    }
}

impl Printer {
    pub fn new() -> Printer {
        Printer {
            name: "Printer",
            counter: 0,
        }
    }
}

#[no_mangle]
pub fn get_configurator<'a>(config: String) -> Pin<Box<dyn TransformsFromConfig + Send + Sync>> {
    Box::pin(PrinterConfig {})
}

#[async_trait]
impl Transform for Printer {
    #[no_mangle]
    async fn transform<'a>(&'a mut self, mut qd: Wrapper<'a>) -> ChainResponse {
        info!("Request content: {:?}", qd.message);
        self.counter += 1;
        let response = qd.call_next_transform().await;
        info!("Response content: {:?}", response);
        response
    }

    #[no_mangle]
    fn get_name(&self) -> &'static str {
        self.name
    }
}
