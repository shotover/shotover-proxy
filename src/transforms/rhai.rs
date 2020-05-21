use crate::transforms::chain::{TransformChain, Wrapper, Transform, ChainResponse};
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use crate::transforms::{Transforms, TransformsFromConfig};
use crate::config::ConfigError;
use crate::runtimes::rhai::RhaiEnvironment;
use crate::config::topology::TopicHolder;
use slog::Logger;


#[derive(Clone)]
pub struct RhaiTransform {
    name: &'static str,
    function_env: RhaiEnvironment,
    logger: Logger
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct RhaiConfig {
    #[serde(rename = "config_values")]
    pub rhai_script: String

}

#[async_trait]
impl TransformsFromConfig for RhaiConfig {
    async fn get_source(&self, _: &TopicHolder, logger: &Logger) -> Result<Transforms, ConfigError> {
        Ok(Transforms::Rhai(RhaiTransform {
            name: "rhai",
            function_env: RhaiEnvironment::new(&self.rhai_script)?,
            logger: logger.clone()
        }))
    }
}


#[async_trait]
impl Transform for RhaiTransform {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse {
        //TODO rhai cannot parse rust enums, so make rhai functions that call the various messages
        // and process this via a match arm against the message type
        let mut qd_mod = self.function_env.call_rhai_transform_func(qd.clone()).await?;
        let response = self.call_next_transform(qd_mod, t).await;
        let result = self.function_env.call_rhai_transform_response_func(response).await?;
        return result;
    }

    fn get_name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod rhai_transform_tests {
    use super::RhaiConfig;
    use crate::transforms::{TransformsFromConfig, Transforms};
    use crate::config::topology::TopicHolder;
    use std::error::Error;
    use crate::transforms::chain::{Wrapper};
    use crate::message::{Message, QueryMessage, QueryType};
    use crate::protocols::cassandra_protocol2::RawFrame;
    use slog::info;
    use sloggers::Build;
    use sloggers::terminal::{TerminalLoggerBuilder, Destination};
    use sloggers::types::Severity;

    const TEST_STRING: &str = r###"


    "###;

    #[tokio::test(threaded_scheduler)]
    async fn test_rhai_script() -> Result<(), Box<dyn Error>> {
        let t_holder = TopicHolder {
            topics_rx: Default::default(),
            topics_tx: Default::default()
        };
        let rhai_t = RhaiConfig {
            rhai_script: String::from(TEST_STRING)
        };
        let wrapper: Wrapper = Wrapper::new(Message::Query(QueryMessage {
            original: RawFrame::NONE,
            query_string: "".to_string(),
            namespace: vec![String::from("keyspace"), String::from("old")],
            primary_key: Default::default(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None
        }));

        let mut builder = TerminalLoggerBuilder::new();
        builder.level(Severity::Debug);
        builder.destination(Destination::Stderr);

        let logger = builder.build().unwrap();

        if let Transforms::Rhai(mut rhai) = rhai_t.get_source(&t_holder, &logger).await? {
            let result = rhai.function_env.call_rhai_transform_func(wrapper).await?;
            if let Message::Query(qm) = &result.message {
                assert_eq!(qm.namespace, vec![String::from("keyspace"), String::from("new")])
            } else {
                panic!()
            }
        } else {
            panic!()
        }
        Ok(())
    }
}
