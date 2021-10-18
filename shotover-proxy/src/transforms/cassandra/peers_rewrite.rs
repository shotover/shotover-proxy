use crate::{
    config::topology::TopicHolder,
    error::ChainResponse,
    message::MessageDetails,
    transforms::{Transform, Transforms, Wrapper},
};
use crate::{message::QueryType, protocols::RawFrame};
use anyhow::Result;
use async_trait::async_trait;
use cassandra_proto::{
    frame::{
        frame_response::ResponseBody,
        frame_result::{BodyResResultRows, ResResultBody},
        Frame, IntoBytes, Opcode, Version,
    },
    types::CBytes,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct CassandraPeersRewriteConfig {
    pub emulate_single_node: bool,
    #[serde(rename = "new_port")]
    pub port: Option<u32>,
}

impl CassandraPeersRewriteConfig {
    pub async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::CassandraPeersRewrite(CassandraPeersRewrite {
            emulate_single_node: self.emulate_single_node,
            port: self.port,
        }))
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {
    emulate_single_node: bool,
    port: Option<u32>,
}

#[async_trait]
impl Transform for CassandraPeersRewrite {
    async fn prep_transform_chain(
        &mut self,
        _t: &mut crate::transforms::chain::TransformChain,
    ) -> Result<()> {
        Ok(())
    }

    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        // Find the indices of queries to system.peers & system.peers_v2
        let system_peers = message_wrapper
            .messages
            .iter()
            .enumerate()
            .filter(|(_, m)| is_system_peers(&m.details) || is_system_peers_v2(&m.details))
            .map(|(i, _)| i)
            .collect::<Vec<_>>();

        let mut response = message_wrapper.call_next_transform().await?;

        for i in system_peers {
            if let Some(new_port) = self.port {
                rewrite_port(&mut response[i].original, new_port);
            }

            if self.emulate_single_node {
                emulate_single_node(&mut response[i].original);
            }
        }

        Ok(response)
    }
}

fn emulate_single_node(original: &mut RawFrame) {
    if let RawFrame::Cassandra(ref mut frame) = original {
        if let Ok(ResponseBody::Result(ResResultBody::Rows(rows))) = frame.get_body() {
            let body = BodyResResultRows {
                metadata: rows.metadata,
                rows_count: 0_i32,
                rows_content: Vec::<Vec<CBytes>>::new(),
            };

            *original = RawFrame::Cassandra(Frame {
                version: Version::Response,
                flags: frame.flags.clone(),
                opcode: Opcode::Result,
                stream: frame.stream,
                body: body.into_cbytes(),
                tracing_id: frame.tracing_id,
                warnings: Vec::new(),
            });
        } else {
            panic!("Expected ResResultBody::Rows");
        }
    } else {
        panic!("Expected RawFrame::Cassandra");
    }
}

fn rewrite_port(original: &mut RawFrame, new_port: u32) {
    if let RawFrame::Cassandra(ref mut frame) = original {
        if let Ok(ResponseBody::Result(ResResultBody::Rows(results))) = frame.get_body() {
            let port_column_index = results
                .metadata
                .col_specs
                .iter()
                .position(|col| col.name.as_str() == "native_port");

            if let Some(i) = port_column_index {
                let mut new_results = results.rows_content.clone();

                for row in new_results.iter_mut() {
                    row[i] = CBytes::new(new_port.to_be_bytes().into());
                }

                let body = BodyResResultRows {
                    metadata: results.metadata,
                    rows_count: new_results.len() as i32,
                    rows_content: new_results,
                };

                *original = RawFrame::Cassandra(Frame {
                    version: Version::Response,
                    flags: frame.flags.clone(),
                    opcode: Opcode::Result,
                    stream: frame.stream,
                    tracing_id: frame.tracing_id,
                    warnings: Vec::new(),
                    body: body.into_cbytes(),
                });
            }
        }
    } else {
        panic!("Expected RawFrame::Cassandra");
    }
}

fn is_system_peers(message_details: &MessageDetails) -> bool {
    if let MessageDetails::Query(message_details) = &message_details {
        if QueryType::Read != message_details.query_type {
            // only want to modify read queries
            return false;
        }

        message_details.namespace.eq(&["system", "peers"])
    } else {
        false
    }
}

fn is_system_peers_v2(message_details: &MessageDetails) -> bool {
    if let MessageDetails::Query(message_details) = &message_details {
        if QueryType::Read != message_details.query_type {
            // only want to modify read queries
            return false;
        }

        message_details.namespace.eq(&["system", "peers_v2"])
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::message::{QueryMessage, QueryType};
    use std::collections::HashMap;

    #[test]
    fn test_is_system_peers() {
        let mut message_details = QueryMessage {
            query_string: "SELECT * from system.peers".to_string(),
            namespace: vec!["system".to_string(), "peers".to_string()],
            primary_key: HashMap::new(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None,
        };

        assert!(is_system_peers(&MessageDetails::Query(
            message_details.clone()
        )));

        message_details.namespace = vec!["notsystem".to_string(), "notpeers".to_string()];
        assert!(!is_system_peers(&MessageDetails::Query(
            message_details.clone()
        )));

        message_details.namespace = vec!["system".to_string(), "peers".to_string()];
        message_details.query_type = QueryType::Write;
        assert!(!is_system_peers(&MessageDetails::Query(
            message_details.clone()
        )));

        message_details.query_type = QueryType::SchemaChange;
        assert!(!is_system_peers(&MessageDetails::Query(
            message_details.clone()
        )));

        message_details.namespace = vec!["system".to_string(), "peers_v2".to_string()];
        message_details.query_type = QueryType::Read;
        assert!(!is_system_peers(&MessageDetails::Query(message_details)));
    }

    #[test]
    fn test_is_system_peers_v2() {
        let mut message_details = QueryMessage {
            query_string: "SELECT * from system.peers_v2".to_string(),
            namespace: vec!["system".to_string(), "peers_v2".to_string()],
            primary_key: HashMap::new(),
            query_values: None,
            projection: None,
            query_type: QueryType::Read,
            ast: None,
        };

        assert!(is_system_peers_v2(&MessageDetails::Query(
            message_details.clone()
        )));

        message_details.namespace = vec!["notsystem".to_string(), "notpeers".to_string()];
        assert!(!is_system_peers_v2(&MessageDetails::Query(
            message_details.clone()
        )));

        message_details.namespace = vec!["system".to_string(), "peers_v2".to_string()];
        message_details.query_type = QueryType::Write;
        assert!(!is_system_peers_v2(&MessageDetails::Query(
            message_details.clone()
        )));

        message_details.namespace = vec!["system".to_string(), "peers".to_string()];
        message_details.query_type = QueryType::Read;
        assert!(!is_system_peers_v2(&MessageDetails::Query(
            message_details.clone()
        )));

        message_details.query_type = QueryType::SchemaChange;
        assert!(!is_system_peers_v2(&MessageDetails::Query(message_details)));
    }

    #[test]
    fn test_emulate_single_node() {}

    #[test]
    fn test_rewrite_port() {}
}
