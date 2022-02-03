use crate::frame::{CassandraFrame, Frame};
use crate::{
    config::topology::TopicHolder,
    error::ChainResponse,
    transforms::{Transform, Transforms, Wrapper},
};
use anyhow::Result;
use async_trait::async_trait;
use cassandra_protocol::{
    frame::{
        frame_request::RequestBody,
        frame_response::ResponseBody,
        frame_result::{BodyResResultRows, ResResultBody},
        Serialize as CassandraSerialize,
    },
    types::CBytes,
};
use regex::Regex;
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct CassandraPeersRewriteConfig {
    pub emulate_single_node: bool,
    pub new_port: Option<u32>,
}

impl CassandraPeersRewriteConfig {
    pub async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::CassandraPeersRewrite(CassandraPeersRewrite {
            emulate_single_node: self.emulate_single_node,
            port: self.new_port,
            system_re: Regex::new(r#"\b(system)\b"#)?,
            peers_re: Regex::new(r#"\b(peers)\b"#)?,
            peers_v2_re: Regex::new(r#"\b(peers_v2)\b"#)?,
        }))
    }
}

#[derive(Clone)]
pub struct CassandraPeersRewrite {
    emulate_single_node: bool,
    port: Option<u32>,

    // Cache compiled regex on transform struct so we don't have to recompile for every message
    system_re: Regex,
    peers_re: Regex,
    peers_v2_re: Regex,
}

#[async_trait]
impl Transform for CassandraPeersRewrite {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        // Find the indices of queries to system.peers & system.peers_v2
        let system_peers = message_wrapper
            .messages
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                self.is_system_peers(&m.original) || self.is_system_peers_v2(&m.original)
            })
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

impl CassandraPeersRewrite {
    fn is_system_peers(&self, original: &Frame) -> bool {
        if let Frame::Cassandra(frame) = original {
            if let Ok(RequestBody::Query(body_req_query)) = frame.request_body() {
                if self.system_re.is_match(&body_req_query.query)
                    && self.peers_re.is_match(&body_req_query.query)
                {
                    return true;
                }
            }
        }
        false
    }

    fn is_system_peers_v2(&self, original: &Frame) -> bool {
        if let Frame::Cassandra(frame) = original {
            if let Ok(RequestBody::Query(body_req_query)) = frame.request_body() {
                if self.system_re.is_match(&body_req_query.query)
                    && self.peers_v2_re.is_match(&body_req_query.query)
                {
                    return true;
                }
            }
        }
        false
    }
}

fn emulate_single_node(original: &mut Frame) {
    if let Frame::Cassandra(ref mut frame) = original {
        if let Ok(ResponseBody::Result(ResResultBody::Rows(rows))) = frame.response_body() {
            let body = ResResultBody::Rows(BodyResResultRows {
                metadata: rows.metadata,
                rows_count: 0,
                rows_content: Vec::<Vec<CBytes>>::new(),
            });

            *original = Frame::Cassandra(CassandraFrame {
                version: frame.version,
                direction: frame.direction,
                flags: frame.flags,
                opcode: frame.opcode,
                stream_id: frame.stream_id,
                body: body.serialize_to_vec(),
                tracing_id: frame.tracing_id,
                warnings: frame.warnings.clone(),
            });
        } else {
            panic!("Expected ResResultBody::Rows, got {:?}", frame);
        }
    } else {
        panic!("Expected Frame::Cassandra, got {:?}", original);
    }
}

fn rewrite_port(original: &mut Frame, new_port: u32) {
    if let Frame::Cassandra(ref mut frame) = original {
        if let Ok(ResponseBody::Result(ResResultBody::Rows(mut rows))) = frame.response_body() {
            let port_column_index = rows
                .metadata
                .col_specs
                .iter()
                .position(|col| col.name.as_str() == "native_port");

            if let Some(i) = port_column_index {
                for row in rows.rows_content.iter_mut() {
                    row[i] = CBytes::new(new_port.to_be_bytes().into());
                }

                let body = ResResultBody::Rows(BodyResResultRows {
                    metadata: rows.metadata,
                    rows_count: rows.rows_count,
                    rows_content: rows.rows_content,
                });
                *original = Frame::Cassandra(CassandraFrame {
                    version: frame.version,
                    direction: frame.direction,
                    flags: frame.flags,
                    opcode: frame.opcode,
                    stream_id: frame.stream_id,
                    tracing_id: frame.tracing_id,
                    warnings: frame.warnings.clone(),
                    body: body.serialize_to_vec(),
                });
            }
        } else {
            panic!("Expected ResResultBody::Rows, got {:?}", frame);
        }
    } else {
        panic!("Expected Frame::Cassandra, got {:?}", original);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use cassandra_protocol::{
        consistency::Consistency,
        frame::{
            Direction, Opcode,
            {frame_query::BodyReqQuery, Flags, Version},
        },
        query::QueryParams,
    };

    fn create_query_frame(query: String) -> Frame {
        let body = BodyReqQuery {
            query,
            query_params: QueryParams {
                consistency: Consistency::One,
                with_names: false,
                values: None,
                page_size: Some(5000),
                paging_state: None,
                serial_consistency: None,
                timestamp: Some(1643855761086585),
            },
        };

        Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            direction: Direction::Request,
            flags: Flags::default(),
            stream_id: 0,
            opcode: Opcode::Query,
            tracing_id: None,
            body: body.serialize_to_vec(),
            warnings: vec![],
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_is_system_peers() {
        let transform = {
            let config = CassandraPeersRewriteConfig {
                emulate_single_node: false,
                new_port: Some(9043),
            };
            match config.get_source(&TopicHolder::default()).await.unwrap() {
                Transforms::CassandraPeersRewrite(c) => c,
                _ => panic!(),
            }
        };

        assert!(
            transform.is_system_peers(&create_query_frame("SELECT * FROM system.peers;".into()))
        );

        assert!(!transform.is_system_peers(&create_query_frame(
            "SELECT * FROM not_system.peers;".into()
        )));

        assert!(!transform.is_system_peers(&create_query_frame("".into())));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_is_system_peers_v2() {
        let transform = {
            let config = CassandraPeersRewriteConfig {
                emulate_single_node: false,
                new_port: Some(9043),
            };
            match config.get_source(&TopicHolder::default()).await.unwrap() {
                Transforms::CassandraPeersRewrite(c) => c,
                _ => panic!(),
            }
        };

        assert!(transform
            .is_system_peers_v2(&create_query_frame("SELECT * FROM system.peers_v2;".into())));

        assert!(!transform.is_system_peers_v2(&create_query_frame(
            "SELECT * FROM not_system.peers_v2;".into()
        )));

        assert!(!transform.is_system_peers_v2(&create_query_frame("".into())));
    }

    #[test]
    fn test_emulate_single_node() {
        let body = vec![
            0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 115, 121, 115, 116, 101, 109, 0, 5, 112, 101,
            101, 114, 115, 0, 4, 112, 101, 101, 114, 0, 16, 0, 0, 0, 2, 0, 0, 0, 4, 172, 16, 1, 2,
            0, 0, 0, 4, 172, 16, 1, 4,
        ];

        let mut frame = Frame::Cassandra(CassandraFrame {
            version: Version::V4,
            direction: Direction::Response,
            flags: Flags::default(),
            opcode: Opcode::Result,
            stream_id: 0,
            body,
            tracing_id: None,
            warnings: vec![],
        });

        emulate_single_node(&mut frame);

        let expected = vec![
            0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 115, 121, 115, 116, 101, 109, 0, 5, 112, 101,
            101, 114, 115, 0, 4, 112, 101, 101, 114, 0, 16, 0, 0, 0, 0,
        ];

        match frame {
            Frame::Cassandra(frame) => {
                assert_eq!(expected, frame.body);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_rewrite_port() {
        // Test rewrites `native_port` column when included
        {
            let body = vec![
                0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 115, 121, 115, 116, 101, 109, 0, 8, 112,
                101, 101, 114, 115, 95, 118, 50, 0, 11, 110, 97, 116, 105, 118, 101, 95, 112, 111,
                114, 116, 0, 9, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 35, 82, 0, 0, 0, 4, 0, 0, 35, 82,
            ];
            let mut frame = Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                direction: Direction::Response,
                flags: Flags::default(),
                opcode: Opcode::Result,
                stream_id: 0,
                tracing_id: None,
                warnings: vec![],
                body,
            });
            rewrite_port(&mut frame, 9043);

            let expected = vec![
                0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 115, 121, 115, 116, 101, 109, 0, 8, 112,
                101, 101, 114, 115, 95, 118, 50, 0, 11, 110, 97, 116, 105, 118, 101, 95, 112, 111,
                114, 116, 0, 9, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 35, 83, 0, 0, 0, 4, 0, 0, 35, 83,
            ];

            match frame {
                Frame::Cassandra(frame) => {
                    assert_eq!(expected, frame.body);
                }
                _ => panic!(),
            }
        }

        // Test does not rewrite anything when `native_port` column not included
        {
            let body = vec![
                0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 115, 121, 115, 116, 101, 109, 0, 5, 112,
                101, 101, 114, 115, 0, 7, 104, 111, 115, 116, 95, 105, 100, 0, 12, 0, 0, 0, 2, 0,
                0, 0, 16, 178, 130, 182, 150, 234, 231, 77, 138, 182, 23, 172, 103, 230, 124, 69,
                246, 0, 0, 0, 16, 222, 154, 109, 171, 210, 65, 66, 71, 166, 94, 171, 120, 97, 207,
                182, 37,
            ];

            let mut frame = Frame::Cassandra(CassandraFrame {
                version: Version::V4,
                direction: Direction::Response,
                flags: Flags::default(),
                opcode: Opcode::Result,
                stream_id: 0,
                tracing_id: None,
                warnings: vec![],
                body: body.clone(),
            });
            rewrite_port(&mut frame, 9043);

            match frame {
                Frame::Cassandra(frame) => {
                    assert_eq!(body, frame.body);
                }
                _ => panic!(),
            }
        }
    }
}
