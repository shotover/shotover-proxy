use crate::frame::Frame;
use crate::frame::MessageType;
use crate::frame::RedisFrame;
use crate::message::{MessageIdMap, Messages};
use crate::transforms::DownChainProtocol;
use crate::transforms::TransformContextBuilder;
use crate::transforms::TransformContextConfig;
use crate::transforms::UpChainProtocol;
use crate::transforms::{ChainState, Transform, TransformBuilder, TransformConfig};
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct RedisClusterPortsRewriteConfig {
    pub new_port: u16,
}

const NAME: &str = "RedisClusterPortsRewrite";
#[typetag::serde(name = "RedisClusterPortsRewrite")]
#[async_trait(?Send)]
impl TransformConfig for RedisClusterPortsRewriteConfig {
    async fn get_builder(
        &self,
        _transform_context: TransformContextConfig,
    ) -> Result<Box<dyn TransformBuilder>> {
        Ok(Box::new(RedisClusterPortsRewrite::new(self.new_port)))
    }

    fn up_chain_protocol(&self) -> UpChainProtocol {
        UpChainProtocol::MustBeOneOf(vec![MessageType::Redis])
    }

    fn down_chain_protocol(&self) -> DownChainProtocol {
        DownChainProtocol::SameAsUpChain
    }
}

impl TransformBuilder for RedisClusterPortsRewrite {
    fn build(&self, _transform_context: TransformContextBuilder) -> Box<dyn Transform> {
        Box::new(self.clone())
    }

    fn get_name(&self) -> &'static str {
        NAME
    }
}

#[derive(Clone)]
pub struct RedisClusterPortsRewrite {
    new_port: u16,
    request_type: MessageIdMap<RequestType>,
}

#[derive(Clone)]
enum RequestType {
    ClusterSlot,
    ClusterNodes,
}

impl RedisClusterPortsRewrite {
    pub fn new(new_port: u16) -> Self {
        RedisClusterPortsRewrite {
            new_port,
            request_type: MessageIdMap::default(),
        }
    }
}

#[async_trait]
impl Transform for RedisClusterPortsRewrite {
    fn get_name(&self) -> &'static str {
        NAME
    }

    async fn transform<'shorter, 'longer: 'shorter>(
        &mut self,
        chain_state: &'shorter mut ChainState<'longer>,
    ) -> Result<Messages> {
        for message in chain_state.requests.iter_mut() {
            let message_id = message.id();
            if let Some(frame) = message.frame() {
                if is_cluster_slots(frame) {
                    self.request_type
                        .insert(message_id, RequestType::ClusterSlot);
                }

                if is_cluster_nodes(frame) {
                    self.request_type
                        .insert(message_id, RequestType::ClusterNodes);
                }
            }
        }

        let mut responses = chain_state.call_next_transform().await?;

        for response in &mut responses {
            if let Some(request_id) = response.request_id() {
                match self.request_type.remove(&request_id) {
                    // Rewrite the ports in the cluster slots responses
                    Some(RequestType::ClusterSlot) => {
                        if let Some(frame) = response.frame() {
                            rewrite_port_slot(frame, self.new_port)
                                .context("failed to rewrite CLUSTER SLOTS port")?;
                            response.invalidate_cache();
                        }
                    }
                    // Rewrite the ports in the cluster nodes responses
                    Some(RequestType::ClusterNodes) => {
                        if let Some(frame) = response.frame() {
                            rewrite_port_node(frame, self.new_port)
                                .context("failed to rewrite CLUSTER NODES port")?;
                            response.invalidate_cache();
                        }
                    }
                    None => {}
                }
            }
        }

        Ok(responses)
    }
}

/// Rewrites the ports of a response to a CLUSTER SLOTS message to `new_port`
fn rewrite_port_slot(frame: &mut Frame, new_port: u16) -> Result<()> {
    if let Frame::Redis(RedisFrame::Array(array)) = frame {
        for elem in array.iter_mut() {
            if let RedisFrame::Array(slot) = elem {
                for (index, mut frame) in slot.iter_mut().enumerate() {
                    match (index, &mut frame) {
                        (0..=1, _) => {}
                        (_, RedisFrame::Array(target)) => match target.as_mut_slice() {
                            [RedisFrame::BulkString(_ip), RedisFrame::Integer(port), ..] => {
                                *port = new_port.into();
                            }
                            _ => bail!("expected host-port in slot map but was: {:?}", frame),
                        },
                        _ => bail!("unexpected value in slot map: {:?}", frame),
                    }
                }
            };
        }
    }
    Ok(())
}

/// Get a mutable reference to the CSV string inside a response to CLUSTER NODES or REPLICAS
fn get_buffer(frame: &mut Frame) -> Option<&mut Bytes> {
    // CLUSTER NODES
    if let Frame::Redis(RedisFrame::BulkString(buf)) = frame {
        return Some(buf);
    }

    // CLUSTER REPLICAS
    if let Frame::Redis(RedisFrame::Array(array)) = frame {
        for item in array.iter_mut() {
            if let RedisFrame::BulkString(buf) = item {
                return Some(buf);
            }
        }
    }

    None
}

/// Rewrites the ports of a response to a CLUSTER NODES message to `new_port`
fn rewrite_port_node(frame: &mut Frame, new_port: u16) -> Result<()> {
    if let Some(buf) = get_buffer(frame) {
        let mut bytes_writer = BytesMut::new().writer();

        {
            let read_cursor = std::io::Cursor::new(&buf);

            let mut reader = csv::ReaderBuilder::new()
                .delimiter(b' ')
                .has_headers(false)
                .flexible(true) // flexible because the last fields is an arbitrary number of tokens
                .from_reader(read_cursor);

            let mut writer = csv::WriterBuilder::new()
                .delimiter(b' ')
                .flexible(true)
                .from_writer(&mut bytes_writer);

            for result in reader.records() {
                let record = result?;
                let mut record_iter = record.into_iter();

                // Write the id field
                let id = record_iter
                    .next()
                    .ok_or_else(|| anyhow!("CLUSTER NODES response missing id field"))?;
                writer.write_field(id)?;

                // Modify and rewrite the port field
                let ip = record_iter
                    .next()
                    .ok_or_else(|| anyhow!("CLUSTER NODES response missing address field"))?;

                let split = ip.split([':', '@']).collect::<Vec<&str>>();

                if split.len() < 3 {
                    bail!("IP address not in valid format: {ip}");
                }
                let new_ip = format!("{}:{}@{}", split[0], new_port, split[2]);

                writer.write_field(&*new_ip)?;

                // Write the last of the record
                writer.write_record(record_iter)?;
            }
            writer.flush()?;
        }

        *buf = bytes_writer.into_inner().freeze();
    }

    Ok(())
}

/// Determines if the supplied Redis Frame is a `CLUSTER NODES` request
/// or `CLUSTER REPLICAS` which returns the same response as `CLUSTER NODES`
fn is_cluster_nodes(frame: &Frame) -> bool {
    if let Frame::Redis(RedisFrame::Array(array)) = frame {
        match array.as_slice() {
            [RedisFrame::BulkString(one), RedisFrame::BulkString(two), ..] => {
                one.eq_ignore_ascii_case(b"CLUSTER")
                    && (two.eq_ignore_ascii_case(b"NODES") || two.eq_ignore_ascii_case(b"REPLICAS"))
            }
            [..] => false,
        }
    } else {
        false
    }
}

/// Determines if the supplied Redis Frame is a `CLUSTER SLOTS` request
fn is_cluster_slots(frame: &Frame) -> bool {
    if let Frame::Redis(RedisFrame::Array(array)) = frame {
        match array.as_slice() {
            [RedisFrame::BulkString(one), RedisFrame::BulkString(two), ..] => {
                one.eq_ignore_ascii_case(b"CLUSTER") && two.eq_ignore_ascii_case(b"SLOTS")
            }
            [..] => false,
        }
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::codec::redis::RedisDecoder;
    use crate::codec::Direction;
    use crate::transforms::redis::sink_cluster::parse_slots;
    use pretty_assertions::assert_eq;
    use tokio_util::codec::Decoder;

    #[test]
    fn test_is_cluster_slots() {
        let combos = [
            (b"cluster", b"slots"),
            (b"CLUSTER", b"SLOTS"),
            (b"cluster", b"SLOTS"),
            (b"CLUSTER", b"slots"),
        ];

        for combo in combos {
            let frame = Frame::Redis(RedisFrame::Array(vec![
                RedisFrame::BulkString(Bytes::from_static(combo.0)),
                RedisFrame::BulkString(Bytes::from_static(combo.1)),
            ]));
            assert!(is_cluster_slots(&frame));
        }

        let frame = Frame::Redis(RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"GET")),
            RedisFrame::BulkString(Bytes::from_static(b"key1")),
        ]));

        assert!(!is_cluster_slots(&frame));
    }

    #[test]
    fn test_is_cluster_nodes() {
        let combos = [
            //nodes
            (b"cluster", b"nodes" as &[u8]),
            (b"CLUSTER", b"NODES" as &[u8]),
            (b"cluster", b"NODES" as &[u8]),
            (b"CLUSTER", b"nodes" as &[u8]),
            // replicas
            (b"cluster", b"replicas" as &[u8]),
            (b"CLUSTER", b"REPLICAS" as &[u8]),
            (b"cluster", b"replicas" as &[u8]),
            (b"CLUSTER", b"replicas" as &[u8]),
        ];

        for combo in combos {
            let frame = Frame::Redis(RedisFrame::Array(vec![
                RedisFrame::BulkString(Bytes::from_static(combo.0)),
                RedisFrame::BulkString(Bytes::from_static(combo.1)),
            ]));
            assert!(is_cluster_nodes(&frame));
        }

        let frame = Frame::Redis(RedisFrame::Array(vec![
            RedisFrame::BulkString(Bytes::from_static(b"GET")),
            RedisFrame::BulkString(Bytes::from_static(b"key1")),
        ]));

        assert!(!is_cluster_nodes(&frame));
    }

    #[test]
    fn test_rewrite_port_slots() {
        let slots_pcap: &[u8] = b"*3\r\n*4\r\n:10923\r\n:16383\r\n*3\r\n$12\r\n192.168.80.6\r\n:6379\r\n$40\r\n3a7c357ed75d2aa01fca1e14ef3735a2b2b8ffac\r\n*3\r\n$12\r\n192.168.80.3\r\n:6379\r\n$40\r\n77c01b0ddd8668fff05e3f6a8aaf5f3ccd454a79\r\n*4\r\n:5461\r\n:10922\r\n*3\r\n$12\r\n192.168.80.5\r\n:6379\r\n$40\r\n969c6215d064e68593d384541ceeb57e9520dbed\r\n*3\r\n$12\r\n192.168.80.2\r\n:6379\r\n$40\r\n3929f69990a75be7b2d49594c57fe620862e6fd6\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$12\r\n192.168.80.7\r\n:6379\r\n$40\r\n15d52a65d1fc7a53e34bf9193415aa39136882b2\r\n*3\r\n$12\r\n192.168.80.4\r\n:6379\r\n$40\r\ncd023916a3528fae7e606a10d8289a665d6c47b0\r\n";
        let mut codec = RedisDecoder::new(None, Direction::Sink);
        let mut message = codec
            .decode(&mut slots_pcap.into())
            .unwrap()
            .unwrap()
            .pop()
            .unwrap();

        rewrite_port_slot(message.frame().unwrap(), 6380).unwrap();

        let slots_frames = match message.frame().unwrap() {
            Frame::Redis(RedisFrame::Array(frames)) => frames,
            frame => panic!("bad input: {frame:?}"),
        };

        let slots = parse_slots(slots_frames).unwrap();

        let nodes = vec![
            "192.168.80.2:6380",
            "192.168.80.3:6380",
            "192.168.80.4:6380",
            "192.168.80.5:6380",
            "192.168.80.6:6380",
            "192.168.80.7:6380",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let masters = vec![
            (5460u16, "192.168.80.7:6380".to_string()),
            (10922u16, "192.168.80.5:6380".to_string()),
            (16383u16, "192.168.80.6:6380".to_string()),
        ];

        let replicas = vec![
            (5460u16, "192.168.80.4:6380".to_string()),
            (10922u16, "192.168.80.2:6380".to_string()),
            (16383u16, "192.168.80.3:6380".to_string()),
        ];

        assert_eq!(slots.nodes, nodes);
        assert_eq!(slots.masters.into_iter().collect::<Vec<_>>(), masters);
        assert_eq!(slots.replicas.into_iter().collect::<Vec<_>>(), replicas);
    }

    #[test]
    fn test_rewrite_port_nodes() {
        let bulk_string = b"b5657714579550445f271ff65e47f3d39a36806c :0@0 slave,noaddr 95566d2da3382ea88f549ab5766db9a6f4fbc44b 1634273478445 1634273478445 1 disconnected
95566d2da3382ea88f549ab5766db9a6f4fbc44b :0@0 master,noaddr - 1634273478445 1634273478445 1 disconnected 0-5460
c852007a1c3b726534e6866456c1f2002fc442d9 172.31.0.6:6379@16379 myself,master - 0 1634273501000 3 connected 10923-16383
847c2efa4f5dcbca969f30a903ee54c5deb285f6 172.31.0.5:6379@16379 slave 2ee0e46acaec3fcb09fdff3ced0c267ffa2b78d3 0 1634273501428 2 connected
2ee0e46acaec3fcb09fdff3ced0c267ffa2b78d3 :0@0 master,noaddr - 1634273478446 1634273478446 2 disconnected 5461-10922
f9553ea7fc23905476efec1f949b4b3e41a44103 :0@0 slave,noaddr c852007a1c3b726534e6866456c1f2002fc442d9 1634273478445 1634273478445 3 disconnected
";

        // bulk_string with port numbers replaced
        let expected_string = b"b5657714579550445f271ff65e47f3d39a36806c :1234@0 slave,noaddr 95566d2da3382ea88f549ab5766db9a6f4fbc44b 1634273478445 1634273478445 1 disconnected
95566d2da3382ea88f549ab5766db9a6f4fbc44b :1234@0 master,noaddr - 1634273478445 1634273478445 1 disconnected 0-5460
c852007a1c3b726534e6866456c1f2002fc442d9 172.31.0.6:1234@16379 myself,master - 0 1634273501000 3 connected 10923-16383
847c2efa4f5dcbca969f30a903ee54c5deb285f6 172.31.0.5:1234@16379 slave 2ee0e46acaec3fcb09fdff3ced0c267ffa2b78d3 0 1634273501428 2 connected
2ee0e46acaec3fcb09fdff3ced0c267ffa2b78d3 :1234@0 master,noaddr - 1634273478446 1634273478446 2 disconnected 5461-10922
f9553ea7fc23905476efec1f949b4b3e41a44103 :1234@0 slave,noaddr c852007a1c3b726534e6866456c1f2002fc442d9 1634273478445 1634273478445 3 disconnected
";

        let mut raw_frame = Frame::Redis(RedisFrame::BulkString(Bytes::from_static(bulk_string)));
        rewrite_port_node(&mut raw_frame, 1234).unwrap();

        assert_eq!(
            raw_frame,
            Frame::Redis(RedisFrame::BulkString(Bytes::from_static(expected_string)))
        );
    }
}
