use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use redis_protocol::resp2::prelude::Frame;
use serde::Deserialize;

use crate::config::topology::TopicHolder;
use crate::error::ChainResponse;
use crate::protocols::RawFrame;
use crate::transforms::{Transform, Transforms, Wrapper};

#[derive(Deserialize, Debug, Clone)]
pub struct RedisClusterPortsRewriteConfig {
    pub new_port: u16,
}

impl RedisClusterPortsRewriteConfig {
    pub async fn get_source(&self, _topics: &TopicHolder) -> Result<Transforms> {
        Ok(Transforms::RedisClusterPortsRewrite(
            RedisClusterPortsRewrite {
                new_port: self.new_port,
            },
        ))
    }
}

#[derive(Clone)]
pub struct RedisClusterPortsRewrite {
    new_port: u16,
}

impl RedisClusterPortsRewrite {
    pub fn new(new_port: u16) -> Self {
        RedisClusterPortsRewrite { new_port }
    }
}

#[async_trait]
impl Transform for RedisClusterPortsRewrite {
    async fn transform<'a>(&'a mut self, message_wrapper: Wrapper<'a>) -> ChainResponse {
        // Optimization for when messages are not cluster messages (e.g. SET key, or GET key)
        if message_wrapper
            .messages
            .iter()
            .all(|m| !is_cluster_message(&m.original))
        {
            return message_wrapper.call_next_transform().await;
        }

        // Find the indices of cluster slot messages
        let mut cluster_slots_indices = vec![];
        let mut cluster_nodes_indices = vec![];

        for (i, message) in message_wrapper.messages.iter().enumerate() {
            if is_cluster_slots(&message.original) {
                cluster_slots_indices.push(i);
            }

            if is_cluster_nodes(&message.original) {
                cluster_nodes_indices.push(i);
            }
        }

        let mut response = message_wrapper.call_next_transform().await?;

        // Rewrite the ports in the cluster slots responses
        for i in cluster_slots_indices {
            rewrite_port_slot(&mut response[i].original, self.new_port)
                .context("failed to rewrite CLUSTER SLOTS port")?;
        }

        // Rewrite the ports in the cluster nodes responses
        for i in cluster_nodes_indices {
            rewrite_port_node(&mut response[i].original, self.new_port)
                .context("failed to rewrite CLUSTER NODES port")?;
        }

        Ok(response)
    }

    fn get_name(&self) -> &'static str {
        "RedisClusterPortsRewrite"
    }
}

/// Rewrites the ports of a response to a CLUSTER SLOTS message to `new_port`
fn rewrite_port_slot(frame: &mut RawFrame, new_port: u16) -> Result<()> {
    if let RawFrame::Redis(Frame::Array(ref mut array)) = frame {
        for elem in array.iter_mut() {
            if let Frame::Array(slot) = elem {
                for (index, mut frame) in slot.iter_mut().enumerate() {
                    match (index, &mut frame) {
                        (0..=1, _) => {}
                        (_, Frame::Array(target)) => match target.as_mut_slice() {
                            [Frame::BulkString(_ip), Frame::Integer(port), ..] => {
                                *port = new_port.into();
                            }
                            _ => bail!("expected host-port in slot map but was: {:?}", frame),
                        },
                        _ => bail!("unexpected value in slot map: {:?}", frame),
                    }
                }
            };
        }
        Ok(())
    } else {
        bail!("ClusterTopologyRewrite intercepted an incorrect message")
    }
}

/// Get a mutable reference to the CSV string inside a response to CLUSTER NODES or REPLICAS
fn get_buffer(frame: &mut RawFrame) -> Result<&mut Vec<u8>> {
    // CLUSTER NODES
    if let RawFrame::Redis(Frame::BulkString(ref mut buf)) = frame {
        return Ok(buf);
    }

    // CLUSTER REPLICAS
    if let RawFrame::Redis(Frame::Array(array)) = frame {
        for item in array.iter_mut() {
            if let Frame::BulkString(ref mut buf) = item {
                return Ok(buf);
            }
        }
    }

    bail!("RedisClusterTopologyRewrite intercepted an incorrect message");
}

/// Rewrites the ports of a response to a CLUSTER NODES message to `new_port`
fn rewrite_port_node(frame: &mut RawFrame, new_port: u16) -> Result<()> {
    let buf = get_buffer(frame)?;

    let read_cursor = std::io::Cursor::new(buf.clone());

    buf.clear();
    let write_cursor = std::io::Cursor::new(buf);

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b' ')
        .has_headers(false)
        .flexible(true) // flexible because the last fields is an arbitrary number of tokens
        .from_reader(read_cursor);

    let mut writer = csv::WriterBuilder::new()
        .delimiter(b' ')
        .flexible(true)
        .from_writer(write_cursor);

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

        let split = ip.split(|c| c == ':' || c == '@').collect::<Vec<&str>>();

        let new_ip = format!("{}:{}@{}", split[0], new_port, split[2]);

        writer.write_field(&*new_ip)?;

        // Write the last of the record
        writer.write_record(record_iter)?;
    }

    writer.flush()?;

    Ok(())
}

fn is_cluster_message(frame: &RawFrame) -> bool {
    if let RawFrame::Redis(Frame::Array(array)) = frame {
        if let Frame::BulkString(b) = &array[0] {
            return b.to_ascii_uppercase() == b"CLUSTER";
        }
    }
    false
}

/// Determines if the supplied Redis Frame is a `CLUSTER NODES` request
/// or `CLUSTER REPLICAS` which returns the same response as `CLUSTER NODES`
fn is_cluster_nodes(frame: &RawFrame) -> bool {
    let args = if let RawFrame::Redis(Frame::Array(array)) = frame {
        array
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => Some(b.to_ascii_uppercase()),
                _ => None,
            })
            .take_while(Option::is_some)
            .map(Option::unwrap)
            .collect::<Vec<Vec<u8>>>()
    } else {
        return false;
    };

    args[0] == b"CLUSTER" && (args[1] == b"REPLICAS" || args[1] == b"NODES")
}

/// Determines if the supplied Redis Frame is a `CLUSTER SLOTS` request
fn is_cluster_slots(frame: &RawFrame) -> bool {
    let args = if let RawFrame::Redis(Frame::Array(array)) = frame {
        array
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => Some(b.to_ascii_uppercase()),
                _ => None,
            })
            .take_while(Option::is_some)
            .map(Option::unwrap)
    } else {
        return false;
    };

    args.eq([b"CLUSTER", b"SLOTS" as &[u8]])
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocols::redis_codec::{DecodeType, RedisCodec};
    use crate::transforms::redis::sink_cluster::parse_slots;
    use tokio_util::codec::Decoder;

    #[test]
    fn test_is_cluster_message() {
        let cluster_messages = [b"cluster", b"CLUSTER"];

        for msg in cluster_messages {
            let frame = RawFrame::Redis(Frame::Array(vec![Frame::BulkString(msg.to_vec())]));
            assert!(is_cluster_message(&frame));
        }

        let frame = RawFrame::Redis(Frame::Array(vec![Frame::BulkString(
            b"notcluster".to_vec(),
        )]));
        assert!(!is_cluster_message(&frame));
    }

    #[test]
    fn test_is_cluster_slots() {
        let combos = [
            (b"cluster", b"slots"),
            (b"CLUSTER", b"SLOTS"),
            (b"cluster", b"SLOTS"),
            (b"CLUSTER", b"slots"),
        ];

        for combo in combos {
            let frame = RawFrame::Redis(Frame::Array(vec![
                Frame::BulkString(combo.0.to_vec()),
                Frame::BulkString(combo.1.to_vec()),
            ]));
            assert!(is_cluster_slots(&frame));
        }

        let frame = RawFrame::Redis(Frame::Array(vec![
            Frame::BulkString(b"GET".to_vec()),
            Frame::BulkString(b"key1".to_vec()),
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
            let frame = RawFrame::Redis(Frame::Array(vec![
                Frame::BulkString(combo.0.to_vec()),
                Frame::BulkString(combo.1.to_vec()),
            ]));
            assert!(is_cluster_nodes(&frame));
        }

        let frame = RawFrame::Redis(Frame::Array(vec![
            Frame::BulkString(b"GET".to_vec()),
            Frame::BulkString(b"key1".to_vec()),
        ]));

        assert!(!is_cluster_nodes(&frame));
    }

    #[test]
    fn test_rewrite_port_slots() {
        let slots_pcap: &[u8] = b"*3\r\n*4\r\n:10923\r\n:16383\r\n*3\r\n$12\r\n192.168.80.6\r\n:6379\r\n$40\r\n3a7c357ed75d2aa01fca1e14ef3735a2b2b8ffac\r\n*3\r\n$12\r\n192.168.80.3\r\n:6379\r\n$40\r\n77c01b0ddd8668fff05e3f6a8aaf5f3ccd454a79\r\n*4\r\n:5461\r\n:10922\r\n*3\r\n$12\r\n192.168.80.5\r\n:6379\r\n$40\r\n969c6215d064e68593d384541ceeb57e9520dbed\r\n*3\r\n$12\r\n192.168.80.2\r\n:6379\r\n$40\r\n3929f69990a75be7b2d49594c57fe620862e6fd6\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$12\r\n192.168.80.7\r\n:6379\r\n$40\r\n15d52a65d1fc7a53e34bf9193415aa39136882b2\r\n*3\r\n$12\r\n192.168.80.4\r\n:6379\r\n$40\r\ncd023916a3528fae7e606a10d8289a665d6c47b0\r\n";
        let mut codec = RedisCodec::new(DecodeType::Response, 3);
        let mut raw_frame = codec
            .decode(&mut slots_pcap.into())
            .unwrap()
            .unwrap()
            .pop()
            .unwrap()
            .original;

        rewrite_port_slot(&mut raw_frame, 2004).unwrap();

        let slots_frames = if let RawFrame::Redis(Frame::Array(frames)) = raw_frame {
            frames
        } else {
            panic!("bad input: {:?}", raw_frame)
        };

        let slots = parse_slots(&slots_frames).unwrap();

        let nodes = vec![
            "192.168.80.2:2004",
            "192.168.80.3:2004",
            "192.168.80.4:2004",
            "192.168.80.5:2004",
            "192.168.80.6:2004",
            "192.168.80.7:2004",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let masters = vec![
            (5460u16, "192.168.80.7:2004".to_string()),
            (10922u16, "192.168.80.5:2004".to_string()),
            (16383u16, "192.168.80.6:2004".to_string()),
        ];

        let replicas = vec![
            (5460u16, "192.168.80.4:2004".to_string()),
            (10922u16, "192.168.80.2:2004".to_string()),
            (16383u16, "192.168.80.3:2004".to_string()),
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

        let mut raw_frame = RawFrame::Redis(Frame::BulkString(bulk_string.to_vec()));
        rewrite_port_node(&mut raw_frame, 1234).unwrap();

        let rewritten = if let RawFrame::Redis(Frame::BulkString(string)) = raw_frame.clone() {
            string
        } else {
            panic!("bad input: {:?}", raw_frame)
        };

        assert_eq!(rewritten, expected_string);
    }
}
