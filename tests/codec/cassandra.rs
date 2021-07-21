use anyhow::{anyhow, Result};
use bytes::{Bytes};
use futures::SinkExt;
use shotover_proxy::protocols::cassandra_protocol2::CassandraCodec2;
use std::collections::HashMap;
use tokio::io::BufWriter;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::io::StreamReader;

async fn check_vec_of_bytes(packet_stream: Vec<Bytes>) -> Result<()> {
    let mut pk_map: HashMap<String, Vec<String>> = HashMap::new();
    let mut comparator_iter = packet_stream.clone().into_iter();
    pk_map.insert("test.simple".to_string(), vec!["pk".to_string()]);
    pk_map.insert(
        "test.clustering".to_string(),
        vec!["pk".to_string(), "clustering".to_string()],
    );

    let codec = CassandraCodec2::new(pk_map.clone(), true);
    let write_codec = CassandraCodec2::new(pk_map.clone(), true);
    let stream = tokio_stream::iter(
        packet_stream
            .iter()
            .cloned()
            .map(|b| Ok(b))
            .collect::<Vec<anyhow::Result<Bytes, std::io::Error>>>(),
    );

    let byte_stream = StreamReader::new(stream);
    let mut reader = FramedRead::new(byte_stream, codec);

    for frame in reader.next().await {
        if let Ok(frame) = frame {
            let recv_buffer = BufWriter::new(Vec::new());
            let mut writer = FramedWrite::new(recv_buffer, write_codec.clone());
            writer.send(frame.clone()).await?;
            let results = Bytes::from(writer.into_inner().into_inner());
            if let Some(orig_bytes) = comparator_iter.next() {
                assert_eq!(orig_bytes, results);
            } else {
                return Err(anyhow!("packet count mismatch"));
            }
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test() -> Result<()> {
    let mut capture = crate::codec::util::packet_capture::PacketCapture::new();
    println!("doing some cql");
    let packets = capture.parse_from_file("./test_tmp/cql_mixed.pcap", None)?;
    let mut client_packets: Vec<Bytes> = Vec::new();
    let mut server_packets: Vec<Bytes> = Vec::new();
    // let mut server_packets: Vec<anyhow::Result<Bytes, std::io::Error>> = Vec::new();
    for packet in packets {
        if let Ok(packet) = packet {
            let (_src_address, src_port, _dst_addr, dst_port, protocol, _length, _timestamp) =
                capture.get_packet_details(&packet);
            match (src_port.as_str(), dst_port.as_str(), protocol.as_str()) {
                ("9042", _, "Tcp") => server_packets.push(Bytes::from(packet.remaining)),
                (_, "9042", "Tcp") => client_packets.push(Bytes::from(packet.remaining)),
                _ => {}
            }
        }
    }
    check_vec_of_bytes(client_packets).await?;
    check_vec_of_bytes(server_packets).await
}
