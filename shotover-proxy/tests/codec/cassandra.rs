use bytes::Bytes;
use futures::SinkExt;
use shotover_proxy::codec::cassandra::CassandraCodec;

use tokio::io::BufWriter;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::io::StreamReader;

async fn check_vec_of_bytes(packet_stream: Vec<Bytes>) {
    let mut comparator_iter = packet_stream.clone().into_iter();

    let codec = CassandraCodec::new();
    let write_codec = CassandraCodec::new();
    let stream = tokio_stream::iter(
        packet_stream
            .into_iter()
            .map(Ok)
            .collect::<Vec<anyhow::Result<Bytes, std::io::Error>>>(),
    );

    let byte_stream = StreamReader::new(stream);
    let mut reader = FramedRead::new(byte_stream, codec);

    for frame in reader.next().await.into_iter().flatten() {
        let recv_buffer = BufWriter::new(Vec::new());
        let mut writer = FramedWrite::new(recv_buffer, write_codec.clone());
        writer.send(frame).await.unwrap();
        let results = Bytes::from(writer.into_inner().into_inner());
        let orig_bytes = comparator_iter.next().expect("packet count mismatch");
        assert_eq!(orig_bytes, results);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cassandra_packet_capture() {
    let test_data = std::path::PathBuf::from("../test_data");
    let cql_mixed = test_data.join("cql_mixed.pcap");
    if !test_data.exists() {
        std::fs::create_dir_all(test_data).unwrap();

        let url = "https://shotover-test-captures.s3.us-east-1.amazonaws.com/cql_mixed.pcap";
        let data = reqwest::get(url).await.unwrap().bytes().await.unwrap();
        std::fs::write(&cql_mixed, &data).unwrap();
    }

    let mut capture = crate::codec::util::packet_capture::PacketCapture::default();
    let packets = capture.parse_from_file(&cql_mixed, None);
    let mut client_packets = Vec::new();
    let mut server_packets = Vec::new();
    for packet in packets.into_iter().flatten() {
        let (_src_address, src_port, _dst_addr, dst_port, protocol, _length, _timestamp) =
            capture.get_packet_details(&packet);
        match (src_port.as_str(), dst_port.as_str(), protocol.as_str()) {
            ("9042", _, "Tcp") => server_packets.push(Bytes::from(packet.remaining)),
            (_, "9042", "Tcp") => client_packets.push(Bytes::from(packet.remaining)),
            _ => {}
        }
    }
    check_vec_of_bytes(client_packets).await;
    check_vec_of_bytes(server_packets).await;
}
