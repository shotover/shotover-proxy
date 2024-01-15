use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, BatchSize, Criterion};
use shotover::codec::kafka::KafkaCodecBuilder;
use shotover::codec::{CodecBuilder, Direction};
use shotover::message::{Message, ProtocolType};
use tokio_util::codec::{Decoder, Encoder};

const KAFKA_REQUESTS: &[(&[u8], &str)] = &[
    (
        include_bytes!("kafka_requests/metadata.bin"),
        "request_metadata",
    ),
    (
        include_bytes!("kafka_requests/list_offsets.bin"),
        "request_list_offsets",
    ),
    (include_bytes!("kafka_requests/fetch.bin"), "request_fetch"),
    (
        include_bytes!("kafka_requests/produce.bin"),
        "request_produce",
    ),
];

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("kafka_codec");
    group.noise_threshold(0.2);

    for (message, file_name) in KAFKA_REQUESTS {
        {
            let mut input = BytesMut::new();
            input.extend_from_slice(message);
            group.bench_function(format!("kafka_decode_{file_name}"), |b| {
                b.iter_batched(
                    || {
                        (
                            // recreate codec since it is stateful
                            KafkaCodecBuilder::new(Direction::Source, "kafka".to_owned()).build(),
                            input.clone(),
                        )
                    },
                    |((mut decoder, _encoder), mut input)| {
                        let mut result = decoder.decode(&mut input).unwrap().unwrap();
                        for message in &mut result {
                            message.frame();
                        }
                        black_box(result)
                    },
                    BatchSize::SmallInput,
                )
            });
        }
        {
            let mut message = Message::from_bytes(
                Bytes::from(message.to_vec()),
                ProtocolType::Kafka {
                    request_header: None,
                },
            );
            // force the message to be parsed and clear raw message
            message.frame();
            message.invalidate_cache();

            let messages = vec![message];

            group.bench_function(format!("kafka_encode_{file_name}"), |b| {
                b.iter_batched(
                    || {
                        (
                            // recreate codec since it is stateful
                            KafkaCodecBuilder::new(Direction::Sink, "kafka".to_owned()).build(),
                            messages.clone(),
                        )
                    },
                    |((_decoder, mut encoder), messages)| {
                        let mut bytes = BytesMut::new();
                        encoder.encode(messages, &mut bytes).unwrap();
                        black_box(bytes)
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }

    {
        let mut input = BytesMut::new();
        for (message, _) in KAFKA_REQUESTS {
            input.extend_from_slice(message);
        }
        group.bench_function("kafka_decode_all", |b| {
            b.iter_batched(
                || {
                    (
                        // recreate codec since it is stateful
                        KafkaCodecBuilder::new(Direction::Source, "kafka".to_owned()).build(),
                        input.clone(),
                    )
                },
                |((mut decoder, _encoder), mut input)| {
                    let mut result = decoder.decode(&mut input).unwrap().unwrap();
                    for message in &mut result {
                        message.frame();
                    }
                    black_box(result)
                },
                BatchSize::SmallInput,
            )
        });
    }

    {
        let mut messages = vec![];
        for (message, _) in KAFKA_REQUESTS {
            let mut message = Message::from_bytes(
                Bytes::from(message.to_vec()),
                ProtocolType::Kafka {
                    request_header: None,
                },
            );
            // force the message to be parsed and clear raw message
            message.frame();
            message.invalidate_cache();

            messages.push(message);
        }

        group.bench_function("kafka_encode_all", |b| {
            b.iter_batched(
                || {
                    (
                        // recreate codec since it is stateful
                        KafkaCodecBuilder::new(Direction::Sink, "kafka".to_owned()).build(),
                        messages.clone(),
                    )
                },
                |((_decoder, mut encoder), messages)| {
                    let mut bytes = BytesMut::new();
                    encoder.encode(messages, &mut bytes).unwrap();
                    black_box(bytes)
                },
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(benches, criterion_benchmark);
