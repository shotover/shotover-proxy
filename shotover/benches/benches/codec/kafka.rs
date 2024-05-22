use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, BatchSize, Criterion};
use shotover::codec::kafka::KafkaCodecBuilder;
use shotover::codec::{CodecBuilder, CodecState, Direction};
use shotover::message::Message;
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
        // is acks=0
        include_bytes!("kafka_requests/produce.bin"),
        "request_produce",
    ),
];

fn criterion_benchmark(c: &mut Criterion) {
    crate::init();
    let mut group = c.benchmark_group("kafka_codec");
    group.noise_threshold(0.2);

    for (message, file_name) in KAFKA_REQUESTS {
        {
            let mut input = BytesMut::new();
            input.extend_from_slice(message);
            group.bench_function(format!("decode_{file_name}_create"), |b| {
                b.iter_batched(
                    || {
                        (
                            // recreate codec since it is stateful
                            KafkaCodecBuilder::new(Direction::Source, "kafka".to_owned()).build(),
                            input.clone(),
                        )
                    },
                    |((mut decoder, encoder), mut input)| {
                        let mut message =
                            decoder.decode(&mut input).unwrap().unwrap().pop().unwrap();
                        message.frame();

                        // avoid measuring any drops
                        (decoder, encoder, input, message)
                    },
                    BatchSize::SmallInput,
                )
            });
            group.bench_function(format!("decode_{file_name}_drop"), |b| {
                b.iter_batched(
                    || {
                        // Recreate everything from scratch to ensure that we dont have any `Bytes` references held onto preventing a full drop
                        let (mut decoder, encoder) =
                            KafkaCodecBuilder::new(Direction::Source, "kafka".to_owned()).build();
                        let mut input = input.clone();
                        let mut message =
                            decoder.decode(&mut input).unwrap().unwrap().pop().unwrap();
                        message.frame();
                        assert!(decoder.decode(&mut input).unwrap().is_none());
                        (decoder, encoder, message)
                    },
                    |(decoder, encoder, message)| {
                        std::mem::drop(message);

                        // avoid measuring any drops other than the message
                        (decoder, encoder)
                    },
                    BatchSize::SmallInput,
                )
            });
        }
        {
            let mut message = Message::from_bytes(
                Bytes::from(message.to_vec()),
                CodecState::Kafka {
                    request_header: None,
                },
            );
            // force the message to be parsed and clear raw message
            message.frame();
            message.invalidate_cache();

            let messages = vec![message];

            let mut bytes = BytesMut::new();
            group.bench_function(format!("encode_{file_name}"), |b| {
                b.iter_batched(
                    || {
                        (
                            // recreate codec since it is stateful
                            KafkaCodecBuilder::new(Direction::Sink, "kafka".to_owned()).build(),
                            messages.clone(),
                        )
                    },
                    |((decoder, mut encoder), messages)| {
                        bytes.clear();
                        encoder.encode(messages, &mut bytes).unwrap();
                        (encoder, decoder)
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }

    {
        let mut messages = vec![];
        for (message, _) in KAFKA_REQUESTS {
            let mut message = Message::from_bytes(
                Bytes::from(message.to_vec()),
                CodecState::Kafka {
                    request_header: None,
                },
            );
            // force the message to be parsed and clear raw message
            message.frame();
            message.invalidate_cache();

            messages.push(message);
        }

        let mut bytes = BytesMut::new();
        group.bench_function("encode_all", |b| {
            b.iter_batched(
                || {
                    (
                        // recreate codec since it is stateful
                        KafkaCodecBuilder::new(Direction::Sink, "kafka".to_owned()).build(),
                        messages.clone(),
                    )
                },
                |((decoder, mut encoder), messages)| {
                    bytes.clear();
                    encoder.encode(messages, &mut bytes).unwrap();
                    (encoder, decoder)
                },
                BatchSize::SmallInput,
            )
        });
    }
}

criterion_group!(benches, criterion_benchmark);
