use bytes::{Bytes, BytesMut};
use cassandra_protocol::frame::{CheckEnvelopeSizeError, Opcode};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread::JoinHandle;

struct Worker {
    handle: JoinHandle<()>,
    connection_tx: mpsc::Sender<Connection>,
}

// Spawns a single thread which will reply to cassandra messages with dummy responses.
// No attempt at correctness is made here, its purely just whatever is enough to get the benchmark to complete
// What we want to test is the speed of a basic message moving through shotover
pub fn start(cores: usize, port: u16) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut workers = vec![];

        for _ in 0..cores {
            workers.push(Worker::spawn());
        }

        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();

        let mut worker_index = 0;
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            stream.set_nonblocking(true).unwrap();
            stream.set_nodelay(true).unwrap();

            // This allocation algorithm relies on the fact that the bencher will equally distribute
            // load accross all connections without terminating any for the length of the bench
            workers[worker_index]
                .connection_tx
                .send(Connection {
                    stream,
                    buffer: BytesMut::with_capacity(10000),
                })
                .unwrap();
            worker_index = (worker_index + 1) % workers.len();
        }

        for worker in workers {
            worker.handle.join().unwrap();
        }
    })
}

impl Worker {
    fn spawn() -> Worker {
        let (connection_tx, connection_rx) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            let mut connections: Vec<Connection> = vec![];
            loop {
                for connection in connection_rx.try_iter() {
                    connections.push(connection);
                }
                for connection in &mut connections {
                    while let Some(message) = connection.get_message() {
                        let stream_id1 = message[2];
                        let stream_id2 = message[3];
                        match Opcode::try_from(message[4]).unwrap() {
                            Opcode::Options => connection.send_message(&[
                                0x84, 0x00, stream_id1, stream_id2, 0x06, 0x00, 0x00, 0x00, 0x66,
                                0x00, 0x03, 0x00, 0x11, 0x50, 0x52, 0x4f, 0x54, 0x4f, 0x43, 0x4f,
                                0x4c, 0x5f, 0x56, 0x45, 0x52, 0x53, 0x49, 0x4f, 0x4e, 0x53, 0x00,
                                0x04, 0x00, 0x04, 0x33, 0x2f, 0x76, 0x33, 0x00, 0x04, 0x34, 0x2f,
                                0x76, 0x34, 0x00, 0x04, 0x35, 0x2f, 0x76, 0x35, 0x00, 0x09, 0x36,
                                0x2f, 0x76, 0x36, 0x2d, 0x62, 0x65, 0x74, 0x61, 0x00, 0x0b, 0x43,
                                0x4f, 0x4d, 0x50, 0x52, 0x45, 0x53, 0x53, 0x49, 0x4f, 0x4e, 0x00,
                                0x02, 0x00, 0x06, 0x73, 0x6e, 0x61, 0x70, 0x70, 0x79, 0x00, 0x03,
                                0x6c, 0x7a, 0x34, 0x00, 0x0b, 0x43, 0x51, 0x4c, 0x5f, 0x56, 0x45,
                                0x52, 0x53, 0x49, 0x4f, 0x4e, 0x00, 0x01, 0x00, 0x05, 0x33, 0x2e,
                                0x34, 0x2e, 0x35,
                            ]),
                            Opcode::Startup | Opcode::Register => connection
                                .send_message(&[0x84, 0, stream_id1, stream_id2, 2, 0, 0, 0, 0]),
                            Opcode::Prepare => connection.send_message(&[
                                0x84, 0x00, stream_id1, stream_id2, 0x08, 0x00, 0x00, 0x00, 0x54,
                                0x00, 0x00, 0x00, 0x04, 0x00, 0x10, 0x73, 0x8d, 0x0d, 0x1d, 0x8d,
                                0xcd, 0xf4, 0x7a, 0xbe, 0x14, 0xb6, 0x16, 0xb6, 0x29, 0xaa, 0xce,
                                0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                                0x01, 0x00, 0x00, 0x00, 0x05, 0x6c, 0x61, 0x74, 0x74, 0x65, 0x00,
                                0x05, 0x62, 0x61, 0x73, 0x69, 0x63, 0x00, 0x02, 0x69, 0x64, 0x00,
                                0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x05,
                                0x6c, 0x61, 0x74, 0x74, 0x65, 0x00, 0x05, 0x62, 0x61, 0x73, 0x69,
                                0x63, 0x00, 0x02, 0x69, 0x64, 0x00, 0x02,
                            ]),
                            // Just assume this is the query that is being benchmarked
                            Opcode::Execute => connection.send_message(&[
                                0x84, 0x00, stream_id1, stream_id2, 0x08, 0x00, 0x00, 0x00, 0x04,
                                0x00, 0x00, 0x00, 0x01,
                            ]),
                            Opcode::Query => {
                                let query_len =
                                    u32::from_be_bytes(message[9..13].try_into().unwrap()) as usize;
                                let query =
                                    std::str::from_utf8(&message[13..13 + query_len]).unwrap();
                                match query {
                                "select peer, data_center, rack, tokens from system.peers" => {
                                    connection.send_message(&[
                                        0x84, 0x00, stream_id1, stream_id2, 0x08, 0x00, 0x00, 0x00,
                                        0x4a, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00,
                                        0x00, 0x00, 0x04, 0x00, 0x06, 0x73, 0x79, 0x73, 0x74, 0x65,
                                        0x6d, 0x00, 0x05, 0x70, 0x65, 0x65, 0x72, 0x73, 0x00, 0x04,
                                        0x70, 0x65, 0x65, 0x72, 0x00, 0x10, 0x00, 0x0b, 0x64, 0x61,
                                        0x74, 0x61, 0x5f, 0x63, 0x65, 0x6e, 0x74, 0x65, 0x72, 0x00,
                                        0x0d, 0x00, 0x04, 0x72, 0x61, 0x63, 0x6b, 0x00, 0x0d, 0x00,
                                        0x06, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x73, 0x00, 0x22, 0x00,
                                        0x0d, 0x00, 0x00, 0x00, 0x00,
                                    ])
                                }
                                "select rpc_address, data_center, rack, tokens from system.local" => {
                                    connection.send_message(&[
                                        0x84, 0x00, stream_id1, stream_id2, 0x08, 0x00, 0x00, 0x00,
                                        0x7e, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00,
                                        0x00, 0x00, 0x04, 0x00, 0x06, 0x73, 0x79, 0x73, 0x74, 0x65,
                                        0x6d, 0x00, 0x05, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x00, 0x0b,
                                        0x72, 0x70, 0x63, 0x5f, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73,
                                        0x73, 0x00, 0x10, 0x00, 0x0b, 0x64, 0x61, 0x74, 0x61, 0x5f,
                                        0x63, 0x65, 0x6e, 0x74, 0x65, 0x72, 0x00, 0x0d, 0x00, 0x04,
                                        0x72, 0x61, 0x63, 0x6b, 0x00, 0x0d, 0x00, 0x06, 0x74, 0x6f,
                                        0x6b, 0x65, 0x6e, 0x73, 0x00, 0x22, 0x00, 0x0d, 0x00, 0x00,
                                        0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xac, 0x13, 0x00, 0x02,
                                        0x00, 0x00, 0x00, 0x0b, 0x64, 0x61, 0x74, 0x61, 0x63, 0x65,
                                        0x6e, 0x74, 0x65, 0x72, 0x31, 0x00, 0x00, 0x00, 0x05, 0x72,
                                        0x61, 0x63, 0x6b, 0x31, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00,
                                        0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x30,
                                    ]);
                                }
                                "select keyspace_name, replication from system_schema.keyspaces" => {
                                    connection.send_message(&[
                                        0x84, 0x00, stream_id1, stream_id2, 0x08, 0x00, 0x00, 0x00, 0x04,
                                        0x00, 0x00, 0x00, 0x01
                                    ]);
                                }
                                "SELECT cluster_name, release_version FROM system.local" => {
                                    connection.send_message(&[
                                        0x84, 0x0, stream_id1, stream_id2, 0x08, 0x00, 0x00, 0x00, 0x04,
                                        0x00, 0x00, 0x00, 0x01
                                    ]);
                                }
                                query => todo!("Unhandled query {query:?}"),
                            }
                            }

                            op => todo!("Unhandled opcode {op} in message: {message:?}"),
                        }
                    }
                }
            }
        });
        Worker {
            handle,
            connection_tx,
        }
    }
}

struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    fn get_message(&mut self) -> Option<Bytes> {
        if let Some(message) = self.message_from_buffer() {
            return Some(message);
        }

        let mut bytes = [0u8; 2048];
        match self.stream.read(&mut bytes).map_err(|e| e.kind()) {
            Ok(size) => self.buffer.extend(&bytes[..size]),
            Err(std::io::ErrorKind::WouldBlock) => {}
            Err(err) => panic!("unexpected error when reading {err}"),
        }

        self.message_from_buffer()
    }

    fn message_from_buffer(&mut self) -> Option<Bytes> {
        match cassandra_protocol::frame::Envelope::check_envelope_size(&self.buffer) {
            Ok(message_size) => Some(self.buffer.split_to(message_size).freeze()),
            Err(CheckEnvelopeSizeError::NotEnoughBytes) => None,
            Err(err) => panic!("envelope error: {err:?}"),
        }
    }

    fn send_message(&mut self, message: &[u8]) {
        self.stream.write_all(message).unwrap();
    }
}
