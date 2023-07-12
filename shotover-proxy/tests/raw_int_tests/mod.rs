use serial_test::serial;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread;
use test_helpers::shotover_process::ShotoverProcessBuilder;

fn handle_client(mut stream: TcpStream) {
    // read 20 bytes at a time from stream echoing back to stream
    loop {
        let mut read = [0; 1028];
        match stream.read(&mut read) {
            Ok(n) => {
                if n == 0 {
                    // connection was closed
                    break;
                }
                println!("read: {:?}", &read[0..n]);
                stream.write_all(&read[0..n]).unwrap();
            }
            Err(err) => {
                println!("{}", err);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn passthrough() {
    let shotover = ShotoverProcessBuilder::new_with_topology(
        "tests/test-configs/raw-passthrough/topology.yaml",
    )
    .start()
    .await;

    let message = b"Hello world!";
    thread::spawn(|| {
        let listener = TcpListener::bind("127.0.0.1:9000").unwrap();
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    thread::spawn(move || {
                        handle_client(stream);
                    });
                }
                Err(_) => {
                    println!("Error");
                }
            }
        }
    });

    let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();

    stream.write_all(message).unwrap();
    let mut response = [0; 12];
    let _ = stream.read(&mut response).unwrap();
    assert_eq!(&response, message);
    shotover.shutdown_and_then_consume_events(&[]).await;
}
