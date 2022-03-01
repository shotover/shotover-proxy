pub mod docker_compose;
pub mod shotover_process;

pub fn wait_for_socket_to_open(address: &str, port: u16) {
    let mut tries = 0;
    while std::net::TcpStream::connect((address, port)).is_err() {
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(tries < 50, "Ran out of retries to connect to the socket");
        tries += 1;
    }
}
