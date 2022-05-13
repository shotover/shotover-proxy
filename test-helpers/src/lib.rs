pub mod cert;
pub mod docker_compose;
pub mod lazy;
pub mod shotover_process;

use anyhow::{bail, Result};

pub fn wait_for_socket_to_open(address: &str, port: u16) {
    try_wait_for_socket_to_open(address, port).unwrap();
}

pub fn try_wait_for_socket_to_open(address: &str, port: u16) -> Result<()> {
    let mut tries = 0;
    while std::net::TcpStream::connect((address, port)).is_err() {
        std::thread::sleep(std::time::Duration::from_millis(100));
        if tries > 50 {
            bail!("Ran out of retries to connect to the socket");
        }
        tries += 1;
    }
    Ok(())
}
