use std::{net::IpAddr, time::Duration};

use tokio::{net::TcpStream, time::Instant};

use crate::ssh::SshConnection;

pub struct Ec2Instance {
    public_ip: IpAddr,
    private_ip: IpAddr,
    client_private_key: String,
    ssh: SshConnection,
}

impl Ec2Instance {
    pub fn public_ip(&self) -> IpAddr {
        self.public_ip
    }

    pub fn private_ip(&self) -> IpAddr {
        self.private_ip
    }

    pub fn client_private_key(&self) -> &str {
        &self.client_private_key
    }

    pub fn ssh(&self) -> &SshConnection {
        &self.ssh
    }

    pub(crate) async fn new(
        public_ip: IpAddr,
        private_ip: IpAddr,
        host_public_key_bytes: Vec<u8>,
        client_private_key: &str,
    ) -> Self {
        loop {
            let start = Instant::now();
            // We retry many times before we are able to succesfully make an ssh connection.
            // Each error is expected and so is logged as a `info!` that describes the underlying startup process that is supposed to cause the error.
            // A numbered comment is left before each `info!` to demonstrate the order each error occurs in.
            match tokio::time::timeout(Duration::from_secs(10), TcpStream::connect((public_ip, 22)))
                .await
            {
                Err(_) => {
                    // 1.
                    tracing::info!("Timed out connecting to {public_ip} over ssh, the host is probably not accessible yet, retrying");
                    continue;
                }
                Ok(Err(e)) => {
                    // 2.
                    tracing::info!("failed to connect to {public_ip}:22, the host probably hasnt started their ssh service yet, retrying, error was {e}");
                    tokio::time::sleep_until(start + Duration::from_secs(1)).await;
                    continue;
                }
                Ok(Ok(stream)) => {
                    match SshConnection::new(
                        stream,
                        public_ip,
                        host_public_key_bytes.clone(),
                        client_private_key,
                    )
                    .await
                    {
                        Err(err) => {
                            // 3.
                            tracing::info!("Failed to make ssh connection to server, the host has probably not run its user-data script yet, retrying, error was: {err:?}");
                            tokio::time::sleep_until(start + Duration::from_secs(1)).await;
                            continue;
                        }
                        // 4. Then finally we have a working ssh connection.
                        Ok(ssh) => {
                            break Ec2Instance {
                                ssh,
                                public_ip,
                                private_ip,
                                client_private_key: client_private_key.to_owned(),
                            };
                        }
                    };
                }
            };
        }
    }
}
