use anyhow::{anyhow, Result};
use openssl::ssl::Ssl;
use openssl::ssl::{SslAcceptor, SslConnector, SslFiletype, SslMethod};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_openssl::SslStream;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TlsConfig {
    /// Path to the certificate authority in PEM format
    pub certificate_authority_path: String,
    /// Path to the certificate in PEM format
    pub certificate_path: String,
    /// Path to the private key in PEM format
    pub private_key_path: String,
}

#[derive(Clone)]
pub struct TlsAcceptor {
    acceptor: Arc<SslAcceptor>,
}

impl TlsAcceptor {
    pub fn new(tls_config: TlsConfig) -> Result<TlsAcceptor> {
        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())?;
        builder.set_ca_file(tls_config.certificate_authority_path)?;
        builder.set_private_key_file(tls_config.private_key_path, SslFiletype::PEM)?;
        builder.set_certificate_chain_file(tls_config.certificate_path)?;
        builder.check_private_key()?;

        Ok(TlsAcceptor {
            acceptor: Arc::new(builder.build()),
        })
    }

    pub async fn accept(&self, tcp_stream: TcpStream) -> Result<SslStream<TcpStream>> {
        let ssl = Ssl::new(self.acceptor.context())?;
        let mut ssl_stream = SslStream::new(ssl, tcp_stream)?;

        Pin::new(&mut ssl_stream)
            .accept()
            .await
            .map_err(|x| anyhow!("Failed to accept TLS connection: {}", x))?;

        Ok(ssl_stream)
    }
}

#[derive(Clone)]
pub struct TlsConnector {
    connector: Arc<SslConnector>,
}

impl TlsConnector {
    pub fn new(tls_config: TlsConfig) -> Result<TlsConnector> {
        let mut builder = SslConnector::builder(SslMethod::tls())?;
        builder.set_ca_file(tls_config.certificate_authority_path)?;
        builder.set_private_key_file(tls_config.private_key_path, SslFiletype::PEM)?;
        builder.set_certificate_chain_file(tls_config.certificate_path)?;

        Ok(TlsConnector {
            connector: Arc::new(builder.build()),
        })
    }

    pub async fn connect_unverified_hostname(
        &self,
        tcp_stream: TcpStream,
    ) -> Result<SslStream<TcpStream>> {
        let ssl = self
            .connector
            .configure()?
            .verify_hostname(false)
            .into_ssl("localhost")?;

        let mut ssl_stream = SslStream::new(ssl, tcp_stream)?;
        Pin::new(&mut ssl_stream).connect().await?;

        Ok(ssl_stream)
    }

    pub async fn connect(&self, tcp_stream: TcpStream) -> Result<SslStream<TcpStream>> {
        let ssl = self.connector.configure()?.into_ssl("localhost")?;

        let mut ssl_stream = SslStream::new(ssl, tcp_stream)?;
        Pin::new(&mut ssl_stream).connect().await?;

        Ok(ssl_stream)
    }
}

/// A trait object can only consist of one trait + special language traits like Send/Sync etc
/// So we need to use this trait when creating trait objects that need both AsyncRead and AsyncWrite
pub trait AsyncStream: AsyncRead + AsyncWrite {}

/// We need to tell rust that these types implement AsyncStream even though they already implement AsyncRead and AsyncWrite
impl AsyncStream for tokio_openssl::SslStream<TcpStream> {}
impl AsyncStream for TcpStream {}
