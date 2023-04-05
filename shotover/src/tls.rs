use crate::tcp;
use anyhow::{anyhow, Error, Result};
use openssl::ssl::{ErrorCode, Ssl};
use openssl::ssl::{SslAcceptor, SslConnector, SslFiletype, SslMethod};
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_openssl::SslStream;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TlsAcceptorConfig {
    /// Path to the certificate authority in PEM format
    pub certificate_authority_path: Option<String>,
    /// Path to the certificate in PEM format
    pub certificate_path: String,
    /// Path to the private key in PEM format
    pub private_key_path: String,
}

#[derive(Clone)]
pub struct TlsAcceptor {
    acceptor: Arc<SslAcceptor>,
}

pub enum AcceptError {
    /// The client decided it didnt need the connection anymore and politely disconnected before the handshake completed.
    /// This can occur during regular use and indicates the connection should be quietly discarded.
    Disconnected,
    Failure(Error),
}

pub fn check_file_field(field_name: &str, file_path: &str) -> Result<()> {
    if Path::new(file_path).exists() {
        Ok(())
    } else {
        Err(anyhow!(
            "configured {field_name} does not exist '{file_path}'"
        ))
    }
}

impl TlsAcceptor {
    pub fn new(tls_config: TlsAcceptorConfig) -> Result<TlsAcceptor> {
        // openssl's errors are really bad so we do our own checks so we can provide reasonable errors
        check_file_field("private_key_path", &tls_config.private_key_path)?;
        check_file_field("certificate_path", &tls_config.certificate_path)?;

        let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls())
            .map_err(openssl_stack_error_to_anyhow)?;

        if let Some(path) = tls_config.certificate_authority_path.as_ref() {
            check_file_field("certificate_authority_path", path)?;
            builder
                .set_ca_file(path)
                .map_err(openssl_stack_error_to_anyhow)?;
            return Err(anyhow!("Client auth is not yet supported in shotover"));
        }

        builder
            .set_private_key_file(tls_config.private_key_path, SslFiletype::PEM)
            .map_err(openssl_stack_error_to_anyhow)?;
        builder
            .set_certificate_chain_file(tls_config.certificate_path)
            .map_err(openssl_stack_error_to_anyhow)?;
        builder
            .check_private_key()
            .map_err(openssl_stack_error_to_anyhow)?;

        Ok(TlsAcceptor {
            acceptor: Arc::new(builder.build()),
        })
    }

    pub async fn accept(&self, tcp_stream: TcpStream) -> Result<SslStream<TcpStream>, AcceptError> {
        let ssl = Ssl::new(self.acceptor.context())
            .map_err(|e| AcceptError::Failure(openssl_stack_error_to_anyhow(e)))?;
        let mut ssl_stream = SslStream::new(ssl, tcp_stream)
            .map_err(|e| AcceptError::Failure(openssl_stack_error_to_anyhow(e)))?;

        Pin::new(&mut ssl_stream).accept().await.map_err(|e| {
            // This is the internal logic that results in the "unexpected EOF" error in the ssl::error::Error display impl
            if e.code() == ErrorCode::SYSCALL && e.io_error().is_none() {
                AcceptError::Disconnected
            } else {
                AcceptError::Failure(
                    openssl_ssl_error_to_anyhow(e).context("Failed to accept TLS connection"),
                )
            }
        })?;
        Ok(ssl_stream)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TlsConnectorConfig {
    /// Path to the certificate authority in PEM format
    pub certificate_authority_path: String,
    /// Path to the certificate in PEM format
    pub certificate_path: Option<String>,
    /// Path to the private key in PEM format
    pub private_key_path: Option<String>,
    /// enable/disable verifying the hostname of the destination's certificate.
    pub verify_hostname: bool,
}

#[derive(Clone, Debug)]
pub struct TlsConnector {
    connector: Arc<SslConnector>,
    verify_hostname: bool,
}

impl TlsConnector {
    pub fn new(tls_config: TlsConnectorConfig) -> Result<TlsConnector> {
        check_file_field(
            "certificate_authority_path",
            &tls_config.certificate_authority_path,
        )?;
        let mut builder =
            SslConnector::builder(SslMethod::tls()).map_err(openssl_stack_error_to_anyhow)?;
        builder
            .set_ca_file(tls_config.certificate_authority_path)
            .map_err(openssl_stack_error_to_anyhow)?;

        if let Some(private_key_path) = tls_config.private_key_path {
            check_file_field("private_key_path", &private_key_path)?;
            builder
                .set_private_key_file(private_key_path, SslFiletype::PEM)
                .map_err(openssl_stack_error_to_anyhow)?;
        }

        if let Some(certificate_path) = tls_config.certificate_path {
            check_file_field("certificate_path", &certificate_path)?;
            builder
                .set_certificate_chain_file(certificate_path)
                .map_err(openssl_stack_error_to_anyhow)?;
        }

        Ok(TlsConnector {
            connector: Arc::new(builder.build()),
            verify_hostname: tls_config.verify_hostname,
        })
    }

    pub async fn connect<A: ToSocketAddrs + ToHostname + std::fmt::Debug>(
        &self,
        connect_timeout: Duration,
        address: A,
    ) -> Result<SslStream<TcpStream>> {
        let ssl = self
            .connector
            .configure()
            .map_err(openssl_stack_error_to_anyhow)?
            .verify_hostname(self.verify_hostname)
            .into_ssl(&address.to_hostname())
            .map_err(openssl_stack_error_to_anyhow)?;

        let tcp_stream = tcp::tcp_stream(connect_timeout, address).await?;
        let mut ssl_stream =
            SslStream::new(ssl, tcp_stream).map_err(openssl_stack_error_to_anyhow)?;
        Pin::new(&mut ssl_stream).connect().await.map_err(|e| {
            openssl_ssl_error_to_anyhow(e)
                .context("Failed to establish TLS connection to destination")
        })?;

        Ok(ssl_stream)
    }
}

// Always use these openssl_* conversion methods instead of directly converting to anyhow

fn openssl_ssl_error_to_anyhow(error: openssl::ssl::Error) -> anyhow::Error {
    if let Some(stack) = error.ssl_error() {
        openssl_stack_error_to_anyhow(stack.clone())
    } else {
        anyhow!("{error}")
    }
}

fn openssl_stack_error_to_anyhow(error: openssl::error::ErrorStack) -> anyhow::Error {
    let mut anyhow_stack: Option<anyhow::Error> = None;
    for inner in error.errors() {
        let anyhow_error = openssl_error_to_anyhow(inner.clone());
        anyhow_stack = Some(match anyhow_stack {
            Some(anyhow) => anyhow.context(anyhow_error),
            None => anyhow_error,
        });
    }
    match anyhow_stack {
        Some(anyhow_stack) => anyhow_stack,
        None => anyhow!("{error}"),
    }
}

fn openssl_error_to_anyhow(error: openssl::error::Error) -> anyhow::Error {
    let mut fmt = String::new();
    write!(fmt, "error 0x{:08X} ", error.code()).unwrap();
    match error.reason() {
        Some(r) => write!(fmt, "'{}", r).unwrap(),
        None => write!(fmt, "'Unknown'").unwrap(),
    }
    if let Some(data) = error.data() {
        write!(fmt, ": {}' ", data).unwrap();
    } else {
        write!(fmt, "' ").unwrap();
    }
    write!(fmt, "occurred in ").unwrap();
    match error.function() {
        Some(f) => write!(fmt, "function '{}' ", f).unwrap(),
        None => write!(fmt, "function 'Unknown' ").unwrap(),
    }
    write!(fmt, "in file '{}:{}' ", error.file(), error.line()).unwrap();
    match error.library() {
        Some(l) => write!(fmt, "in library '{}'", l).unwrap(),
        None => write!(fmt, "in library 'Unknown'").unwrap(),
    }

    anyhow!(fmt)
}

/// A trait object can only consist of one trait + special language traits like Send/Sync etc
/// So we need to use this trait when creating trait objects that need both AsyncRead and AsyncWrite
pub trait AsyncStream: AsyncRead + AsyncWrite {}

/// We need to tell rust that these types implement AsyncStream even though they already implement AsyncRead and AsyncWrite
impl AsyncStream for tokio_openssl::SslStream<TcpStream> {}
impl AsyncStream for TcpStream {}

/// Allows retrieving the hostname from any ToSocketAddrs type
pub trait ToHostname {
    fn to_hostname(&self) -> String;
}

/// Implement for all reference types
impl<T: ToHostname + ?Sized> ToHostname for &T {
    fn to_hostname(&self) -> String {
        (**self).to_hostname()
    }
}

impl ToHostname for String {
    fn to_hostname(&self) -> String {
        self.split(':').next().unwrap_or("").to_owned()
    }
}

impl ToHostname for &str {
    fn to_hostname(&self) -> String {
        self.split(':').next().unwrap_or("").to_owned()
    }
}

impl ToHostname for (&str, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
}

impl ToHostname for (String, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
}

impl ToHostname for (IpAddr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
}

impl ToHostname for (Ipv4Addr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
}

impl ToHostname for (Ipv6Addr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
}

impl ToHostname for SocketAddr {
    fn to_hostname(&self) -> String {
        self.ip().to_string()
    }
}
