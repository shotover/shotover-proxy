//! Use to establish a TLS connection to a DB in a sink transform

use crate::tcp;
use anyhow::{anyhow, bail, Context, Error, Result};
use rustls::client::{InvalidDnsNameError, ServerCertVerified, ServerCertVerifier, WebPkiVerifier};
use rustls::server::{AllowAnyAuthenticatedClient, NoClientAuth};
use rustls::{Certificate, CertificateError, PrivateKey, RootCertStore, ServerName};
use rustls_pemfile::{certs, Item};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, ErrorKind};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_rustls::client::TlsStream as TlsStreamClient;
use tokio_rustls::server::TlsStream as TlsStreamServer;
use tokio_rustls::{TlsAcceptor as RustlsAcceptor, TlsConnector as RustlsConnector};

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
    acceptor: RustlsAcceptor,
}

pub enum AcceptError {
    /// The client decided it didnt need the connection anymore and politely disconnected before the handshake completed.
    /// This can occur during regular use and indicates the connection should be quietly discarded.
    Disconnected,
    Failure(Error),
}

fn load_ca(path: &str) -> Result<RootCertStore> {
    let mut pem = BufReader::new(File::open(path)?);
    let certs = rustls_pemfile::certs(&mut pem).context("Error while parsing PEM")?;

    let mut root_cert_store = RootCertStore::empty();
    for cert in certs {
        root_cert_store
            .add(&Certificate(cert))
            .context("Failed to add cert to cert store")?;
    }
    Ok(root_cert_store)
}

fn load_certs(path: &str) -> Result<Vec<Certificate>> {
    certs(&mut BufReader::new(File::open(path)?))
        .context("Error while parsing PEM")
        .map(|certs| certs.into_iter().map(Certificate).collect())
}

fn load_private_key(path: &str) -> Result<PrivateKey> {
    let keys = rustls_pemfile::read_all(&mut BufReader::new(File::open(path)?))
        .context("Error while parsing PEM")?;
    keys.into_iter()
        .find_map(|item| match item {
            Item::RSAKey(x) | Item::PKCS8Key(x) => Some(PrivateKey(x)),
            _ => None,
        })
        .ok_or_else(|| anyhow!("No suitable keys found in PEM"))
}

impl TlsAcceptor {
    pub fn new(tls_config: TlsAcceptorConfig) -> Result<TlsAcceptor> {
        let client_cert_verifier =
            if let Some(path) = tls_config.certificate_authority_path.as_ref() {
                let root_cert_store = load_ca(path).map_err(|err| {
                    anyhow!(err).context(format!(
                        "Failed to read file {path} configured at 'certificate_authority_path'"
                    ))
                })?;
                AllowAnyAuthenticatedClient::new(root_cert_store).boxed()
            } else {
                NoClientAuth::boxed()
            };

        let private_key = load_private_key(&tls_config.private_key_path).map_err(|err| {
            anyhow!(err).context(format!(
                "Failed to read file {} configured at 'private_key_path",
                tls_config.private_key_path,
            ))
        })?;
        let certs = load_certs(&tls_config.certificate_path).map_err(|err| {
            anyhow!(err).context(format!(
                "Failed to read file {} configured at 'certificate_path'",
                tls_config.private_key_path,
            ))
        })?;

        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(client_cert_verifier)
            .with_single_cert(certs, private_key)?;

        Ok(TlsAcceptor {
            acceptor: RustlsAcceptor::from(Arc::new(config)),
        })
    }

    pub async fn accept(
        &self,
        tcp_stream: TcpStream,
    ) -> Result<TlsStreamServer<TcpStream>, AcceptError> {
        self.acceptor
            .accept(tcp_stream)
            .await
            .map_err(|err| match err.kind() {
                ErrorKind::UnexpectedEof => AcceptError::Disconnected,
                _ => AcceptError::Failure(anyhow!(err).context("Failed to accept TLS connection")),
            })
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

#[derive(Clone)]
pub struct TlsConnector {
    connector: RustlsConnector,
}

impl TlsConnector {
    pub fn new(tls_config: TlsConnectorConfig) -> Result<TlsConnector> {
        let root_cert_store = load_ca(&tls_config.certificate_authority_path).map_err(|err| {
            anyhow!(err).context(format!(
                "Failed to read file {} configured at 'certificate_authority_path'",
                tls_config.certificate_authority_path,
            ))
        })?;

        let private_key = tls_config
            .private_key_path
            .as_ref()
            .map(|path| {
                load_private_key(path).map_err(|err| {
                    anyhow!(err).context(format!(
                        "Failed to read file {path} configured at 'private_key_path",
                    ))
                })
            })
            .transpose()?;
        let certs = tls_config
            .certificate_path
            .as_ref()
            .map(|path| {
                load_certs(path).map_err(|err| {
                    anyhow!(err).context(format!(
                        "Failed to read file {path} configured at 'certificate_path'",
                    ))
                })
            })
            .transpose()?;

        let config_builder = rustls::ClientConfig::builder().with_safe_defaults();
        let config = match (private_key, certs, tls_config.verify_hostname) {
            (Some(private_key), Some(certs), true) => config_builder
                .with_root_certificates(root_cert_store)
                .with_single_cert(certs, private_key)?,
            (Some(private_key), Some(certs), false) => config_builder
                .with_custom_certificate_verifier(Arc::new(SkipVerifyHostName::new(
                    root_cert_store,
                )))
                .with_single_cert(certs, private_key)?,
            (None, None, true) => config_builder
                .with_root_certificates(root_cert_store)
                .with_no_client_auth(),
            (None, None, false) => config_builder
                .with_custom_certificate_verifier(Arc::new(SkipVerifyHostName::new(
                    root_cert_store,
                )))
                .with_no_client_auth(),

            (Some(_), None, _) => {
                bail!("private_key_path was specified but certificate_path was not: Either enable both or none")
            }
            (None, Some(_), _) => {
                bail!("certificate_path was specified but private_key_path was not: Either enable both or none")
            }
        };

        Ok(TlsConnector {
            connector: RustlsConnector::from(Arc::new(config)),
        })
    }

    pub async fn connect<A: ToSocketAddrs + ToHostname + std::fmt::Debug>(
        &self,
        connect_timeout: Duration,
        address: A,
    ) -> Result<TlsStreamClient<TcpStream>> {
        let servername = address.to_servername()?;
        let tcp_stream = tcp::tcp_stream(connect_timeout, address).await?;
        self.connector
            .connect(servername, tcp_stream)
            .await
            .context("Failed to establish TLS connection to destination")
    }
}

pub struct SkipVerifyHostName {
    verifier: WebPkiVerifier,
}

impl SkipVerifyHostName {
    pub fn new(roots: RootCertStore) -> Self {
        SkipVerifyHostName {
            verifier: WebPkiVerifier::new(roots, None),
        }
    }
}

// This recreates the verify_hostname(false) functionality from openssl.
// This adds an opening for MitM attacks but we provide this functionality because there are some
// circumstances where providing a cert per instance in a cluster is difficult and this allows at least some security by sharing a single cert between all instances.
// Note that the SAN dnsname wildcards (e.g. *foo.com) wouldnt help here because we need to refer to destinations by ip address and there is no such wildcard functionality for ip addresses.
impl ServerCertVerifier for SkipVerifyHostName {
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: std::time::SystemTime,
    ) -> std::result::Result<rustls::client::ServerCertVerified, rustls::Error> {
        match self.verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            scts,
            ocsp_response,
            now,
        ) {
            Ok(result) => Ok(result),
            Err(rustls::Error::InvalidCertificate(CertificateError::NotValidForName)) => {
                Ok(ServerCertVerified::assertion())
            }
            Err(err) => Err(err),
        }
    }
}

/// A trait object can only consist of one trait + special language traits like Send/Sync etc
/// So we need to use this trait when creating trait objects that need both AsyncRead and AsyncWrite
pub trait AsyncStream: AsyncRead + AsyncWrite {}

/// We need to tell rust that these types implement AsyncStream even though they already implement AsyncRead and AsyncWrite
impl AsyncStream for TlsStreamClient<TcpStream> {}
impl AsyncStream for TlsStreamServer<TcpStream> {}
impl AsyncStream for TcpStream {}

/// Allows retrieving the hostname from any ToSocketAddrs type
pub trait ToHostname {
    fn to_hostname(&self) -> String;
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError>;
}

/// Implement for all reference types
impl<T: ToHostname + ?Sized> ToHostname for &T {
    fn to_hostname(&self) -> String {
        (**self).to_hostname()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        (**self).to_servername()
    }
}

impl ToHostname for String {
    fn to_hostname(&self) -> String {
        self.split(':').next().unwrap_or("").to_owned()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        ServerName::try_from(self.to_hostname().as_str())
    }
}

impl ToHostname for &str {
    fn to_hostname(&self) -> String {
        self.split(':').next().unwrap_or("").to_owned()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        ServerName::try_from(self.split(':').next().unwrap_or(""))
    }
}

impl ToHostname for (&str, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        ServerName::try_from(self.0)
    }
}

impl ToHostname for (String, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        ServerName::try_from(self.0.as_str())
    }
}

impl ToHostname for (IpAddr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        Ok(ServerName::IpAddress(self.0))
    }
}

impl ToHostname for (Ipv4Addr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        Ok(ServerName::IpAddress(IpAddr::V4(self.0)))
    }
}

impl ToHostname for (Ipv6Addr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        Ok(ServerName::IpAddress(IpAddr::V6(self.0)))
    }
}

impl ToHostname for SocketAddr {
    fn to_hostname(&self) -> String {
        self.ip().to_string()
    }
    fn to_servername(&self) -> Result<ServerName, InvalidDnsNameError> {
        Ok(ServerName::IpAddress(self.ip()))
    }
}
