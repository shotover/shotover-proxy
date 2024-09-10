//! Use to establish a TLS connection to a DB in a sink transform

use crate::tcp;
use anyhow::{anyhow, bail, Context, Error, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::server::WebPkiClientVerifier;
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use rustls_pemfile::Item;
use rustls_pki_types::{CertificateDer, InvalidDnsNameError, PrivateKeyDer, ServerName, UnixTime};
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
#[serde(deny_unknown_fields)]
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
    let mut root_cert_store = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut pem) {
        root_cert_store
            .add(cert.context("Error while parsing PEM")?)
            .context("Failed to add cert to cert store")?;
    }
    Ok(root_cert_store)
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut BufReader::new(File::open(path)?))
        .collect::<Result<Vec<_>, _>>()
        .context("Error while parsing PEM")
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>> {
    for key in rustls_pemfile::read_all(&mut BufReader::new(File::open(path)?)) {
        match key.context("Error while parsing PEM")? {
            Item::Pkcs8Key(x) => return Ok(x.into()),
            Item::Pkcs1Key(x) => return Ok(x.into()),
            _ => {}
        }
    }
    Err(anyhow!("No suitable keys found in PEM"))
}

impl TlsAcceptor {
    pub fn new(tls_config: &TlsAcceptorConfig) -> Result<TlsAcceptor, Vec<String>> {
        // TODO: report multiple errors back to the user
        // The validation and anyhow error reporting has merged here and they were originally intended to be seperate.
        // We should probably replace the error reporting Vec<String> system with a better typed error system. (anyhow maybe?)
        Self::new_inner(tls_config).map_err(|x| vec![format!("{x:?}")])
    }

    fn new_inner(tls_config: &TlsAcceptorConfig) -> Result<TlsAcceptor> {
        let client_cert_verifier =
            if let Some(path) = tls_config.certificate_authority_path.as_ref() {
                let root_cert_store = load_ca(path).with_context(|| {
                    format!("Failed to read file {path} configured at 'certificate_authority_path'")
                })?;
                WebPkiClientVerifier::builder(root_cert_store.into())
                    .build()
                    .unwrap()
            } else {
                WebPkiClientVerifier::no_client_auth()
            };

        let private_key = load_private_key(&tls_config.private_key_path).with_context(|| {
            format!(
                "Failed to read file {} configured at 'private_key_path",
                tls_config.private_key_path,
            )
        })?;
        let certs = load_certs(&tls_config.certificate_path).with_context(|| {
            format!(
                "Failed to read file {} configured at 'certificate_path'",
                tls_config.private_key_path,
            )
        })?;

        let config = rustls::ServerConfig::builder()
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
#[serde(deny_unknown_fields)]
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
    pub fn new(tls_config: &TlsConnectorConfig) -> Result<TlsConnector> {
        let root_cert_store =
            load_ca(&tls_config.certificate_authority_path).with_context(|| {
                format!(
                    "Failed to read file {} configured at 'certificate_authority_path'",
                    tls_config.certificate_authority_path,
                )
            })?;

        let private_key = tls_config
            .private_key_path
            .as_ref()
            .map(|path| {
                load_private_key(path).with_context(|| {
                    format!("Failed to read file {path} configured at 'private_key_path",)
                })
            })
            .transpose()?;
        let certs = tls_config
            .certificate_path
            .as_ref()
            .map(|path| {
                load_certs(path).with_context(|| {
                    format!("Failed to read file {path} configured at 'certificate_path'",)
                })
            })
            .transpose()?;

        let config_builder = ClientConfig::builder();
        let config = match (private_key, certs, tls_config.verify_hostname) {
            (Some(private_key), Some(certs), true) => config_builder
                .with_root_certificates(root_cert_store)
                .with_client_auth_cert(certs, private_key)?,
            (Some(private_key), Some(certs), false) => config_builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipVerifyHostName::new(
                    root_cert_store,
                )))
                .with_client_auth_cert(certs, private_key)?,
            (None, None, true) => config_builder
                .with_root_certificates(root_cert_store)
                .with_no_client_auth(),
            (None, None, false) => config_builder
                .dangerous()
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

#[derive(Debug)]
struct SkipVerifyHostName {
    verifier: Arc<WebPkiServerVerifier>,
}

impl SkipVerifyHostName {
    pub fn new(roots: RootCertStore) -> Self {
        SkipVerifyHostName {
            verifier: WebPkiServerVerifier::builder(Arc::new(roots))
                .build()
                .unwrap(),
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
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        match self.verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
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

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.verifier.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.verifier.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.verifier.supported_verify_schemes()
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
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError>;
}

/// Implement for all reference types
impl<T: ToHostname + ?Sized> ToHostname for &T {
    fn to_hostname(&self) -> String {
        (**self).to_hostname()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        (**self).to_servername()
    }
}

impl ToHostname for String {
    fn to_hostname(&self) -> String {
        self.split(':').next().unwrap_or("").to_owned()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        ServerName::try_from(self.to_hostname())
    }
}

impl ToHostname for &str {
    fn to_hostname(&self) -> String {
        self.split(':').next().unwrap_or("").to_owned()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        ServerName::try_from(self.split(':').next().unwrap_or("").to_owned())
    }
}

impl ToHostname for (&str, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        ServerName::try_from(self.0.to_owned())
    }
}

impl ToHostname for (String, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        ServerName::try_from(self.0.clone())
    }
}

impl ToHostname for (IpAddr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        Ok(ServerName::IpAddress(self.0.into()))
    }
}

impl ToHostname for (Ipv4Addr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        Ok(ServerName::IpAddress(IpAddr::V4(self.0).into()))
    }
}

impl ToHostname for (Ipv6Addr, u16) {
    fn to_hostname(&self) -> String {
        self.0.to_string()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        Ok(ServerName::IpAddress(IpAddr::V6(self.0).into()))
    }
}

impl ToHostname for SocketAddr {
    fn to_hostname(&self) -> String {
        self.ip().to_string()
    }
    fn to_servername(&self) -> Result<ServerName<'static>, InvalidDnsNameError> {
        Ok(ServerName::IpAddress(self.ip().into()))
    }
}
