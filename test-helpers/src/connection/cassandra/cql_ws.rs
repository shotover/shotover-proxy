use anyhow::{Context, Result};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::Envelope;
use cassandra_protocol::frame::Flags;
use cassandra_protocol::frame::Opcode;
use cassandra_protocol::frame::Version;
use cassandra_protocol::frame::message_query::BodyReqQuery;
use cassandra_protocol::frame::message_response::ResponseBody;
use cassandra_protocol::frame::message_result::{BodyResResultRows, ResResultBody};
use cassandra_protocol::query::query_params::QueryParams;
use cassandra_protocol::types::cassandra_type::{CassandraType, wrapper_fn};
use futures_util::{SinkExt, StreamExt};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::{CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_tungstenite::Connector;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::error::Error;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::handshake::server::Request;

pub struct CqlWsSession {
    in_rx: UnboundedReceiver<Message>,
    out_tx: UnboundedSender<Message>,
}

impl CqlWsSession {
    fn construct_request(uri: &str) -> Request {
        let uri = uri.parse::<http::Uri>().unwrap();

        let authority = uri.authority().unwrap().as_str();
        let host = authority
            .find('@')
            .map(|idx| authority.split_at(idx + 1).1)
            .unwrap_or_else(|| authority);

        if host.is_empty() {
            panic!("Empty host name");
        }

        http::Request::builder()
            .method("GET")
            .header("Host", host)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", generate_key())
            .header(
                "Sec-WebSocket-Protocol",
                "cql".parse::<http::HeaderValue>().unwrap(),
            )
            .uri(uri)
            .body(())
            .unwrap()
    }

    pub async fn new(address: &str) -> Self {
        let (ws_stream, _) = tokio_tungstenite::connect_async(Self::construct_request(address))
            .await
            .unwrap();

        let (in_tx, in_rx) = unbounded_channel::<Message>();
        let (out_tx, out_rx) = unbounded_channel::<Message>();

        spawn_read_write_tasks(ws_stream, in_tx, out_rx);

        let mut session = Self { in_rx, out_tx };
        session.startup().await;
        session
    }

    pub async fn new_tls(address: &str, ca_path: &str) -> Self {
        let root_cert_store = load_ca(ca_path).unwrap();

        let tls_client_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipVerifyHostName::new(root_cert_store)))
            .with_no_client_auth();

        let (ws_stream, _) = tokio_tungstenite::connect_async_tls_with_config(
            Self::construct_request(address),
            None,
            false,
            Some(Connector::Rustls(Arc::new(tls_client_config))),
        )
        .await
        .unwrap();

        let (in_tx, in_rx) = unbounded_channel::<Message>();
        let (out_tx, out_rx) = unbounded_channel::<Message>();

        spawn_read_write_tasks(ws_stream, in_tx, out_rx);

        let mut session = Self { in_rx, out_tx };
        session.startup().await;
        session
    }

    async fn startup(&mut self) {
        let envelope = Envelope::new_req_startup(None, Version::V4);
        self.out_tx.send(Self::encode(envelope)).unwrap();

        let envelope = Self::decode(self.in_rx.recv().await.unwrap());

        match envelope.opcode {
            Opcode::Ready => println!("cql-ws: received: {:?}", envelope),
            Opcode::Authenticate => {
                todo!();
            }
            _ => panic!("expected to receive a ready or authenticate message"),
        }
    }

    pub async fn query(&mut self, query: &str) -> Vec<Vec<CassandraType>> {
        let envelope = Envelope::new_query(
            BodyReqQuery {
                query: query.into(),
                query_params: QueryParams::default(),
            },
            Flags::empty(),
            Version::V4,
        );

        self.out_tx.send(Self::encode(envelope)).unwrap();

        let envelope = Self::decode(self.in_rx.recv().await.unwrap());

        if let ResponseBody::Result(ResResultBody::Rows(BodyResResultRows {
            rows_content,
            metadata,
            ..
        })) = envelope.response_body().unwrap()
        {
            let mut result_values = vec![];
            for row in &rows_content {
                let mut row_result_values = vec![];
                for (i, col_spec) in metadata.col_specs.iter().enumerate() {
                    let wrapper = wrapper_fn(&col_spec.col_type.id);
                    let value = wrapper(&row[i], &col_spec.col_type, envelope.version).unwrap();

                    row_result_values.push(value);
                }
                result_values.push(row_result_values);
            }

            result_values
        } else {
            panic!("unexpected to recieve a result envelope");
        }
    }

    fn encode(envelope: Envelope) -> Message {
        let data = envelope.encode_with(Compression::None).unwrap();
        Message::Binary(data.into())
    }

    fn decode(ws_message: Message) -> Envelope {
        match ws_message {
            Message::Binary(data) => {
                Envelope::from_buffer(data.as_ref(), Compression::None)
                    .unwrap()
                    .envelope
            }
            _ => panic!("expected to receive a binary message"),
        }
    }

    pub async fn send_raw_ws_message(&mut self, ws_message: Message) {
        self.out_tx.send(ws_message).unwrap();
    }

    pub async fn wait_for_raw_ws_message_resp(&mut self) -> Message {
        self.in_rx.recv().await.unwrap()
    }
}

fn spawn_read_write_tasks<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    ws_stream: WebSocketStream<S>,
    in_tx: UnboundedSender<Message>,
    mut out_rx: UnboundedReceiver<Message>,
) {
    let (mut write, mut read) = ws_stream.split();

    // read task
    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = read.next() => {
                    if let Some(message) = result {
                        match message {
                            Ok(ws_message @ Message::Binary(_)) => {
                                in_tx.send(ws_message).unwrap();
                            }
                            Ok(Message::Close(_)) => {
                                return;
                            }
                            Ok(_) => panic!("expected to recieve a binary message"),
                            Err(err) => panic!("{err}")
                        }
                    }
                }
                _ = in_tx.closed() => {
                    return;
                }
            }
        }
    });

    // write task
    tokio::spawn(async move {
        loop {
            if let Some(ws_message) = out_rx.recv().await {
                write.send(ws_message).await.unwrap();
            } else {
                match write.send(Message::Close(None)).await {
                    Ok(_) => {}
                    Err(Error::Protocol(ProtocolError::SendAfterClosing)) => {}
                    Err(err) => panic!("{err}"),
                }
                break;
            }
        }
    });
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

#[derive(Debug)]
pub struct SkipVerifyHostName {
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
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
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
