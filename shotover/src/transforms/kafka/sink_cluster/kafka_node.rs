use super::scram_over_mtls::AuthorizeScramOverMtls;
use crate::codec::{CodecBuilder, Direction, kafka::KafkaCodecBuilder};
use crate::connection::SinkConnection;
use crate::frame::Frame;
use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::message::Message;
use crate::tls::TlsConnector;
use crate::transforms::kafka::sink_cluster::SASL_SCRAM_MECHANISMS;
use crate::transforms::kafka::sink_cluster::scram_over_mtls::OriginalScramState;
use anyhow::{Context, Result, anyhow};
use atomic_enum::atomic_enum;
use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::{ApiKey, BrokerId, RequestHeader, SaslAuthenticateRequest};
use kafka_protocol::protocol::StrBytes;
use sasl::client::Mechanism;
use sasl::client::mechanisms::Scram;
use sasl::common::ChannelBinding;
use sasl::common::scram::Sha256;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;

pub struct ConnectionFactory {
    tls: Option<TlsConnector>,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
    auth_requests: Vec<Message>,
    force_run_chain: Arc<Notify>,
}

impl ConnectionFactory {
    pub fn new(
        tls: Option<TlsConnector>,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
        force_run_chain: Arc<Notify>,
    ) -> Self {
        ConnectionFactory {
            tls,
            connect_timeout,
            auth_requests: vec![],
            force_run_chain,
            read_timeout,
        }
    }

    pub fn add_auth_request(&mut self, message: Message) {
        self.auth_requests.push(message);
    }

    pub async fn create_connection_unauthed(
        &self,
        kafka_address: &KafkaAddress,
    ) -> Result<SinkConnection> {
        let codec = KafkaCodecBuilder::new(Direction::Sink, "KafkaSinkCluster".to_owned());
        let address = (kafka_address.host.to_string(), kafka_address.port as u16);

        SinkConnection::new(
            address,
            codec,
            &self.tls,
            self.connect_timeout,
            self.force_run_chain.clone(),
            self.read_timeout,
        )
        .await
    }

    pub async fn create_connection(
        &self,
        kafka_address: &KafkaAddress,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        sasl_mechanism: &Option<String>,
    ) -> Result<SinkConnection> {
        let codec = KafkaCodecBuilder::new(Direction::Sink, "KafkaSinkCluster".to_owned());
        let address = (kafka_address.host.to_string(), kafka_address.port as u16);
        let mut connection = SinkConnection::new(
            address,
            codec,
            &self.tls,
            self.connect_timeout,
            self.force_run_chain.clone(),
            self.read_timeout,
        )
        .await
        .context("Failed to create sink connection")?;

        if !self.auth_requests.is_empty() {
            if let Some(scram_over_mtls) = authorize_scram_over_mtls {
                if let Some(sasl_mechanism) = sasl_mechanism {
                    if SASL_SCRAM_MECHANISMS.contains(&sasl_mechanism.as_str()) {
                        self.scram_over_mtls(scram_over_mtls, &mut connection)
                            .await?;
                    } else {
                        self.replay_sasl(&mut connection).await?;
                    }
                } else {
                    self.replay_sasl(&mut connection).await?;
                }
            } else {
                self.replay_sasl(&mut connection).await?;
            }
        }

        Ok(connection)
    }

    async fn scram_over_mtls(
        &self,
        scram_over_mtls: &AuthorizeScramOverMtls,
        connection: &mut SinkConnection,
    ) -> Result<()> {
        if matches!(
            scram_over_mtls.original_scram_state,
            OriginalScramState::AuthSuccess
        ) {
            // The original connection is authorized, so we are free to authorize more sessions
            self.perform_tokenauth_scram_exchange(scram_over_mtls, connection)
                .await
                .context("Failed to perform delegation token SCRAM exchange")
        } else {
            // If the original session has not authenticated yet, this is probably the first outgoing connection.
            // So just create it with no outgoing connections, the client will perform the remainder of the scram handshake.
            //
            // If the original session failed to authenticate we cannot authorize this session.
            // So just perform no scram handshake and let kafka uphold the authorization requirements for us.
            Ok(())
        }
    }

    /// authorize_scram_over_mtls creates new connections via delegation tokens.
    /// Kafka implements delegation tokens as just a special case of SCRAM.
    /// In particular kafka utilizes scram's concept of extensions to send `tokenauth=true` to the server,
    /// indicating that the user and password are actually a delegation token's token_id and HMAC.
    ///
    /// This method implements a full SCRAM authentication exchange with the kafka delegation token extension.
    /// The `client-first`, `server-first` etc messages are as named in the SCRAM RFC:
    /// https://datatracker.ietf.org/doc/html/rfc5802
    async fn perform_tokenauth_scram_exchange(
        &self,
        scram_over_mtls: &AuthorizeScramOverMtls,
        connection: &mut SinkConnection,
    ) -> Result<()> {
        // send/receive SaslHandshake
        let mut sasl_handshake_request = self.auth_requests.first().unwrap().clone();
        if let Some(Frame::Kafka(KafkaFrame::Request { header, .. })) =
            sasl_handshake_request.frame()
        {
            // If the request is version 0 it requires SaslAuthenticate messages to be sent as raw bytes which is impossible.
            // So instead force it to version 1.
            if header.request_api_version == 0 {
                header.request_api_version = 1;
                sasl_handshake_request.invalidate_cache();
            }
        }
        connection.send(vec![sasl_handshake_request])?;
        let mut handshake_response = connection.recv().await?.pop().unwrap();
        if let Some(Frame::Kafka(KafkaFrame::Response {
            body: ResponseBody::SaslHandshake(handshake_response),
            ..
        })) = handshake_response.frame()
        {
            if let Some(err) = ResponseError::try_from_code(handshake_response.error_code) {
                return Err(anyhow!(
                    "kafka responded to SaslHandshake with error {err}, server supported mechanisms: {:?}",
                    handshake_response.mechanisms,
                ));
            }
        } else {
            return Err(anyhow!(
                "Unexpected response to SaslHandshake request: {handshake_response:?}"
            ));
        }

        let delegation_token = scram_over_mtls.get_token_for_user().await?;

        // SCRAM client-first
        let mut scram = Scram::<Sha256>::new(
            delegation_token.token_id,
            delegation_token.hmac.to_string(),
            ChannelBinding::None,
        )
        .map_err(|x| anyhow!("{x:?}"))?
        .with_first_extensions("tokenauth=true".to_owned());
        connection
            .send(vec![Self::create_auth_request(scram.initial())])
            .context("Failed to send first SCRAM request")?;

        // SCRAM server-first
        let first_scram_response = connection
            .recv()
            .await
            .context("Failed to receive first scram response")?
            .pop()
            .unwrap();
        let first_scram_response = Self::process_auth_response(first_scram_response)
            .context("first response to delegation token SCRAM reported an error")?;

        // SCRAM client-final
        let final_scram_request = scram
            .response(&first_scram_response)
            .context("Failed to generate final scram request")?;
        connection
            .send(vec![Self::create_auth_request(final_scram_request)])
            .context("Failed to send final SCRAM request")?;

        // SCRAM server-final
        let final_scram_response = connection
            .recv()
            .await
            .context("Failed to receive second scram response")?
            .pop()
            .unwrap();
        let final_scram_response = Self::process_auth_response(final_scram_response)
            .context("final response to delegation token SCRAM reported an error")?;
        scram
            .success(&final_scram_response)
            .context("Server gave invalid final response to delegation token SCRAM")
    }

    /// For SASL plain it is sufficient to replay the requests made in a successful exchange.
    /// This method performs such a replay on the passed connection.
    async fn replay_sasl(&self, connection: &mut SinkConnection) -> Result<()> {
        connection.send(self.auth_requests.clone())?;
        let mut received_count = 0;
        let mut received = vec![];
        while received_count < self.auth_requests.len() {
            connection.recv_into(&mut received).await?;
            received_count += received.len();
        }

        // Check that the authenticate response was a success
        Self::process_auth_response(received.pop().unwrap())
            .map(|_| ())
            .context("Unexpected response to replayed SASL requests")
    }

    fn create_auth_request(bytes: Vec<u8>) -> Message {
        Message::from_frame(Frame::Kafka(KafkaFrame::Request {
            header: RequestHeader::default()
                .with_request_api_key(ApiKey::SaslAuthenticate as i16)
                .with_request_api_version(2),
            body: RequestBody::SaslAuthenticate(
                SaslAuthenticateRequest::default().with_auth_bytes(bytes.into()),
            ),
        }))
    }

    fn process_auth_response(mut response: Message) -> Result<Bytes> {
        if let Some(Frame::Kafka(KafkaFrame::Response {
            body: ResponseBody::SaslAuthenticate(auth_response),
            ..
        })) = response.frame()
        {
            if let Some(err) = ResponseError::try_from_code(auth_response.error_code) {
                Err(anyhow!(
                    "kafka responded to SaslAuthenticate with error: {err}, {}",
                    auth_response
                        .error_message
                        .as_ref()
                        .map(|x| x.as_str())
                        .unwrap_or_default()
                ))
            } else {
                Ok(auth_response.auth_bytes.clone())
            }
        } else {
            Err(anyhow!(
                "Unexpected response to SaslAuthenticate request: {response:?}"
            ))
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct KafkaAddress {
    pub host: StrBytes,
    pub port: i32,
}

impl KafkaAddress {
    pub fn new(host: StrBytes, port: i32) -> Self {
        KafkaAddress { host, port }
    }

    pub fn from_str(address: &str) -> Result<Self> {
        let mut address_iter = address.split(':');
        Ok(KafkaAddress {
            host: StrBytes::from_string(
                address_iter
                    .next()
                    .ok_or_else(|| anyhow!("Address must include ':' seperator"))?
                    .to_owned(),
            ),
            port: address_iter
                .next()
                .ok_or_else(|| anyhow!("Address must include port after ':'"))?
                .parse()
                .map_err(|_| anyhow!("Failed to parse address port as integer"))?,
        })
    }
}

impl Display for KafkaAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host.as_str(), self.port)
    }
}

#[derive(Debug, Clone)]
pub struct KafkaNode {
    pub broker_id: BrokerId,
    pub rack: Option<StrBytes>,
    pub kafka_address: KafkaAddress,
    state: Arc<AtomicKafkaNodeState>,
}

impl KafkaNode {
    pub fn new(broker_id: BrokerId, kafka_address: KafkaAddress, rack: Option<StrBytes>) -> Self {
        KafkaNode {
            broker_id,
            kafka_address,
            rack,
            state: Arc::new(AtomicKafkaNodeState::new(KafkaNodeState::Up)),
        }
    }

    pub fn is_up(&self) -> bool {
        self.state.load(Ordering::Relaxed) == KafkaNodeState::Up
    }

    pub fn set_state(&self, state: KafkaNodeState) {
        self.state.store(state, Ordering::Relaxed)
    }
}

#[atomic_enum]
#[derive(PartialEq)]
pub enum KafkaNodeState {
    Up,
    Down,
}
