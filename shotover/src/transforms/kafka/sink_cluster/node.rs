use super::scram_over_mtls::AuthorizeScramOverMtls;
use crate::codec::{kafka::KafkaCodecBuilder, CodecBuilder, Direction};
use crate::connection::SinkConnection;
use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::frame::Frame;
use crate::message::Message;
use crate::tls::TlsConnector;
use crate::transforms::kafka::sink_cluster::SASL_SCRAM_MECHANISMS;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use derivative::Derivative;
use kafka_protocol::messages::{ApiKey, BrokerId, RequestHeader, SaslAuthenticateRequest};
use kafka_protocol::protocol::{Builder, StrBytes};
use kafka_protocol::ResponseError;
use sasl::client::mechanisms::Scram;
use sasl::client::Mechanism;
use sasl::common::scram::Sha256;
use sasl::common::ChannelBinding;
use std::sync::Arc;
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
                        self.perform_tokenauth_scram_exchange(scram_over_mtls, &mut connection)
                            .await
                            .context("Failed to perform delegation token SCRAM exchange")?;
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
        let mut auth_requests = self.auth_requests.clone();

        // send/receive SaslHandshake
        connection.send(vec![auth_requests.remove(0)])?;
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

        let delegation_token = scram_over_mtls
            .token_task
            .get_token_for_user(scram_over_mtls.username.clone())
            .await?;

        // SCRAM client-first
        let mut scram = Scram::<Sha256>::new(
            delegation_token.token_id,
            delegation_token.hmac.to_string(),
            ChannelBinding::None,
            "tokenauth=true".to_owned(),
            String::new(),
        )
        .map_err(|x| anyhow!("{x:?}"))?;
        connection.send(vec![Self::create_auth_request(scram.initial())?])?;

        // SCRAM server-first
        let first_scram_response = connection.recv().await?.pop().unwrap();
        let first_scram_response = Self::process_auth_response(first_scram_response)
            .context("first response to delegation token SCRAM reported an error")?;

        // SCRAM client-final
        let final_scram_request = scram.response(&first_scram_response)?;
        connection.send(vec![Self::create_auth_request(final_scram_request)?])?;

        // SCRAM server-final
        let final_scram_response = connection.recv().await?.pop().unwrap();
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

    fn create_auth_request(bytes: Vec<u8>) -> Result<Message> {
        Ok(Message::from_frame(Frame::Kafka(KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::SaslAuthenticateKey as i16)
                .request_api_version(2)
                .build()
                .unwrap(),
            body: RequestBody::SaslAuthenticate(
                SaslAuthenticateRequest::builder()
                    .auth_bytes(bytes.into())
                    .build()?,
            ),
        })))
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

#[derive(Derivative)]
#[derivative(Debug)]
pub struct KafkaNode {
    pub broker_id: BrokerId,
    pub rack: Option<StrBytes>,
    pub kafka_address: KafkaAddress,
    #[derivative(Debug = "ignore")]
    connection: Option<SinkConnection>,
}

impl Clone for KafkaNode {
    fn clone(&self) -> Self {
        Self {
            broker_id: self.broker_id,
            rack: self.rack.clone(),
            kafka_address: self.kafka_address.clone(),
            connection: None,
        }
    }
}

impl KafkaNode {
    pub fn new(broker_id: BrokerId, kafka_address: KafkaAddress, rack: Option<StrBytes>) -> Self {
        KafkaNode {
            broker_id,
            kafka_address,
            rack,
            connection: None,
        }
    }

    pub async fn get_connection(
        &mut self,
        connection_factory: &ConnectionFactory,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
        sasl_mechanism: &Option<String>,
    ) -> Result<&mut SinkConnection> {
        if self.connection.is_none() {
            self.connection = Some(
                connection_factory
                    .create_connection(
                        &self.kafka_address,
                        authorize_scram_over_mtls,
                        sasl_mechanism,
                    )
                    .await
                    .context("Failed to create a new connection")?,
            );
        }
        Ok(self.connection.as_mut().unwrap())
    }

    pub fn get_connection_if_open(&mut self) -> Option<&mut SinkConnection> {
        self.connection.as_mut()
    }
}
