use super::node::{ConnectionFactory, KafkaAddress};
use crate::{
    connection::SinkConnection,
    frame::{
        kafka::{KafkaFrame, RequestBody, ResponseBody},
        Frame,
    },
    message::Message,
    tls::{TlsConnector, TlsConnectorConfig},
};
use anyhow::{anyhow, Result};
use kafka_protocol::{
    messages::{ApiKey, CreateDelegationTokenRequest, RequestHeader},
    protocol::{Builder, StrBytes},
};
use rand::rngs::SmallRng;
use rand::{prelude::SliceRandom, SeedableRng};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio::sync::{mpsc, oneshot};

pub struct TokenRequest {
    username: String,
    response_tx: oneshot::Sender<DelegationToken>,
}

#[derive(Clone)]
pub struct TokenTask {
    tx: mpsc::Sender<TokenRequest>,
}

impl TokenTask {
    #[allow(clippy::new_without_default)]
    pub fn new(
        mtls_connection_factory: ConnectionFactory,
        mtls_port_contact_points: Vec<String>,
    ) -> TokenTask {
        let (tx, mut rx) = mpsc::channel::<TokenRequest>(1000);
        tokio::spawn(async move {
            loop {
                match task(&mut rx, &mtls_connection_factory, &mtls_port_contact_points).await {
                    Ok(()) => {
                        // shotover is shutting down, terminate the task
                        break;
                    }
                    Err(err) => {
                        tracing::error!("Token task restarting due to failure, error was {err:?}");
                    }
                }
            }
        });
        TokenTask { tx }
    }

    pub async fn get_token_for_user(&self, username: String) -> Result<DelegationToken> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(TokenRequest {
                username,
                response_tx,
            })
            .await?;
        Ok(response_rx.await?)
    }
}

async fn task(
    rx: &mut mpsc::Receiver<TokenRequest>,
    mtls_connection_factory: &ConnectionFactory,
    mtls_port_contact_points: &[String],
) -> Result<()> {
    let mut rng = SmallRng::from_rng(rand::thread_rng())?;
    let mut username_to_token = HashMap::new();
    while let Some(request) = rx.recv().await {
        let token = if let Some(token) = username_to_token.get(&request.username).cloned() {
            token
        } else {
            let address =
                KafkaAddress::from_str(mtls_port_contact_points.choose(&mut rng).unwrap())?;
            let mut connection = mtls_connection_factory
                // Must be unauthed since mTLS is its own auth.
                .create_connection_unauthed(&address)
                .await?;

            let token =
                create_delegation_token_for_user(&mut connection, request.username.clone()).await?;

            // TODO: This sleep is currently load bearing...
            //       Need to delay progression until token has propagated.
            tokio::time::sleep(std::time::Duration::from_secs(4)).await;

            username_to_token.insert(request.username, token.clone());
            token
        };
        request.response_tx.send(token).ok();
    }

    // rx returned None which indicates shotover is shutting down
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AuthorizeScramOverMtlsConfig {
    pub mtls_port_contact_points: Vec<String>,
    pub tls: TlsConnectorConfig,
}

impl AuthorizeScramOverMtlsConfig {
    pub fn get_builder(
        &self,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
    ) -> Result<AuthorizeScramOverMtlsBuilder> {
        let mtls_connection_factory = ConnectionFactory::new(
            Some(TlsConnector::new(self.tls.clone())?),
            connect_timeout,
            read_timeout,
            Arc::new(Notify::new()),
        );
        Ok(AuthorizeScramOverMtlsBuilder {
            token_task: TokenTask::new(
                mtls_connection_factory,
                self.mtls_port_contact_points.clone(),
            ),
        })
    }
}

pub struct AuthorizeScramOverMtlsBuilder {
    pub token_task: TokenTask,
}

impl AuthorizeScramOverMtlsBuilder {
    pub fn build(&self) -> AuthorizeScramOverMtls {
        AuthorizeScramOverMtls {
            original_scram_state: OriginalScramState::WaitingOnServerFirst,
            token_task: self.token_task.clone(),
            delegation_token: DelegationToken {
                token_id: String::new(),
                hmac: vec![],
            },
        }
    }
}

pub struct AuthorizeScramOverMtls {
    /// Tracks the state of the original scram connections responses created from the clients actual requests
    pub original_scram_state: OriginalScramState,
    /// Shared task that fetches and caches delegation tokens
    pub token_task: TokenTask,
    /// The delegation token generated from the username used in the original scram auth
    pub delegation_token: DelegationToken,
}

pub enum OriginalScramState {
    WaitingOnServerFirst,
    WaitingOnServerFinal,
    AuthFailed,
    AuthSuccess,
}

pub async fn create_delegation_token_for_user(
    connection: &mut SinkConnection,
    username: String,
) -> Result<DelegationToken> {
    connection.send(vec![Message::from_frame(Frame::Kafka(
        KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::CreateDelegationTokenKey as i16)
                .request_api_version(3)
                .build()
                .unwrap(),
            body: RequestBody::CreateDelegationToken(
                CreateDelegationTokenRequest::builder()
                    .owner_principal_type(Some(StrBytes::from_static_str("User")))
                    .owner_principal_name(Some(StrBytes::from_string(username)))
                    .build()
                    .unwrap(),
            ),
        },
    ))])?;
    let mut response = connection.recv().await?.pop().unwrap();
    if let Some(Frame::Kafka(KafkaFrame::Response {
        body: ResponseBody::CreateDelegationToken(response),
        ..
    })) = response.frame()
    {
        Ok(DelegationToken {
            token_id: response.token_id.as_str().to_owned(),
            hmac: response.hmac.to_vec(),
        })
    } else {
        Err(anyhow!(
            "Unexpected response to CreateDelegationToken {response:?}"
        ))
    }
}

#[derive(Clone)]
pub struct DelegationToken {
    pub token_id: String,
    // TODO: store as base64 string
    pub hmac: Vec<u8>,
}
