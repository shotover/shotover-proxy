use super::kafka_node::{ConnectionFactory, KafkaAddress};
use crate::{
    connection::SinkConnection,
    tls::{TlsConnector, TlsConnectorConfig},
};
use anyhow::{Context, Result};
use futures::stream::FuturesUnordered;
use kafka_protocol::protocol::StrBytes;
use metrics::{histogram, Histogram};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;

pub(crate) mod connection;
mod create_token;
mod recreate_token_queue;

pub struct TokenRequest {
    username: String,
    response_tx: oneshot::Sender<DelegationToken>,
}

/// A background tokio task for managing kafka delegation tokens.
#[derive(Clone)]
pub struct TokenTask {
    tx: mpsc::Sender<TokenRequest>,
}

impl TokenTask {
    pub fn new(
        mtls_connection_factory: ConnectionFactory,
        mtls_port_contact_points: Vec<KafkaAddress>,
        delegation_token_lifetime: Duration,
    ) -> TokenTask {
        let token_creation_time_metric =
            histogram!("shotover_kafka_delegation_token_creation_seconds");
        let (tx, mut rx) = mpsc::channel::<TokenRequest>(1000);
        tokio::spawn(async move {
            loop {
                match task(
                    &mut rx,
                    &mtls_connection_factory,
                    &mtls_port_contact_points,
                    delegation_token_lifetime,
                    &token_creation_time_metric,
                )
                .await
                {
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

    /// Informs the token task that we will need this token soon so it should start creating it if needed.
    pub async fn prefetch_token_for_user(&self, username: String) -> Result<()> {
        let (response_tx, _response_rx) = oneshot::channel();
        self.tx
            .send(TokenRequest {
                username,
                response_tx,
            })
            .await
            .context("Failed to request delegation token from token task")
    }

    /// Request a token from the task.
    /// If the task has a token for the user cached it will return it quickly.
    /// If the task does not have a token for the user cached it will:
    /// * request a new token from kafka this can take > 500ms
    /// * cache the token for future use
    /// * return the token
    pub async fn get_token_for_user(&self, username: String) -> Result<DelegationToken> {
        let (response_tx, response_rx) = oneshot::channel();
        self.tx
            .send(TokenRequest {
                username,
                response_tx,
            })
            .await
            .context("Failed to request delegation token from token task")?;
        response_rx
            .await
            .context("Token task encountered an error before it could respond to request for token")
    }
}

async fn task(
    rx: &mut mpsc::Receiver<TokenRequest>,
    mtls_connection_factory: &ConnectionFactory,
    mtls_addresses: &[KafkaAddress],
    delegation_token_lifetime: Duration,
    token_creation_time_metric: &Histogram,
) -> Result<()> {
    let mut rng = SmallRng::from_rng(rand::thread_rng())?;
    let mut username_to_token = HashMap::new();
    let mut recreate_queue =
        recreate_token_queue::RecreateTokenQueue::new(delegation_token_lifetime);
    let mut nodes = vec![];

    loop {
        tokio::select! {
            biased;
            username = recreate_queue.next() => {
                let instant = Instant::now();
                let token = create_token::create_token_with_timeout(
                    &mut nodes,
                    &mut rng,
                    mtls_connection_factory,
                    &username,
                    delegation_token_lifetime
                ).await
                .with_context(|| format!("Failed to recreate delegation token for {username:?}"))?;
                username_to_token.insert(username.clone(), token);
                recreate_queue.push(username.clone());

                let passed = instant.elapsed();
                tracing::info!("Delegation token for {username:?} recreated in {passed:?}");
                token_creation_time_metric.record(passed);

                // TODO: We would expire the old token here if it were possible, but it is not since kafka will not allow users (even super users) to expire a token belonging to another user.
                // See details in https://github.com/shotover/shotover-proxy/pull/1685
                // However, at this point, the token will automatically expire itself in delegation_token_lifetime / 2, so it is not a huge concern.
            }
            result = rx.recv() => {
                if let Some(request) = result {
                    let instant = Instant::now();

                    // initialize nodes if uninitialized
                    if nodes.is_empty() {
                        let mut futures = FuturesUnordered::new();
                        for address in mtls_addresses {
                            futures.push(async move {
                                let connection = match mtls_connection_factory
                                    // Must be unauthed since mTLS is its own auth.
                                    .create_connection_unauthed(address)
                                    .await
                                {
                                    Ok(connection) => Some(connection),
                                    Err(err) => {
                                        tracing::error!("Token Task: Failed to create connection for {address:?} during nodes list init {err}");
                                        None
                                    }
                                };
                                Node {
                                    connection,
                                    address: address.clone(),
                                }
                            });
                        }
                        while let Some(node) = futures.next().await {
                            nodes.push(node);
                        }
                    }

                    let token = if let Some(token) = username_to_token.get(&request.username).cloned() {
                        token
                    } else {
                        let token = create_token::create_token_with_timeout(
                            &mut nodes,
                            &mut rng,
                            mtls_connection_factory,
                            &request.username,
                            delegation_token_lifetime,
                        ).await
                        .with_context(|| format!("Failed to create delegation token for {:?}", request.username))?;

                        username_to_token.insert(request.username.clone(), token.clone());
                        recreate_queue.push(request.username.clone());

                        let passed = instant.elapsed();
                        tracing::info!("Delegation token for {:?} created in {passed:?}", request.username);
                        token_creation_time_metric.record(passed);

                        token
                    };
                    request.response_tx.send(token).ok();
                }
                else {
                    // rx returned None which indicates shotover is shutting down
                    return Ok(())
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AuthorizeScramOverMtlsConfig {
    pub mtls_port_contact_points: Vec<String>,
    pub tls: TlsConnectorConfig,
    pub delegation_token_lifetime_seconds: u64,
}

impl AuthorizeScramOverMtlsConfig {
    pub fn get_builder(
        &self,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
    ) -> Result<AuthorizeScramOverMtlsBuilder> {
        let mtls_connection_factory = ConnectionFactory::new(
            Some(TlsConnector::new(&self.tls)?),
            connect_timeout,
            read_timeout,
            Arc::new(Notify::new()),
        );
        let contact_points: Result<Vec<_>> = self
            .mtls_port_contact_points
            .iter()
            .map(|x| KafkaAddress::from_str(x))
            .collect();
        let delegation_token_lifetime = Duration::from_secs(self.delegation_token_lifetime_seconds);
        Ok(AuthorizeScramOverMtlsBuilder {
            token_task: TokenTask::new(
                mtls_connection_factory,
                contact_points?,
                delegation_token_lifetime,
            ),
            delegation_token_lifetime,
        })
    }
}

pub struct AuthorizeScramOverMtlsBuilder {
    pub token_task: TokenTask,
    pub delegation_token_lifetime: Duration,
}

impl AuthorizeScramOverMtlsBuilder {
    pub fn build(&self) -> AuthorizeScramOverMtls {
        AuthorizeScramOverMtls {
            original_scram_state: OriginalScramState::WaitingOnServerFirst,
            token_task: self.token_task.clone(),
            username: String::new(),
            delegation_token_lifetime: self.delegation_token_lifetime,
        }
    }
}

pub struct AuthorizeScramOverMtls {
    /// Tracks the state of the original scram connections responses created from the clients actual requests
    pub original_scram_state: OriginalScramState,
    /// Shared task that fetches delegation tokens
    token_task: TokenTask,
    /// The username used in the original scram auth to generate the delegation token
    username: String,
    pub delegation_token_lifetime: Duration,
}

impl AuthorizeScramOverMtls {
    pub async fn set_username(&mut self, username: String) -> Result<()> {
        self.token_task
            .prefetch_token_for_user(username.clone())
            .await?;
        self.username = username;
        Ok(())
    }

    pub async fn get_token_for_user(&self) -> Result<DelegationToken> {
        if !matches!(self.original_scram_state, OriginalScramState::AuthSuccess) {
            // This should be enforced by logic that occurs before calling this method.
            // This is a final check to enforce security, if this panic occurs it indicates a bug elsewhere in shotover.
            panic!("Cannot hand out tokens to a connection that has not authenticated yet.")
        }

        self.token_task
            .get_token_for_user(self.username.clone())
            .await
    }
}

pub enum OriginalScramState {
    WaitingOnServerFirst,
    WaitingOnServerFinal,
    AuthFailed,
    AuthSuccess,
}

struct Node {
    address: KafkaAddress,
    connection: Option<SinkConnection>,
}

#[derive(Clone)]
pub struct DelegationToken {
    pub token_id: String,
    pub hmac: StrBytes,
}
