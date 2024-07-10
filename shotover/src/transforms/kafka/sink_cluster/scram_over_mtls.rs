use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures::stream::FuturesUnordered;
use kafka_protocol::protocol::StrBytes;
use metrics::{histogram, Histogram};
use rand::rngs::SmallRng;
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;

use crate::{
    connection::SinkConnection,
    tls::{TlsConnector, TlsConnectorConfig},
};

use super::node::{ConnectionFactory, KafkaAddress};

mod create_token;
mod expire_token;
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
    #[allow(clippy::new_without_default)]
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
    let mut username_to_token: HashMap<String, DelegationToken> = HashMap::new();
    let mut recreate_queue =
        recreate_token_queue::RecreateTokenQueue::new(delegation_token_lifetime);
    let mut nodes = vec![];

    loop {
        tokio::select! {
            biased;
            username = recreate_queue.next() => {
                let old_token = username_to_token.get(&username).cloned();
                let instant = Instant::now();
                let new_token = create_token::create_token_with_timeout(
                    &mut nodes,
                    &mut rng,
                    mtls_connection_factory,
                    &username,
                    delegation_token_lifetime
                ).await
                .with_context(|| format!("Failed to recreate delegation token for {:?}", username))?;

                username_to_token.insert(username.clone(), new_token);
                recreate_queue.push(username.clone());

                let passed = instant.elapsed();
                tracing::info!("Delegation token for {username:?} recreated in {passed:?}");
                token_creation_time_metric.record(passed);

                if let Some(old_token) = old_token {
                    expire_token::expire_delegation_token_with_timeout(
                        &mut nodes,
                        &mut rng, &username,
                        old_token.hmac,
                        mtls_connection_factory
                    ).await
                    .with_context(|| format!("Failed to expire old delegation token for {:?}", username))?;
                    tracing::info!("Expired old delegation token for {username:?}");
                }
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
        Ok(AuthorizeScramOverMtlsBuilder {
            token_task: TokenTask::new(
                mtls_connection_factory,
                contact_points?,
                Duration::from_secs(self.delegation_token_lifetime_seconds),
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
            username: String::new(),
        }
    }
}

pub struct AuthorizeScramOverMtls {
    /// Tracks the state of the original scram connections responses created from the clients actual requests
    pub original_scram_state: OriginalScramState,
    /// Shared task that fetches delegation tokens
    pub token_task: TokenTask,
    /// The username used in the original scram auth to generate the delegation token
    pub username: String,
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
