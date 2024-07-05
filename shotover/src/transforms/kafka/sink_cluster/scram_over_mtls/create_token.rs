use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose, Engine};
use kafka_protocol::{
    messages::{
        describe_delegation_token_request::DescribeDelegationTokenOwner, ApiKey,
        CreateDelegationTokenRequest, CreateDelegationTokenResponse,
        DescribeDelegationTokenRequest, MetadataRequest, RequestHeader,
    },
    protocol::{Builder, StrBytes},
    ResponseError,
};
use rand::{rngs::SmallRng, seq::IteratorRandom};

use crate::transforms::kafka::sink_cluster::node::{ConnectionFactory, KafkaAddress};
use crate::transforms::kafka::sink_cluster::scram_over_mtls::{DelegationToken, Node};
use crate::{
    connection::SinkConnection,
    frame::{
        kafka::{KafkaFrame, RequestBody, ResponseBody},
        Frame,
    },
    message::Message,
};

pub(crate) async fn create_token_with_timeout(
    nodes: &mut Vec<Node>,
    rng: &mut SmallRng,
    mtls_connection_factory: &ConnectionFactory,
    username: &str,
    token_lifetime: Duration,
) -> Result<DelegationToken> {
    let username = StrBytes::from_string(username.to_owned());
    // We apply a 120s timeout to token creation:
    // * It needs to be low enough to avoid the task getting permanently stuck if the cluster gets in a bad state and never fully propagates the token.
    // * It needs to be high enough to avoid catching cases of slow token propagation.
    //   + From our testing delegation tokens should be propagated within 0.5s to 1s on unloaded kafka clusters of size 15 to 30 nodes.
    tokio::time::timeout(
        Duration::from_secs(120),
        create_delegation_token_for_user_with_wait(
            nodes,
            username.clone(),
            rng,
            mtls_connection_factory,
            token_lifetime,
        ),
    )
    .await
    .with_context(|| format!("Delegation token creation for {username:?} timedout"))?
}

/// populate existing nodes
/// If no nodes have a connection open an error will be returned.
async fn find_new_brokers(nodes: &mut Vec<Node>, rng: &mut SmallRng) -> Result<()> {
    let Some(node) = nodes
        .iter_mut()
        .filter(|node| node.connection.is_some())
        .choose(rng)
    else {
        return Err(anyhow!("No nodes have an open connection"));
    };
    let connection = node
        .connection
        .as_mut()
        .expect("Guaranteed due to above filter");

    let request = Message::from_frame(Frame::Kafka(KafkaFrame::Request {
        header: RequestHeader::builder()
            .request_api_key(ApiKey::MetadataKey as i16)
            .request_api_version(4)
            .correlation_id(0)
            .build()
            .unwrap(),
        body: RequestBody::Metadata(MetadataRequest::builder().build().unwrap()),
    }));
    connection.send(vec![request])?;

    let response = connection.recv().await?.remove(0);
    match response.into_frame() {
        Some(Frame::Kafka(KafkaFrame::Response {
            body: ResponseBody::Metadata(metadata),
            ..
        })) => {
            let new_nodes: Vec<Node> = metadata
                .brokers
                .into_values()
                .filter_map(|broker| {
                    let address = KafkaAddress::new(broker.host, broker.port);
                    if nodes.iter().any(|node| node.address == address) {
                        None
                    } else {
                        Some(Node {
                            address,
                            connection: None,
                        })
                    }
                })
                .collect();
            nodes.extend(new_nodes);
            Ok(())
        }
        other => Err(anyhow!(
            "Unexpected message returned to metadata request {other:?}"
        )),
    }
}

/// Create a delegation token for the provided user.
/// If no nodes have a connection open an error will be returned.
async fn create_delegation_token_for_user(
    nodes: &mut [Node],
    username: &StrBytes,
    rng: &mut SmallRng,
    token_lifetime: Duration,
) -> Result<CreateDelegationTokenResponse> {
    let Some(node) = nodes
        .iter_mut()
        .filter(|node| node.connection.is_some())
        .choose(rng)
    else {
        return Err(anyhow!("No nodes have an open connection"));
    };
    let connection = node
        .connection
        .as_mut()
        .expect("Guaranteed due to above filter");

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
                    .max_lifetime_ms(token_lifetime.as_millis() as i64)
                    .owner_principal_name(Some(username.clone()))
                    .build()
                    .unwrap(),
            ),
        },
    ))])?;

    let response = connection.recv().await?.pop().unwrap();
    match response.into_frame() {
        Some(Frame::Kafka(KafkaFrame::Response {
            body: ResponseBody::CreateDelegationToken(response),
            ..
        })) => {
            if let Some(err) = ResponseError::try_from_code(response.error_code) {
                Err(anyhow!(
                    "kafka responded to CreateDelegationToken with error {err}",
                ))
            } else {
                Ok(response)
            }
        }
        response => Err(anyhow!(
            "Unexpected response to CreateDelegationToken {response:?}"
        )),
    }
}

async fn create_delegation_token_for_user_with_wait(
    nodes: &mut Vec<Node>,
    username: StrBytes,
    rng: &mut SmallRng,
    mtls_connection_factory: &ConnectionFactory,
    token_lifetime: Duration,
) -> Result<DelegationToken> {
    let create_response =
        create_delegation_token_for_user(nodes, &username, rng, token_lifetime).await?;
    // we specifically run find_new_brokers:
    // * after token creation since we are waiting for token propagation anyway.
    // * before waiting on brokers because we need to wait on the entire cluster,
    //   so we want our node list to be as up to date as possible.
    find_new_brokers(nodes, rng).await?;
    wait_until_delegation_token_ready_on_all_brokers(
        nodes,
        &create_response,
        username,
        mtls_connection_factory,
    )
    .await?;

    Ok(DelegationToken {
        token_id: create_response.token_id.as_str().to_owned(),
        hmac: StrBytes::from_string(general_purpose::STANDARD.encode(&create_response.hmac)),
    })
}

/// Wait until delegation token is ready on all brokers.
/// Will create connections for all nodes that dont have one yet.
/// If a broker is inaccessible it will count as ready to prevent a node going down from stopping delegation token creation.
async fn wait_until_delegation_token_ready_on_all_brokers(
    nodes: &mut [Node],
    create_response: &CreateDelegationTokenResponse,
    username: StrBytes,
    mtls_connection_factory: &ConnectionFactory,
) -> Result<()> {
    let nodes_len = nodes.len();
    for (i, node) in nodes.iter_mut().enumerate() {
        let address = &node.address;
        if node.connection.is_none() {
            node.connection = match mtls_connection_factory
                // Must be unauthed since mTLS is its own auth.
                .create_connection_unauthed(address)
                .await
            {
                Ok(connection) => Some(connection),
                Err(err) => {
                    tracing::error!("Token Task: Failed to create connection for {address:?} during token wait {err}");
                    None
                }
            };
        }
        if let Some(connection) = &mut node.connection {
            while !is_delegation_token_ready(connection, create_response, username.clone())
                .await
                .with_context(|| {
                    format!("Failed to check delegation token was ready on broker {address:?}. Successful connections {i}/{nodes_len}")
                })?
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tracing::debug!("finished checking token is ready on broker {address:?}");
        }
    }

    Ok(())
}

/// Returns Ok(true) if the token is ready
/// Returns Ok(false) if the token is not ready
/// Returns Err(_) if an error occured with the kafka connection.
async fn is_delegation_token_ready(
    connection: &mut SinkConnection,
    create_response: &CreateDelegationTokenResponse,
    username: StrBytes,
) -> Result<bool> {
    // TODO: Create a single request Message, convert it into raw bytes, and then reuse for all following requests
    //       This will avoid many allocations for each sent request
    //       It is left as a TODO since shotover does not currently support this. But we should support it in the future.
    connection.send(vec![Message::from_frame(Frame::Kafka(
        KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::DescribeDelegationTokenKey as i16)
                .request_api_version(3)
                .build()
                .unwrap(),
            body: RequestBody::DescribeDelegationToken(
                DescribeDelegationTokenRequest::builder()
                    .owners(Some(vec![DescribeDelegationTokenOwner::builder()
                        .principal_type(StrBytes::from_static_str("User"))
                        .principal_name(username)
                        .build()
                        .unwrap()]))
                    .build()
                    .unwrap(),
            ),
        },
    ))])?;
    let mut response = connection.recv().await?.pop().unwrap();
    if let Some(Frame::Kafka(KafkaFrame::Response {
        body: ResponseBody::DescribeDelegationToken(response),
        ..
    })) = response.frame()
    {
        if let Some(err) = ResponseError::try_from_code(response.error_code) {
            return Err(anyhow!(
                "Kafka's response to DescribeDelegationToken was an error: {err}"
            ));
        }
        if response
            .tokens
            .iter()
            .any(|x| x.hmac == create_response.hmac && x.token_id == create_response.token_id)
        {
            Ok(true)
        } else {
            Ok(false)
        }
    } else {
        Err(anyhow!(
            "Unexpected response to CreateDelegationToken {response:?}"
        ))
    }
}
