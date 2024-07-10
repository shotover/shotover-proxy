use std::time::Duration;

use anyhow::{anyhow, Context};
use kafka_protocol::messages::{
    ApiKey, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse, RequestHeader,
};
use kafka_protocol::protocol::{Builder, StrBytes};
use kafka_protocol::ResponseError;
use rand::prelude::SmallRng;

use crate::connection::SinkConnection;
use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::frame::Frame;
use crate::message::Message;
use crate::transforms::kafka::sink_cluster::node::ConnectionFactory;
use crate::transforms::kafka::sink_cluster::scram_over_mtls::create_token::{
    create_node_connection_if_none, describe_delegation_token_for_user, find_new_brokers,
    get_node_connection,
};
use crate::transforms::kafka::sink_cluster::scram_over_mtls::Node;

pub(crate) async fn expire_delegation_token_with_timeout(
    nodes: &mut Vec<Node>,
    rng: &mut SmallRng,
    username: &str,
    hmac: StrBytes,
    mtls_connection_factory: &ConnectionFactory,
) -> anyhow::Result<()> {
    let username = StrBytes::from_string(username.to_owned());
    // We apply a 120s timeout to token expiration with the same reason as create_token_with_timeout
    tokio::time::timeout(
        Duration::from_secs(120),
        expire_delegation_token_with_wait(
            nodes,
            username.clone(),
            hmac.clone(),
            rng,
            mtls_connection_factory,
        ),
    )
    .await
    .with_context(|| format!("Delegation token expiration for {username:?} timedout"))?
}

async fn expire_delegation_token_with_wait(
    nodes: &mut Vec<Node>,
    username: StrBytes,
    hmac: StrBytes,
    rng: &mut SmallRng,
    mtls_connection_factory: &ConnectionFactory,
) -> anyhow::Result<()> {
    expire_delegation_token(nodes, hmac.clone(), rng).await?;
    // We specifically run find_new_brokers with the same reason as create_delegation_token_for_user_with_wait
    find_new_brokers(nodes, rng).await?;
    wait_until_delegation_token_expired_on_all_brokers(
        nodes,
        username,
        hmac,
        mtls_connection_factory,
    )
    .await?;

    Ok(())
}

/// Expire the provided delegation token.
pub(crate) async fn expire_delegation_token(
    nodes: &mut [Node],
    hmac: StrBytes,
    rng: &mut SmallRng,
) -> anyhow::Result<ExpireDelegationTokenResponse> {
    let connection = get_node_connection(nodes, rng).await?;

    connection.send(vec![Message::from_frame(Frame::Kafka(
        KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::ExpireDelegationTokenKey as i16)
                .request_api_version(3)
                .build()
                .unwrap(),
            body: RequestBody::ExpireDelegationToken(
                ExpireDelegationTokenRequest::builder()
                    .hmac(hmac.into_bytes())
                    .expiry_time_period_ms(-1) // Invalidate the token immediately
                    .build()
                    .unwrap(),
            ),
        },
    ))])?;

    let response = connection.recv().await?.pop().unwrap();
    match response.into_frame() {
        Some(Frame::Kafka(KafkaFrame::Response {
            body: ResponseBody::ExpireDelegationToken(response),
            ..
        })) => {
            if let Some(err) = ResponseError::try_from_code(response.error_code) {
                Err(anyhow!(
                    "Kafka responded to ExpireDelegationToken with error {err}",
                ))
            } else {
                Ok(response)
            }
        }
        response => Err(anyhow!(
            "Unexpected response to ExpireDelegationToken {response:?}"
        )),
    }
}

/// Wait until delegation token is expired on all brokers.
/// Will create connections for all nodes that dont have one yet.
/// If a broker is inaccessible it will count as ready to prevent a node going down from stopping expiring delegation token.
async fn wait_until_delegation_token_expired_on_all_brokers(
    nodes: &mut [Node],
    username: StrBytes,
    hmac: StrBytes,
    mtls_connection_factory: &ConnectionFactory,
) -> anyhow::Result<()> {
    let nodes_len = nodes.len();
    for (i, node) in nodes.iter_mut().enumerate() {
        create_node_connection_if_none(node, mtls_connection_factory).await?;
        if let Some(connection) = &mut node.connection {
            let address = &node.address;
            while !is_delegation_token_expired(connection, username.clone(), hmac.clone())
                .await
                .with_context(|| {
                    format!("Failed to check delegation token was expired on broker {address:?}. Successful connections {i}/{nodes_len}")
                })?
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tracing::debug!("Finished checking token is expired on broker {address:?}");
        }
    }

    Ok(())
}

/// Returns Ok(true) if the token is not present on the broker.
/// Returns Ok(false) if the token is still present on the broker.
/// Returns Err(_) if an error occurred with the kafka connection.
async fn is_delegation_token_expired(
    connection: &mut SinkConnection,
    username: StrBytes,
    hmac: StrBytes,
) -> anyhow::Result<bool> {
    let response = describe_delegation_token_for_user(connection, username).await?;
    if !response
        .tokens
        .iter()
        .any(|x| x.hmac == hmac.clone().into_bytes())
    {
        Ok(true)
    } else {
        Ok(false)
    }
}
