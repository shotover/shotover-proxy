use std::time::Duration;

use anyhow::{anyhow, Context};
use base64::engine::general_purpose;
use base64::Engine;
use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, ExpireDelegationTokenRequest, RequestHeader};
use kafka_protocol::protocol::{Builder, StrBytes};
use kafka_protocol::ResponseError;
use rand::prelude::SmallRng;

use crate::frame::kafka::{KafkaFrame, RequestBody, ResponseBody};
use crate::frame::Frame;
use crate::message::Message;
use crate::transforms::kafka::sink_cluster::scram_over_mtls::create_token::get_node_connection;
use crate::transforms::kafka::sink_cluster::scram_over_mtls::Node;

pub(crate) async fn expire_delegation_token_with_timeout(
    nodes: &mut [Node],
    rng: &mut SmallRng,
    username: &str,
    hmac: StrBytes,
) -> anyhow::Result<()> {
    let username = StrBytes::from_string(username.to_owned());
    // We apply a 120s timeout to token expiration with the same reason as create_token_with_timeout
    tokio::time::timeout(
        Duration::from_secs(120),
        expire_delegation_token(nodes, hmac.clone(), rng),
    )
    .await
    .with_context(|| format!("Delegation token expiration for {username:?} timedout"))?
}

/// Expire the provided delegation token.
pub(crate) async fn expire_delegation_token(
    nodes: &mut [Node],
    hmac: StrBytes,
    rng: &mut SmallRng,
) -> anyhow::Result<()> {
    let connection = get_node_connection(nodes, rng).await?;
    let hmac_raw_bytes = Bytes::from(general_purpose::STANDARD.decode(hmac.to_string()).unwrap());

    connection.send(vec![Message::from_frame(Frame::Kafka(
        KafkaFrame::Request {
            header: RequestHeader::builder()
                .request_api_key(ApiKey::ExpireDelegationTokenKey as i16)
                .request_api_version(2)
                .build()
                .unwrap(),
            body: RequestBody::ExpireDelegationToken(
                ExpireDelegationTokenRequest::builder()
                    .hmac(hmac_raw_bytes)
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
                Ok(())
            }
        }
        response => Err(anyhow!(
            "Unexpected response to ExpireDelegationToken {response:?}"
        )),
    }
}
