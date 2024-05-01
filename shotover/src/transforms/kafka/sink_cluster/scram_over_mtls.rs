use super::node::{ConnectionFactory, KafkaAddress};
use crate::{
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
use rand::prelude::SliceRandom;
use rand::rngs::SmallRng;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AuthorizeScramOverMtlsConfig {
    pub mtls_port_contact_points: Vec<String>,
    pub tls: TlsConnectorConfig,
}

impl AuthorizeScramOverMtlsConfig {
    pub fn get_builder(&self) -> Result<AuthorizeScramOverMtlsBuilder> {
        Ok(AuthorizeScramOverMtlsBuilder {
            mtls_port_contact_points: self.mtls_port_contact_points.clone(),
            tls: TlsConnector::new(self.tls.clone())?,
        })
    }
}

pub struct AuthorizeScramOverMtlsBuilder {
    pub mtls_port_contact_points: Vec<String>,
    pub tls: TlsConnector,
}

impl AuthorizeScramOverMtlsBuilder {
    pub fn build(
        &self,
        connect_timeout: Duration,
        read_timeout: Option<Duration>,
    ) -> AuthorizeScramOverMtls {
        let mtls_connection_factory = ConnectionFactory::new(
            Some(self.tls.clone()),
            connect_timeout,
            read_timeout,
            Arc::new(Notify::new()),
        );
        AuthorizeScramOverMtls {
            mtls_port_contact_points: self.mtls_port_contact_points.clone(),
            mtls_connection_factory,
            delegation_token: DelegationToken {
                token_id: String::new(),
                hmac: vec![],
            },
            original_scram_state: OriginalScramState::WaitingOnServerFirst,
        }
    }
}

pub struct AuthorizeScramOverMtls {
    /// The destination to create mTLS connections from
    pub mtls_port_contact_points: Vec<String>,
    /// connection factory to create mTLS connections from
    pub mtls_connection_factory: ConnectionFactory,
    /// Tracks the state of the original scram connections responses created from the clients actual requests
    pub original_scram_state: OriginalScramState,
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
    scram_over_mtls: &AuthorizeScramOverMtls,
    username: String,
    rng: &mut SmallRng,
) -> Result<DelegationToken> {
    let address = KafkaAddress::from_str(
        scram_over_mtls
            .mtls_port_contact_points
            .choose(rng)
            .unwrap(),
    )?;
    let mut connection = scram_over_mtls
        .mtls_connection_factory
        // Must be unauthed since mTLS is its own auth.
        .create_connection_unauthed(&address)
        .await?;

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

pub struct DelegationToken {
    pub token_id: String,
    pub hmac: Vec<u8>,
}
