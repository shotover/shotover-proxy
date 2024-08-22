use crate::{
    connection::{ConnectionError, SinkConnection},
    message::Message,
    transforms::kafka::sink_cluster::connections::ConnectionState,
};
use anyhow::{anyhow, Result};
use std::time::{Duration, Instant};

use super::AuthorizeScramOverMtls;

pub struct ScramOverMtlsConnection {
    connection: SinkConnection,
    /// When a connection is recreated to avoid timeouts,
    /// the old connection will be kept around until all responses have been received from it.
    old_connection: Option<SinkConnection>,
    created_at: Instant,
    timeout: Duration,
}

impl ScramOverMtlsConnection {
    pub fn new(
        connection: SinkConnection,
        old_connection: Option<ScramOverMtlsConnection>,
        authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>,
    ) -> Result<Self> {
        let old_connection = old_connection
            .map(|x| x.into_old_connection())
            .transpose()?
            .flatten();
        Ok(ScramOverMtlsConnection {
            connection,
            old_connection,
            created_at: Instant::now(),
            timeout: Self::calculate_timeout(authorize_scram_over_mtls),
        })
    }

    fn into_old_connection(self) -> Result<Option<SinkConnection>> {
        if self.old_connection.is_some() {
            return Err(anyhow!("The connection to be replaced had an old_connection. For this to occur a response needs to have been pending for longer than the timeout period which indicates other problems."));
        }
        if self.connection.pending_requests_count() == 0 {
            Ok(None)
        } else {
            Ok(Some(self.connection))
        }
    }

    /// Attempts to receive messages, if there are no messages available it immediately returns an empty vec.
    /// If there is a problem with the connection an error is returned.
    pub fn try_recv_into(&mut self, responses: &mut Vec<Message>) -> Result<(), ConnectionError> {
        // ensure old connection is completely drained before receiving from new connection
        if let Some(old_connection) = &mut self.old_connection {
            old_connection.try_recv_into(responses)?;
            if old_connection.pending_requests_count() == 0 {
                self.old_connection = None;
                self.connection.try_recv_into(responses)?;
            }
            Ok(())
        } else {
            self.connection.try_recv_into(responses)
        }
    }

    /// Send messages.
    /// If there is a problem with the connection an error is returned.
    pub fn send(&mut self, messages: Vec<Message>) -> Result<(), ConnectionError> {
        self.connection.send(messages)
    }

    /// Receives messages, if there are no messages available it awaits until there are messages.
    /// If there is a problem with the connection an error is returned.
    pub async fn recv(&mut self) -> Result<Vec<Message>, ConnectionError> {
        // ensure old connection is completely drained before receiving from new connection
        if let Some(old_connection) = &mut self.old_connection {
            let mut received = old_connection.recv().await?;
            if old_connection.pending_requests_count() == 0 {
                self.old_connection = None;
                // Do not use `recv` method here since we already have at least one message due to previous `recv`,
                // so we avoid blocking by calling `try_recv_into` instead.
                self.connection.try_recv_into(&mut received)?;
            }
            Ok(received)
        } else {
            self.connection.recv().await
        }
    }

    pub fn pending_requests_count(&self) -> usize {
        self.connection.pending_requests_count()
            + self
                .old_connection
                .as_ref()
                .map(|x| x.pending_requests_count())
                .unwrap_or_default()
    }

    fn calculate_timeout(authorize_scram_over_mtls: &Option<AuthorizeScramOverMtls>) -> Duration {
        // The delegation token is recreated after `0.5 * delegation_token_lifetime`
        // Consider what happens when we match that timing for our connection timeout,
        // in this timeline going from left to right:
        //
        //     create token t1      create token t2
        // |--------------------|--------------------|
        // |                    ^ all connections created after this point use token t2 instead of token t1
        // |                                         |
        // |             token t1 lifetime           |
        // |-----------------------------------------|
        // |                                         ^
        // |                   after this point, connections still alive that were authed with token t1 will be closed by the broker.
        // |                                         |
        // |                                         |
        // |                                         |
        // |                                 token t2 lifetime
        // |                    |-----------------------------------------|
        // |                    ^ all connections created after this point use token t2
        // |                                         |
        // |                                         |
        // |                                         |
        // |      connection lifetime using token t1 |
        // |          |--------------------|         |
        // This case is fine, the connection exists entirely within the lifetime of token t1.
        // |                                         |
        // |                                         |
        // |                                         |
        // |                            connection lifetime using token t2
        // |                                |--------------------|
        // This case is fine, the connection exists entirely within the lifetime of token t2.
        // |                                         |
        // |                                         |
        // |                                         |
        // |              connection lifetime using token t?
        // |                    |--------------------|
        // This case is a race condition.
        // We could start with either token t2 or t1.
        // If we start with t1 we could go past the end of t1's lifetime.
        // To avoid this issue we reduce the size of the connection lifetime by a further 25%
        //
        // At low values of delegation_token_lifetime all of this falls apart since something
        // like a VM migration could delay shotover execution for many seconds.
        // However for sufficently large delegation_token_lifetime values (> 1 hour) this should be fine.
        authorize_scram_over_mtls
            .as_ref()
            .unwrap()
            .delegation_token_lifetime
            .mul_f32(
                // match token recreation time
                0.5 *
                // further reduce connection timeout
                0.75,
            )
    }

    pub fn state(&self, recent_instant: Instant) -> ConnectionState {
        // Since we cant be 100% exact with time anyway, we use a recent instant that can be reused to reduce syscalls.
        if recent_instant.duration_since(self.created_at) > self.timeout {
            ConnectionState::AtRiskOfAuthTokenExpiry
        } else {
            ConnectionState::Open
        }
    }
}
