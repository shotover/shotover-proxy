use std::collections::HashSet;

use crate::{
    connection::{ConnectionError, SinkConnection},
    frame::{CassandraFrame, Frame},
    message::Message,
};
use anyhow::Result;
use cassandra_protocol::frame::Version;
use fnv::FnvBuildHasher;

/// Wraps SinkConnection to:
/// * convert connection errors into cassandra error messages
/// * provide recv_all_pending method to await all pending responses
pub struct CassandraConnection {
    connection: SinkConnection,
    pending_request_stream_ids: HashSet<i16, FnvBuildHasher>,
}

impl CassandraConnection {
    pub fn new(connection: SinkConnection) -> Self {
        CassandraConnection {
            connection,
            pending_request_stream_ids: Default::default(),
        }
    }

    pub fn send(&mut self, requests: Vec<Message>) -> Result<(), ConnectionError> {
        self.pending_request_stream_ids
            .extend(requests.iter().map(|x| x.stream_id().unwrap()));
        self.connection.send(requests)
    }

    /// receive 0 or more responses
    pub fn try_recv(&mut self, responses: &mut Vec<Message>, version: Version) -> Result<(), ()> {
        let previous_len = responses.len();
        match self.connection.try_recv_into(responses) {
            Ok(()) => {
                self.process_responses(&responses[previous_len..]);
                Ok(())
            }
            Err(err) => {
                responses.extend(self.pending_into_errors(err, version));
                Err(())
            }
        }
    }

    /// Receive a response for every pending request
    pub async fn recv_all_pending(
        &mut self,
        responses: &mut Vec<Message>,
        version: Version,
    ) -> Result<(), ()> {
        // connection.pending_requests_count() does not neccesarily equal pending_request_stream_ids.len()
        // since the client could reuse stream_ids
        if self.connection.pending_requests_count() == 0 {
            // There are no pending responses to await but we still need to check for any pending events.
            return self.try_recv(responses, version);
        }

        while self.connection.pending_requests_count() > 0 {
            let previous_len = responses.len();
            let recv_result = self.connection.recv_into(responses).await;
            // we need to process responses even if there was an error
            // because it might have received some actual responses before hitting the error.
            self.process_responses(&responses[previous_len..]);
            if let Err(err) = recv_result {
                responses.extend(self.pending_into_errors(err, version));
                return Err(());
            }
        }
        Ok(())
    }

    fn process_responses(&mut self, responses: &[Message]) {
        for response in responses {
            if response.request_id().is_some() {
                let stream_id = response.stream_id().unwrap();
                if !self.pending_request_stream_ids.remove(&stream_id) {
                    tracing::warn!("received response to stream id {stream_id} but that stream id was never sent or was already received");
                }
            }
        }
    }

    fn pending_into_errors(
        &self,
        err: ConnectionError,
        version: Version,
    ) -> impl Iterator<Item = Message> + '_ {
        self.pending_request_stream_ids
            .iter()
            .cloned()
            .map(move |stream_id| {
                Message::from_frame(Frame::Cassandra(CassandraFrame::shotover_error(
                    stream_id,
                    version,
                    &format!("{err}"),
                )))
            })
    }

    pub fn into_sink_connection(self) -> SinkConnection {
        self.connection
    }
}
