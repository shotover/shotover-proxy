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
    // Does not neccesarily equal pending_request_stream_ids.len() since the client could reuse stream_ids
    pending_request_count: usize,
}

impl CassandraConnection {
    pub fn new(connection: SinkConnection) -> Self {
        CassandraConnection {
            connection,
            pending_request_stream_ids: Default::default(),
            pending_request_count: 0,
        }
    }

    pub fn send(&mut self, requests: Vec<Message>) -> Result<()> {
        self.pending_request_count += requests.len();
        self.pending_request_stream_ids
            .extend(requests.iter().map(|x| x.stream_id().unwrap()));
        Ok(self.connection.send(requests)?)
    }

    /// receive 0 or more responses
    pub fn try_recv(&mut self, version: Version) -> Result<Vec<Message>, Vec<Message>> {
        match self.connection.try_recv() {
            Ok(results) => {
                self.process_results(&results);
                Ok(results)
            }
            Err(err) => Err(self.pending_into_errors(err, version)),
        }
    }

    /// Receive a response for every pending request
    pub async fn recv_all_pending(&mut self) -> Result<Vec<Message>, ConnectionError> {
        if self.pending_request_count == 0 {
            return Ok(vec![]);
        }

        let mut results = vec![];
        while self.pending_request_count > 0 {
            let new_results = self.connection.recv().await?;
            self.process_results(&new_results);
            results.extend(new_results);
        }
        Ok(results)
    }

    fn process_results(&mut self, responses: &[Message]) {
        for response in responses {
            if response.request_id().is_some() {
                let stream_id = response.stream_id().unwrap();
                if !self.pending_request_stream_ids.remove(&stream_id) {
                    tracing::warn!("received response to stream id {stream_id} but that stream id was never sent or was already received");
                }
                self.pending_request_count -= 1;
            }
        }
    }

    fn pending_into_errors(&self, err: ConnectionError, version: Version) -> Vec<Message> {
        self.pending_request_stream_ids
            .iter()
            .map(|stream_id| {
                Message::from_frame(Frame::Cassandra(CassandraFrame::shotover_error(
                    *stream_id,
                    version,
                    &format!("{err}"),
                )))
            })
            .collect()
    }

    pub fn into_sink_connection(self) -> SinkConnection {
        self.connection
    }
}
