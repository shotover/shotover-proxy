use crate::{frame::Frame, message::Message, transforms::util::Response};
use kafka_protocol::messages::ProduceRequest;
use tokio::sync::oneshot;

/// when a produce request has acks set to 0, the kafka instance will return no response.
/// In order to maintain shotover transform invariants we need to return a dummy response instead.
pub fn produce_channel(
    produce: &ProduceRequest,
) -> (
    Option<oneshot::Sender<Response>>,
    oneshot::Receiver<Response>,
) {
    let (tx, rx) = oneshot::channel();
    let return_chan = if produce.acks == 0 {
        tx.send(Response {
            response: Ok(Message::from_frame(Frame::Dummy)),
        })
        .unwrap();
        None
    } else {
        Some(tx)
    };
    (return_chan, rx)
}
