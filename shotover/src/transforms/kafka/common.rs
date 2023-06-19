use crate::{
    frame::{
        kafka::{KafkaFrame, RequestBody},
        Frame,
    },
    message::Message,
    transforms::util::{cluster_connection_pool::Connection, Request, Response},
};
use anyhow::{anyhow, Result};
use tokio::sync::oneshot;

pub fn send_requests(
    messages: Vec<Message>,
    outbound: &mut Connection,
) -> Result<Vec<oneshot::Receiver<Response>>> {
    messages
        .into_iter()
        .map(|mut message| {
            // when a produce request has acks set to 0, the kafka instance will return no response.
            // In order to maintain shotover transform invariants we need to return a dummy response instead.
            let acks0 = if let Some(Frame::Kafka(KafkaFrame::Request {
                body: RequestBody::Produce(produce),
                ..
            })) = message.frame()
            {
                produce.acks == 0
            } else {
                false
            };

            let (tx, rx) = oneshot::channel();
            let return_chan = if acks0 {
                tx.send(Response {
                    original: Message::from_frame(Frame::Dummy),
                    response: Ok(Message::from_frame(Frame::Dummy)),
                })
                .unwrap();
                None
            } else {
                Some(tx)
            };
            outbound
                .send(Request {
                    message,
                    return_chan,
                })
                .map(|_| rx)
                .map_err(|_| anyhow!("Failed to send"))
        })
        .collect()
}
