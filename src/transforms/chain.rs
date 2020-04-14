use std::{error, fmt};
use crate::message::Message;
use tokio::time::Instant;
use metrics::{timing};
use std::borrow::BorrowMut;
use std::future::Future;
use futures::future::BoxFuture;
use async_trait::async_trait;


#[derive(Debug, Clone)]
struct QueryData {
    query: String,
}

//TODO change this to be generic to messages type
#[derive(Debug)]
pub struct Wrapper {
    pub message: Message,
    next_transform: usize
}

impl Wrapper  {
    pub fn new(m: Message) -> Self {
        Wrapper {
            message: m,
            next_transform: 0
        }
    }
}

#[derive(Debug)]
struct ResponseData {
    response: Message,
}


#[derive(Debug, Clone)]
pub struct RequestError;

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error!!R!!")
        // unimplemented!()
    }
}

impl error::Error for RequestError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

// Option 1

//TODO change Transform to maintain the InnerChain internally so we don't have to expose this
pub type InnerChain<'a, 'c> = Vec<&'a dyn Transform<'a, 'c>>;

//Option 2

#[async_trait]
pub trait Transform<'a, 'c>: Send + Sync  {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain<'a,'c>) -> ChainResponse<'c>;

    fn get_name(&self) -> &'static str;

    async fn instrument_transform(&self, mut qd: Wrapper, t: &TransformChain<'a,'c>) -> ChainResponse<'c> {
        let start = Instant::now();
        let result = self.transform(qd, t).await;
        let end = Instant::now();
        timing!("", start, end, "transform" => self.get_name(), "" => "");
        return result;
    }

    async fn call_next_transform(&self, mut qd: Wrapper, transforms: &TransformChain<'a,'c>) -> ChainResponse<'c> {
        let next = qd.next_transform;
        qd.next_transform += 1;
        return match transforms.chain.get(next) {
            Some(t) => {
                t.instrument_transform(qd, transforms).await
            },
            None => {
                Err(RequestError{})
            }
        }
    }
}

#[derive(Clone)]
pub struct TransformChain<'a, 'c> {
    name: &'static str,
    chain: InnerChain<'a, 'c>
}

pub type ChainResponse<'a> = Result<Message, RequestError>;

impl <'a, 'c> TransformChain<'a, 'c> {
    pub fn new(transform_list: Vec<&'a dyn Transform<'a, 'c>>, name: &'static str) -> Self {
        TransformChain {
            name,
            chain: transform_list
        }
    }

    pub async fn process_request(&self, mut wrapper: Wrapper) -> ChainResponse<'c> {
        let start = Instant::now();
        let result = match self.chain.get(wrapper.next_transform) {
            Some(t) => {
                wrapper.next_transform += 1;
                t.instrument_transform(wrapper, &self).await
            },
            None => ChainResponse::Err(RequestError{})
        };
        let end = Instant::now();
        timing!("", start, end, "chain" => self.name, "" => "");
        return result;
    }
}