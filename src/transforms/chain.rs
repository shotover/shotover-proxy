use crate::message::Message;
use crate::transforms::Transforms;
use async_trait::async_trait;
use metrics::timing;
use pyo3::PyErr;
use std::{error, fmt, io};
use tokio::time::Instant;
use futures::io::Error;
use std::fmt::Display;
use serde::export::Formatter;

#[derive(Debug, Clone)]
struct QueryData {
    query: String,
}

//TODO change this to be generic to messages type
#[derive(Debug, Clone)]
pub struct Wrapper {
    pub message: Message,
    next_transform: usize,
    pub modified: bool,
}

impl Display for Wrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_fmt(format_args!("{:#?}", self.message))
    }
}

impl Wrapper {
    pub fn new(m: Message) -> Self {
        Wrapper {
            message: m,
            next_transform: 0,
            modified: false,
        }
    }

    pub fn reset(&mut self) {
        self.next_transform = 0;
    }
}

#[derive(Debug)]
struct ResponseData {
    response: Message,
}

#[derive(Debug, Clone)]
pub struct RequestError;

impl From<io::Error> for RequestError {
    fn from(_: Error) -> Self {
        return RequestError {}
    }
}

impl From<pyo3::PyErr> for RequestError {
    fn from(_: PyErr) -> Self {
        return RequestError {};
    }
}

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

//TODO change Transform to maintain the InnerChain internally so we don't have to expose this
pub type InnerChain = Vec<Transforms>;

#[async_trait]
// pub trait Transform<'a, 'c>: Send+ Sync  {
pub trait Transform: Send + Sync {
    async fn transform(&self, mut qd: Wrapper, t: &TransformChain) -> ChainResponse;

    fn get_name(&self) -> &'static str;

    async fn instrument_transform(&self, qd: Wrapper, t: &TransformChain) -> ChainResponse {
        let start = Instant::now();
        let result = self.transform(qd, t).await;
        let end = Instant::now();
        timing!("", start, end, "transform" => self.get_name(), "" => "");
        return result;
    }

    async fn call_next_transform(
        &self,
        mut qd: Wrapper,
        transforms: &TransformChain,
    ) -> ChainResponse {
        let next = qd.next_transform;
        qd.next_transform += 1;
        return match transforms.chain.get(next) {
            Some(t) => t.instrument_transform(qd, transforms).await,
            None => {
                println!("No more transforms left in the chain");

                Err(RequestError {})
            },
        };
    }
}

// TODO: Remove thread safe requirements for transforms
// Currently the way this chain struct is built, requires that all Transforms are thread safe and
// implement with Sync

#[derive(Clone)]
pub struct TransformChain {
    name: String,
    chain: InnerChain,
}

pub type ChainResponse = Result<Message, RequestError>;

impl TransformChain {
    pub fn new(transform_list: Vec<Transforms>, name: String) -> Self {
        TransformChain {
            name,
            chain: transform_list,
        }
    }

    pub async fn process_request(&self, mut wrapper: Wrapper) -> ChainResponse {
        let start = Instant::now();
        let result = match self.chain.get(wrapper.next_transform) {
            Some(t) => {
                wrapper.next_transform += 1;
                t.instrument_transform(wrapper, &self).await
            }
            None => {
                println!("No more transforms left in the chain");

                Err(RequestError {})
            },
        };
        let end = Instant::now();
        timing!("", start, end, "chain" => self.name.clone(), "" => "");
        return result;
    }
}
