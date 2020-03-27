use std::{error, fmt};
use crate::message::Message;
use tokio::time::Instant;
use metrics::{timing, counter};

#[derive(Debug, Clone)]
struct QueryData {
    query: String,
}

//TODO change this to be generic to messages type
#[derive(Debug, Clone)]
pub struct Wrapper {
    pub message: Message,
    next_transform: usize
}

impl Wrapper {
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


#[derive(Debug)]
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
pub type InnerChain<'a> = Vec<&'a dyn Transform>;

//Option 2

pub trait Transform: Send + Sync  {
    fn transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse;

    fn get_name(&self) -> &'static str;

    fn instrument_transform(&self, qd: &mut Wrapper, t: &TransformChain) -> ChainResponse {
        let start = Instant::now();
        let result = self.transform(qd, t);
        let end = Instant::now();
        timing!("", start, end, "transform" => self.get_name(), "" => "");
        return result;
    }

    fn call_next_transform(&self, qd: &mut Wrapper, transforms: &TransformChain) -> ChainResponse {
        let next = qd.next_transform;
        qd.next_transform += 1;
        return match transforms.chain.get(next) {
            Some(t) => {
                t.instrument_transform(qd, transforms)
            },
            None => {
                Err(RequestError{})
            }
        }
    }
}

#[derive(Clone)]
pub struct TransformChain<'a> {
    name: &'static str,
    chain: InnerChain<'a>
}

pub type ChainResponse = Result<Message, RequestError>;

impl <'a> TransformChain<'a> {
    pub fn new(transform_list: Vec<&'a dyn Transform>, name: &'static str) -> Self {
        TransformChain {
            name,
            chain: transform_list
        }
    }

    pub fn process_request(&self, wrapper: &mut Wrapper) -> ChainResponse {
        let start = Instant::now();
        let result = match self.chain.get(wrapper.next_transform) {
            Some(t) => {
                wrapper.next_transform += 1;
                t.instrument_transform(wrapper, &self)
            },
            None => ChainResponse::Err(RequestError{})
        };
        let end = Instant::now();
        timing!("", start, end, "chain" => self.name, "" => "");
        return result;


    }
}