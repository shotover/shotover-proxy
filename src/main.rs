#![warn(rust_2018_idioms)]
#![recursion_limit="256"]

use tokio::stream::{ StreamExt};
use tokio_util::codec::{Framed};

use futures::FutureExt;
use futures::SinkExt;

use std::env;
use std::error::Error;

use rust_practice::cassandra_protocol::{CassandraFrame, MessageType, Direction, RawFrame};
use rust_practice::transforms::chain::{Transform, TransformChain, Wrapper, ChainResponse};
use rust_practice::message::{QueryType, Message, QueryMessage, QueryResponse, Value, RawMessage};
use rust_practice::message::Message::{Query, Response, Bypass};
use rust_practice::cassandra_protocol::RawFrame::CASSANDRA;

use tokio::net::{TcpListener, TcpStream};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use sqlparser::ast::{SetExpr, TableFactor, Value as SQLValue, Expr, Statement, BinaryOperator};
use sqlparser::ast::Statement::{Insert, Update, Delete};
use sqlparser::ast::Expr::{Identifier, BinaryOp};
use std::borrow::{Borrow, BorrowMut};
use chrono::DateTime;
use std::str::FromStr;
use futures::executor::block_on;
use rust_practice::transforms::codec_destination::{CodecDestination, CodecConfiguration};
use tokio::sync::Mutex;
use std::sync::Arc;
use cassandra_proto::frame::{Frame, Opcode};
use cassandra_proto::frame::frame_response::ResponseBody;
use rust_practice::protocols::cassandra_protocol2::CassandraCodec2;
use rust_practice::transforms::noop::NoOp;
use rust_practice::transforms::printer::Printer;
use rust_practice::transforms::query::QueryTypeFilter;
use rust_practice::transforms::forward::Forward;
use rust_practice::transforms::redis_cache::SimpleRedisCache;
use rust_practice::transforms;
use rust_practice::protocols::cassandra_helper::process_cassandra_frame;
use rust_practice::transforms::mpsc::{AsyncMpsc, AsyncMpscTee};
use std::sync::mpsc::Receiver;
use rust_practice::transforms::cassandra_source::CassandraSource;
use tokio::runtime::{Handle, Runtime};


#[tokio::main(core_threads = 4)]
async fn main() -> Result<(), Box<dyn Error>> {
    let listen_addr = env::args()
    .nth(1)
    .unwrap_or_else(|| "127.0.0.1:9043".to_string());

    let server_addr = env::args()
    .nth(2)
    .unwrap_or_else(|| "127.0.0.1:9042".to_string());
    let f = transforms::Transforms::CodecDestination(CodecDestination::new_from_config(server_addr).await);
    let chain = TransformChain::new(vec![f], "test");

    let _ = tokio::spawn(async move {
        let source = CassandraSource::new(chain, listen_addr);
        source.join_handle.await
    }).await?;

    Ok(())
}