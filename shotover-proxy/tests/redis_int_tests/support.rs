use std::io;
use std::thread::sleep;
use std::time::Duration;

use redis::{RedisResult, Value};
use tracing::{info, info_span};

pub struct TestContext {
    pub client: redis::Client,
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

impl TestContext {
    // IDEA: Use typed-builder instead of multiple constructors?

    pub fn new_auth() -> TestContext {
        TestContext::new_internal("redis://default:shotover@127.0.0.1:6379/", true)
    }

    pub fn new() -> TestContext {
        TestContext::new_internal("redis://127.0.0.1:6379/", true)
    }

    pub fn new_without_test() -> Self {
        TestContext::new_internal("redis://127.0.0.1:6379/", false)
    }

    pub fn new_internal(conn_string: &str, test: bool) -> TestContext {
        info!("using connection string: {}", conn_string);

        let client = redis::Client::open(conn_string).unwrap();
        let mut con;

        let attempts = 300;
        let mut current_attempt = 0;

        loop {
            current_attempt += 1;
            let span = info_span!("connection_test", attempt = current_attempt);
            let _span_ctx = span.enter();

            if current_attempt > attempts {
                panic!("Could not connect!")
            }

            let millisecond = Duration::from_millis(100 * current_attempt);

            match client.get_connection() {
                Err(err) => {
                    if err.is_connection_refusal() {
                        info!("{}: {}", err.category(), err);
                        sleep(millisecond);
                    } else {
                        panic!("Could not connect: {}", err);
                    }
                }
                Ok(x) => {
                    con = x;

                    if !test {
                        break;
                    }

                    let result: RedisResult<Option<String>> =
                        redis::cmd("GET").arg("nosdjkghsdjghsdkghj").query(&mut con);
                    match result {
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            info!(
                                "Could not execute dummy query {}, retrying again - retries {}",
                                e, current_attempt
                            );
                            sleep(millisecond);
                        }
                    }
                }
            }
        }

        if test {
            redis::cmd("FLUSHDB").execute(&mut con);
        }

        TestContext { client }
    }

    pub fn connection(&self) -> redis::Connection {
        self.client.get_connection().unwrap()
    }

    pub async fn async_connection(&self) -> RedisResult<redis::aio::Connection> {
        self.client.get_async_connection().await
    }

    #[cfg(feature = "tokio-rt-core")]
    pub fn multiplexed_async_connection(
        &self,
    ) -> impl Future<Output = RedisResult<redis::aio::MultiplexedConnection>> {
        let client = self.client.clone();
        async move { client.get_multiplexed_tokio_connection().await }
    }
}

pub fn encode_value<W>(value: &Value, writer: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    #![allow(clippy::write_with_newline)]
    match *value {
        Value::Nil => write!(writer, "$-1\r\n"),
        Value::Int(val) => write!(writer, ":{}\r\n", val),
        Value::Data(ref val) => {
            write!(writer, "${}\r\n", val.len())?;
            writer.write_all(val)?;
            writer.write_all(b"\r\n")
        }
        Value::Bulk(ref values) => {
            write!(writer, "*{}\r\n", values.len())?;
            for val in values.iter() {
                encode_value(val, writer)?;
            }
            Ok(())
        }
        Value::Okay => write!(writer, "+OK\r\n"),
        Value::Status(ref s) => write!(writer, "+{}\r\n", s),
    }
}
