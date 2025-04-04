use pretty_assertions::assert_eq;
use redis::Cmd;
use redis::aio::MultiplexedConnection;

pub async fn assert_nil(cmd: &mut Cmd, connection: &mut MultiplexedConnection) {
    assert_eq!(
        cmd.query_async::<Option<String>>(connection).await,
        Ok(None)
    );
}

pub async fn assert_ok(cmd: &mut Cmd, connection: &mut MultiplexedConnection) {
    assert_eq!(cmd.query_async(connection).await, Ok("OK".to_string()));
}

pub async fn assert_int(cmd: &mut Cmd, connection: &mut MultiplexedConnection, value: i64) {
    assert_eq!(cmd.query_async(connection).await, Ok(value));
}

pub async fn assert_bytes(cmd: &mut Cmd, connection: &mut MultiplexedConnection, value: &[u8]) {
    assert_eq!(cmd.query_async(connection).await, Ok(value.to_vec()));
}
