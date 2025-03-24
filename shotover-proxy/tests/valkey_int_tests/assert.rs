use pretty_assertions::assert_eq;
use redis::Cmd;

pub async fn assert_nil(cmd: &mut Cmd, connection: &mut redis::Connection) {
    assert_eq!(cmd.query::<Option<String>>(connection), Ok(None));
}

pub async fn assert_ok(cmd: &mut Cmd, connection: &mut redis::Connection) {
    assert_eq!(cmd.query::<String>(connection), Ok("OK".to_string()));
}

pub async fn assert_int(cmd: &mut Cmd, connection: &mut redis::Connection, value: i64) {
    assert_eq!(cmd.query::<i64>(connection), Ok(value));
}

pub async fn assert_bytes(cmd: &mut Cmd, connection: &mut redis::Connection, value: &[u8]) {
    assert_eq!(cmd.query::<Vec<u8>>(connection), Ok(value.to_vec()));
}
