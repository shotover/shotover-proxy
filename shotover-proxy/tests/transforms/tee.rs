use crate::shotover_process;
use hyper::{body, Body, Client, Method, Request, Response};
use test_helpers::connection::redis_connection;
use test_helpers::docker_compose::docker_compose;
use test_helpers::shotover_process::{EventMatcher, Level};

#[tokio::test(flavor = "multi_thread")]
async fn test_ignore_matches() {
    let shotover = shotover_process("tests/test-configs/tee/ignore.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ignore_with_mismatch() {
    let shotover = shotover_process("tests/test-configs/tee/ignore_with_mismatch.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_log_matches() {
    let shotover = shotover_process("tests/test-configs/tee/log.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_log_with_mismatch() {
    let shotover = shotover_process("tests/test-configs/tee/log_with_mismatch.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Warn)
            .with_target("shotover::transforms::tee")
            .with_message(
                r#"Tee mismatch: 
chain response: ["Redis BulkString(b\"42\"))"] 
tee response: ["Redis BulkString(b\"41\"))"]"#,
            )])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fail_matches() {
    let shotover = shotover_process("tests/test-configs/tee/fail.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("42", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fail_with_mismatch() {
    let shotover = shotover_process("tests/test-configs/tee/fail_with_mismatch.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let err = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap_err()
        .to_string();

    let expected = "An error was signalled by the server - ResponseError: ERR The responses from the Tee subchain and down-chain did not match and behavior is set to fail on mismatch";
    assert_eq!(expected, err);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subchain_matches() {
    let _compose = docker_compose("tests/test-configs/redis/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/tee/subchain.yaml")
        .start()
        .await;

    let mut shotover_connection = redis_connection::new_async("127.0.0.1", 6379).await;
    let mut mismatch_chain_redis = redis_connection::new_async("127.0.0.1", 1111).await;
    redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut mismatch_chain_redis)
        .await
        .unwrap();

    let mut result = redis::cmd("SET")
        .arg("key")
        .arg("notmyvalue")
        .query_async::<_, String>(&mut shotover_connection)
        .await
        .unwrap();

    assert_eq!(result, "42");

    result = redis::cmd("GET")
        .arg("key")
        .query_async::<_, String>(&mut mismatch_chain_redis)
        .await
        .unwrap();

    assert_eq!(result, "myvalue");
    shotover.shutdown_and_then_consume_events(&[]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subchain_with_mismatch() {
    let _compose = docker_compose("tests/test-configs/redis/passthrough/docker-compose.yaml");
    let shotover = shotover_process("tests/test-configs/tee/subchain_with_mismatch.yaml")
        .start()
        .await;

    let mut shotover_connection = redis_connection::new_async("127.0.0.1", 6379).await;
    let mut mismatch_chain_redis = redis_connection::new_async("127.0.0.1", 1111).await;

    // Set the value on the top level chain redis
    let mut result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut shotover_connection)
        .await
        .unwrap();

    assert_eq!(result, "42");

    // When the mismatch occurs, the value should be sent to the mismatch chain's redis
    result = redis::cmd("GET")
        .arg("key")
        .query_async::<_, String>(&mut mismatch_chain_redis)
        .await
        .unwrap();

    assert_eq!("myvalue", result);
    shotover.shutdown_and_then_consume_events(&[]).await;
}

async fn read_response_body(res: Response<Body>) -> Result<String, hyper::Error> {
    let bytes = body::to_bytes(res.into_body()).await?;
    Ok(String::from_utf8(bytes.to_vec()).expect("response was not valid utf-8"))
}

async fn hyper_request(uri: String, method: Method, body: Body) -> Response<Body> {
    let client = Client::new();

    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(body)
        .expect("request builder");

    client.request(req).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_switch_subchain() {
    let shotover = shotover_process("tests/test-configs/tee/switch_chain.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6379).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("a", result);

    let res = hyper_request(
        "http://localhost:8061/subchain/current".to_string(),
        Method::GET,
        Body::empty(),
    )
    .await;
    let body = read_response_body(res).await.unwrap();
    assert_eq!("chain_a", body);

    let _ = hyper_request(
        "http://localhost:8061/subchain/switch".to_string(),
        Method::PUT,
        Body::from("chain_b"),
    )
    .await;

    let res = hyper_request(
        "http://localhost:8061/subchain/current".to_string(),
        Method::GET,
        Body::empty(),
    )
    .await;
    let body = read_response_body(res).await.unwrap();
    assert_eq!("chain_b", body);

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("a", result);

    let event_matcher = &[EventMatcher::new()
        .with_level(Level::Warn)
        .with_target("shotover::transforms::tee")
        .with_message(
            r#"Tee mismatch: 
chain response: ["Redis BulkString(b\"a\"))"] 
tee response: ["Redis BulkString(b\"b\"))"]"#,
        )];

    shotover
        .shutdown_and_then_consume_events(event_matcher)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_switch_main_chain() {
    let shotover = shotover_process("tests/test-configs/tee/switch_chain.yaml")
        .start()
        .await;

    let mut connection = redis_connection::new_async("127.0.0.1", 6378).await;

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("a", result);

    let _ = hyper_request(
        "http://localhost:1234/switch".to_string(),
        Method::PUT,
        Body::from("true"),
    )
    .await;

    let res = hyper_request(
        "http://localhost:1234/switched".to_string(),
        Method::GET,
        Body::empty(),
    )
    .await;
    let body = read_response_body(res).await.unwrap();
    assert_eq!("true", body);

    let result = redis::cmd("SET")
        .arg("key")
        .arg("myvalue")
        .query_async::<_, String>(&mut connection)
        .await
        .unwrap();

    assert_eq!("b", result);

    shotover
        .shutdown_and_then_consume_events(&[EventMatcher::new()
            .with_level(Level::Warn)
            .with_count(tokio_bin_process::event_matcher::Count::Times(2))])
        .await;
}
