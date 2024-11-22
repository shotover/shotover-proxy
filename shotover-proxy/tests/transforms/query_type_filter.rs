use crate::shotover_process;
use pretty_assertions::assert_eq;
use test_helpers::connection::valkey_connection;

async fn test_pipeline(connection: &mut redis::aio::Connection) {
    // using individual queries tests QueryTypeFilter with a MessageWrapper containing a single message at a time.
    for _ in 0..100 {
        // Because this is a write it should be filtered out and replaced with an error
        let result = redis::cmd("SET")
            .arg("key")
            .arg("myvalue")
            .query_async::<_, ()>(connection)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            "An error was signalled by the server - ResponseError: Message was filtered out by shotover",
            result
        );

        // Because this is a read it should not be filtered out and gets the DebugReturner value of 42
        let result: String = redis::cmd("GET")
            .arg("key")
            .query_async(connection)
            .await
            .unwrap();
        assert_eq!("42", result);
    }

    // using a pipeline tests QueryTypeFilter with a MessageWrapper containing multiple messages at a time.
    for _ in 0..100 {
        // Because there is a set which is a write it should be filtered out and replaced with an error and the entire pipeline will fail as a result
        let result = redis::pipe()
            .cmd("SET")
            .arg("some_key")
            .arg("some_value")
            .arg("some_key")
            .ignore()
            .cmd("GET")
            .arg("some_key")
            .ignore()
            .cmd("GET")
            .arg("some_key")
            .query_async::<_, ()>(connection)
            .await
            .unwrap_err()
            .to_string();

        assert_eq!(
            "An error was signalled by the server - ResponseError: Message was filtered out by shotover",
            result
        );

        // Because this is all GETs which are reads, no messages will be filtered out and the entire pipeline will succeed
        let result: Vec<String> = redis::pipe()
            .cmd("GET")
            .arg("some_key")
            .ignore()
            .cmd("GET")
            .arg("some_key")
            .ignore()
            .cmd("GET")
            .arg("some_key")
            .query_async(connection)
            .await
            .unwrap();

        assert_eq!(result, vec!("42".to_string()));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_type_filter() {
    let shotover = shotover_process("tests/test-configs/query_type_filter/simple.yaml")
        .start()
        .await;

    let mut deny_connection = valkey_connection::new_async("127.0.0.1", 6379).await;
    let mut allow_connection = valkey_connection::new_async("127.0.0.1", 6380).await;

    test_pipeline(&mut deny_connection).await;
    test_pipeline(&mut allow_connection).await;

    shotover.shutdown_and_then_consume_events(&[]).await;
}
