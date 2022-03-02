use crate::helpers::ShotoverManager;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_type_filter() {
    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-configs/query_type_filter/simple.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;

    // using individual queries tests QueryTypeFilter with a MessageWrapper containing a single message at a time.
    for _ in 0..100 {
        // Because this is a write it should be filtered out and replaced with an error
        let result = redis::cmd("SET")
            .arg("key")
            .arg("myvalue")
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            "An error was signalled by the server: Message was filtered out by shotover",
            result
        );

        // Because this is a read it should not be filtered out and gets the DebugReturner value of 42
        let result: String = redis::cmd("GET")
            .arg("key")
            .query_async(&mut connection)
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
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap_err()
            .to_string();

        assert_eq!(
            "An error was signalled by the server: Message was filtered out by shotover",
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
            .query_async(&mut connection)
            .await
            .unwrap();

        assert_eq!(result, vec!("42".to_string()));
    }
}
