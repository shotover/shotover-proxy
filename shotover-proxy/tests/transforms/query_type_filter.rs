use crate::helpers::ShotoverManager;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_query_type_filter() {
    let shotover_manager =
        ShotoverManager::from_topology_file("tests/test-topologies/query_type_filter/simple.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;

    for _ in 0..100 {
        // Because this is a write it should be filtered out and replaced with redis null
        let result = redis::cmd("SET")
            .arg("key")
            .arg("myvalue")
            .query_async::<_, Option<i32>>(&mut connection)
            .await
            .unwrap();
        assert_eq!(None, result);

        // Because this is a read it should not be filtered out and gets the DebugReturner value of 42
        let result = redis::cmd("GET")
            .arg("key")
            .query_async::<_, String>(&mut connection)
            .await
            .unwrap();
        assert_eq!("42", result);
    }
}
