use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use rand::{thread_rng, Rng};
use rand_distr::Alphanumeric;
use redis::aio::Connection;
use redis::cluster::ClusterConnection;
use redis::{AsyncCommands, Commands, ErrorKind, RedisError, Value};
use serial_test::serial;
use std::thread::sleep;
use std::time::Duration;
use tracing::trace;

use crate::helpers::ShotoverManager;
use shotover_proxy::tls::TlsConfig;
use test_helpers::docker_compose::DockerCompose;

// Debug rust is pretty slow, so keep the stress test small.
// CI runs in both debug and release so the larger iteration count does get run there.
#[cfg(debug_assertions)]
const STRESS_TEST_MULTIPLIER: usize = 1;

#[cfg(not(debug_assertions))]
const STRESS_TEST_MULTIPLIER: usize = 100;

async fn test_args(connection: &mut Connection) {
    redis::cmd("SET")
        .arg("key1")
        .arg(b"foo")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&["key2", "bar"])
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["key1", "key2"])
            .query_async(connection)
            .await,
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

async fn test_getset(connection: &mut Connection) {
    for _ in 0..100 * STRESS_TEST_MULTIPLIER {
        redis::cmd("SET")
            .arg("foo")
            .arg(42)
            .query_async::<_, ()>(connection)
            .await
            .unwrap();
        assert_eq!(
            redis::cmd("GET").arg("foo").query_async(connection).await,
            Ok(42)
        );

        redis::cmd("SET")
            .arg("bar")
            .arg("foo")
            .query_async::<_, ()>(connection)
            .await
            .unwrap();
        assert_eq!(
            redis::cmd("GET").arg("bar").query_async(connection).await,
            Ok(b"foo".to_vec())
        );
    }

    // ensure every possible byte is passed through unmodified
    let every_byte: Vec<u8> = (0..=255).collect();
    redis::cmd("SET")
        .arg("bar")
        .arg(&every_byte)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("GET").arg("bar").query_async(connection).await,
        Ok(every_byte)
    );

    // ensure non-uppercase commands are handled properly
    redis::cmd("SeT")
        .arg("bar")
        .arg("foo")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("get").arg("bar").query_async(connection).await,
        Ok(b"foo".to_vec())
    );
}

async fn test_incr(connection: &mut Connection) {
    redis::cmd("SET")
        .arg("foo")
        .arg(42)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("INCR").arg("foo").query_async(connection).await,
        Ok(43usize)
    );
}

async fn test_info(connection: &mut Connection) {
    let info: redis::InfoDict = redis::cmd("INFO").query_async(connection).await.unwrap();
    assert_eq!(
        info.find(&"role"),
        Some(&Value::Status("master".to_string()))
    );
    assert_eq!(info.get("role"), Some("master".to_string()));
    assert_eq!(info.get("loading"), Some(false));
    assert!(!info.is_empty());
    assert!(info.contains_key(&"role"));
}

async fn test_hash_ops(connection: &mut Connection) {
    redis::cmd("FLUSHDB")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg("foo")
        .arg("key_1")
        .arg(1)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg("foo")
        .arg("key_2")
        .arg(2)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    let h: HashMap<String, i32> = redis::cmd("HGETALL")
        .arg("foo")
        .query_async(connection)
        .await
        .unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));

    let h: BTreeMap<String, i32> = redis::cmd("HGETALL")
        .arg("foo")
        .query_async(connection)
        .await
        .unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
}

async fn test_set_ops(connection: &mut Connection) {
    redis::cmd("FLUSHDB")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("SADD")
        .arg("foo")
        .arg(1)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("SADD")
        .arg("foo")
        .arg(2)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("SADD")
        .arg("foo")
        .arg(3)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    let mut s: Vec<i32> = redis::cmd("SMEMBERS")
        .arg("foo")
        .query_async(connection)
        .await
        .unwrap();
    s.sort_unstable();
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);

    let set: HashSet<i32> = redis::cmd("SMEMBERS")
        .arg("foo")
        .query_async(connection)
        .await
        .unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));

    let set: BTreeSet<i32> = redis::cmd("SMEMBERS")
        .arg("foo")
        .query_async(connection)
        .await
        .unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));
}

async fn test_scan(connection: &mut Connection) {
    redis::cmd("SADD")
        .arg("foo")
        .arg(1)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("SADD")
        .arg("foo")
        .arg(2)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("SADD")
        .arg("foo")
        .arg(3)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    let (cur, mut s): (i32, Vec<i32>) = redis::cmd("SSCAN")
        .arg("foo")
        .arg(0)
        .query_async(connection)
        .await
        .unwrap();
    s.sort_unstable();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

async fn test_optionals(connection: &mut Connection) {
    redis::cmd("SET")
        .arg("foo")
        .arg(1)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg("foo")
        .arg("missing")
        .query_async(connection)
        .await
        .unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);

    let a = redis::cmd("GET")
        .arg("missing")
        .query_async(connection)
        .await
        .unwrap_or(0i32);
    assert_eq!(a, 0i32);
}

async fn test_scanning(connection: &mut Connection) {
    redis::cmd("FLUSHDB")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    let mut unseen = HashSet::<usize>::new();

    for x in 0..100 * STRESS_TEST_MULTIPLIER {
        let _a: i64 = redis::cmd("SADD")
            .arg("foo")
            .arg(x)
            .query_async(connection)
            .await
            .unwrap();
        unseen.insert(x);
    }

    assert_eq!(unseen.len(), 100 * STRESS_TEST_MULTIPLIER);

    let mut iter = redis::cmd("SSCAN")
        .arg("foo")
        .cursor_arg(0)
        .clone()
        .iter_async(connection)
        .await
        .unwrap();

    while let Some(x) = iter.next_item().await {
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

async fn test_filtered_scanning(connection: &mut Connection) {
    redis::cmd("FLUSHDB")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    let mut unseen = HashSet::<usize>::new();

    for x in 0..3000 {
        let _: () = connection
            .hset("foo", format!("key_{}_{}", x % 100, x), x)
            .await
            .unwrap();
        if x % 100 == 0 {
            unseen.insert(x);
        }
    }

    let mut iter = connection.hscan_match("foo", "key_0_*").await.unwrap();

    while let Some(x) = iter.next_item().await {
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

async fn test_pipeline_error(connection: &mut Connection) {
    let ((_k1, _k2),): ((i32, i32),) = redis::pipe()
        .cmd("SET")
        .arg("k{x}ey_1")
        .arg(42)
        .ignore()
        .cmd("SET")
        .arg("k{x}ey_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["k{x}ey_1", "k{x}ey_2"])
        .query_async(connection)
        .await
        .unwrap();

    assert_eq!(
        redis::pipe()
            .cmd("SET")
            .arg("k{x}ey_1")
            .arg(42)
            .cmd("SESDFSDFSDFT")
            .arg("k{x}ey_2")
            .arg(43)
            .cmd("GET")
            .arg("k{x}ey_1")
            .query_async(connection)
            .await,
        Err::<Value, RedisError>(RedisError::from((
            ErrorKind::ResponseError,
            "An error was signalled by the server",
            "unknown command `SESDFSDFSDFT`, with args beginning with: `k{x}ey_2`, `43`, "
                .to_string()
        )))
    );
}

async fn test_pipeline(connection: &mut Connection) {
    let ((k1, k2),): ((i32, i32),) = redis::pipe()
        .cmd("SET")
        .arg("k{x}ey_1")
        .arg(42)
        .ignore()
        .cmd("SET")
        .arg("k{x}ey_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["k{x}ey_1", "k{x}ey_2"])
        .query_async(connection)
        .await
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

async fn test_empty_pipeline(connection: &mut Connection) {
    let _: () = redis::pipe()
        .cmd("PING")
        .ignore()
        .query_async(connection)
        .await
        .unwrap();
    let _: () = redis::pipe().query_async(connection).await.unwrap();
}

async fn test_pipeline_transaction(connection: &mut Connection) {
    let ((k1, k2),): ((i32, i32),) = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg("k{x}ey_1")
        .arg(42)
        .ignore()
        .cmd("SET")
        .arg("k{x}ey_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["k{x}ey_1", "k{x}ey_2"])
        .query_async(connection)
        .await
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

async fn test_pipeline_reuse_query(connection: &mut Connection) {
    let mut pl = redis::pipe();

    let ((k1,),): ((i32,),) = pl
        .cmd("SET")
        .arg("p{x}key_1")
        .arg(42)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .query_async(connection)
        .await
        .unwrap();

    assert_eq!(k1, 42);

    redis::cmd("DEL")
        .arg("p{x}key_1")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    // The internal commands vector of the pipeline still contains the previous commands.
    let ((k1,), (k2, k3)): ((i32,), (i32, i32)) = pl
        .cmd("SET")
        .arg("p{x}key_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .arg(&["p{x}key_2"])
        .query_async(connection)
        .await
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 42);
    assert_eq!(k3, 43);
}

async fn test_pipeline_reuse_query_clear(connection: &mut Connection) {
    let mut pl = redis::pipe();

    let ((k1,),): ((i32,),) = pl
        .cmd("SET")
        .arg("p{x}key_1")
        .arg(44)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .query_async(connection)
        .await
        .unwrap();
    pl.clear();

    assert_eq!(k1, 44);

    redis::cmd("DEL")
        .arg("p{x}key_1")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    let ((k1, k2),): ((bool, i32),) = pl
        .cmd("SET")
        .arg("p{x}key_2")
        .arg(45)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .arg(&["p{x}key_2"])
        .query_async(connection)
        .await
        .unwrap();
    pl.clear();

    assert!(!k1);
    assert_eq!(k2, 45);
}

async fn test_real_transaction(connection: &mut Connection) {
    let key = "the_key";
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg(42)
        .query_async(connection)
        .await
        .unwrap();

    loop {
        let _: () = redis::cmd("WATCH")
            .arg(key)
            .query_async(connection)
            .await
            .unwrap();
        let val: isize = redis::cmd("GET")
            .arg(key)
            .query_async(connection)
            .await
            .unwrap();
        let response: Option<(isize,)> = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query_async(connection)
            .await
            .unwrap();

        if let Some(response) = response {
            assert_eq!(response, (43,));
            break;
        }
    }
}

async fn test_script(connection: &mut Connection) {
    let script = redis::Script::new(
        r"
       return {redis.call('GET', KEYS[1]), ARGV[1]}
    ",
    );

    let _: () = redis::cmd("SET")
        .arg("my_key")
        .arg("foo")
        .query_async(connection)
        .await
        .unwrap();
    let response = script.key("my_key").arg(42).invoke_async(connection).await;

    assert_eq!(response, Ok(("foo".to_string(), 42)));
}

async fn test_tuple_args(connection: &mut Connection) {
    redis::cmd("FLUSHDB")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("HMSET")
        .arg("my_key")
        .arg(&[("field_1", 42), ("field_2", 23)])
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    assert_eq!(
        redis::cmd("HGET")
            .arg("my_key")
            .arg("field_1")
            .query_async(connection)
            .await,
        Ok(42)
    );
    assert_eq!(
        redis::cmd("HGET")
            .arg("my_key")
            .arg("field_2")
            .query_async(connection)
            .await,
        Ok(23)
    );
}

async fn test_nice_api(connection: &mut Connection) {
    assert_eq!(connection.set("my_key", 42).await, Ok(()));
    assert_eq!(connection.get("my_key").await, Ok(42));

    let (k1, k2): (i32, i32) = redis::pipe()
        .atomic()
        .set("key_1", 42)
        .ignore()
        .set("key_2", 43)
        .ignore()
        .get("key_1")
        .get("key_2")
        .query_async(connection)
        .await
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

async fn test_auto_m_versions(connection: &mut Connection) {
    assert_eq!(
        connection.set_multiple(&[("key1", 1), ("key2", 2)]).await,
        Ok(())
    );
    assert_eq!(connection.get(&["key1", "key2"]).await, Ok((1, 2)));
}

async fn test_nice_hash_api(connection: &mut Connection) {
    assert_eq!(
        connection
            .hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)])
            .await,
        Ok(())
    );

    let hm: HashMap<String, isize> = connection.hgetall("my_hash").await.unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let hm: BTreeMap<String, isize> = connection.hgetall("my_hash").await.unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let v: Vec<(String, isize)> = connection.hgetall("my_hash").await.unwrap();
    assert_eq!(
        v,
        vec![
            ("f1".to_string(), 1),
            ("f2".to_string(), 2),
            ("f3".to_string(), 4),
            ("f4".to_string(), 8),
        ]
    );

    assert_eq!(connection.hget("my_hash", &["f2", "f4"]).await, Ok((2, 8)));
    assert_eq!(connection.hincr("my_hash", "f1", 1).await, Ok(2));
    assert_eq!(connection.hincr("my_hash", "f2", 1.5f32).await, Ok(3.5f32));
    assert_eq!(connection.hexists("my_hash", "f2").await, Ok(true));
    assert_eq!(connection.hdel("my_hash", &["f1", "f2"]).await, Ok(()));
    assert_eq!(connection.hexists("my_hash", "f2").await, Ok(false));

    let mut iter: redis::AsyncIter<'_, (String, isize)> =
        connection.hscan("my_hash").await.unwrap();
    let mut found = HashSet::new();
    while let Some(item) = iter.next_item().await {
        found.insert(item);
    }

    assert_eq!(found.len(), 2);
    assert!(found.contains(&("f3".to_string(), 4)));
    assert!(found.contains(&("f4".to_string(), 8)));
}

async fn test_nice_list_api(connection: &mut Connection) {
    assert_eq!(connection.rpush("my_list", &[1, 2, 3, 4]).await, Ok(4));
    assert_eq!(connection.rpush("my_list", &[5, 6, 7, 8]).await, Ok(8));
    assert_eq!(connection.llen("my_list").await, Ok(8));

    assert_eq!(connection.lpop("my_list", None).await, Ok(1));
    assert_eq!(connection.llen("my_list").await, Ok(7));

    assert_eq!(connection.lrange("my_list", 0, 2).await, Ok((2, 3, 4)));

    assert_eq!(connection.lset("my_list", 0, 4).await, Ok(true));
    assert_eq!(connection.lrange("my_list", 0, 2).await, Ok((4, 3, 4)));
}

async fn test_tuple_decoding_regression(connection: &mut Connection) {
    assert_eq!(connection.del("my_zset").await, Ok(()));
    assert_eq!(connection.zadd("my_zset", "one", 1).await, Ok(1));
    assert_eq!(connection.zadd("my_zset", "two", 2).await, Ok(1));

    let vec: Vec<(String, u32)> = connection
        .zrangebyscore_withscores("my_zset", 0, 10)
        .await
        .unwrap();
    assert_eq!(vec.len(), 2);

    assert_eq!(connection.del("my_zset").await, Ok(1));

    let vec: Vec<(String, u32)> = connection
        .zrangebyscore_withscores("my_zset", 0, 10)
        .await
        .unwrap();
    assert_eq!(vec.len(), 0);
}

async fn test_bit_operations(connection: &mut Connection) {
    assert_eq!(connection.setbit("bitvec", 10, true).await, Ok(false));
    assert_eq!(connection.getbit("bitvec", 10).await, Ok(true));
}

async fn test_cluster_basics(connection: &mut Connection) {
    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&["{x}key2", "bar"])
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query_async(connection)
            .await,
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

async fn test_cluster_eval(connection: &mut Connection) {
    let rv = redis::cmd("EVAL")
        .arg(
            r#"
            redis.call("SET", KEYS[1], "1");
            redis.call("SET", KEYS[2], "2");
            return redis.call("MGET", KEYS[1], KEYS[2]);
        "#,
        )
        .arg(2)
        .arg("{x}a")
        .arg("{x}b")
        .query_async(connection)
        .await;

    assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
}

async fn test_cluster_script(connection: &mut Connection) {
    let script = redis::Script::new(
        r#"
        redis.call("SET", KEYS[1], "1");
        redis.call("SET", KEYS[2], "2");
        return redis.call("MGET", KEYS[1], KEYS[2]);
    "#,
    );

    let rv = script
        .key("{x}a")
        .key("{x}b")
        .invoke_async(&mut *connection)
        .await;
    assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
}

async fn test_auth(connection: &mut Connection) {
    // Command should fail on unauthenticated connection.
    assert_eq!(
        redis::cmd("GET")
            .arg("without authenticating")
            .query_async::<_, String>(connection)
            .await
            .unwrap_err()
            .code(),
        Some("NOAUTH")
    );

    // Authenticating with incorrect password should fail.
    assert_eq!(
        redis::cmd("AUTH")
            .arg("with a bad password")
            .query_async::<_, ()>(connection)
            .await
            .unwrap_err()
            .code(),
        Some("WRONGPASS")
    );

    // Switch to default superuser.
    redis::cmd("AUTH")
        .arg("shotover")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    // Set random value to be checked later.
    let expected_foo: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect();
    redis::cmd("SET")
        .arg("foo")
        .arg(&expected_foo)
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    // Read-only user with no other permissions should not be able to auth.
    redis::cmd("ACL")
        .arg(&["SETUSER", "brokenuser", "+@read", "on", ">password"])
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("AUTH")
            .arg("brokenuser")
            .arg("password")
            .query_async::<_, ()>(connection)
            .await
            .unwrap_err()
            .code(),
        Some("NOPERM")
    );

    // Read-only user with CLUSTER SLOTS permission should be able to auth, but cannot perform writes.
    // We then use this user to read back the original value that should not have been overwritten.
    redis::cmd("ACL")
        .arg(&[
            "SETUSER",
            "testuser",
            "+@read",
            "+cluster|slots",
            "on",
            ">password",
            "allkeys",
        ])
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    redis::cmd("AUTH")
        .arg("testuser")
        .arg("password")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("SET")
            .arg("foo")
            .arg("fail")
            .query_async::<_, ()>(connection)
            .await
            .unwrap_err()
            .code(),
        Some("NOPERM")
    );
    assert_eq!(
        redis::cmd("GET").arg("foo").query_async(connection).await,
        Ok(expected_foo)
    );
}

async fn test_auth_isolation(shotover_manager: &ShotoverManager, connection: &mut Connection) {
    // ensure we are authenticated as the default superuser to setup for the auth isolation test.
    redis::cmd("AUTH")
        .arg("shotover")
        .query_async::<_, ()>(connection)
        .await
        .unwrap();

    // Create users with only access to their own key, and test their permissions using new connections.
    for i in 1..=100 {
        let user = format!("user-{i}");
        let pass = format!("pass-{i}");
        let key = format!("key-{i}");

        redis::cmd("ACL")
            .arg(&[
                "SETUSER",
                &user,
                "+@read",
                "+cluster|slots",
                "on",
                &format!(">{pass}"),
                &format!("~{key}"),
            ])
            .query_async::<_, ()>(connection)
            .await
            .unwrap();

        let mut new_connection = shotover_manager.redis_connection_async(6379).await;

        assert_eq!(
            redis::cmd("GET")
                .arg("without authenticating")
                .query_async::<_, ()>(&mut new_connection)
                .await
                .unwrap_err()
                .code(),
            Some("NOAUTH")
        );

        redis::cmd("AUTH")
            .arg(&user)
            .arg(&pass)
            .query_async::<_, ()>(&mut new_connection)
            .await
            .unwrap();

        redis::cmd("GET")
            .arg(&key)
            .query_async::<_, ()>(&mut new_connection)
            .await
            .unwrap();

        assert_eq!(
            redis::cmd("GET")
                .arg("foo")
                .query_async::<_, ()>(&mut new_connection)
                .await
                .unwrap_err()
                .code(),
            Some("NOPERM")
        );
    }
}

async fn test_cluster_ports_rewrite_slots(connection: &mut Connection, port: u16) {
    let res: Value = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(connection)
        .await
        .unwrap();

    assert_cluster_ports_rewrite_slots(res, port);

    let (r1, r2, r3): (Value, Value, Value) = redis::pipe()
        .cmd("SET")
        .arg("key1")
        .arg(42)
        .cmd("CLUSTER")
        .arg("SLOTS")
        .cmd("GET")
        .arg("key1")
        .query_async(connection)
        .await
        .unwrap();

    assert!(matches!(r1, Value::Okay));
    assert_cluster_ports_rewrite_slots(r2, port);
    assert_eq!(r3, Value::Data(b"42".to_vec()));
}

fn assert_cluster_ports_rewrite_slots(res: Value, new_port: u16) {
    let mut assertion_run = false;
    if let Value::Bulk(bulks) = &res {
        for bulk in bulks {
            if let Value::Bulk(b) = bulk {
                for tuple in b.iter().enumerate() {
                    if let (2..=3, Value::Bulk(val)) = tuple {
                        assert_eq!(val[1], Value::Int(new_port.into()));
                        assertion_run = true;
                    }
                }
            }
        }
    }
    if !assertion_run {
        panic!(
            "CLUSTER SLOTS result did not contain a port, result was: {:?}",
            res
        );
    }
}

async fn get_master_id(connection: &mut Connection) -> String {
    let res: Value = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(connection)
        .await
        .unwrap();

    if let Value::Data(data) = &res {
        let read_cursor = std::io::Cursor::new(data);
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b' ')
            .has_headers(false)
            .flexible(true) // flexible because the last fields is an arbitrary number of tokens
            .from_reader(read_cursor);

        for result in reader.records() {
            let record = result.unwrap();

            let is_master = record[2].split(',').any(|x| x == "master");

            if is_master {
                return record[0].to_string();
            }
        }
    }

    panic!("Could not find master node in cluster");
}

async fn test_cluster_ports_rewrite_nodes(connection: &mut Connection, new_port: u16) {
    let mut res = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(connection)
        .await
        .unwrap();

    assert_cluster_ports_rewrite_nodes(res, new_port);

    // Get an id to use for cluster replicas test
    let id = get_master_id(connection).await;

    res = redis::cmd("CLUSTER")
        .arg("REPLICAS")
        .arg(id)
        .query_async(connection)
        .await
        .unwrap();

    assert_cluster_ports_rewrite_nodes(res, new_port);

    let (r1, r2, r3): (Value, Value, Value) = redis::pipe()
        .cmd("SET")
        .arg("key1")
        .arg(42)
        .cmd("CLUSTER")
        .arg("NODES")
        .cmd("GET")
        .arg("key1")
        .query_async(connection)
        .await
        .unwrap();

    assert!(matches!(r1, Value::Okay));
    assert_cluster_ports_rewrite_nodes(r2, new_port);
    assert_eq!(r3, Value::Data(b"42".to_vec()));
}

fn assert_cluster_ports_rewrite_nodes(res: Value, new_port: u16) {
    let mut assertion_run = false;

    let data = if let Value::Bulk(data) = &res {
        if let Value::Data(item) = &data[0] {
            item.to_vec()
        } else {
            panic!("Invalid response from Redis")
        }
    } else if let Value::Data(data) = &res {
        data.to_vec()
    } else {
        panic!("Invalid response from Redis");
    };

    let read_cursor = std::io::Cursor::new(data);

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b' ')
        .has_headers(false)
        .flexible(true) // flexible because the last fields is an arbitrary number of tokens
        .from_reader(read_cursor);

    for result in reader.records() {
        let record = result.unwrap();

        let port = record[1]
            .split(|c| c == ':' || c == '@')
            .collect::<Vec<&str>>();

        assert_eq!(port[1].parse::<u16>().unwrap(), new_port);
        assertion_run = true;
    }

    assert!(assertion_run);
}

async fn test_cluster_pipe(connection: &mut Connection) {
    //do this a few times to be sure we are not hitting a single master
    for i in 0..100 * STRESS_TEST_MULTIPLIER {
        // make sure there are no overlaps etc
        let key1 = format!("key{i}");
        let key2 = format!("{i}key");

        let (k1, k2): (i32, i32) = redis::pipe()
            .cmd("SET")
            .arg(&key1)
            .arg(42)
            .ignore()
            .cmd("SET")
            .arg(&key2)
            .arg(43)
            .ignore()
            .cmd("GET")
            .arg(&key1)
            .cmd("GET")
            .arg(&key2)
            .query_async(connection)
            .await
            .unwrap();
        trace!("Iteration {}, k1 = {}, k2 = {}", i, k1, k2);

        assert_eq!(k1, 42);
        assert_eq!(k2, 43);
    }

    for _ in 0..2 * STRESS_TEST_MULTIPLIER {
        let mut pipe = redis::pipe();
        for i in 0..100 {
            let key1 = format!("{i}kaey");
            pipe.cmd("SET").arg(&key1).arg(i);
        }

        let _: Vec<String> = pipe.query_async(connection).await.unwrap();

        let mut pipe = redis::pipe();

        for i in 0..100 {
            let key1 = format!("{i}kaey");
            pipe.cmd("GET").arg(&key1);
        }

        let mut results: Vec<i32> = pipe.query_async(connection).await.unwrap();

        for i in 0..100 {
            let result = results.remove(0);
            assert_eq!(i, result);
        }
    }
}

async fn test_cluster_replication(
    connection: &mut Connection,
    replication_connection: &mut ClusterConnection,
) {
    // According to the coalesce config the writes are only flushed to the replication cluster after 2000 total writes pass through shotover
    for _ in 0..1000 {
        // 2000 writes havent occured yet so this must be true
        assert!(replication_connection.get::<&str, i32>("foo").is_err());
        assert!(replication_connection.get::<&str, i32>("bar").is_err());

        redis::cmd("SET")
            .arg("foo")
            .arg(42)
            .query_async::<_, ()>(connection)
            .await
            .unwrap();
        assert_eq!(
            redis::cmd("GET").arg("foo").query_async(connection).await,
            Ok(42)
        );

        redis::cmd("SET")
            .arg("bar")
            .arg("blah")
            .query_async::<_, ()>(connection)
            .await
            .unwrap();
        assert_eq!(
            redis::cmd("GET").arg("bar").query_async(connection).await,
            Ok(b"blah".to_vec())
        );
    }

    // 2000 writes have now occured, so this should be true
    // although we do need to account for the race condition of shotover returning a response before flushing to the replication cluster
    let mut value1 = Ok(1); // These dummy values are fine because they get overwritten on the first loop
    let mut value2 = Ok(b"".to_vec());
    for _ in 0..100 {
        sleep(Duration::from_millis(100));
        value1 = replication_connection.get("foo");
        value2 = replication_connection.get("bar");
        if value1.is_ok() && value2.is_ok() {
            break;
        }
    }
    assert_eq!(value1, Ok(42));
    assert_eq!(value2, Ok(b"blah".to_vec()));
}

// This test case is picky about the ordering of connection auth so we take a ShotoverManager and make all the connections ourselves
async fn test_dr_auth(shotover_manager: &ShotoverManager) {
    // setup 3 different connections in different states
    let mut connection_shotover_noauth = shotover_manager.redis_connection_async(6379).await;

    let mut connection_shotover_auth = shotover_manager.redis_connection_async(6379).await;
    redis::cmd("AUTH")
        .arg("default")
        .arg("shotover")
        .query_async::<_, ()>(&mut connection_shotover_auth)
        .await
        .unwrap();

    let mut connection_dr_auth = shotover_manager.redis_connection_async(2120).await;
    redis::cmd("AUTH")
        .arg("default")
        .arg("shotover")
        .query_async::<_, ()>(&mut connection_dr_auth)
        .await
        .unwrap();

    // writing to shotover when authed should succeed
    redis::cmd("SET")
        .arg("authed_write")
        .arg(42)
        .query_async::<_, ()>(&mut connection_shotover_auth)
        .await
        .unwrap();
    assert_eq!(
        redis::cmd("GET")
            .arg("authed_write")
            .query_async(&mut connection_shotover_auth)
            .await,
        Ok(42)
    );

    // writing to shotover when not authed should not write to either destination
    // important that this comes after the previous authed test
    // to ensure that auth'ing once doesnt result in other connections being considered auth'd
    for _ in 0..2000 {
        // Need to run 2000 times to ensure we hit the configured coallesce max
        redis::cmd("SET")
            .arg("authed_write")
            .arg(1111)
            .query_async::<_, ()>(&mut connection_shotover_noauth)
            .await
            .unwrap_err();
    }

    // Need to delay a bit to avoid race condition on the write to the dr cluster
    sleep(Duration::from_secs(1));
    assert_eq!(
        redis::cmd("GET")
            .arg("authed_write")
            .query_async(&mut connection_shotover_auth)
            .await,
        Ok(42)
    );
    assert_eq!(
        redis::cmd("GET")
            .arg("authed_write")
            .query_async::<_, Option<i32>>(&mut connection_dr_auth)
            .await,
        Ok(None)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_passthrough() {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml")
        .wait_for("Ready to accept connections");
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");
    let mut connection = shotover_manager.redis_connection_async(6379).await;

    run_all(&mut connection).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_tls() {
    let _compose = DockerCompose::new("examples/redis-cluster-tls/docker-compose.yml")
        .wait_for_n("Cluster state changed", 6);
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster-tls/topology.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;

    run_all_cluster_safe(&mut connection).await;
    test_cluster_ports_rewrite_slots(&mut connection, 6379).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_source_tls_and_single_tls() {
    let _compose = DockerCompose::new("examples/redis-tls/docker-compose.yml")
        .wait_for("Ready to accept connections");
    let shotover_manager = ShotoverManager::from_topology_file("examples/redis-tls/topology.yaml");

    let tls_config = TlsConfig {
        certificate_authority_path: "examples/redis-tls/tls_keys/ca.crt".into(),
        certificate_path: "examples/redis-tls/tls_keys/redis.crt".into(),
        private_key_path: "examples/redis-tls/tls_keys/redis.key".into(),
    };

    let mut connection = shotover_manager
        .redis_connection_async_tls(6380, tls_config)
        .await;

    run_all(&mut connection).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_ports_rewrite() {
    let _compose =
        DockerCompose::new("tests/test-topologies/redis-cluster-ports-rewrite/docker-compose.yml")
            .wait_for_n("Cluster state changed", 6);

    let shotover_manager = ShotoverManager::from_topology_file(
        "tests/test-topologies/redis-cluster-ports-rewrite/topology.yaml",
    );

    let mut connection = shotover_manager.redis_connection_async(6380).await;

    run_all_cluster_safe(&mut connection).await;

    test_cluster_ports_rewrite_slots(&mut connection, 6380).await;

    test_cluster_ports_rewrite_nodes(&mut connection, 6380).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_active_active_redis() {
    let _compose = DockerCompose::new("examples/redis-multi/docker-compose.yml")
        .wait_for("Ready to accept connections");
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-multi/topology.yaml");
    let mut connection = shotover_manager.redis_connection_async(6379).await;

    run_all_active_safe(&mut connection).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_auth_redis() {
    let _compose = DockerCompose::new("examples/redis-cluster-auth/docker-compose.yml")
        .wait_for_n("Cluster state changed", 6);
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster-auth/topology.yaml");
    let mut connection = shotover_manager.redis_connection_async(6379).await;

    test_auth(&mut connection).await;
    test_auth_isolation(&shotover_manager, &mut connection).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_redis() {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml")
        .wait_for_n("Cluster state changed", 6);
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;
    let connection = &mut connection;

    run_all_cluster_safe(connection).await;
    test_cluster_ports_rewrite_slots(connection, 6379).await;
    test_cluster_ports_rewrite_nodes(connection, 6379).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn test_cluster_dr_redis() {
    let _compose = DockerCompose::new("examples/redis-cluster-dr/docker-compose.yml")
        .wait_for_n("Cluster state changed", 12);

    let nodes = vec![
        "redis://127.0.0.1:2120/",
        "redis://127.0.0.1:2121/",
        "redis://127.0.0.1:2122/",
        "redis://127.0.0.1:2123/",
        "redis://127.0.0.1:2124/",
        "redis://127.0.0.1:2125/",
    ];
    let client = redis::cluster::ClusterClientBuilder::new(nodes)
        .password("shotover".to_string())
        .open()
        .unwrap();
    let mut replication_connection = client.get_connection().unwrap();

    // test coalesce sends messages on shotover shutdown
    {
        let shotover_manager =
            ShotoverManager::from_topology_file("examples/redis-cluster-dr/topology.yaml");
        let mut connection = shotover_manager.redis_connection_async(6379).await;
        redis::cmd("AUTH")
            .arg("default")
            .arg("shotover")
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();

        redis::cmd("SET")
            .arg("key1")
            .arg(42)
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();
        redis::cmd("SET")
            .arg("key2")
            .arg(358)
            .query_async::<_, ()>(&mut connection)
            .await
            .unwrap();

        // shotover is shutdown here because shotover_manager goes out of scope and is dropped.
    }
    sleep(Duration::from_secs(1));
    assert_eq!(replication_connection.get::<&str, i32>("key1").unwrap(), 42);
    assert_eq!(
        replication_connection.get::<&str, i32>("key2").unwrap(),
        358
    );

    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster-dr/topology.yaml");

    let mut connection = shotover_manager.redis_connection_async(6379).await;
    redis::cmd("AUTH")
        .arg("default")
        .arg("shotover")
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap();

    test_cluster_replication(&mut connection, &mut replication_connection).await;
    test_dr_auth(&shotover_manager).await;
    run_all_cluster_safe(&mut connection).await;
}

async fn run_all_active_safe(connection: &mut Connection) {
    test_cluster_basics(connection).await;
    test_cluster_eval(connection).await;
    test_cluster_script(connection).await; //TODO: script does not seem to be loading in the server?
                                           // test_cluster_pipeline(); // we do support pipelining!!
    test_getset(connection).await;
    test_incr(connection).await;
    test_info(connection).await;
    // test_hash_ops(connection).await;
    test_set_ops(connection).await;
    test_scan(connection).await;
    test_optionals(connection).await;
    // test_scanning(connection).await;
    // test_filtered_scanning(connection).await;
    // test_pipeline(connection).await;
    test_empty_pipeline(connection).await;
    // TODO: Pipeline transactions currently don't work (though it tries very hard)
    // Current each cmd in a pipeline is treated as a single request, which means on a cluster
    // basis they end up getting routed to different masters. This results in very occasionally will
    // the transaction resolve (the exec and the multi both go to the right server).
    // test_pipeline_transaction(connection).await;
    // test_pipeline_reuse_query(connection).await;
    // test_pipeline_reuse_query_clear(connection).await;
    // test_real_transaction(connection).await;
    test_script(connection).await;
    test_tuple_args(connection).await;
    // test_nice_api(connection).await;
    test_auto_m_versions(connection).await;
    test_nice_hash_api(connection).await;
    test_nice_list_api(connection).await;
    test_tuple_decoding_regression(connection).await;
    test_bit_operations(connection).await;
}

async fn run_all_cluster_safe(connection: &mut Connection) {
    test_cluster_pipe(connection).await;
    test_pipeline_error(connection).await; //TODO: script does not seem to be loading in the server?
    for _i in 0..1999 {
        test_script(connection).await;
    }

    test_cluster_basics(connection).await;
    test_cluster_eval(connection).await;
    test_cluster_script(connection).await; //TODO: script does not seem to be loading in the server?
    test_getset(connection).await;
    test_incr(connection).await;
    // test_info().await;
    // test_hash_ops().await;
    test_set_ops(connection).await;
    test_scan(connection).await;
    // test_optionals().await;
    test_scanning(connection).await;
    test_filtered_scanning(connection).await;
    test_pipeline(connection).await; // NGET Issues
    test_empty_pipeline(connection).await;
    // TODO: Pipeline transactions currently don't work (though it tries very hard)
    // Current each cmd in a pipeline is treated as a single request, which means on a cluster
    // basis they end up getting routed to different masters. This results in very occasionally will
    // the transaction resolve (the exec and the multi both go to the right server).
    // test_pipeline_transaction().await;
    test_pipeline_reuse_query(connection).await;
    test_pipeline_reuse_query_clear(connection).await;
    // test_real_transaction().await;
    test_script(connection).await;
    test_tuple_args(connection).await;
    // test_nice_api().await;
    // test_auto_m_versions().await;
    test_nice_hash_api(connection).await;
    test_nice_list_api(connection).await;
    test_tuple_decoding_regression(connection).await;
    test_bit_operations(connection).await;
}

async fn run_all(connection: &mut Connection) {
    test_args(connection).await;
    test_getset(connection).await;
    test_incr(connection).await;
    test_info(connection).await;
    test_hash_ops(connection).await;
    test_set_ops(connection).await;
    test_scan(connection).await;
    test_optionals(connection).await;
    test_scanning(connection).await;
    test_filtered_scanning(connection).await;
    test_pipeline(connection).await;
    test_empty_pipeline(connection).await;
    test_pipeline_transaction(connection).await;
    test_pipeline_reuse_query(connection).await;
    test_pipeline_reuse_query_clear(connection).await;
    test_real_transaction(connection).await;
    // test_pubsub().await;
    // test_pubsub_unsubscribe().await;
    // test_pubsub_unsubscribe_no_subs().await;
    // test_pubsub_unsubscribe_one_sub().await;
    // test_pubsub_unsubscribe_one_sub_one_psub().await;
    // scoped_pubsub();
    test_script(connection).await;
    test_tuple_args(connection).await;
    test_nice_api(connection).await;
    test_auto_m_versions(connection).await;
    test_nice_hash_api(connection).await;
    test_nice_list_api(connection).await;
    test_tuple_decoding_regression(connection).await;
    test_bit_operations(connection).await;
}
