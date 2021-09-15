#![allow(clippy::let_unit_value)]

use crate::helpers::ShotoverManager;
use shotover_proxy::tls::TlsConfig;

use test_helpers::docker_compose::DockerCompose;

use redis::Connection;
use redis::{Commands, ErrorKind, RedisError, Value};
use serial_test::serial;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use tracing::{info, trace};

fn test_args(connection: &mut Connection) {
    redis::cmd("SET")
        .arg("key1")
        .arg(b"foo")
        .execute(connection);
    redis::cmd("SET").arg(&["key2", "bar"]).execute(connection);

    assert_eq!(
        redis::cmd("MGET").arg(&["key1", "key2"]).query(connection),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

fn test_getset(connection: &mut Connection) {
    for _ in 1..10000 {
        redis::cmd("SET").arg("foo").arg(42).execute(connection);
        assert_eq!(redis::cmd("GET").arg("foo").query(connection), Ok(42));

        redis::cmd("SET").arg("bar").arg("foo").execute(connection);
        assert_eq!(
            redis::cmd("GET").arg("bar").query(connection),
            Ok(b"foo".to_vec())
        );
    }
}

fn test_incr(connection: &mut Connection) {
    redis::cmd("SET").arg("foo").arg(42).execute(connection);
    assert_eq!(redis::cmd("INCR").arg("foo").query(connection), Ok(43usize));
}

fn test_info(connection: &mut Connection) {
    let info: redis::InfoDict = redis::cmd("INFO").query(connection).unwrap();
    assert_eq!(
        info.find(&"role"),
        Some(&redis::Value::Status("master".to_string()))
    );
    assert_eq!(info.get("role"), Some("master".to_string()));
    assert_eq!(info.get("loading"), Some(false));
    assert!(!info.is_empty());
    assert!(info.contains_key(&"role"));
}

fn test_hash_ops(connection: &mut Connection) {
    redis::cmd("FLUSHDB").execute(connection);
    redis::cmd("HSET")
        .arg("foo")
        .arg("key_1")
        .arg(1)
        .execute(connection);
    redis::cmd("HSET")
        .arg("foo")
        .arg("key_2")
        .arg(2)
        .execute(connection);

    let h: HashMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(connection).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));

    let h: BTreeMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(connection).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
}

fn test_set_ops(connection: &mut Connection) {
    redis::cmd("FLUSHDB").execute(connection);
    redis::cmd("SADD").arg("foo").arg(1).execute(connection);
    redis::cmd("SADD").arg("foo").arg(2).execute(connection);
    redis::cmd("SADD").arg("foo").arg(3).execute(connection);

    let mut s: Vec<i32> = redis::cmd("SMEMBERS").arg("foo").query(connection).unwrap();
    s.sort_unstable();
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);

    let set: HashSet<i32> = redis::cmd("SMEMBERS").arg("foo").query(connection).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));

    let set: BTreeSet<i32> = redis::cmd("SMEMBERS").arg("foo").query(connection).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));
}

fn test_scan(connection: &mut Connection) {
    redis::cmd("SADD").arg("foo").arg(1).execute(connection);
    redis::cmd("SADD").arg("foo").arg(2).execute(connection);
    redis::cmd("SADD").arg("foo").arg(3).execute(connection);

    let (cur, mut s): (i32, Vec<i32>) = redis::cmd("SSCAN")
        .arg("foo")
        .arg(0)
        .query(connection)
        .unwrap();
    s.sort_unstable();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

fn test_optionals(connection: &mut Connection) {
    redis::cmd("SET").arg("foo").arg(1).execute(connection);

    let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg("foo")
        .arg("missing")
        .query(connection)
        .unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);

    let a = redis::cmd("GET")
        .arg("missing")
        .query(connection)
        .unwrap_or(0i32);
    assert_eq!(a, 0i32);
}

fn test_scanning(connection: &mut Connection) {
    redis::cmd("FLUSHDB").execute(connection);
    let mut unseen = HashSet::<usize>::new();

    for x in 0..1000 {
        let _a: i64 = redis::cmd("SADD")
            .arg("foo")
            .arg(x)
            .query(connection)
            .unwrap();
        unseen.insert(x);
    }

    assert_eq!(unseen.len(), 1000);

    let iter = redis::cmd("SSCAN")
        .arg("foo")
        .cursor_arg(0)
        .clone()
        .iter(connection)
        .unwrap();

    for x in iter {
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

fn test_filtered_scanning(connection: &mut Connection) {
    redis::cmd("FLUSHDB").execute(connection);
    let mut unseen = HashSet::<usize>::new();

    for x in 0..3000 {
        let _: () = connection
            .hset("foo", format!("key_{}_{}", x % 100, x), x)
            .unwrap();
        if x % 100 == 0 {
            unseen.insert(x);
        }
    }

    let iter = connection.hscan_match("foo", "key_0_*").unwrap();

    for x in iter {
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

fn test_pipeline_error(connection: &mut Connection) {
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
        .query(connection)
        .unwrap();

    let packed = redis::pipe()
        .cmd("SET")
        .arg("k{x}ey_1")
        .arg(42)
        .cmd("SESDFSDFSDFT")
        .arg("k{x}ey_2")
        .arg(43)
        .cmd("GET")
        .arg("k{x}ey_1")
        .get_packed_pipeline();

    let _r = connection.send_packed_command(&packed); // Don't unwrap the results as the driver will throw an exception and disconnect

    assert_eq!(connection.recv_response(), Ok(redis::Value::Okay));

    assert_eq!(
        connection.recv_response(),
        Err::<Value, RedisError>(RedisError::from((
            ErrorKind::ResponseError,
            "An error was signalled by the server",
            "unknown command `SESDFSDFSDFT`, with args beginning with: `k{x}ey_2`, `43`, "
                .to_string()
        )))
    );
    assert_eq!(
        connection.recv_response(),
        Ok(redis::Value::Data(Vec::from("42")))
    );
}

fn test_pipeline(connection: &mut Connection) {
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
        .query(connection)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

fn test_empty_pipeline(connection: &mut Connection) {
    let _: () = redis::pipe()
        .cmd("PING")
        .ignore()
        .query(connection)
        .unwrap();
    let _: () = redis::pipe().query(connection).unwrap();
}

fn test_pipeline_transaction(connection: &mut Connection) {
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
        .query(connection)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

fn test_pipeline_reuse_query(connection: &mut Connection) {
    let mut pl = redis::pipe();

    let ((k1,),): ((i32,),) = pl
        .cmd("SET")
        .arg("p{x}key_1")
        .arg(42)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .query(connection)
        .unwrap();

    assert_eq!(k1, 42);

    redis::cmd("DEL").arg("p{x}key_1").execute(connection);

    // The internal commands vector of the pipeline still contains the previous commands.
    let ((k1,), (k2, k3)): ((i32,), (i32, i32)) = pl
        .cmd("SET")
        .arg("p{x}key_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .arg(&["p{x}key_2"])
        .query(connection)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 42);
    assert_eq!(k3, 43);
}

fn test_pipeline_reuse_query_clear(connection: &mut Connection) {
    let mut pl = redis::pipe();

    let ((k1,),): ((i32,),) = pl
        .cmd("SET")
        .arg("p{x}key_1")
        .arg(44)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .query(connection)
        .unwrap();
    pl.clear();

    assert_eq!(k1, 44);

    redis::cmd("DEL").arg("p{x}key_1").execute(connection);

    let ((k1, k2),): ((bool, i32),) = pl
        .cmd("SET")
        .arg("p{x}key_2")
        .arg(45)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .arg(&["p{x}key_2"])
        .query(connection)
        .unwrap();
    pl.clear();

    assert_eq!(k1, false);
    assert_eq!(k2, 45);
}

fn test_real_transaction(connection: &mut Connection) {
    let key = "the_key";
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg(42)
        .query(connection)
        .unwrap();

    loop {
        let _: () = redis::cmd("WATCH").arg(key).query(connection).unwrap();
        let val: isize = redis::cmd("GET").arg(key).query(connection).unwrap();
        let response: Option<(isize,)> = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query(connection)
            .unwrap();

        if let Some(response) = response {
            assert_eq!(response, (43,));
            break;
        }
    }
}

fn test_real_transaction_highlevel(connection: &mut Connection) {
    let key = "the_key";
    let _: () = redis::cmd("SET")
        .arg(key)
        .arg(42)
        .query(connection)
        .unwrap();

    let response: (isize,) = redis::transaction(connection, &[key], |connection, pipe| {
        let val: isize = redis::cmd("GET").arg(key).query(connection)?;
        pipe.cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query(connection)
    })
    .unwrap();

    assert_eq!(response, (43,));
}

fn test_script(connection: &mut Connection) {
    let script = redis::Script::new(
        r"
       return {redis.call('GET', KEYS[1]), ARGV[1]}
    ",
    );

    let _: () = redis::cmd("SET")
        .arg("my_key")
        .arg("foo")
        .query(connection)
        .unwrap();
    let response = script.key("my_key").arg(42).invoke(connection);

    assert_eq!(response, Ok(("foo".to_string(), 42)));
}

fn test_tuple_args(connection: &mut Connection) {
    redis::cmd("FLUSHDB").execute(connection);
    redis::cmd("HMSET")
        .arg("my_key")
        .arg(&[("field_1", 42), ("field_2", 23)])
        .execute(connection);

    assert_eq!(
        redis::cmd("HGET")
            .arg("my_key")
            .arg("field_1")
            .query(connection),
        Ok(42)
    );
    assert_eq!(
        redis::cmd("HGET")
            .arg("my_key")
            .arg("field_2")
            .query(connection),
        Ok(23)
    );
}

fn test_nice_api(connection: &mut Connection) {
    assert_eq!(connection.set("my_key", 42), Ok(()));
    assert_eq!(connection.get("my_key"), Ok(42));

    let (k1, k2): (i32, i32) = redis::pipe()
        .atomic()
        .set("key_1", 42)
        .ignore()
        .set("key_2", 43)
        .ignore()
        .get("key_1")
        .get("key_2")
        .query(connection)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

fn test_auto_m_versions(connection: &mut Connection) {
    assert_eq!(connection.set_multiple(&[("key1", 1), ("key2", 2)]), Ok(()));
    assert_eq!(connection.get(&["key1", "key2"]), Ok((1, 2)));
}

fn test_nice_hash_api(connection: &mut Connection) {
    assert_eq!(
        connection.hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)]),
        Ok(())
    );

    let hm: HashMap<String, isize> = connection.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let hm: BTreeMap<String, isize> = connection.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let v: Vec<(String, isize)> = connection.hgetall("my_hash").unwrap();
    assert_eq!(
        v,
        vec![
            ("f1".to_string(), 1),
            ("f2".to_string(), 2),
            ("f3".to_string(), 4),
            ("f4".to_string(), 8),
        ]
    );

    assert_eq!(connection.hget("my_hash", &["f2", "f4"]), Ok((2, 8)));
    assert_eq!(connection.hincr("my_hash", "f1", 1), Ok(2));
    assert_eq!(connection.hincr("my_hash", "f2", 1.5f32), Ok(3.5f32));
    assert_eq!(connection.hexists("my_hash", "f2"), Ok(true));
    assert_eq!(connection.hdel("my_hash", &["f1", "f2"]), Ok(()));
    assert_eq!(connection.hexists("my_hash", "f2"), Ok(false));

    let iter: redis::Iter<'_, (String, isize)> = connection.hscan("my_hash").unwrap();
    let mut found = HashSet::new();
    for item in iter {
        found.insert(item);
    }

    assert_eq!(found.len(), 2);
    assert_eq!(found.contains(&("f3".to_string(), 4)), true);
    assert_eq!(found.contains(&("f4".to_string(), 8)), true);
}

fn test_nice_list_api(connection: &mut Connection) {
    assert_eq!(connection.rpush("my_list", &[1, 2, 3, 4]), Ok(4));
    assert_eq!(connection.rpush("my_list", &[5, 6, 7, 8]), Ok(8));
    assert_eq!(connection.llen("my_list"), Ok(8));

    assert_eq!(connection.lpop("my_list", None), Ok(1));
    assert_eq!(connection.llen("my_list"), Ok(7));

    assert_eq!(connection.lrange("my_list", 0, 2), Ok((2, 3, 4)));

    assert_eq!(connection.lset("my_list", 0, 4), Ok(true));
    assert_eq!(connection.lrange("my_list", 0, 2), Ok((4, 3, 4)));
}

fn test_tuple_decoding_regression(connection: &mut Connection) {
    assert_eq!(connection.del("my_zset"), Ok(()));
    assert_eq!(connection.zadd("my_zset", "one", 1), Ok(1));
    assert_eq!(connection.zadd("my_zset", "two", 2), Ok(1));

    let vec: Vec<(String, u32)> = connection
        .zrangebyscore_withscores("my_zset", 0, 10)
        .unwrap();
    assert_eq!(vec.len(), 2);

    assert_eq!(connection.del("my_zset"), Ok(1));

    let vec: Vec<(String, u32)> = connection
        .zrangebyscore_withscores("my_zset", 0, 10)
        .unwrap();
    assert_eq!(vec.len(), 0);
}

fn test_bit_operations(connection: &mut Connection) {
    assert_eq!(connection.setbit("bitvec", 10, true), Ok(false));
    assert_eq!(connection.getbit("bitvec", 10), Ok(true));
}

fn test_cluster_basics(connection: &mut Connection) {
    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(connection);
    info!("one");
    redis::cmd("SET")
        .arg(&["{x}key2", "bar"])
        .execute(connection);
    info!("two");

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(connection),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

fn test_cluster_eval(connection: &mut Connection) {
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
        .query(connection);

    assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
}

#[allow(dead_code)]
fn test_cluster_script(connection: &mut Connection) {
    let script = redis::Script::new(
        r#"
        redis.call("SET", KEYS[1], "1");
        redis.call("SET", KEYS[2], "2");
        return redis.call("MGET", KEYS[1], KEYS[2]);
    "#,
    );

    let rv = script.key("{x}a").key("{x}b").invoke(connection);
    assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
}

#[test]
#[serial(redis)]
fn test_pass_through() {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");
    _compose.wait_for("Ready to accept connections").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");
    let mut connection = shotover_manager.redis_connection(6379);

    run_all(&mut connection);
}

#[tokio::test(flavor = "multi_thread")]
#[serial(redis)]
async fn test_tls() {
    let _compose = DockerCompose::new("examples/redis-tls/docker-compose.yml");
    _compose.wait_for("Ready to accept connections").unwrap();
    let shotover_manager = ShotoverManager::from_topology_file("examples/redis-tls/topology.yaml");

    let tls_config = TlsConfig {
        certificate_authority_path: "examples/redis-tls/tls_keys/ca.crt".into(),
        certificate_path: "examples/redis-tls/tls_keys/redis.crt".into(),
        private_key_path: "examples/redis-tls/tls_keys/redis.key".into(),
    };

    let mut connection = shotover_manager
        .async_tls_redis_connection(6379, tls_config)
        .await;

    redis::cmd("SET")
        .arg("key1")
        .arg(b"foo")
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&["key2", "bar"])
        .query_async::<_, ()>(&mut connection)
        .await
        .unwrap();

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["key1", "key2"])
            .query_async(&mut connection)
            .await,
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

// #[test]
// #[serial(redis)]
#[allow(dead_code)]
fn test_pass_through_one() {
    let _compose = DockerCompose::new("examples/redis-passthrough/docker-compose.yml");
    _compose.wait_for("Cluster correctly created").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-passthrough/topology.yaml");
    let mut connection = shotover_manager.redis_connection(6379);

    test_real_transaction(&mut connection);
}

#[test]
#[serial(redis)]
fn test_active_active_redis() {
    let _compose = DockerCompose::new("examples/redis-multi/docker-compose.yml");
    _compose.wait_for("Ready to accept connections").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-multi/topology.yaml");
    let mut connection = shotover_manager.redis_connection(6379);

    run_all_active_safe(&mut connection);
}

#[test]
#[serial(redis)]
fn test_active_one_active_redis() {
    let _compose = DockerCompose::new("examples/redis-multi/docker-compose.yml");
    _compose.wait_for("Ready to accept connections").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-multi/topology.yaml");
    let mut connection = shotover_manager.redis_connection(6379);

    // test_args();
    test_cluster_basics(&mut connection);

    // test_pipeline();
    test_getset(&mut connection);
}

#[test]
#[serial(redis)]
fn test_pass_redis_cluster_one() {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml");
    _compose.wait_for("Cluster correctly created").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");
    let mut connection = shotover_manager.redis_connection(6379);

    // test_args()test_args;
    test_pipeline_error(&mut connection); //TODO: script does not seem to be loading in the server?
}

// TODO Re-enable Redis Auth support
// #[test]
// #[serial(redis)]
fn _test_cluster_auth_redis() {
    let _compose = DockerCompose::new("examples/redis-cluster-auth/docker-compose.yml");
    _compose.wait_for("Cluster correctly created").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster-auth/topology.yaml");
    let mut connection = shotover_manager.redis_connection(6379);

    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut connection);
    redis::cmd("SET")
        .arg(&["{x}key2", "bar"])
        .execute(&mut connection);

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(&mut connection),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );

    // create a user, auth as them, try to set a key but should fail as they have no access
    redis::cmd("ACL")
        .arg(&["SETUSER", "testuser", "+@read", "on", ">password"])
        .execute(&mut connection);
    redis::cmd("AUTH")
        .arg("testuser")
        .arg("password")
        .execute(&mut connection);
    if let Ok(_s) = redis::cmd("SET")
        .arg("{x}key2")
        .arg("fail")
        .query::<String>(&mut connection)
    {
        panic!("This should fail!")
    }
    // assert_eq!(
    //     redis::cmd("GET").arg("{x}key2").query(&mut connection),
    //     Ok("bar".to_string())
    // );

    // set auth context back to default user using non acl style auth command
    redis::cmd("AUTH").arg("shotover").execute(&mut connection);
    redis::cmd("SET")
        .arg("{x}key3")
        .arg(b"food")
        .execute(&mut connection);

    assert_eq!(
        redis::cmd("GET").arg("{x}key3").query(&mut connection),
        Ok("food".to_string())
    );
}

#[test]
#[serial(redis)]
fn test_cluster_all_redis() {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml");
    _compose.wait_for("Cluster correctly created").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");

    // panic!("Loooks like we are getting some out of order issues with pipelined request");
    let mut connection = shotover_manager.redis_connection(6379);
    run_all_cluster_safe(&mut connection);
}

#[test]
#[serial(redis)]
fn test_cluster_all_script_redis() {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml");
    _compose.wait_for("Cluster correctly created").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");
    // panic!("Loooks like we are getting some out of order issues with pipelined request");
    let mut connection = shotover_manager.redis_connection(6379);
    for _i in 0..1999 {
        test_script(&mut connection);
    }
}

#[test]
#[serial(redis)]
fn test_cluster_all_pipeline_safe_redis() {
    let _compose = DockerCompose::new("examples/redis-cluster/docker-compose.yml");
    _compose.wait_for("Cluster correctly created").unwrap();
    let shotover_manager =
        ShotoverManager::from_topology_file("examples/redis-cluster/topology.yaml");
    let mut connection = shotover_manager.redis_connection(6379);
    let connection = &mut connection;

    test_cluster_script(connection);
    test_script(connection);
    test_cluster_script(connection);

    //do this a few times to be sure we are not hitting a single master
    info!("key string formating");
    for i in 0..2000 {
        // make sure there are no overlaps etc
        let key1 = format!("key{}", i);
        let key2 = format!("{}key", i);

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
            .query(connection)
            .unwrap();
        trace!("Iteration {}, k1 = {}, k2 = {}", i, k1, k2);

        assert_eq!(k1, 42);
        assert_eq!(k2, 43);
    }

    info!("pipelining in cluster mode");
    for _ in 0..200 {
        let mut pipe = redis::pipe();
        for i in 0..1000 {
            let key1 = format!("{}kaey", i);
            pipe.cmd("SET").arg(&key1).arg(i);
        }

        let _: Vec<String> = pipe.query(connection).unwrap();

        let mut pipe = redis::pipe();

        for i in 0..1000 {
            let key1 = format!("{}kaey", i);
            pipe.cmd("GET").arg(&key1);
        }

        let mut results: Vec<i32> = pipe.query(connection).unwrap();

        for i in 0..1000 {
            let result = results.remove(0);
            assert_eq!(i, result);
        }
    }

    test_cluster_basics(connection);
    test_cluster_eval(connection);
    test_cluster_script(connection); //TODO: script does not seem to be loading in the server?
    test_getset(connection);
    test_incr(connection);
    // test_info();
    // test_hash_ops();
    test_set_ops(connection);
    test_scan(connection);
    // test_optionals();
    test_pipeline_error(connection);
    test_scanning(connection);
    test_filtered_scanning(connection);
    test_pipeline(connection); // NGET Issues
    test_empty_pipeline(connection);
    // TODO: Pipeline transactions currently don't work (though it tries very hard)
    // Current each cmd in a pipeline is treated as a single request, which means on a cluster
    // basis they end up getting routed to different masters. This results in very occasionally will
    // the transaction resolve (the exec and the multi both go to the right server).
    // test_pipeline_transaction();
    test_pipeline_reuse_query(connection);
    test_pipeline_reuse_query_clear(connection);
    // test_real_transaction();
    // test_real_transaction_highlevel();
    test_script(connection);
    test_tuple_args(connection);
    // test_nice_api();
    // test_auto_m_versions();
    test_nice_hash_api(connection);
    test_nice_list_api(connection);
    test_tuple_decoding_regression(connection);
    test_bit_operations(connection);
}

fn run_all_active_safe(connection: &mut Connection) {
    test_cluster_basics(connection);
    test_cluster_eval(connection);
    test_cluster_script(connection); //TODO: script does not seem to be loading in the server?
                                     // test_cluster_pipeline(); // we do support pipelining!!
    test_getset(connection);
    test_incr(connection);
    // test_info();
    // test_hash_ops();
    test_set_ops(connection);
    test_scan(connection);
    // test_optionals();
    // test_scanning(); // TODO scanning doesnt work
    // test_filtered_scanning();
    test_pipeline(connection); // NGET Issues
    test_empty_pipeline(connection);
    // TODO: Pipeline transactions currently don't work (though it tries very hard)
    // Current each cmd in a pipeline is treated as a single request, which means on a cluster
    // basis they end up getting routed to different masters. This results in very occasionally will
    // the transaction resolve (the exec and the multi both go to the right server).
    // test_pipeline_transaction();
    // test_pipeline_reuse_query();
    // test_pipeline_reuse_query_clear();
    // test_real_transaction();
    // test_real_transaction_highlevel();
    test_script(connection);
    test_tuple_args(connection);
    // test_nice_api();
    // test_auto_m_versions();
    test_nice_hash_api(connection);
    test_nice_list_api(connection);
    test_tuple_decoding_regression(connection);
    test_bit_operations(connection);
    // test_invalid_protocol();
}

fn run_all_cluster_safe(connection: &mut Connection) {
    test_cluster_basics(connection);
    test_cluster_eval(connection);
    test_cluster_script(connection); //TODO: script does not seem to be loading in the server?
    test_getset(connection);
    test_incr(connection);
    // test_info();
    // test_hash_ops();
    test_set_ops(connection);
    test_scan(connection);
    // test_optionals();
    test_scanning(connection);
    test_filtered_scanning(connection);
    test_pipeline(connection); // NGET Issues
    test_empty_pipeline(connection);
    // TODO: Pipeline transactions currently don't work (though it tries very hard)
    // Current each cmd in a pipeline is treated as a single request, which means on a cluster
    // basis they end up getting routed to different masters. This results in very occasionally will
    // the transaction resolve (the exec and the multi both go to the right server).
    // test_pipeline_transaction();
    test_pipeline_reuse_query(connection);
    test_pipeline_reuse_query_clear(connection);
    // test_real_transaction();
    // test_real_transaction_highlevel();
    test_script(connection);
    test_tuple_args(connection);
    // test_nice_api();
    // test_auto_m_versions();
    test_nice_hash_api(connection);
    test_nice_list_api(connection);
    test_tuple_decoding_regression(connection);
    test_bit_operations(connection);
    // test_invalid_protocol();
}

fn run_all(connection: &mut Connection) {
    test_args(connection);
    test_getset(connection);
    test_incr(connection);
    test_info(connection);
    test_hash_ops(connection);
    test_set_ops(connection);
    test_scan(connection);
    test_optionals(connection);
    test_scanning(connection);
    test_filtered_scanning(connection);
    test_pipeline(connection);
    test_empty_pipeline(connection);
    test_pipeline_transaction(connection);
    test_pipeline_reuse_query(connection);
    test_pipeline_reuse_query_clear(connection);
    test_real_transaction(connection);
    test_real_transaction_highlevel(connection);
    // test_pubsub();
    // test_pubsub_unsubscribe();
    // test_pubsub_unsubscribe_no_subs();
    // test_pubsub_unsubscribe_one_sub();
    // test_pubsub_unsubscribe_one_sub_one_psub();
    // scoped_pubsub();
    test_script(connection);
    test_tuple_args(connection);
    test_nice_api(connection);
    test_auto_m_versions(connection);
    test_nice_hash_api(connection);
    test_nice_list_api(connection);
    test_tuple_decoding_regression(connection);
    test_bit_operations(connection);
    // test_invalid_protocol();
}
