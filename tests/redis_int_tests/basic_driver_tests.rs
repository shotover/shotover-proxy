#![allow(clippy::let_unit_value)]
use anyhow::Result;

use redis::{Commands, ErrorKind, RedisError, Value};

use crate::load_docker_compose;
use crate::redis_int_tests::support::TestContext;

use shotover_proxy::config::topology::Topology;
use std::collections::{BTreeMap, BTreeSet};
use std::collections::{HashMap, HashSet};
use std::process::exit;
use tokio::runtime;
use tracing::info;
use tracing::trace;
use tracing::Level;

fn try_register_cleanup() {
    let _ = ctrlc::set_handler(move || {
        exit(0);
    });
}

fn test_args() {
    info!("test_args");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("key1").arg(b"foo").execute(&mut con);
    redis::cmd("SET").arg(&["key2", "bar"]).execute(&mut con);

    assert_eq!(
        redis::cmd("MGET").arg(&["key1", "key2"]).query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

fn test_getset() {
    info!("test_getset");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    for _ in 1..10000 {
        redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg("foo").query(&mut con), Ok(42));

        redis::cmd("SET").arg("bar").arg("foo").execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg("bar").query(&mut con),
            Ok(b"foo".to_vec())
        );
    }
}

fn test_incr() {
    info!("test_incr");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
    assert_eq!(redis::cmd("INCR").arg("foo").query(&mut con), Ok(43usize));
}

fn test_info() {
    info!("test_info");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let info: redis::InfoDict = redis::cmd("INFO").query(&mut con).unwrap();
    assert_eq!(
        info.find(&"role"),
        Some(&redis::Value::Status("master".to_string()))
    );
    assert_eq!(info.get("role"), Some("master".to_string()));
    assert_eq!(info.get("loading"), Some(false));
    assert!(!info.is_empty());
    assert!(info.contains_key(&"role"));
}

fn test_hash_ops() {
    info!("test_hash_ops");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("HSET")
        .arg("foo")
        .arg("key_1")
        .arg(1)
        .execute(&mut con);
    redis::cmd("HSET")
        .arg("foo")
        .arg("key_2")
        .arg(2)
        .execute(&mut con);

    let h: HashMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&mut con).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));

    let h: BTreeMap<String, i32> = redis::cmd("HGETALL").arg("foo").query(&mut con).unwrap();
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
}

fn test_set_ops() {
    info!("test_set_ops");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SADD").arg("foo").arg(1).execute(&mut con);
    redis::cmd("SADD").arg("foo").arg(2).execute(&mut con);
    redis::cmd("SADD").arg("foo").arg(3).execute(&mut con);

    let mut s: Vec<i32> = redis::cmd("SMEMBERS").arg("foo").query(&mut con).unwrap();
    s.sort_unstable();
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);

    let set: HashSet<i32> = redis::cmd("SMEMBERS").arg("foo").query(&mut con).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));

    let set: BTreeSet<i32> = redis::cmd("SMEMBERS").arg("foo").query(&mut con).unwrap();
    assert_eq!(set.len(), 3);
    assert!(set.contains(&1i32));
    assert!(set.contains(&2i32));
    assert!(set.contains(&3i32));
}

fn test_scan() {
    info!("test_scan");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SADD").arg("foo").arg(1).execute(&mut con);
    redis::cmd("SADD").arg("foo").arg(2).execute(&mut con);
    redis::cmd("SADD").arg("foo").arg(3).execute(&mut con);

    let (cur, mut s): (i32, Vec<i32>) = redis::cmd("SSCAN")
        .arg("foo")
        .arg(0)
        .query(&mut con)
        .unwrap();
    s.sort_unstable();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

fn test_optionals() {
    info!("test_optionals");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET").arg("foo").arg(1).execute(&mut con);

    let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg("foo")
        .arg("missing")
        .query(&mut con)
        .unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);

    let a = redis::cmd("GET")
        .arg("missing")
        .query(&mut con)
        .unwrap_or(0i32);
    assert_eq!(a, 0i32);
}

fn test_scanning() {
    info!("test_scanning");
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let mut unseen = HashSet::new();

    for x in 0..1000 {
        let _a: i64 = redis::cmd("SADD")
            .arg("foo")
            .arg(x)
            .query(&mut con)
            .unwrap();
        unseen.insert(x);
    }

    assert_eq!(unseen.len(), 1000);

    let iter = redis::cmd("SSCAN")
        .arg("foo")
        .cursor_arg(0)
        .clone()
        .iter(&mut con)
        .unwrap();

    for x in iter {
        // type inference limitations
        let x: usize = x;
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

fn test_filtered_scanning() {
    info!("test_filtered_scanning");
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let mut unseen = HashSet::new();

    for x in 0..3000 {
        let _: () = con
            .hset("foo", format!("key_{}_{}", x % 100, x), x)
            .unwrap();
        if x % 100 == 0 {
            unseen.insert(x);
        }
    }

    let iter = con.hscan_match("foo", "key_0_*").unwrap();

    for x in iter {
        // type inference limitations
        let x: usize = x;
        unseen.remove(&x);
    }

    assert_eq!(unseen.len(), 0);
}

fn test_pipeline_error() {
    info!("test_pipeline_error");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

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
        .query(&mut con)
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

    let _r = con.send_packed_command(&packed); // Don't unwrap the results as the driver will throw an exception and disconnect

    assert_eq!(con.recv_response(), Ok(redis::Value::Okay));

    assert_eq!(
        con.recv_response(),
        Err::<Value, RedisError>(RedisError::from((
            ErrorKind::ResponseError,
            "An error was signalled by the server",
            "unknown command `SESDFSDFSDFT`, with args beginning with: `k{x}ey_2`, `43`, "
                .to_string()
        )))
    );
    assert_eq!(con.recv_response(), Ok(redis::Value::Data(Vec::from("42"))));
}

fn test_pipeline() {
    info!("test_pipeline");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

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
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

fn test_empty_pipeline() {
    info!("test_empty_pipeline");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let _: () = redis::pipe().cmd("PING").ignore().query(&mut con).unwrap();

    let _: () = redis::pipe().query(&mut con).unwrap();
}

fn test_pipeline_transaction() {
    info!("test_pipeline_transaction");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

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
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

fn test_pipeline_reuse_query() {
    info!("test_pipeline_reuse_query");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let mut pl = redis::pipe();

    let ((k1,),): ((i32,),) = pl
        .cmd("SET")
        .arg("p{x}key_1")
        .arg(42)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);

    redis::cmd("DEL").arg("p{x}key_1").execute(&mut con);

    // The internal commands vector of the pipeline still contains the previous commands.
    let ((k1,), (k2, k3)): ((i32,), (i32, i32)) = pl
        .cmd("SET")
        .arg("p{x}key_2")
        .arg(43)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .arg(&["p{x}key_2"])
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 42);
    assert_eq!(k3, 43);
}

fn test_pipeline_reuse_query_clear() {
    info!("test_pipeline_reuse_query_clear");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let mut pl = redis::pipe();

    let ((k1,),): ((i32,),) = pl
        .cmd("SET")
        .arg("p{x}key_1")
        .arg(44)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .query(&mut con)
        .unwrap();
    pl.clear();

    assert_eq!(k1, 44);

    redis::cmd("DEL").arg("p{x}key_1").execute(&mut con);

    let ((k1, k2),): ((bool, i32),) = pl
        .cmd("SET")
        .arg("p{x}key_2")
        .arg(45)
        .ignore()
        .cmd("MGET")
        .arg(&["p{x}key_1"])
        .arg(&["p{x}key_2"])
        .query(&mut con)
        .unwrap();
    pl.clear();

    assert_eq!(k1, false);
    assert_eq!(k2, 45);
}

fn test_real_transaction() {
    info!("test_real_transaction");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let key = "the_key";
    let _: () = redis::cmd("SET").arg(key).arg(42).query(&mut con).unwrap();

    loop {
        let _: () = redis::cmd("WATCH").arg(key).query(&mut con).unwrap();
        let val: isize = redis::cmd("GET").arg(key).query(&mut con).unwrap();
        let response: Option<(isize,)> = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query(&mut con)
            .unwrap();

        match response {
            None => {
                continue;
            }
            Some(response) => {
                assert_eq!(response, (43,));
                break;
            }
        }
    }
}

fn test_real_transaction_highlevel() {
    info!("test_real_transaction_highlevel");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let key = "the_key";
    let _: () = redis::cmd("SET").arg(key).arg(42).query(&mut con).unwrap();

    let response: (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
        let val: isize = redis::cmd("GET").arg(key).query(con)?;
        pipe.cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query(con)
    })
    .unwrap();

    assert_eq!(response, (43,));
}

fn test_script() {
    info!("test_script");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let script = redis::Script::new(
        r"
       return {redis.call('GET', KEYS[1]), ARGV[1]}
    ",
    );

    let _: () = redis::cmd("SET")
        .arg("my_key")
        .arg("foo")
        .query(&mut con)
        .unwrap();
    let response = script.key("my_key").arg(42).invoke(&mut con);

    assert_eq!(response, Ok(("foo".to_string(), 42)));
}

fn test_tuple_args() {
    info!("test_tuple_args");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("HMSET")
        .arg("my_key")
        .arg(&[("field_1", 42), ("field_2", 23)])
        .execute(&mut con);

    assert_eq!(
        redis::cmd("HGET")
            .arg("my_key")
            .arg("field_1")
            .query(&mut con),
        Ok(42)
    );
    assert_eq!(
        redis::cmd("HGET")
            .arg("my_key")
            .arg("field_2")
            .query(&mut con),
        Ok(23)
    );
}

fn test_nice_api() {
    info!("test_nice_api");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.set("my_key", 42), Ok(()));
    assert_eq!(con.get("my_key"), Ok(42));

    let (k1, k2): (i32, i32) = redis::pipe()
        .atomic()
        .set("key_1", 42)
        .ignore()
        .set("key_2", 43)
        .ignore()
        .get("key_1")
        .get("key_2")
        .query(&mut con)
        .unwrap();

    assert_eq!(k1, 42);
    assert_eq!(k2, 43);
}

fn test_auto_m_versions() {
    info!("test_auto_m_versions");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.set_multiple(&[("key1", 1), ("key2", 2)]), Ok(()));
    assert_eq!(con.get(&["key1", "key2"]), Ok((1, 2)));
}

fn test_nice_hash_api() {
    info!("test_nice_hash_api");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(
        con.hset_multiple("my_hash", &[("f1", 1), ("f2", 2), ("f3", 4), ("f4", 8)]),
        Ok(())
    );

    let hm: HashMap<String, isize> = con.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let hm: BTreeMap<String, isize> = con.hgetall("my_hash").unwrap();
    assert_eq!(hm.get("f1"), Some(&1));
    assert_eq!(hm.get("f2"), Some(&2));
    assert_eq!(hm.get("f3"), Some(&4));
    assert_eq!(hm.get("f4"), Some(&8));
    assert_eq!(hm.len(), 4);

    let v: Vec<(String, isize)> = con.hgetall("my_hash").unwrap();
    assert_eq!(
        v,
        vec![
            ("f1".to_string(), 1),
            ("f2".to_string(), 2),
            ("f3".to_string(), 4),
            ("f4".to_string(), 8),
        ]
    );

    assert_eq!(con.hget("my_hash", &["f2", "f4"]), Ok((2, 8)));
    assert_eq!(con.hincr("my_hash", "f1", 1), Ok(2));
    assert_eq!(con.hincr("my_hash", "f2", 1.5f32), Ok(3.5f32));
    assert_eq!(con.hexists("my_hash", "f2"), Ok(true));
    assert_eq!(con.hdel("my_hash", &["f1", "f2"]), Ok(()));
    assert_eq!(con.hexists("my_hash", "f2"), Ok(false));

    let iter: redis::Iter<'_, (String, isize)> = con.hscan("my_hash").unwrap();
    let mut found = HashSet::new();
    for item in iter {
        found.insert(item);
    }

    assert_eq!(found.len(), 2);
    assert_eq!(found.contains(&("f3".to_string(), 4)), true);
    assert_eq!(found.contains(&("f4".to_string(), 8)), true);
}

fn test_nice_list_api() {
    info!("test_nice_list_api");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.rpush("my_list", &[1, 2, 3, 4]), Ok(4));
    assert_eq!(con.rpush("my_list", &[5, 6, 7, 8]), Ok(8));
    assert_eq!(con.llen("my_list"), Ok(8));

    assert_eq!(con.lpop("my_list"), Ok(1));
    assert_eq!(con.llen("my_list"), Ok(7));

    assert_eq!(con.lrange("my_list", 0, 2), Ok((2, 3, 4)));

    assert_eq!(con.lset("my_list", 0, 4), Ok(true));
    assert_eq!(con.lrange("my_list", 0, 2), Ok((4, 3, 4)));
}

fn test_tuple_decoding_regression() {
    info!("test_tuple_decoding_regression");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.del("my_zset"), Ok(()));
    assert_eq!(con.zadd("my_zset", "one", 1), Ok(1));
    assert_eq!(con.zadd("my_zset", "two", 2), Ok(1));

    let vec: Vec<(String, u32)> = con.zrangebyscore_withscores("my_zset", 0, 10).unwrap();
    assert_eq!(vec.len(), 2);

    assert_eq!(con.del("my_zset"), Ok(1));

    let vec: Vec<(String, u32)> = con.zrangebyscore_withscores("my_zset", 0, 10).unwrap();
    assert_eq!(vec.len(), 0);
}

fn test_bit_operations() {
    info!("test_bit_operations");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    assert_eq!(con.setbit("bitvec", 10, true), Ok(false));
    assert_eq!(con.getbit("bitvec", 10), Ok(true));
}

fn test_cluster_basics() {
    info!("test_cluster_basics");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut con);
    info!("one");
    redis::cmd("SET").arg(&["{x}key2", "bar"]).execute(&mut con);
    info!("two");

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

fn test_cluster_eval() {
    info!("test_cluster_eval");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

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
        .query(&mut con);

    assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
}

#[allow(dead_code)]
fn test_cluster_script() {
    info!("test_cluster_script");
    let ctx = TestContext::new();
    let mut con = ctx.connection();

    let script = redis::Script::new(
        r#"
        redis.call("SET", KEYS[1], "1");
        redis.call("SET", KEYS[2], "2");
        return redis.call("MGET", KEYS[1], KEYS[2]);
    "#,
    );

    let rv = script.key("{x}a").key("{x}b").invoke(&mut con);
    assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
}

#[test]
fn test_pass_through() -> Result<()> {
    try_register_cleanup();
    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();
    let compose_config = "examples/redis-passthrough/docker-compose.yml".to_string();
    load_docker_compose(compose_config)?;
    run_all("examples/redis-passthrough/config.yaml".to_string())?;
    Ok(())
}

// #[test]
#[allow(dead_code)]
fn test_pass_through_one() -> Result<()> {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-passthrough/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();
    let compose_config = "examples/redis-passthrough/docker-compose.yml".to_string();
    load_docker_compose(compose_config)?;

    test_real_transaction();

    Ok(())
}

#[test]
fn test_active_active_redis() -> Result<()> {
    try_register_cleanup();
    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();
    let compose_config = "examples/redis-multi/docker-compose.yml".to_string();
    load_docker_compose(compose_config)?;
    run_all_active_safe("examples/redis-multi/config.yaml".to_string())?;
    Ok(())
}

#[test]
// #[allow(dead_code)]
fn test_active_one_active_redis() -> Result<()> {
    try_register_cleanup();
    let compose_config = "examples/redis-multi/docker-compose.yml".to_string();
    load_docker_compose(compose_config)?;

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-multi/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    // test_args();
    test_cluster_basics();

    // test_pipeline();
    test_getset();

    Ok(())
}

#[test]
// #[allow(dead_code)]
fn test_pass_redis_cluster_one() -> Result<()> {
    let compose_config = "examples/redis-cluster/docker-compose.yml".to_string();
    load_docker_compose(compose_config)?;

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-cluster/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    // test_args()test_args;
    test_pipeline_error(); //TODO: script does not seem to be loading in the server?

    Ok(())
}

// TODO Re-enable Redis Auth support
// #[test]
fn _test_cluster_auth_redis() -> Result<()> {
    info!("test_cluster_auth_redis");
    try_register_cleanup();
    let compose_config = "examples/redis-cluster-auth/docker-compose.yml".to_string();
    load_docker_compose(compose_config)?;
    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-cluster-auth/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let ctx = TestContext::new_auth();
    let mut con = ctx.connection();

    redis::cmd("SET")
        .arg("{x}key1")
        .arg(b"foo")
        .execute(&mut con);
    redis::cmd("SET").arg(&["{x}key2", "bar"]).execute(&mut con);

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["{x}key1", "{x}key2"])
            .query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );

    // create a user, auth as them, try to set a key but should fail as they have no access
    redis::cmd("ACL")
        .arg(&["SETUSER", "testuser", "+@read", "on", ">password"])
        .execute(&mut con);
    redis::cmd("AUTH")
        .arg("testuser")
        .arg("password")
        .execute(&mut con);
    if let Ok(_s) = redis::cmd("SET")
        .arg("{x}key2")
        .arg("fail")
        .query::<String>(&mut con)
    {
        panic!("This should fail!")
    }
    // assert_eq!(
    //     redis::cmd("GET").arg("{x}key2").query(&mut con),
    //     Ok("bar".to_string())
    // );

    // set auth context back to default user using non acl style auth command
    redis::cmd("AUTH").arg("shotover").execute(&mut con);
    redis::cmd("SET")
        .arg("{x}key3")
        .arg(b"food")
        .execute(&mut con);

    assert_eq!(
        redis::cmd("GET").arg("{x}key3").query(&mut con),
        Ok("food".to_string())
    );

    Ok(())
}

#[test]
fn test_cluster_all_redis() -> Result<()> {
    try_register_cleanup();
    let compose_config = "examples/redis-cluster/docker-compose.yml".to_string();
    load_docker_compose(compose_config)?;
    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();
    // panic!("Loooks like we are getting some out of order issues with pipelined request");
    run_all_cluster_safe("examples/redis-cluster/config.yaml".to_string())?;
    Ok(())
}

#[test]
fn test_cluster_all_script_redis() -> Result<()> {
    try_register_cleanup();
    let compose_config = "examples/redis-cluster/docker-compose.yml".to_string();
    load_docker_compose(compose_config)?;
    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-cluster/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });
    // panic!("Loooks like we are getting some out of order issues with pipelined request");
    for _i in 0..1999 {
        test_script();
    }
    Ok(())
}

#[test]
fn test_cluster_all_pipeline_safe_redis() -> Result<()> {
    info!("test_cluster_all_pipeline_safe_redis");

    try_register_cleanup();
    let compose_config = "examples/redis-cluster/docker-compose.yml".to_string();

    load_docker_compose(compose_config)?;
    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();

    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file("examples/redis-cluster/config.yaml".to_string())
                .unwrap()
                .run_chains()
                .await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    info!("Starting test");

    let ctx = TestContext::new();
    let mut con = ctx.connection();

    test_cluster_script();
    test_script();
    test_cluster_script();

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
            .query(&mut con)
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

        let _: Vec<String> = pipe.query(&mut con).unwrap();

        let mut pipe = redis::pipe();

        for i in 0..1000 {
            let key1 = format!("{}kaey", i);
            pipe.cmd("GET").arg(&key1);
        }

        let mut results: Vec<i32> = pipe.query(&mut con).unwrap();

        for i in 0..1000 {
            let result = results.remove(0);
            assert_eq!(i, result);
        }
    }

    test_cluster_basics();
    test_cluster_eval();
    test_cluster_script(); //TODO: script does not seem to be loading in the server?
    test_getset();
    test_incr();
    // test_info();
    // test_hash_ops();
    test_set_ops();
    test_scan();
    // test_optionals();
    test_pipeline_error();
    test_scanning();
    test_filtered_scanning();
    test_pipeline(); // NGET Issues
    test_empty_pipeline();
    // TODO: Pipeline transactions currently don't work (though it tries very hard)
    // Current each cmd in a pipeline is treated as a single request, which means on a cluster
    // basis they end up getting routed to different masters. This results in very occasionally will
    // the transaction resolve (the exec and the multi both go to the right server).
    // test_pipeline_transaction();
    test_pipeline_reuse_query();
    test_pipeline_reuse_query_clear();
    // test_real_transaction();
    // test_real_transaction_highlevel();
    test_script();
    test_tuple_args();
    // test_nice_api();
    // test_auto_m_versions();
    test_nice_hash_api();
    test_nice_list_api();
    test_tuple_decoding_regression();
    test_bit_operations();

    Ok(())
}

fn run_all_active_safe(config: String) -> Result<()> {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file(config).unwrap().run_chains().await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    test_cluster_basics();
    test_cluster_eval();
    test_cluster_script(); //TODO: script does not seem to be loading in the server?
                           // test_cluster_pipeline(); // we do support pipelining!!
    test_getset();
    test_incr();
    // test_info();
    // test_hash_ops();
    test_set_ops();
    test_scan();
    // test_optionals();
    // test_scanning(); // TODO scanning doesnt work
    // test_filtered_scanning();
    test_pipeline(); // NGET Issues
    test_empty_pipeline();
    // TODO: Pipeline transactions currently don't work (though it tries very hard)
    // Current each cmd in a pipeline is treated as a single request, which means on a cluster
    // basis they end up getting routed to different masters. This results in very occasionally will
    // the transaction resolve (the exec and the multi both go to the right server).
    // test_pipeline_transaction();
    // test_pipeline_reuse_query();
    // test_pipeline_reuse_query_clear();
    // test_real_transaction();
    // test_real_transaction_highlevel();
    test_script();
    test_tuple_args();
    // test_nice_api();
    // test_auto_m_versions();
    test_nice_hash_api();
    test_nice_list_api();
    test_tuple_decoding_regression();
    test_bit_operations();
    // test_invalid_protocol();
    Ok(())
}

fn run_all_cluster_safe(config: String) -> Result<()> {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file(config).unwrap().run_chains().await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    test_cluster_basics();
    test_cluster_eval();
    test_cluster_script(); //TODO: script does not seem to be loading in the server?
    test_getset();
    test_incr();
    // test_info();
    // test_hash_ops();
    test_set_ops();
    test_scan();
    // test_optionals();
    test_scanning();
    test_filtered_scanning();
    test_pipeline(); // NGET Issues
    test_empty_pipeline();
    // TODO: Pipeline transactions currently don't work (though it tries very hard)
    // Current each cmd in a pipeline is treated as a single request, which means on a cluster
    // basis they end up getting routed to different masters. This results in very occasionally will
    // the transaction resolve (the exec and the multi both go to the right server).
    // test_pipeline_transaction();
    test_pipeline_reuse_query();
    test_pipeline_reuse_query_clear();
    // test_real_transaction();
    // test_real_transaction_highlevel();
    test_script();
    test_tuple_args();
    // test_nice_api();
    // test_auto_m_versions();
    test_nice_hash_api();
    test_nice_list_api();
    test_tuple_decoding_regression();
    test_bit_operations();
    // test_invalid_protocol();
    Ok(())
}

fn run_all(config: String) -> Result<()> {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .worker_threads(4)
        .build()
        .unwrap();
    let _jh: _ = rt.spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) =
            Topology::from_file(config).unwrap().run_chains().await
        {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok::<(), anyhow::Error>(())
    });

    test_args();
    test_getset();
    test_incr();
    test_info();
    test_hash_ops();
    test_set_ops();
    test_scan();
    test_optionals();
    test_scanning();
    test_filtered_scanning();
    test_pipeline();
    test_empty_pipeline();
    test_pipeline_transaction();
    test_pipeline_reuse_query();
    test_pipeline_reuse_query_clear();
    test_real_transaction();
    test_real_transaction_highlevel();
    // test_pubsub();
    // test_pubsub_unsubscribe();
    // test_pubsub_unsubscribe_no_subs();
    // test_pubsub_unsubscribe_one_sub();
    // test_pubsub_unsubscribe_one_sub_one_psub();
    // scoped_pubsub();
    test_script();
    test_tuple_args();
    test_nice_api();
    test_auto_m_versions();
    test_nice_hash_api();
    test_nice_list_api();
    test_tuple_decoding_regression();
    test_bit_operations();
    // test_invalid_protocol();
    Ok(())
}
