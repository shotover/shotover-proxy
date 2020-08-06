use anyhow::Result;
use instaproxy::config::topology::Topology;
use proptest::prelude::*;
use redis::{Commands, Connection, FromRedisValue, RedisFuture, RedisResult, ToRedisArgs};
use std::error::Error;
use std::process::Command;
use std::{thread, time};
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tracing::info;
use tracing::Level;

const lua1: &str = r###"
return {KEYS[1],ARGV[1],ARGV[2]}
"###;

const lua2: &str = r###"
return {KEYS[1],ARGV[1],ARGV[2]}
"###;

fn start_proxy(config: String) -> JoinHandle<Result<()>> {
    let _subscriber = tracing_subscriber::fmt()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::DEBUG)
        // completes the builder and sets the constructed `Subscriber` as the default.
        .init();

    return tokio::spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) = Topology::from_file(config)?.run_chains().await {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok(())
    });
}

fn get_redis_conn() -> Result<Connection> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    Ok(client.get_connection()?)
}

/*
[0 127.0.0.1:52827] "TTL" "demo-36:channel1:user2"
[0 127.0.0.1:52827] "EVALSHA" "b69f00dd93d2e307021730efc81ead4fb194181d" "1" "c:demo-36:channel1" "640"
"user2" "1509861014.276593"
[0 lua] "zadd" "c:demo-36:channel1" "1509861014.276593" "user2"
[0 lua] "zcard" "c:demo-36:channel1"
[0 lua] "expire" "c:demo-36:channel1" "640"
[0 127.0.0.1:52827] "EVALSHA" "52b7ca41781c75791909c2b6d372bc34dff3b532" "1" "updates" "demo-36:channel1"
"1509861014.276593"
[0 lua] "zscore" "updates" "demo-36:channel1"
[0 127.0.0.1:52827] "SADD" "demo-36:uuids:user2" "channel1"
[0 127.0.0.1:52827] "EXPIRE" "demo-36:uuids:user2" "640"
[0 127.0.0.1:52827] "SADD" "channels:demo-36" "channel1"
[0 127.0.0.1:52827] "EXPIRE" "channels:demo-36" "640"

Update/Ping

[0 127.0.0.1:52827] "TTL" "demo-36:channel1:user2"
[0 127.0.0.1:52827] "EVALSHA" "b69f00dd93d2e307021730efc81ead4fb194181d" "1" "c:demo-36:channel1" "640"
"user2" "1509861050.767142"
[0 lua] "zadd" "c:demo-36:channel1" "1509861050.767142" "user2"
[0 lua] "expire" "c:demo-36:channel1" "640"
[0 127.0.0.1:52827] "EXPIRE" "demo-36:uuids:user2" "640"
[0 127.0.0.1:52827] "EXPIRE" "channels:demo-36" "640"
 */

fn load_lua<RV>(con: &mut redis::Connection, script: String) -> RedisResult<RV>
where
    RV: FromRedisValue,
{
    let mut command = redis::cmd("SCRIPT");
    command.arg("LOAD");
    command.arg(script.as_str());

    command.query(con)
}

fn run_register_flow<BK, BCK, BKU>(
    connection: &mut Connection,
    build_key: BK,
    build_c_key: BCK,
    build_key_user: BKU,
    channel: &str,
) -> Result<()>
where
    BK: Fn() -> String,
    BCK: Fn() -> String,
    BKU: Fn() -> String,
{
    let func_sha1: String = load_lua(connection, lua1.to_string())?;
    let func_sha2: String = load_lua(connection, lua2.to_string())?;

    let time: usize = 640;
    // panic!("Currently consistent scatter doesn't drain the late response when we are doing N < M required responses");
    // panic!("This test only works with N == M replicas are set as the consistency level");
    // Maybe some sort of response tagging so the unifier knows what to discard???? but then things get weird.

    info!("Loaded lua scripts -> {} and {}", func_sha1, func_sha2);

    let f: i32 = connection.ttl(build_key())?; // TODO: Should be ttl in seconds
    info!("---> {}", f);

    let test: RedisResult<String> = redis::cmd("EVALSHA")
        .arg(func_sha1.to_string())
        .arg(1)
        .arg(build_c_key().as_str())
        .arg(640)
        .arg("user2")
        .arg("1509861014.276593")
        .query(connection);

    let _ = redis::cmd("EVALSHA")
        .arg(func_sha2.to_string())
        .arg("52b7ca41781c75791909c2b6d372bc34dff3b532")
        .arg(1)
        .arg("updates")
        .arg(build_key().as_str())
        .arg("1509861014.276593")
        .query(connection)?;

    let _: i32 = connection.sadd(build_key_user(), channel)?;
    let _: i32 = connection.expire(build_key_user(), time)?;
    let _: i32 = connection.sadd(build_key(), channel)?;
    let _: i32 = connection.expire(build_key(), time)?;
    Ok(())
}

fn run_basic_pipelined(connection: &mut Connection) -> Result<()> {
    let pipel: Vec<i64> = redis::pipe()
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .query(connection)?;
    info!("Got the following {:#?}", pipel);
    let pipel: Vec<i64> = redis::pipe()
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .query(connection)?;
    info!("Got the following {:#?}", pipel);
    let pipel: Vec<i64> = redis::pipe()
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .cmd("INCR")
        .arg("key")
        .query(connection)?;
    info!("Got the following {:#?}", pipel);
    Ok(())
}

fn run_register_flow_pipelined<BK, BCK, BKU>(
    connection: &mut Connection,
    build_key: BK,
    build_c_key: BCK,
    build_key_user: BKU,
    channel: &str,
) -> Result<()>
where
    BK: Fn() -> String,
    BCK: Fn() -> String,
    BKU: Fn() -> String,
{
    let func_sha1 = "TODOSOMESHA";
    let func_sha2 = "TODOSOMESHA";
    let time: usize = 640;

    let pipel = redis::pipe()
        .ttl(build_key())
        .ignore()
        .cmd("EVALSHA")
        .arg(func_sha1.to_string())
        .arg("1")
        .arg(build_c_key().as_str())
        .arg("640")
        .arg("user2")
        .arg("1509861014.276593")
        .ignore()
        .cmd("EVALSHA")
        .arg(func_sha2.to_string())
        .arg("52b7ca41781c75791909c2b6d372bc34dff3b532")
        .arg("1")
        .arg("updates")
        .arg(build_key().as_str())
        .arg("1509861014.276593")
        .ignore()
        .sadd(build_key_user(), channel)
        .ignore()
        .expire(build_key_user(), time)
        .ignore()
        .sadd(build_key(), channel)
        .ignore()
        .expire(build_key(), time)
        .ignore()
        .query(connection)?;

    info!("pipelined --");

    Ok(())
}

#[test]
fn test_presence_fresh_join_single_workflow() -> Result<()> {
    let subkey = "demo-36";
    let channel = "channel1";
    let user = "user2";

    let build_key = || -> String { format!("{}:{}", subkey, channel) };

    let build_c_key = || -> String { format!("c:{}:{}", subkey, channel) };

    let build_key_user = || -> String { format!("{}:{}:{}", subkey, channel, user) };
    let mut rt = runtime::Builder::new()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .threaded_scheduler()
        .core_threads(4)
        .build()
        .unwrap();

    let delaytime = time::Duration::from_secs(3);

    rt.block_on(async {
        let jh = start_proxy("examples/redis-multi/config.yaml".to_string());

        thread::sleep(delaytime);

        let mut connection = get_redis_conn().unwrap();

        // run_register_flow_pipelined(
        //     &mut connection,
        //     build_key,
        //     build_c_key,
        //     build_key_user,
        //     channel,
        // )
        // .unwrap();

        run_register_flow(
            &mut connection,
            build_key,
            build_c_key,
            build_key_user,
            channel,
        )
        .unwrap();
    });

    let delaytime = time::Duration::from_secs(3);

    rt.shutdown_timeout(delaytime);

    Ok(())
}

#[test]
fn test_simple_pipeline_workflow() -> Result<()> {
    let mut rt = runtime::Builder::new()
        .enable_all()
        .thread_name("RPProxy-Thread")
        .threaded_scheduler()
        .core_threads(4)
        .build()
        .unwrap();

    let delaytime = time::Duration::from_secs(2);

    rt.block_on(async {
        let jh = start_proxy("examples/redis-multi/config.yaml".to_string());

        thread::sleep(delaytime);

        let mut connection = get_redis_conn().unwrap();

        run_basic_pipelined(&mut connection).unwrap();
    });

    let delaytime = time::Duration::from_secs(3);

    rt.shutdown_timeout(delaytime);

    Ok(())
}
