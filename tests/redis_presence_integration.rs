use anyhow::Result;
use instaproxy::config::topology::Topology;
use redis::{Commands, Connection, FromRedisValue, RedisResult};
use std::{thread, time};
use tokio::task::JoinHandle;
use tracing::info;

const LUA1: &str = r###"
return {KEYS[1],ARGV[1],ARGV[2]}
"###;

const LUA2: &str = r###"
return {KEYS[1],ARGV[1],ARGV[2]}
"###;

pub fn start_proxy(config: String) -> JoinHandle<Result<()>> {
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

fn load_lua<RV>(con: &mut redis::Connection, script: String) -> RedisResult<RV>
where
    RV: FromRedisValue,
{
    let mut command = redis::cmd("SCRIPT");
    command.arg("LOAD");
    command.arg(script.as_str());

    command.query(con)
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

async fn run_register_flow_pipelined<BK, BCK, BKU>(
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
    let func_sha1: String = load_lua(connection, LUA1.to_string()).unwrap();
    let func_sha2: String = load_lua(connection, LUA2.to_string()).unwrap();
    let time: usize = 640;

    let _pipel = redis::pipe()
        .ttl(build_key())
        .ignore()
        .cmd("EVALSHA")
        .arg(func_sha1.to_string())
        .arg(1)
        .arg(build_c_key().as_str())
        .arg(640)
        .arg("user2")
        .arg("1509861014.276593")
        .ignore()
        .cmd("EVALSHA")
        .arg(func_sha2.to_string())
        .arg(1)
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

async fn test_presence_fresh_join_single_workflow() -> Result<()> {
    let subkey = "demo-36";
    let channel = "channel1";
    let user = "user2";

    let build_key = || -> String { format!("{}:{}", subkey, channel) };

    let build_c_key = || -> String { format!("c:{}:{}", subkey, channel) };

    let build_key_user = || -> String { format!("{}:{}:{}", subkey, channel, user) };

    let connection = &mut get_redis_conn().unwrap();

    let func_sha1: String = load_lua(connection, LUA1.to_string()).unwrap();
    let func_sha2: String = load_lua(connection, LUA2.to_string()).unwrap();

    let time: usize = 640;

    info!("Loaded lua scripts -> {} and {}", func_sha1, func_sha2);

    let f: i32 = connection.ttl(build_key()).unwrap(); // TODO: Should be ttl in seconds
    info!("---> {}", f);

    let _test1: RedisResult<String> = redis::cmd("EVALSHA")
        .arg(func_sha1.to_string())
        .arg(1)
        .arg(build_c_key().as_str())
        .arg(640)
        .arg("user2")
        .arg("1509861014.276593")
        .query(connection);

    let _test2: RedisResult<String> = redis::cmd("EVALSHA")
        .arg(func_sha2.to_string())
        .arg(1)
        .arg("updates")
        .arg(build_key().as_str())
        .arg("1509861014.276593")
        .query(connection);

    let a: i32 = connection.sadd(build_key_user(), channel)?;
    let b: i32 = connection.expire(build_key_user(), time)?;
    let c: i32 = connection.sadd(build_key(), channel)?;
    let d: i32 = connection.expire(build_key(), time)?;

    info!("Got the following {:?}", a);
    info!("Got the following {:?}", b);
    info!("Got the following {:?}", c);
    info!("Got the following {:?}", d);

    Ok(())
}
async fn test_presence_fresh_join_pipeline_workflow() -> Result<()> {
    let subkey = "demo-36";
    let channel = "channel1";
    let user = "user2";

    let build_key = || -> String { format!("{}:{}", subkey, channel) };

    let build_c_key = || -> String { format!("c:{}:{}", subkey, channel) };

    let build_key_user = || -> String { format!("{}:{}:{}", subkey, channel, user) };

    let mut connection = get_redis_conn().unwrap();

    run_register_flow_pipelined(
        &mut connection,
        build_key,
        build_c_key,
        build_key_user,
        channel,
    )
    .await
    .unwrap();

    Ok(())
}

async fn test_simple_pipeline_workflow() -> Result<()> {
    let mut connection = get_redis_conn().unwrap();
    run_basic_pipelined(&mut connection).unwrap();
    Ok(())
}

#[tokio::test(threaded_scheduler)]
async fn run_all() -> Result<()> {
    let delaytime = time::Duration::from_secs(2);
    let _jh = start_proxy("examples/redis-multi/config.yaml".to_string());
    thread::sleep(delaytime);
    test_simple_pipeline_workflow().await?;
    test_presence_fresh_join_pipeline_workflow().await?;
    test_presence_fresh_join_single_workflow().await?;
    Ok(())
}
