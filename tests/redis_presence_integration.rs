use anyhow::Result;
use redis::{Commands, Connection, FromRedisValue, RedisResult};
use shotover_proxy::config::topology::Topology;
use std::{thread, time};
use tokio::task::JoinHandle;
use tracing::{info, Level};

const LUA1: &str = r###"
return {KEYS[1],ARGV[1],ARGV[2]}
"###;

const LUA2: &str = r###"
return {KEYS[1],ARGV[1],ARGV[2]}
"###;

pub fn start_proxy(config: String) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        if let Ok((_, mut shutdown_complete_rx)) = Topology::from_file(config)?.run_chains().await {
            //TODO: probably a better way to handle various join handles / threads
            let _ = shutdown_complete_rx.recv().await;
        }
        Ok(())
    })
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
    let time: usize = 640;

    let func_sha1 = redis::Script::new(LUA1);
    let _some: RedisResult<Vec<String>> = func_sha1.invoke(connection);

    let func_sha2 = redis::Script::new(LUA2);
    let _other: RedisResult<Vec<String>> = func_sha1.invoke(connection);

    let pipe: RedisResult<String> = redis::pipe()
        .ttl(build_key())
        .ignore()
        .cmd("EVALSHA")
        .arg(func_sha1.get_hash())
        .arg(1)
        .arg(build_c_key().as_str())
        .arg(640)
        .arg("user2")
        .arg("1509861014.276593")
        .ignore()
        .cmd("EVALSHA")
        .arg(func_sha2.get_hash())
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
        .query(connection);

    info!("pipelined -- {:?}", pipe);

    pipe?;

    Ok(())
}

fn get_redis_conn() -> Result<Connection> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    Ok(client.get_connection()?)
}

async fn test_presence_fresh_join_single_workflow() -> Result<()> {
    let subkey = "demo-36";
    let channel = "channel1";
    let user = "user2";

    let build_key = || -> String { format!("{}:{}", subkey, channel) };

    let build_c_key = || -> String { format!("c:{}:{}", subkey, channel) };

    let build_key_user = || -> String { format!("{}:{}:{}", subkey, channel, user) };

    let connection = &mut get_redis_conn().unwrap();

    let func_sha1 = redis::Script::new(LUA1);
    let _some: RedisResult<Vec<String>> = func_sha1.invoke(connection);

    let func_sha2 = redis::Script::new(LUA2);
    let _other: RedisResult<Vec<String>> = func_sha1.invoke(connection);

    let time: usize = 640;

    info!("Loaded lua scripts -> {:?} and {:?}", func_sha1, func_sha2);

    let f_ttl: i32 = connection.ttl(build_key()).unwrap(); // TODO: Should be ttl in seconds
    info!("---> {}", f_ttl);

    let _test1: RedisResult<String> = redis::cmd("EVALSHA")
        .arg(func_sha1.get_hash())
        .arg(1)
        .arg(build_c_key().as_str())
        .arg(640)
        .arg("user2")
        .arg("1509861014.276593")
        .query(connection);

    let _test2: RedisResult<String> = redis::cmd("EVALSHA")
        .arg(func_sha2.get_hash())
        .arg(1)
        .arg("updates")
        .arg(build_key().as_str())
        .arg("1509861014.276593")
        .query(connection);

    let sadd: i32 = connection.sadd(build_key_user(), channel)?;
    let expire: i32 = connection.expire(build_key_user(), time)?;
    let sadd2: i32 = connection.sadd(build_key(), channel)?;
    let expire2: i32 = connection.expire(build_key(), time)?;

    info!("Got the following {:?}", sadd);
    info!("Got the following {:?}", expire);
    info!("Got the following {:?}", sadd2);
    info!("Got the following {:?}", expire2);

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

// #[tokio::test(threaded_scheduler)]
async fn run_all() -> Result<()> {
    let delaytime = time::Duration::from_secs(2);
    let _subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .try_init();
    let _jh = start_proxy("examples/redis-multi/config.yaml".to_string());
    thread::sleep(delaytime);
    test_simple_pipeline_workflow().await?;
    test_presence_fresh_join_pipeline_workflow().await?;
    test_presence_fresh_join_single_workflow().await?;
    Ok(())
}
