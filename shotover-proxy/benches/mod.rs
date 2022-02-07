use cassandra_cpp::Session;
pub use test_helpers::docker_compose::DockerCompose;

#[path = "../tests/helpers/mod.rs"]
mod helpers;
pub use helpers::ShotoverManager;

pub struct BenchResources {
    _compose: DockerCompose,
    _shotover_manager: ShotoverManager,
    redis: Option<redis::Connection>,
    cassandra: Option<Session>,
}

impl BenchResources {
    pub fn new_redis(shotover_manager: ShotoverManager, compose: DockerCompose) -> Self {
        let mut connection = shotover_manager.redis_connection(6379);
        redis::cmd("FLUSHDB").execute(&mut connection);

        Self {
            _compose: compose,
            _shotover_manager: shotover_manager,
            redis: Some(connection),
            cassandra: None,
        }
    }

    pub fn new_cassandra(shotover_manager: ShotoverManager, compose: DockerCompose) -> Self {
        let connection = shotover_manager.cassandra_connection("127.0.0.1", 9043);

        Self {
            _compose: compose,
            _shotover_manager: shotover_manager,
            cassandra: Some(connection),
            redis: None,
        }
    }

    pub fn redis(&mut self) -> &mut redis::Connection {
        self.redis.as_mut().unwrap()
    }

    pub fn cassandra(&mut self) -> &mut Session {
        self.cassandra.as_mut().unwrap()
    }
}
