use crate::{
    cloud::{
        CloudResources, CloudResourcesRequired, Ec2InstanceWithDocker, Ec2InstanceWithShotover,
        RunningShotover,
    },
    common::{self, Shotover},
    profilers::{self, CloudProfilerRunner, ProfilerRunner},
};
use anyhow::Result;
use async_trait::async_trait;
use aws_throwaway::Ec2Instance;
use cdrs_tokio::{
    cluster::{
        connection_pool::ConnectionPoolConfigBuilder,
        session::{
            Session as CdrsTokioSession, SessionBuilder as CdrsTokioSessionBuilder,
            TcpSessionBuilder,
        },
        NodeTcpConfigBuilder, TcpConnectionManager,
    },
    compression::Compression as CdrsCompression,
    frame::Version,
    load_balancing::RoundRobinLoadBalancingStrategy,
    query::{PreparedQuery as CdrsPrepared, QueryParamsBuilder, QueryValues},
    statement::StatementParams,
    transport::TransportTcp,
};
use itertools::Itertools;
use rand::prelude::*;
use scylla::{
    prepared_statement::PreparedStatement as ScyllaPrepared,
    transport::Compression as ScyllaCompression, Session as ScyllaSession,
    SessionBuilder as ScyllaSessionBuilder,
};
use shotover::{
    config::chain::TransformChainConfig,
    sources::SourceConfig,
    transforms::{
        cassandra::{
            sink_cluster::{CassandraSinkClusterConfig, ShotoverNode},
            sink_single::CassandraSinkSingleConfig,
        },
        debug::force_parse::DebugForceEncodeConfig,
        TransformConfig,
    },
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use test_helpers::{
    docker_compose::{docker_compose, DockerCompose},
    mock_cassandra::MockHandle,
    shotover_process::ShotoverProcessBuilder,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio_bin_process::bin_path;
use uuid::Uuid;
use windsock::{Bench, BenchParameters, BenchTask, Profiling, Report};

const ROW_COUNT: usize = 1000;

#[derive(Clone)]
pub enum Compression {
    None,
    Lz4,
}

#[derive(Clone)]
pub enum CassandraDb {
    Cassandra,
    Mocked,
}

enum CassandraDbInstance {
    #[expect(dead_code, reason = "must be held to delay drop")]
    Compose(DockerCompose),
    #[expect(dead_code)]
    Mocked(MockHandle),
}

#[derive(Clone, Copy, PartialEq)]
pub enum CassandraTopology {
    Single,
    Cluster3,
}

impl CassandraTopology {
    pub fn to_tag(self) -> (String, String) {
        (
            "topology".to_owned(),
            match self {
                CassandraTopology::Single => "single".to_owned(),
                CassandraTopology::Cluster3 => "cluster3".to_owned(),
            },
        )
    }
}

struct CoreCount {
    /// Cores to be assigned to the bench's tokio runtime
    bench: usize,
    /// Cores per shotover instance
    shotover: usize,
    /// Cores per DB instance
    cassandra: usize,
}

#[derive(Clone, PartialEq)]
pub enum Operation {
    ReadI64,
    WriteBlob,
}

impl Operation {
    async fn prepare(&self, session: &Arc<CassandraSession>, db: &CassandraDb) {
        if let CassandraDb::Cassandra = db {
            session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }").await;
            session.await_schema_agreement().await;
            session.query("DROP TABLE IF EXISTS ks.bench").await;
            session.await_schema_agreement().await;

            match self {
                Operation::ReadI64 => {
                    session
                        .query("CREATE TABLE ks.bench(id bigint PRIMARY KEY)")
                        .await;
                    session.await_schema_agreement().await;
                    tokio::time::sleep(Duration::from_secs(1)).await; //TODO: this should not be needed >:[

                    for i in 0..ROW_COUNT {
                        session
                            .query(&format!("INSERT INTO ks.bench(id) VALUES ({})", i))
                            .await;
                    }
                }
                Operation::WriteBlob => {
                    session
                        .query("CREATE TABLE ks.bench(id bigint PRIMARY KEY, data BLOB)")
                        .await;
                    session.await_schema_agreement().await;
                    tokio::time::sleep(Duration::from_secs(1)).await; //TODO: this should not be needed >:[
                }
            }
        }
    }

    async fn run(
        &self,
        session: &Arc<CassandraSession>,
        reporter: UnboundedSender<Report>,
        parameters: BenchParameters,
    ) {
        let bench_query = match self {
            Operation::ReadI64 => {
                session
                    .prepare("SELECT * FROM ks.bench WHERE id = :id")
                    .await
            }
            Operation::WriteBlob => {
                session
                    .prepare("INSERT INTO ks.bench (id, data) VALUES (:id, :data)")
                    .await
            }
        };

        let tasks = BenchTaskCassandra {
            session: session.clone(),
            query: bench_query,
            operation: self.clone(),
        }
        .spawn_tasks(reporter.clone(), parameters.operations_per_second)
        .await;

        // warm up and then start
        tokio::time::sleep(Duration::from_secs(15)).await;
        reporter.send(Report::Start).unwrap();
        let start = Instant::now();

        for _ in 0..parameters.runtime_seconds {
            let second = Instant::now();
            tokio::time::sleep(Duration::from_secs(1)).await;
            reporter
                .send(Report::SecondPassed(second.elapsed()))
                .unwrap();
        }

        reporter.send(Report::FinishedIn(start.elapsed())).unwrap();

        // make sure the tasks complete before we drop the database they are connecting to
        for task in tasks {
            task.await.unwrap();
        }
    }
}

#[derive(Clone)]
pub enum PreparedStatement {
    Scylla(ScyllaPrepared),
    Cdrs(CdrsPrepared),
}

impl PreparedStatement {
    fn as_scylla(&self) -> &ScyllaPrepared {
        match self {
            Self::Scylla(p) => p,
            _ => panic!("Not Scylla"),
        }
    }

    fn as_cdrs(&self) -> &CdrsPrepared {
        match self {
            Self::Cdrs(p) => p,
            _ => panic!("Not Cdrs"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum CassandraProtocol {
    V3,
    V4,
    V5,
}

#[derive(Copy, Clone, PartialEq)]
pub enum CassandraDriver {
    Scylla,
    CdrsTokio,
}

pub enum CassandraSession {
    Scylla(ScyllaSession),
    CdrsTokio(
        CdrsTokioSession<
            TransportTcp,
            TcpConnectionManager,
            RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
        >,
    ),
}

impl CassandraSession {
    async fn query(&self, query: &str) {
        match self {
            Self::Scylla(session) => {
                session.query(query, ()).await.unwrap();
            }
            Self::CdrsTokio(session) => {
                session.query(query).await.unwrap();
            }
        }
    }

    async fn prepare(&self, query: &str) -> PreparedStatement {
        match self {
            Self::Scylla(session) => {
                PreparedStatement::Scylla(session.prepare(query).await.unwrap())
            }
            Self::CdrsTokio(session) => {
                PreparedStatement::Cdrs(session.prepare(query).await.unwrap())
            }
        }
    }

    async fn execute(
        &self,
        prepared_statement: &PreparedStatement,
        value: i64,
    ) -> Result<(), String> {
        match self {
            Self::Scylla(session) => session
                .execute(prepared_statement.as_scylla(), (value,))
                .await
                .map_err(|err| format!("{err:?}"))
                .map(|_| ()),
            Self::CdrsTokio(session) => {
                let query_params = QueryParamsBuilder::new()
                    .with_values(QueryValues::SimpleValues(vec![value.into()]))
                    .build();

                let params = StatementParams {
                    query_params,
                    is_idempotent: false,
                    keyspace: None,
                    token: None,
                    routing_key: None,
                    tracing: true,
                    warnings: false,
                    speculative_execution_policy: None,
                    retry_policy: None,
                    beta_protocol: false,
                };

                session
                    .exec_with_params(prepared_statement.as_cdrs(), &params)
                    .await
                    .map_err(|err| format!("{err:?}"))
                    .map(|_| ())
            }
        }
    }

    async fn execute_blob(
        &self,
        prepared_statement: &PreparedStatement,
        value: i64,
        blob: [u8; 16],
    ) -> Result<(), String> {
        match self {
            Self::Scylla(session) => session
                .execute(prepared_statement.as_scylla(), (value, blob))
                .await
                .map_err(|err| format!("{err:?}"))
                .map(|_| ()),
            Self::CdrsTokio(session) => {
                let query_params = QueryParamsBuilder::new()
                    .with_values(QueryValues::SimpleValues(vec![
                        value.into(),
                        blob.to_vec().into(),
                    ]))
                    .build();

                let params = StatementParams {
                    query_params,
                    is_idempotent: false,
                    keyspace: None,
                    token: None,
                    routing_key: None,
                    tracing: true,
                    warnings: false,
                    speculative_execution_policy: None,
                    retry_policy: None,
                    beta_protocol: false,
                };

                session
                    .exec_with_params(prepared_statement.as_cdrs(), &params)
                    .await
                    .map_err(|err| format!("{err:?}"))
                    .map(|_| ())
            }
        }
    }

    async fn await_schema_agreement(&self) {
        match self {
            Self::Scylla(session) => {
                session.await_schema_agreement().await.unwrap();
            }
            Self::CdrsTokio(_session) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub struct CassandraBench {
    db: CassandraDb,
    topology: CassandraTopology,
    shotover: Shotover,
    compression: Compression,
    operation: Operation,
    protocol: CassandraProtocol,
    driver: CassandraDriver,
    connection_count: usize,
}

impl CassandraBench {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        db: CassandraDb,
        topology: CassandraTopology,
        shotover: Shotover,
        compression: Compression,
        operation: Operation,
        protocol: CassandraProtocol,
        driver: CassandraDriver,
        connection_count: usize,
    ) -> Self {
        CassandraBench {
            db,
            topology,
            shotover,
            compression,
            operation,
            protocol,
            driver,
            connection_count,
        }
    }

    fn core_count(&self) -> CoreCount {
        CoreCount {
            bench: 1,
            shotover: 1,
            // TODO: actually use this to configure actual cassandra instances, currently only affects cassandra-mocked
            cassandra: 1,
        }
    }

    fn generate_topology_yaml(&self, host_address: String, cassandra_address: String) -> String {
        let mut transforms = vec![];
        if let Shotover::ForcedMessageParsed = self.shotover {
            transforms.push(Box::new(DebugForceEncodeConfig {
                encode_requests: true,
                encode_responses: true,
            }) as Box<dyn TransformConfig>);
        }

        match self.topology {
            CassandraTopology::Cluster3 => {
                transforms.push(Box::new(CassandraSinkClusterConfig {
                    first_contact_points: vec![cassandra_address],
                    tls: None,
                    connect_timeout_ms: 3000,
                    local_shotover_host_id: "2dd022d6-2937-4754-89d6-02d2933a8f7a".parse().unwrap(),
                    read_timeout: None,
                    shotover_nodes: vec![ShotoverNode {
                        address: host_address.parse().unwrap(),
                        data_center: "datacenter1".to_owned(),
                        rack: "rack1".to_owned(),
                        host_id: "2dd022d6-2937-4754-89d6-02d2933a8f7a".parse().unwrap(),
                    }],
                }));
            }
            CassandraTopology::Single => {
                transforms.push(Box::new(CassandraSinkSingleConfig {
                    address: cassandra_address,
                    tls: None,
                    connect_timeout_ms: 3000,
                    read_timeout: None,
                }));
            }
        }

        common::generate_topology(SourceConfig::Cassandra(
            shotover::sources::cassandra::CassandraConfig {
                name: "cassandra".to_owned(),
                listen_addr: host_address,
                connection_limit: None,
                hard_connection_limit: None,
                tls: None,
                timeout: None,
                chain: TransformChainConfig(transforms),
                transport: None,
            },
        ))
    }

    async fn run_aws_shotover(
        &self,
        instance: Option<Arc<Ec2InstanceWithShotover>>,
        cassandra_ip: String,
    ) -> Option<RunningShotover> {
        match self.shotover {
            Shotover::Standard | Shotover::ForcedMessageParsed => {
                let instance = instance.unwrap();
                let ip = instance.instance.private_ip().to_string();
                let topology = self
                    .generate_topology_yaml(format!("{ip}:9042"), format!("{cassandra_ip}:9042"));
                Some(instance.run_shotover(&topology).await)
            }
            Shotover::None => None,
        }
    }
}

#[async_trait]
impl Bench for CassandraBench {
    type CloudResourcesRequired = CloudResourcesRequired;
    type CloudResources = CloudResources;

    fn tags(&self) -> HashMap<String, String> {
        [
            (
                "db".to_owned(),
                match &self.db {
                    CassandraDb::Cassandra => "cassandra".to_owned(),
                    CassandraDb::Mocked => "cassandra-mocked".to_owned(),
                },
            ),
            self.topology.to_tag(),
            self.shotover.to_tag(),
            (
                "operation".to_owned(),
                match self.operation {
                    Operation::ReadI64 => "read_i64".to_owned(),
                    Operation::WriteBlob => "write_blob".to_owned(),
                },
            ),
            (
                "compression".to_owned(),
                match self.compression {
                    Compression::None => "none".to_owned(),
                    Compression::Lz4 => "lz4".to_owned(),
                },
            ),
            (
                "protocol".to_owned(),
                match self.protocol {
                    CassandraProtocol::V3 => "v3".to_owned(),
                    CassandraProtocol::V4 => "v4".to_owned(),
                    CassandraProtocol::V5 => "v5".to_owned(),
                },
            ),
            (
                "driver".to_owned(),
                match self.driver {
                    CassandraDriver::Scylla => "scylla".to_owned(),
                    CassandraDriver::CdrsTokio => "cdrs-tokio".to_owned(),
                },
            ),
            (
                "connection_count".to_owned(),
                self.connection_count.to_string(),
            ),
        ]
        .into_iter()
        .collect()
    }

    fn required_cloud_resources(&self) -> CloudResourcesRequired {
        let shotover_instance_count =
            if let Shotover::Standard | Shotover::ForcedMessageParsed = self.shotover {
                1
            } else {
                0
            };
        let docker_instance_count = match self.topology {
            CassandraTopology::Single => 1,
            CassandraTopology::Cluster3 => 3,
        };
        CloudResourcesRequired {
            shotover_instance_count,
            docker_instance_count,
            include_shotover_in_docker_instance: false,
        }
    }

    fn supported_profilers(&self) -> Vec<String> {
        profilers::supported_profilers(self.shotover)
    }

    fn cores_required(&self) -> usize {
        self.core_count().bench
    }

    async fn orchestrate_cloud(
        &self,
        mut resources: CloudResources,
        _running_in_release: bool,
        profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let cassandra_instances = resources.docker;
        let shotover_instance = resources.shotover.pop();
        let bench_instance = resources.bencher.unwrap();

        let cassandra_ip = cassandra_instances[0].instance.private_ip().to_string();
        let shotover_ip = shotover_instance
            .as_ref()
            .map(|x| x.instance.private_ip().to_string());
        let shotover_connect_ip = shotover_instance
            .as_ref()
            .map(|x| x.instance.connect_ip().to_string());

        let mut profiler_instances: HashMap<String, &Ec2Instance> =
            [("bencher".to_owned(), &bench_instance.instance)].into();
        if let Shotover::ForcedMessageParsed | Shotover::Standard = self.shotover {
            profiler_instances.insert(
                "shotover".to_owned(),
                &shotover_instance.as_ref().unwrap().instance,
            );
        }
        match self.topology {
            CassandraTopology::Cluster3 => {
                profiler_instances
                    .insert("cassandra1".to_owned(), &cassandra_instances[0].instance);
                profiler_instances
                    .insert("cassandra2".to_owned(), &cassandra_instances[1].instance);
                profiler_instances
                    .insert("cassandra3".to_owned(), &cassandra_instances[2].instance);
            }
            CassandraTopology::Single => {
                profiler_instances.insert("cassandra".to_owned(), &cassandra_instances[0].instance);
            }
        }
        let mut profiler = CloudProfilerRunner::new(
            self.name(),
            profiling,
            profiler_instances,
            &shotover_connect_ip,
        )
        .await;

        let (_, running_shotover) = futures::join!(
            run_aws_cassandra(&cassandra_instances, self.topology),
            self.run_aws_shotover(shotover_instance, cassandra_ip.clone(),)
        );

        let destination_ip = if running_shotover.is_some() {
            shotover_ip.unwrap()
        } else {
            cassandra_ip
        };
        let destination = format!("{destination_ip}:9042");

        bench_instance
            .run_bencher(&self.run_args(&destination, &parameters), &self.name())
            .await;

        profiler.finish();

        if let Some(running_shotover) = running_shotover {
            running_shotover.shutdown().await;
        }
        Ok(())
    }

    async fn orchestrate_local(
        &self,
        _running_in_release: bool,
        profiling: Profiling,
        parameters: BenchParameters,
    ) -> Result<()> {
        let core_count = self.core_count();

        let address = match (&self.topology, &self.shotover) {
            (CassandraTopology::Single, Shotover::None) => "127.0.0.1:9043",
            (CassandraTopology::Cluster3, Shotover::None) => "172.16.1.2:9044",
            (_, Shotover::Standard | Shotover::ForcedMessageParsed) => "127.0.0.1:9042",
        };
        let config_dir = match &self.topology {
            CassandraTopology::Single => "tests/test-configs/cassandra/passthrough",
            CassandraTopology::Cluster3 => "tests/test-configs/cassandra/cluster-v4",
        };

        let _db_instance = match (&self.db, &self.topology) {
            (CassandraDb::Cassandra, _) => CassandraDbInstance::Compose(docker_compose(&format!(
                "{config_dir}/docker-compose.yaml"
            ))),
            (CassandraDb::Mocked, CassandraTopology::Single) => CassandraDbInstance::Mocked(
                test_helpers::mock_cassandra::start(core_count.cassandra, 9043),
            ),
            (CassandraDb::Mocked, CassandraTopology::Cluster3) => {
                panic!("Mocked cassandra database does not provide a clustered mode")
            }
        };
        let cassandra_address = match &self.topology {
            CassandraTopology::Single => "127.0.0.1:9043".to_owned(),
            CassandraTopology::Cluster3 => "172.16.1.2:9044".to_owned(),
        };
        let mut profiler = ProfilerRunner::new(self.name(), profiling);
        let shotover = match self.shotover {
            Shotover::Standard => {
                let topology_contents =
                    self.generate_topology_yaml(address.to_owned(), cassandra_address);
                let topology_path = std::env::temp_dir().join(Uuid::new_v4().to_string());
                std::fs::write(&topology_path, topology_contents).unwrap();
                Some(
                    ShotoverProcessBuilder::new_with_topology(topology_path.to_str().unwrap())
                        .with_bin(bin_path!("shotover-proxy"))
                        .with_profile(profiler.shotover_profile())
                        .with_cores(core_count.shotover as u32)
                        .start()
                        .await,
                )
            }
            Shotover::None => None,
            Shotover::ForcedMessageParsed => todo!(),
        };
        profiler.run(&shotover).await;

        self.execute_run(address, &parameters).await;

        if let Some(shotover) = shotover {
            shotover.shutdown_and_then_consume_events(&[]).await;
        }

        Ok(())
    }

    async fn run_bencher(
        &self,
        resources: &str,
        parameters: BenchParameters,
        reporter: UnboundedSender<Report>,
    ) {
        // only one string field so we just directly store the value in resources
        let address = resources;

        let session = match (self.protocol, self.driver) {
            (CassandraProtocol::V4, CassandraDriver::Scylla) => Arc::new(CassandraSession::Scylla(
                ScyllaSessionBuilder::new()
                    .known_nodes([address])
                    .user("cassandra", "cassandra")
                    // We do not need to refresh metadata as there is nothing else fiddling with the topology or schema.
                    // By default the metadata refreshes every 60s and that can cause performance issues so we disable it by using an absurdly high refresh interval
                    .cluster_metadata_refresh_interval(Duration::from_secs(10000000000))
                    .pool_size(scylla::transport::session::PoolSize::PerShard(
                        self.connection_count.try_into().unwrap(),
                    ))
                    .compression(match self.compression {
                        Compression::None => None,
                        Compression::Lz4 => Some(ScyllaCompression::Lz4),
                    })
                    .build()
                    .await
                    .unwrap(),
            )),
            (_, CassandraDriver::Scylla) => {
                panic!("Must use CassandraProtocol::V4 with CassandraDriver:Scylla");
            }
            (_, CassandraDriver::CdrsTokio) => {
                let cluster_config = NodeTcpConfigBuilder::new()
                    .with_contact_point(address.into())
                    .with_version(match self.protocol {
                        CassandraProtocol::V3 => Version::V3,
                        CassandraProtocol::V4 => Version::V4,
                        CassandraProtocol::V5 => Version::V5,
                    })
                    .build()
                    .await
                    .unwrap();
                Arc::new(CassandraSession::CdrsTokio(
                    TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
                        .with_compression(match self.compression {
                            Compression::None => CdrsCompression::None,
                            Compression::Lz4 => CdrsCompression::Lz4,
                        })
                        .with_connection_pool_config(
                            ConnectionPoolConfigBuilder::new()
                                .with_local_size(self.connection_count)
                                .with_remote_size(self.connection_count)
                                .build(),
                        )
                        .build()
                        .await
                        .unwrap(),
                ))
            }
        };

        self.operation.prepare(&session, &self.db).await;

        self.operation.run(&session, reporter, parameters).await;
    }
}

async fn run_aws_cassandra(
    cassandra_instances: &[Arc<Ec2InstanceWithDocker>],
    topology: CassandraTopology,
) {
    match topology {
        CassandraTopology::Cluster3 => {
            let nodes = vec![
                AwsNodeInfo {
                    instance: cassandra_instances[0].clone(),
                    tokens: "-1070335827948913273,-1262780231953846581,-1372586501761710870,-1531116802331162256,-163362083013332509,-1695623576168766293,-1804498187836887715,-1946106156776485520,-2056453585786233579,-2215276878049099070,-2343278388525192983,-2548968207073589272,-2718848566609997839,-2810829291456476999,-2960742485753654915,-300136287927625825,-3066171811868387352,-3212944421347252253,-3379856628404362106,-3482091781751694282,-3620468823413622084,-3714264728468904973,-3857538363369767184,-3992822749261253815,-4079256680142421758,-4220202103792919330,-4367898183274149976,-4551025118927524312,-471290094307005363,-4742836653234322740,-4864732456348559975,-5060737053316205324,-5207857922059935586,-52840734170572300,-5392608158602524302,-5591707161765757347,-5719371107113340348,-574774067866083148,-5905113700807367081,-6064244949291727065,-6176664591963928272,-6332078773725710211,-6432427113055333695,-6586359736274194228,-6743512440489798680,-6845093364625549830,-6996410811499579341,-713444869718811462,-7168884701115756243,-7274128064287226174,-7446523579358567501,-7559015074080672349,-7726720852570152935,-7863620627939574516,-7953250106433078478,-8092452711734517630,-8199788990029145821,-8373699889885432815,-8544039658474896486,-859630544771061355,-8759221946503374753,-8919633668743362629,-9019110376775037498,-9173846866235956778,103714383615439684,1098226704217661717,1227871255888263829,1316739704183204187,1491001625227011179,1645946234495158049,1754240148183667031,1905451572012652200,199201432712497741,2016307737474393503,2177599511665263375,2341796809284406230,2450674246426799652,2623990001964297679,2804477461124776252,2937402454393518499,3095961017485413027,3205311148009103264,3421119517345855347,3612339999845077896,362311011506768076,3751193928058060440,3965582965057198705,4105489420026468015,4204758006957390723,4388483760424390846,4576911512875681446,4691569685096124520,4830253606480231274,4921495432287268245,503658101740669817,5080624335143342111,5184264706352018320,5341965577439578406,5472653858019210550,5563061523591362180,5704213330324189109,5856744011612998895,5978488312938289484,617606604743448883,6176989578371539613,6325548347569741056,6457505413000155610,6546847651267682190,6742276931944292552,6886150343510243611,7019379198083928892,7116146590348156274,7283195764438888312,7457031967979733254,7597511608635332781,769362778173404078,7805206394409370938,7971757036117465495,8107365759249009360,8204193150302005303,8330270968875552899,8423008301510322314,8575657061151446857,8719671759152143907,8828129486053943115,8979301877610065508,9147701452061713801,953293656790161474".to_owned(),
                },
                AwsNodeInfo {
                    instance: cassandra_instances[1].clone(),
                    tokens: "-1040723580916052090,-1171411861495684236,-1329112732583244321,-1432753103791920530,-1591882006647994397,-1683123832455031366,-1821807753839138121,-187829091365521586,-1936465926059581195,-2124893678510871797,-2308619431977871918,-2407888018908794625,-2547794473878063936,-2762183510877202201,-2901037439090184745,-3092257921589407295,-3308066290926159377,-336387860563723028,-3417416421449849613,-3575974984541744143,-3708899977810486389,-3889387436970964963,-4062703192508462990,-4171580629650856412,-4335777927269999269,-4497069701460869140,-4607925866922610442,-4759137290751595611,-4867431204440104591,-5022375813708251462,-5196637734752058454,-5285506183046998811,-534889125996973159,-5415150734717600926,-5560083782145101168,-55872025935107032,-5744014660761858564,-5895770834191813759,-6009719337194592824,-6151066427428494567,-6314176006222764901,-6409663055319822958,-6566218173105834944,-656633427322263746,-6676739521948595153,-6813513726862888469,-6984667533242268007,-7088151506801345793,-7226822308654074107,-7373007983706324000,-7583713266884175918,-7776157670889109226,-7885963940696973515,-8044494241266424901,-809164108611073533,-8209001015104028938,-8317875626772150360,-8459483595711748165,-8569831024721496224,-8728654316984361715,-8856655827460455628,-9062345646008851917,-950315915343900461,1084134169700070137,1291828955474108294,1458379597182202851,1593988320313746716,1690815711366742659,1816893529940290255,1909630862575059670,2062279622216184213,2206294320216881263,228899493009029909,2314752047118680471,2465924438674802864,2634324013126451157,2759519768538332193,2914256257999251471,3013732966030926341,3174144688270914218,33470212332419547,3389326976299392483,3559666744888856155,372772904574980968,3733577644745143148,3840913923039771341,3980116528341210490,4069746006834714453,4206645782204136035,4374351560693616620,4486843055415721469,4659238570487062795,4764481933658532727,4936955823274709630,506001759148666249,5088273270148739139,5189854194284490289,5347006898500094741,5500939521718955273,5601287861048578758,5756702042810360696,5869121685482561904,602769151412893631,6028252933966921889,6213995527660948621,6341659473008531622,6540758476171764667,6725508712714353383,6872629581458083647,7068634178425728993,7190529981539966228,7382341515846764656,7565468451500138991,769818325503625668,7713164530981369639,7854109954631867209,7940543885513035153,8075828271404521784,8219101906305383995,8312897811360666885,8451274853022594685,8553510006369926862,8720422213427036715,8867194822905901615,8972624149020634054,9122537343317811969,9214518068164291130,943654529044470610".to_owned(),
                },
                AwsNodeInfo {
                    instance: cassandra_instances[2].clone(),
                    tokens: "-1118567412469902436,-1313996693146512799,-1403338931414039378,-1535295996844453933,-1683854766042655376,-1882356031475905507,-2004100332801196094,-2156631014090005881,-2297782820822832809,-2388190486394984438,-2518878766974616584,-263332735778862210,-2676579638062176669,-2780220009270852878,-2939348912126926745,-3030590737933963714,-3169274659318070469,-3283932831538513543,-3472360583989804145,-3656086337456804266,-3755354924387726973,-3895261379356996284,-403812376434461735,-4109650416356134549,-4248504344569117093,-4439724827068339643,-4655533196405091725,-4764883326928781961,-4923441890020676491,-5056366883289418737,-5236854342449897311,-5410170097987395338,-5519047535129788760,-55637950004824052,-5683244832748931617,-577648579975306678,-5844536606939801488,-5955392772401542790,-6106604196230527959,-6214898109919036939,-6369842719187183810,-6544104640230990802,-6632973088525931159,-6762617640196533274,-6907550687624033516,-7091481566240790912,-7243237739670746107,-7357186242673525172,-744697754066038715,-7498533332907426915,-7661642911701697249,-7757129960798755306,-7913685078584767292,-8024206427427527501,-8160980632341820817,-8332134438721200355,-841465146330266096,-8435618412280278141,-8574289214133006455,-8720474889185256349,-8931180172363108268,-9123624576368041577,-974694000903951379,110912691703270503,1118457533195870516,1286857107647518809,1412052863059399845,1566789352520319123,1666266060551993993,1826677782791981870,2041860070820460135,2212199839409923807,2386110739266210800,246521414834814368,2493447017560838993,2632649622862278142,2722279101355782105,2859178876725203687,3026884655214684272,3139376149936789121,3311771665008130447,3417015028179600379,343348805887810311,3589488917795777282,3740806364669806791,3842387288805557941,3999539993021162393,4153472616240022925,4253820955569646410,4409235137331428348,4521654780003629556,4680786028487989541,469426624461357907,4866528622182016273,4994192567529599274,5193291570692832319,5378041807235421035,5525162675979151299,562163957096127322,5721167272946796645,5843063076061033880,6034874610367832308,6218001546021206643,6365697625502437291,6506643049152934861,6593076980034102805,6728361365925589436,6871635000826451647,6965430905881734537,7103807947543662337,714812716737251865,7206043100890994514,7372955307948104367,7519727917426969267,7625157243541701706,7775070437838879621,7867051162685358782,8036931522221767350,8242621340770163636,8370622851246257550,8529446143509123040,858827414737948915,8639793572518871100,8781401541458468904,8890276153126590327,9054782926964194365,9213313227533645749,967285141639748123".to_owned(),
                },
            ];
            run_aws_cassandra_cluster(nodes).await
        }
        CassandraTopology::Single => run_aws_cassandra_single(cassandra_instances[0].clone()).await,
    }
}

async fn run_aws_cassandra_single(instance: Arc<Ec2InstanceWithDocker>) {
    instance
        .run_container("shotover/cassandra-test:4.0.6-r1", &[])
        .await;
}

async fn run_aws_cassandra_cluster(nodes: Vec<AwsNodeInfo>) {
    let seeds = nodes
        .iter()
        .map(|x| x.instance.instance.private_ip().to_string())
        .join(",");

    let mut tasks = vec![];
    for node in nodes {
        let seeds = seeds.clone();
        tasks.push(tokio::spawn(async move {
            node.instance
                .run_container(
                    "shotover/cassandra-test:4.0.6-r1",
                    &[
                        (
                            "CASSANDRA_ENDPOINT_SNITCH".to_owned(),
                            "GossipingPropertyFileSnitch".to_owned(),
                        ),
                        (
                            "CASSANDRA_CLUSTER_NAME".to_owned(),
                            "TestCluster".to_owned(),
                        ),
                        ("CASSANDRA_DC".to_owned(), "datacenter1".to_owned()),
                        ("CASSANDRA_RACK".to_owned(), "rack1".to_owned()),
                        ("CASSANDRA_SEEDS".to_owned(), seeds),
                        ("CASSANDRA_INITIAL_TOKENS".to_owned(), node.tokens),
                    ],
                )
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

struct AwsNodeInfo {
    instance: Arc<Ec2InstanceWithDocker>,
    tokens: String,
}

#[derive(Clone)]
struct BenchTaskCassandra {
    session: Arc<CassandraSession>,
    query: PreparedStatement,
    operation: Operation,
}

#[async_trait]
impl BenchTask for BenchTaskCassandra {
    async fn run_one_operation(&self) -> Result<(), String> {
        let i = rand::random::<u32>() % ROW_COUNT as u32;
        match self.operation {
            Operation::ReadI64 => self.session.execute(&self.query, i as i64).await,
            Operation::WriteBlob => {
                let blob = {
                    let mut rng = rand::thread_rng();
                    rng.gen::<[u8; 16]>()
                };

                self.session.execute_blob(&self.query, i as i64, blob).await
            }
        }
    }
}
