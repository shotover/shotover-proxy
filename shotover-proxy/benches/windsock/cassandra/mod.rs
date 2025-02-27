mod bench;

use crate::ShotoverBench;
use crate::common::*;
use bench::*;

pub fn benches() -> Vec<ShotoverBench> {
    let mut result = vec![
        Box::new(CassandraBench::new(
            CassandraDb::Mocked,
            CassandraTopology::Single,
            Shotover::None,
            Compression::None,
            Operation::ReadI64,
            CassandraProtocol::V4,
            CassandraDriver::Scylla,
            10,
        )) as ShotoverBench,
        Box::new(CassandraBench::new(
            CassandraDb::Mocked,
            CassandraTopology::Single,
            Shotover::Standard,
            Compression::None,
            Operation::ReadI64,
            CassandraProtocol::V4,
            CassandraDriver::Scylla,
            10,
        )),
    ];
    result.extend(
        itertools::iproduct!(
            [CassandraDb::Cassandra],
            [CassandraTopology::Single, CassandraTopology::Cluster3],
            [Shotover::None, Shotover::Standard],
            [Compression::None, Compression::Lz4],
            [Operation::ReadI64, Operation::WriteBlob],
            [
                CassandraProtocol::V3,
                CassandraProtocol::V4,
                CassandraProtocol::V5
            ],
            [CassandraDriver::Scylla, CassandraDriver::CdrsTokio],
            [1, 10, 100]
        )
        .filter_map(
            |(
                cassandra,
                topology,
                shotover,
                compression,
                operation,
                protocol,
                driver,
                connection_count,
            )| {
                if driver == CassandraDriver::Scylla && protocol != CassandraProtocol::V4 {
                    return None;
                }

                if driver == CassandraDriver::CdrsTokio
                    && (operation != Operation::ReadI64 || topology != CassandraTopology::Single)
                {
                    return None;
                }

                Some(Box::new(CassandraBench::new(
                    cassandra,
                    topology,
                    shotover,
                    compression,
                    operation,
                    protocol,
                    driver,
                    connection_count,
                )) as ShotoverBench)
            },
        ),
    );
    result
}
