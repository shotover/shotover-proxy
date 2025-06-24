use kafka_protocol::{messages::ApiKey, protocol::VersionRange};

// This table defines which api versions KafkaSinkCluster supports.
// * When adding a new message type:
//   + Make sure you have implemented routing logic for the message type
//   + Make sure any fields referring to internal cluster details such as broker ids or addresses are rewritten to refer to shotover nodes
// * When adding a new message type version:
//   + Make sure any new fields do not break any of the requirements listed above
pub(crate) fn versions_supported_by_key(api_key: i16) -> Option<VersionRange> {
    match ApiKey::try_from(api_key) {
        Ok(ApiKey::Produce) => Some(VersionRange { min: 0, max: 11 }),
        Ok(ApiKey::Fetch) => Some(VersionRange { min: 0, max: 16 }),
        Ok(ApiKey::ListOffsets) => Some(VersionRange { min: 0, max: 8 }),
        Ok(ApiKey::Metadata) => Some(VersionRange { min: 0, max: 12 }),
        Ok(ApiKey::OffsetCommit) => Some(VersionRange { min: 0, max: 9 }),
        Ok(ApiKey::OffsetFetch) => Some(VersionRange { min: 0, max: 9 }),
        Ok(ApiKey::FindCoordinator) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::JoinGroup) => Some(VersionRange { min: 0, max: 9 }),
        Ok(ApiKey::Heartbeat) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::LeaveGroup) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::SyncGroup) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::DescribeGroups) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::ListGroups) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::SaslHandshake) => Some(VersionRange { min: 0, max: 1 }),
        Ok(ApiKey::ApiVersions) => Some(VersionRange { min: 0, max: 3 }),
        Ok(ApiKey::CreateTopics) => Some(VersionRange { min: 0, max: 7 }),
        Ok(ApiKey::DeleteTopics) => Some(VersionRange { min: 0, max: 6 }),
        Ok(ApiKey::DeleteRecords) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::InitProducerId) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::OffsetForLeaderEpoch) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::AddPartitionsToTxn) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::AddOffsetsToTxn) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::EndTxn) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::TxnOffsetCommit) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::CreateAcls) => Some(VersionRange { min: 0, max: 3 }),
        Ok(ApiKey::DescribeConfigs) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::AlterConfigs) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::DescribeLogDirs) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::SaslAuthenticate) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::CreatePartitions) => Some(VersionRange { min: 0, max: 3 }),
        Ok(ApiKey::DeleteGroups) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::ElectLeaders) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::AlterPartitionReassignments) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::ListPartitionReassignments) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::OffsetDelete) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::AlterPartition) => Some(VersionRange { min: 0, max: 3 }),
        Ok(ApiKey::DescribeCluster) => Some(VersionRange { min: 0, max: 1 }),
        Ok(ApiKey::DescribeProducers) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::DescribeTransactions) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::ListTransactions) => Some(VersionRange { min: 0, max: 1 }),
        Ok(ApiKey::ConsumerGroupHeartbeat) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::ConsumerGroupDescribe) => Some(VersionRange { min: 0, max: 0 }),
        // This message type has very little documentation available and kafka responds to it with an error code 35 UNSUPPORTED_VERSION
        // So its not clear at all how to implement this and its not even possible to test it.
        // Instead lets just ask the client to not send it at all.
        // We can consider supporting it when kafka itself starts to support it but we will need to be very
        // careful to correctly implement the pagination/cursor logic.
        Ok(ApiKey::DescribeTopicPartitions) => None,
        Ok(_) => None,
        Err(_) => None,
    }
}

// This test gives visibility into the api versions that shotover doesnt support yet.
// If this test is failing after a `cargo update`, you can just alter EXPECTED_ERROR_MESSAGE to include the new versions.
// The actual upgrade can be done later.
#[test]
fn check_api_version_backlog() {
    use std::fmt::Write;
    const EXPECTED_ERROR_MESSAGE: &str = r#"
Fetch kafka-protocol=0..17 shotover=0..16
ListOffsets kafka-protocol=0..9 shotover=0..8
LeaderAndIsr kafka-protocol=0..7 shotover=NotSupported
StopReplica kafka-protocol=0..4 shotover=NotSupported
UpdateMetadata kafka-protocol=0..8 shotover=NotSupported
ControlledShutdown kafka-protocol=0..3 shotover=NotSupported
FindCoordinator kafka-protocol=0..6 shotover=0..5
ApiVersions kafka-protocol=0..4 shotover=0..3
WriteTxnMarkers kafka-protocol=0..1 shotover=NotSupported
DescribeAcls kafka-protocol=0..3 shotover=NotSupported
DeleteAcls kafka-protocol=0..3 shotover=NotSupported
AlterReplicaLogDirs kafka-protocol=0..2 shotover=NotSupported
CreateDelegationToken kafka-protocol=0..3 shotover=NotSupported
RenewDelegationToken kafka-protocol=0..2 shotover=NotSupported
ExpireDelegationToken kafka-protocol=0..2 shotover=NotSupported
DescribeDelegationToken kafka-protocol=0..3 shotover=NotSupported
IncrementalAlterConfigs kafka-protocol=0..1 shotover=NotSupported
DescribeClientQuotas kafka-protocol=0..1 shotover=NotSupported
AlterClientQuotas kafka-protocol=0..1 shotover=NotSupported
DescribeUserScramCredentials kafka-protocol=0..0 shotover=NotSupported
AlterUserScramCredentials kafka-protocol=0..0 shotover=NotSupported
Vote kafka-protocol=0..1 shotover=NotSupported
BeginQuorumEpoch kafka-protocol=0..1 shotover=NotSupported
EndQuorumEpoch kafka-protocol=0..1 shotover=NotSupported
DescribeQuorum kafka-protocol=0..2 shotover=NotSupported
UpdateFeatures kafka-protocol=0..1 shotover=NotSupported
Envelope kafka-protocol=0..0 shotover=NotSupported
FetchSnapshot kafka-protocol=0..1 shotover=NotSupported
BrokerRegistration kafka-protocol=0..4 shotover=NotSupported
BrokerHeartbeat kafka-protocol=0..1 shotover=NotSupported
UnregisterBroker kafka-protocol=0..0 shotover=NotSupported
AllocateProducerIds kafka-protocol=0..0 shotover=NotSupported
ControllerRegistration kafka-protocol=0..0 shotover=NotSupported
GetTelemetrySubscriptions kafka-protocol=0..0 shotover=NotSupported
PushTelemetry kafka-protocol=0..0 shotover=NotSupported
AssignReplicasToDirs kafka-protocol=0..0 shotover=NotSupported
ListClientMetricsResources kafka-protocol=0..0 shotover=NotSupported
DescribeTopicPartitions kafka-protocol=0..0 shotover=NotSupported
"#;

    let mut error_message = String::new();
    for api_key in ApiKey::iter() {
        let shotover_version = versions_supported_by_key(api_key as i16);

        let kafka_protocol_version = api_key.valid_versions();
        if shotover_version != Some(kafka_protocol_version) {
            let shotover_version = match shotover_version {
                Some(version) => format!("{version}"),
                None => "NotSupported".to_owned(),
            };
            writeln!(
                error_message,
                "{api_key:?} kafka-protocol={kafka_protocol_version} shotover={shotover_version}"
            )
            .unwrap();
        }
    }

    pretty_assertions::assert_eq!(
        EXPECTED_ERROR_MESSAGE.trim(),
        error_message.trim(),
        "The list of message types not supported by shotover differs from the expected list defined in EXPECTED_ERROR_MESSAGE",
    );
}
