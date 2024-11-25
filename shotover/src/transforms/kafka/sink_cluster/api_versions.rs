use kafka_protocol::{messages::ApiKey, protocol::VersionRange};

// This table defines which api versions KafkaSinkCluster supports.
// * When adding a new message type:
//   + Make sure you have implemented routing logic for the message type
//   + Make sure any fields referring to internal cluster details such as broker ids or addresses are rewritten to refer to shotover nodes
// * When adding a new message type version:
//   + Make sure any new fields do not break any of the requirements listed above
pub(crate) fn versions_supported_by_key(api_key: i16) -> Option<VersionRange> {
    match ApiKey::try_from(api_key) {
        Ok(ApiKey::ProduceKey) => Some(VersionRange { min: 0, max: 11 }),
        Ok(ApiKey::FetchKey) => Some(VersionRange { min: 0, max: 16 }),
        Ok(ApiKey::ListOffsetsKey) => Some(VersionRange { min: 0, max: 8 }),
        Ok(ApiKey::MetadataKey) => Some(VersionRange { min: 0, max: 12 }),
        Ok(ApiKey::OffsetCommitKey) => Some(VersionRange { min: 0, max: 9 }),
        Ok(ApiKey::OffsetFetchKey) => Some(VersionRange { min: 0, max: 9 }),
        Ok(ApiKey::FindCoordinatorKey) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::JoinGroupKey) => Some(VersionRange { min: 0, max: 9 }),
        Ok(ApiKey::HeartbeatKey) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::LeaveGroupKey) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::SyncGroupKey) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::DescribeGroupsKey) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::ListGroupsKey) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::SaslHandshakeKey) => Some(VersionRange { min: 0, max: 1 }),
        Ok(ApiKey::ApiVersionsKey) => Some(VersionRange { min: 0, max: 3 }),
        Ok(ApiKey::CreateTopicsKey) => Some(VersionRange { min: 0, max: 7 }),
        Ok(ApiKey::DeleteTopicsKey) => Some(VersionRange { min: 0, max: 6 }),
        Ok(ApiKey::DeleteRecordsKey) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::InitProducerIdKey) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::OffsetForLeaderEpochKey) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::AddPartitionsToTxnKey) => Some(VersionRange { min: 0, max: 5 }),
        Ok(ApiKey::AddOffsetsToTxnKey) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::EndTxnKey) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::TxnOffsetCommitKey) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::CreateAclsKey) => Some(VersionRange { min: 0, max: 3 }),
        Ok(ApiKey::DescribeConfigsKey) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::AlterConfigsKey) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::DescribeLogDirsKey) => Some(VersionRange { min: 0, max: 4 }),
        Ok(ApiKey::SaslAuthenticateKey) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::CreatePartitionsKey) => Some(VersionRange { min: 0, max: 3 }),
        Ok(ApiKey::DeleteGroupsKey) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::ElectLeadersKey) => Some(VersionRange { min: 0, max: 2 }),
        Ok(ApiKey::AlterPartitionReassignmentsKey) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::ListPartitionReassignmentsKey) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::OffsetDeleteKey) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::AlterPartitionKey) => Some(VersionRange { min: 0, max: 3 }),
        Ok(ApiKey::DescribeClusterKey) => Some(VersionRange { min: 0, max: 1 }),
        Ok(ApiKey::DescribeProducersKey) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::DescribeTransactionsKey) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::ListTransactionsKey) => Some(VersionRange { min: 0, max: 1 }),
        Ok(ApiKey::ConsumerGroupHeartbeatKey) => Some(VersionRange { min: 0, max: 0 }),
        Ok(ApiKey::ConsumerGroupDescribeKey) => Some(VersionRange { min: 0, max: 0 }),
        // This message type has very little documentation available and kafka responds to it with an error code 35 UNSUPPORTED_VERSION
        // So its not clear at all how to implement this and its not even possible to test it.
        // Instead lets just ask the client to not send it at all.
        // We can consider supporting it when kafka itself starts to support it but we will need to be very
        // careful to correctly implement the pagination/cursor logic.
        Ok(ApiKey::DescribeTopicPartitionsKey) => None,
        Ok(_) => None,
        Err(_) => None,
    }
}
