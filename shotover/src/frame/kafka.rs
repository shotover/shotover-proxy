use crate::codec::kafka::RequestHeader as CodecRequestHeader;
use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, AllocateProducerIdsRequest, AllocateProducerIdsResponse,
    AlterClientQuotasRequest, AlterClientQuotasResponse, AlterConfigsRequest, AlterConfigsResponse,
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, AlterPartitionRequest,
    AlterPartitionResponse, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse,
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiKey,
    ApiVersionsRequest, ApiVersionsResponse, BeginQuorumEpochRequest, BeginQuorumEpochResponse,
    BrokerHeartbeatRequest, BrokerHeartbeatResponse, BrokerRegistrationRequest,
    BrokerRegistrationResponse, ControlledShutdownRequest, ControlledShutdownResponse,
    CreateAclsRequest, CreateAclsResponse, CreateDelegationTokenRequest,
    CreateDelegationTokenResponse, CreatePartitionsRequest, CreatePartitionsResponse,
    CreateTopicsRequest, CreateTopicsResponse, DeleteAclsRequest, DeleteAclsResponse,
    DeleteGroupsRequest, DeleteGroupsResponse, DeleteRecordsRequest, DeleteRecordsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, DescribeAclsRequest, DescribeAclsResponse,
    DescribeClientQuotasRequest, DescribeClientQuotasResponse, DescribeClusterRequest,
    DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    DescribeDelegationTokenRequest, DescribeDelegationTokenResponse, DescribeGroupsRequest,
    DescribeGroupsResponse, DescribeLogDirsRequest, DescribeLogDirsResponse,
    DescribeProducersRequest, DescribeProducersResponse, DescribeQuorumRequest,
    DescribeQuorumResponse, DescribeTransactionsRequest, DescribeTransactionsResponse,
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse, ElectLeadersRequest,
    ElectLeadersResponse, EndQuorumEpochRequest, EndQuorumEpochResponse, EndTxnRequest,
    EndTxnResponse, EnvelopeRequest, EnvelopeResponse, ExpireDelegationTokenRequest,
    ExpireDelegationTokenResponse, FetchRequest, FetchResponse, FetchSnapshotRequest,
    FetchSnapshotResponse, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest,
    HeartbeatResponse, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    InitProducerIdRequest, InitProducerIdResponse, JoinGroupRequest, JoinGroupResponse,
    LeaderAndIsrRequest, LeaderAndIsrResponse, LeaveGroupRequest, LeaveGroupResponse,
    ListGroupsRequest, ListGroupsResponse, ListOffsetsRequest, ListOffsetsResponse,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, ListTransactionsRequest,
    ListTransactionsResponse, MetadataRequest, MetadataResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetDeleteRequest, OffsetDeleteResponse, OffsetFetchRequest,
    OffsetFetchResponse, OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, ProduceRequest,
    ProduceResponse, RenewDelegationTokenRequest, RenewDelegationTokenResponse, RequestHeader,
    ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, StopReplicaRequest, StopReplicaResponse, SyncGroupRequest,
    SyncGroupResponse, TxnOffsetCommitRequest, TxnOffsetCommitResponse, UnregisterBrokerRequest,
    UnregisterBrokerResponse, UpdateFeaturesRequest, UpdateFeaturesResponse, UpdateMetadataRequest,
    UpdateMetadataResponse, VoteRequest, VoteResponse, WriteTxnMarkersRequest,
    WriteTxnMarkersResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use std::fmt::{Display, Formatter, Result as FmtResult};

#[derive(Debug, PartialEq, Clone)]
pub enum KafkaFrame {
    Request {
        header: RequestHeader,
        body: RequestBody,
    },
    Response {
        version: i16,
        header: ResponseHeader,
        body: ResponseBody,
    },
}

impl Display for KafkaFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            KafkaFrame::Request { header, body } => {
                write!(
                    f,
                    "version:{} correlation_id:{}",
                    header.request_api_version, header.correlation_id
                )?;
                if let Some(id) = header.client_id.as_ref() {
                    write!(f, " client_id:{id:?}")?;
                }
                if !header.unknown_tagged_fields.is_empty() {
                    write!(
                        f,
                        " unknown_tagged_fields:{:?}",
                        header.unknown_tagged_fields
                    )?;
                }
                write!(f, " {:?}", body)?;
            }
            KafkaFrame::Response {
                version,
                header,
                body,
            } => {
                write!(
                    f,
                    "version:{version} correlation_id:{}",
                    header.correlation_id
                )?;
                if !header.unknown_tagged_fields.is_empty() {
                    write!(
                        f,
                        " unknown_tagged_fields:{:?}",
                        header.unknown_tagged_fields
                    )?;
                }
                write!(f, " {body:?}",)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum RequestBody {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    OffsetFetch(OffsetFetchRequest),
    ListOffsets(ListOffsetsRequest),
    JoinGroup(JoinGroupRequest),
    SyncGroup(SyncGroupRequest),
    Metadata(MetadataRequest),
    FindCoordinator(FindCoordinatorRequest),
    LeaderAndIsr(LeaderAndIsrRequest),
    Heartbeat(HeartbeatRequest),
    CreateTopics(CreateTopicsRequest),
    DeleteTopics(DeleteTopicsRequest),
    DeleteGroups(DeleteGroupsRequest),
    DescribeConfigs(DescribeConfigsRequest),
    SaslAuthenticate(SaslAuthenticateRequest),
    SaslHandshake(SaslHandshakeRequest),
    StopReplica(StopReplicaRequest),
    UpdateMetadata(UpdateMetadataRequest),
    ControlledShutdown(ControlledShutdownRequest),
    OffsetCommit(OffsetCommitRequest),
    LeaveGroup(LeaveGroupRequest),
    DescribeGroups(DescribeGroupsRequest),
    ListGroups(ListGroupsRequest),
    ApiVersions(ApiVersionsRequest),
    DeleteRecords(DeleteRecordsRequest),
    InitProducerId(InitProducerIdRequest),
    OffsetForLeaderEpoch(OffsetForLeaderEpochRequest),
    AddPartitionsToTxn(AddPartitionsToTxnRequest),
    AddOffsetsToTxn(AddOffsetsToTxnRequest),
    EndTxn(EndTxnRequest),
    WriteTxnMarkers(WriteTxnMarkersRequest),
    TxnOffsetCommit(TxnOffsetCommitRequest),
    DescribeAcls(DescribeAclsRequest),
    CreateAcls(CreateAclsRequest),
    DeleteAcls(DeleteAclsRequest),
    AlterConfigs(AlterConfigsRequest),
    AlterReplicaLogDirs(AlterReplicaLogDirsRequest),
    DescribeLogDirs(DescribeLogDirsRequest),
    CreatePartitions(CreatePartitionsRequest),
    CreateDelegationToken(CreateDelegationTokenRequest),
    RenewDelegationToken(RenewDelegationTokenRequest),
    ExpireDelegationToken(ExpireDelegationTokenRequest),
    DescribeDelegationToken(DescribeDelegationTokenRequest),
    ElectLeaders(ElectLeadersRequest),
    IncrementalAlterConfigs(IncrementalAlterConfigsRequest),
    AlterPartitionReassignments(AlterPartitionReassignmentsRequest),
    ListPartitionReassignments(ListPartitionReassignmentsRequest),
    DescribeCluster(DescribeClusterRequest),
    Envelope(EnvelopeRequest),
    FetchSnapshot(FetchSnapshotRequest),
    ListTransactions(ListTransactionsRequest),
    DescribeTransactions(DescribeTransactionsRequest),
    AllocateProducerIds(AllocateProducerIdsRequest),
    UnregisterBroker(UnregisterBrokerRequest),
    BrokerHeartbeat(BrokerHeartbeatRequest),
    BrokerRegistration(BrokerRegistrationRequest),
    DescribeProducers(DescribeProducersRequest),
    UpdateFeatures(UpdateFeaturesRequest),
    AlterPartition(AlterPartitionRequest),
    EndQuorumEpoch(EndQuorumEpochRequest),
    BeginQuorumEpoch(BeginQuorumEpochRequest),
    Vote(VoteRequest),
    DescribeUserScramCredentials(DescribeUserScramCredentialsRequest),
    AlterScramCredentials(AlterUserScramCredentialsRequest),
    DescribeClientQuotas(DescribeClientQuotasRequest),
    OffsetDelete(OffsetDeleteRequest),
    AlterClientQuotas(AlterClientQuotasRequest),
    DescribeQuorum(DescribeQuorumRequest),
    AlterUserScramCredentials(AlterUserScramCredentialsRequest),
}

#[derive(Debug, PartialEq, Clone)]
pub enum ResponseBody {
    Produce(ProduceResponse),
    FindCoordinator(FindCoordinatorResponse),
    Fetch(FetchResponse),
    OffsetFetch(OffsetFetchResponse),
    ListOffsets(ListOffsetsResponse),
    JoinGroup(JoinGroupResponse),
    SyncGroup(SyncGroupResponse),
    Metadata(MetadataResponse),
    DescribeCluster(DescribeClusterResponse),
    Heartbeat(HeartbeatResponse),
    SaslAuthenticate(SaslAuthenticateResponse),
    SaslHandshake(SaslHandshakeResponse),
    StopReplica(StopReplicaResponse),
    UpdateMetadata(UpdateMetadataResponse),
    ControlledShutdown(ControlledShutdownResponse),
    LeaderAndIsr(LeaderAndIsrResponse),
    OffsetCommit(OffsetCommitResponse),
    LeaveGroup(LeaveGroupResponse),
    DescribeGroups(DescribeGroupsResponse),
    ListGroups(ListGroupsResponse),
    ApiVersions(ApiVersionsResponse),
    CreateTopics(CreateTopicsResponse),
    DeleteTopics(DeleteTopicsResponse),
    DeleteRecords(DeleteRecordsResponse),
    InitProducerId(InitProducerIdResponse),
    OffsetForLeaderEpoch(OffsetForLeaderEpochResponse),
    AddPartitionsToTxn(AddPartitionsToTxnResponse),
    AddOffsetsToTxn(AddOffsetsToTxnResponse),
    EndTxn(EndTxnResponse),
    WriteTxnMarkers(WriteTxnMarkersResponse),
    TxnOffsetCommit(TxnOffsetCommitResponse),
    DescribeAcls(DescribeAclsResponse),
    CreateAcls(CreateAclsResponse),
    DeleteAcls(DeleteAclsResponse),
    AlterConfigs(AlterConfigsResponse),
    AlterReplicaLogDirs(AlterReplicaLogDirsResponse),
    DescribeLogDirs(DescribeLogDirsResponse),
    CreatePartitions(CreatePartitionsResponse),
    CreateDelegationToken(CreateDelegationTokenResponse),
    RenewDelegationToken(RenewDelegationTokenResponse),
    ExpireDelegationToken(ExpireDelegationTokenResponse),
    DescribeDelegationToken(DescribeDelegationTokenResponse),
    ElectLeaders(ElectLeadersResponse),
    IncrementalAlterConfigs(IncrementalAlterConfigsResponse),
    AlterPartitionReassignments(AlterPartitionReassignmentsResponse),
    ListPartitionReassignments(ListPartitionReassignmentsResponse),
    BrokerRegistration(BrokerRegistrationResponse),
    AllocateProducerIds(AllocateProducerIdsResponse),
    ListTransactions(ListTransactionsResponse),
    DescribeTransactions(DescribeTransactionsResponse),
    UnregisterBroker(UnregisterBrokerResponse),
    BrokerHeartbeat(BrokerHeartbeatResponse),
    DescribeProducers(DescribeProducersResponse),
    FetchSnapshot(FetchSnapshotResponse),
    UpdateFeatures(UpdateFeaturesResponse),
    AlterPartition(AlterPartitionResponse),
    EndQuorumEpoch(EndQuorumEpochResponse),
    Envelope(EnvelopeResponse),
    Vote(VoteResponse),
    DescribeUserScramCredentials(DescribeUserScramCredentialsResponse),
    DescribeQuorum(DescribeQuorumResponse),
    BeginQuorumEpoch(BeginQuorumEpochResponse),
    AlterUserScramCredentials(AlterUserScramCredentialsResponse),
    DescribeClientQuotas(DescribeClientQuotasResponse),
    AlterClientQuotas(AlterClientQuotasResponse),
    DescribeConfigs(DescribeConfigsResponse),
    OffsetDelete(OffsetDeleteResponse),
    DeleteGroups(DeleteGroupsResponse),
}

impl ResponseBody {
    fn header_version(&self, version: i16) -> i16 {
        match self {
            ResponseBody::Produce(_) => ProduceResponse::header_version(version),
            ResponseBody::FindCoordinator(_) => FindCoordinatorResponse::header_version(version),
            ResponseBody::Fetch(_) => FetchResponse::header_version(version),
            ResponseBody::OffsetFetch(_) => OffsetFetchResponse::header_version(version),
            ResponseBody::ListOffsets(_) => ListOffsetsResponse::header_version(version),
            ResponseBody::JoinGroup(_) => JoinGroupResponse::header_version(version),
            ResponseBody::SyncGroup(_) => SyncGroupResponse::header_version(version),
            ResponseBody::Metadata(_) => MetadataResponse::header_version(version),
            ResponseBody::DescribeCluster(_) => DescribeClusterResponse::header_version(version),
            ResponseBody::Heartbeat(_) => HeartbeatResponse::header_version(version),
            ResponseBody::SaslAuthenticate(_) => SaslAuthenticateResponse::header_version(version),
            ResponseBody::SaslHandshake(_) => SaslHandshakeResponse::header_version(version),
            ResponseBody::StopReplica(_) => StopReplicaResponse::header_version(version),
            ResponseBody::UpdateMetadata(_) => UpdateMetadataResponse::header_version(version),
            ResponseBody::ControlledShutdown(_) => {
                ControlledShutdownResponse::header_version(version)
            }
            ResponseBody::LeaderAndIsr(_) => LeaderAndIsrResponse::header_version(version),
            ResponseBody::OffsetCommit(_) => OffsetCommitResponse::header_version(version),
            ResponseBody::LeaveGroup(_) => LeaveGroupResponse::header_version(version),
            ResponseBody::DescribeGroups(_) => DescribeGroupsResponse::header_version(version),
            ResponseBody::ListGroups(_) => ListGroupsResponse::header_version(version),
            ResponseBody::ApiVersions(_) => ApiVersionsResponse::header_version(version),
            ResponseBody::CreateTopics(_) => CreateTopicsResponse::header_version(version),
            ResponseBody::DeleteTopics(_) => DeleteTopicsResponse::header_version(version),
            ResponseBody::DeleteRecords(_) => DeleteRecordsResponse::header_version(version),
            ResponseBody::InitProducerId(_) => InitProducerIdResponse::header_version(version),
            ResponseBody::OffsetForLeaderEpoch(_) => {
                OffsetForLeaderEpochResponse::header_version(version)
            }
            ResponseBody::AddPartitionsToTxn(_) => {
                AddPartitionsToTxnResponse::header_version(version)
            }
            ResponseBody::AddOffsetsToTxn(_) => AddOffsetsToTxnResponse::header_version(version),
            ResponseBody::EndTxn(_) => EndTxnResponse::header_version(version),
            ResponseBody::WriteTxnMarkers(_) => WriteTxnMarkersResponse::header_version(version),
            ResponseBody::DescribeAcls(_) => DescribeAclsResponse::header_version(version),
            ResponseBody::CreateAcls(_) => CreateAclsResponse::header_version(version),
            ResponseBody::DeleteAcls(_) => DeleteAclsResponse::header_version(version),
            ResponseBody::TxnOffsetCommit(_) => TxnOffsetCommitResponse::header_version(version),
            ResponseBody::AlterConfigs(_) => AlterConfigsResponse::header_version(version),
            ResponseBody::AlterReplicaLogDirs(_) => {
                AlterReplicaLogDirsResponse::header_version(version)
            }
            ResponseBody::DescribeLogDirs(_) => DescribeLogDirsResponse::header_version(version),
            ResponseBody::CreatePartitions(_) => CreatePartitionsResponse::header_version(version),
            ResponseBody::CreateDelegationToken(_) => {
                CreateDelegationTokenResponse::header_version(version)
            }
            ResponseBody::RenewDelegationToken(_) => {
                RenewDelegationTokenResponse::header_version(version)
            }
            ResponseBody::ExpireDelegationToken(_) => {
                ExpireDelegationTokenResponse::header_version(version)
            }
            ResponseBody::DescribeDelegationToken(_) => {
                DescribeDelegationTokenResponse::header_version(version)
            }
            ResponseBody::ElectLeaders(_) => ElectLeadersResponse::header_version(version),
            ResponseBody::IncrementalAlterConfigs(_) => {
                IncrementalAlterConfigsResponse::header_version(version)
            }
            ResponseBody::AlterPartitionReassignments(_) => {
                AlterPartitionReassignmentsResponse::header_version(version)
            }
            ResponseBody::ListPartitionReassignments(_) => {
                ListPartitionReassignmentsResponse::header_version(version)
            }
            ResponseBody::BrokerRegistration(_) => {
                BrokerRegistrationResponse::header_version(version)
            }
            ResponseBody::AllocateProducerIds(_) => {
                AllocateProducerIdsResponse::header_version(version)
            }
            ResponseBody::ListTransactions(_) => ListTransactionsResponse::header_version(version),
            ResponseBody::DescribeTransactions(_) => {
                DescribeTransactionsResponse::header_version(version)
            }
            ResponseBody::UnregisterBroker(_) => UnregisterBrokerResponse::header_version(version),
            ResponseBody::BrokerHeartbeat(_) => BrokerHeartbeatResponse::header_version(version),
            ResponseBody::DescribeProducers(_) => {
                DescribeProducersResponse::header_version(version)
            }
            ResponseBody::FetchSnapshot(_) => FetchSnapshotResponse::header_version(version),
            ResponseBody::UpdateFeatures(_) => UpdateFeaturesResponse::header_version(version),
            ResponseBody::AlterPartition(_) => AlterPartitionResponse::header_version(version),
            ResponseBody::EndQuorumEpoch(_) => EndQuorumEpochResponse::header_version(version),
            ResponseBody::Envelope(_) => EnvelopeResponse::header_version(version),
            ResponseBody::Vote(_) => VoteResponse::header_version(version),
            ResponseBody::DescribeUserScramCredentials(_) => {
                DescribeUserScramCredentialsResponse::header_version(version)
            }
            ResponseBody::DescribeQuorum(_) => DescribeQuorumResponse::header_version(version),
            ResponseBody::BeginQuorumEpoch(_) => BeginQuorumEpochResponse::header_version(version),
            ResponseBody::AlterUserScramCredentials(_) => {
                AlterUserScramCredentialsResponse::header_version(version)
            }
            ResponseBody::DescribeClientQuotas(_) => {
                DescribeClientQuotasResponse::header_version(version)
            }
            ResponseBody::AlterClientQuotas(_) => {
                AlterClientQuotasResponse::header_version(version)
            }
            ResponseBody::DescribeConfigs(_) => DescribeConfigsResponse::header_version(version),
            ResponseBody::OffsetDelete(_) => OffsetDeleteResponse::header_version(version),
            ResponseBody::DeleteGroups(_) => DeleteGroupsResponse::header_version(version),
        }
    }
}

impl KafkaFrame {
    pub fn from_bytes(
        mut bytes: Bytes,
        request_header: Option<CodecRequestHeader>,
    ) -> Result<Self> {
        // remove length header
        let _ = bytes.split_to(4);

        match request_header {
            Some(request_header) => KafkaFrame::parse_response(bytes, request_header),
            None => KafkaFrame::parse_request(bytes),
        }
    }

    fn parse_request(mut bytes: Bytes) -> Result<Self> {
        let api_key = i16::from_be_bytes(bytes[0..2].try_into().unwrap());
        let api_version = i16::from_be_bytes(bytes[2..4].try_into().unwrap());
        let api_key =
            ApiKey::try_from(api_key).map_err(|_| anyhow!("unknown api key {api_key}"))?;

        let header_version = api_key.request_header_version(api_version);
        let header = RequestHeader::decode(&mut bytes, header_version)
            .context("Failed to decode request header")?;

        let version = header.request_api_version;
        let body = match api_key {
            ApiKey::ProduceKey => RequestBody::Produce(decode(&mut bytes, version)?),
            ApiKey::FetchKey => RequestBody::Fetch(decode(&mut bytes, version)?),
            ApiKey::OffsetFetchKey => RequestBody::OffsetFetch(decode(&mut bytes, version)?),
            ApiKey::ListOffsetsKey => RequestBody::ListOffsets(decode(&mut bytes, version)?),
            ApiKey::JoinGroupKey => RequestBody::JoinGroup(decode(&mut bytes, version)?),
            ApiKey::SyncGroupKey => RequestBody::SyncGroup(decode(&mut bytes, version)?),
            ApiKey::MetadataKey => RequestBody::Metadata(decode(&mut bytes, version)?),
            ApiKey::FindCoordinatorKey => {
                RequestBody::FindCoordinator(decode(&mut bytes, version)?)
            }
            ApiKey::LeaderAndIsrKey => RequestBody::LeaderAndIsr(decode(&mut bytes, version)?),
            ApiKey::HeartbeatKey => RequestBody::Heartbeat(decode(&mut bytes, version)?),
            ApiKey::CreateTopicsKey => RequestBody::CreateTopics(decode(&mut bytes, version)?),
            ApiKey::DeleteTopicsKey => RequestBody::DeleteTopics(decode(&mut bytes, version)?),
            ApiKey::DeleteGroupsKey => RequestBody::DeleteGroups(decode(&mut bytes, version)?),
            ApiKey::DescribeConfigsKey => {
                RequestBody::DescribeConfigs(decode(&mut bytes, version)?)
            }
            ApiKey::SaslAuthenticateKey => {
                RequestBody::SaslAuthenticate(decode(&mut bytes, version)?)
            }
            ApiKey::SaslHandshakeKey => RequestBody::SaslHandshake(decode(&mut bytes, version)?),
            ApiKey::StopReplicaKey => RequestBody::StopReplica(decode(&mut bytes, version)?),
            ApiKey::UpdateMetadataKey => RequestBody::UpdateMetadata(decode(&mut bytes, version)?),
            ApiKey::ControlledShutdownKey => {
                RequestBody::ControlledShutdown(decode(&mut bytes, version)?)
            }
            ApiKey::OffsetCommitKey => RequestBody::OffsetCommit(decode(&mut bytes, version)?),
            ApiKey::LeaveGroupKey => RequestBody::LeaveGroup(decode(&mut bytes, version)?),
            ApiKey::DescribeGroupsKey => RequestBody::DescribeGroups(decode(&mut bytes, version)?),
            ApiKey::ListGroupsKey => RequestBody::ListGroups(decode(&mut bytes, version)?),
            ApiKey::ApiVersionsKey => RequestBody::ApiVersions(decode(&mut bytes, version)?),
            ApiKey::DeleteRecordsKey => RequestBody::DeleteRecords(decode(&mut bytes, version)?),
            ApiKey::InitProducerIdKey => RequestBody::InitProducerId(decode(&mut bytes, version)?),
            ApiKey::OffsetForLeaderEpochKey => {
                RequestBody::OffsetForLeaderEpoch(decode(&mut bytes, version)?)
            }
            ApiKey::AddPartitionsToTxnKey => {
                RequestBody::AddPartitionsToTxn(decode(&mut bytes, version)?)
            }
            ApiKey::AddOffsetsToTxnKey => {
                RequestBody::AddOffsetsToTxn(decode(&mut bytes, version)?)
            }
            ApiKey::EndTxnKey => RequestBody::EndTxn(decode(&mut bytes, version)?),
            ApiKey::WriteTxnMarkersKey => {
                RequestBody::WriteTxnMarkers(decode(&mut bytes, version)?)
            }

            ApiKey::TxnOffsetCommitKey => {
                RequestBody::TxnOffsetCommit(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeAclsKey => RequestBody::DescribeAcls(decode(&mut bytes, version)?),
            ApiKey::CreateAclsKey => RequestBody::CreateAcls(decode(&mut bytes, version)?),
            ApiKey::DeleteAclsKey => RequestBody::DeleteAcls(decode(&mut bytes, version)?),
            ApiKey::AlterConfigsKey => RequestBody::AlterConfigs(decode(&mut bytes, version)?),
            ApiKey::AlterReplicaLogDirsKey => {
                RequestBody::AlterReplicaLogDirs(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeLogDirsKey => {
                RequestBody::DescribeLogDirs(decode(&mut bytes, version)?)
            }
            ApiKey::CreatePartitionsKey => {
                RequestBody::CreatePartitions(decode(&mut bytes, version)?)
            }
            ApiKey::CreateDelegationTokenKey => {
                RequestBody::CreateDelegationToken(decode(&mut bytes, version)?)
            }
            ApiKey::RenewDelegationTokenKey => {
                RequestBody::RenewDelegationToken(decode(&mut bytes, version)?)
            }
            ApiKey::ExpireDelegationTokenKey => {
                RequestBody::ExpireDelegationToken(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeDelegationTokenKey => {
                RequestBody::DescribeDelegationToken(decode(&mut bytes, version)?)
            }
            ApiKey::ElectLeadersKey => RequestBody::ElectLeaders(decode(&mut bytes, version)?),
            ApiKey::IncrementalAlterConfigsKey => {
                RequestBody::IncrementalAlterConfigs(decode(&mut bytes, version)?)
            }
            ApiKey::AlterPartitionReassignmentsKey => {
                RequestBody::AlterPartitionReassignments(decode(&mut bytes, version)?)
            }
            ApiKey::ListPartitionReassignmentsKey => {
                RequestBody::ListPartitionReassignments(decode(&mut bytes, version)?)
            }
            ApiKey::OffsetDeleteKey => RequestBody::OffsetDelete(decode(&mut bytes, version)?),
            ApiKey::DescribeClientQuotasKey => {
                RequestBody::DescribeClientQuotas(decode(&mut bytes, version)?)
            }
            ApiKey::AlterClientQuotasKey => {
                RequestBody::AlterClientQuotas(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeUserScramCredentialsKey => {
                RequestBody::DescribeUserScramCredentials(decode(&mut bytes, version)?)
            }
            ApiKey::AlterUserScramCredentialsKey => {
                RequestBody::AlterUserScramCredentials(decode(&mut bytes, version)?)
            }
            ApiKey::VoteKey => RequestBody::Vote(decode(&mut bytes, version)?),
            ApiKey::BeginQuorumEpochKey => {
                RequestBody::BeginQuorumEpoch(decode(&mut bytes, version)?)
            }
            ApiKey::EndQuorumEpochKey => RequestBody::EndQuorumEpoch(decode(&mut bytes, version)?),
            ApiKey::DescribeQuorumKey => RequestBody::DescribeQuorum(decode(&mut bytes, version)?),
            ApiKey::AlterPartitionKey => RequestBody::AlterPartition(decode(&mut bytes, version)?),
            ApiKey::UpdateFeaturesKey => RequestBody::UpdateFeatures(decode(&mut bytes, version)?),
            ApiKey::EnvelopeKey => RequestBody::Envelope(decode(&mut bytes, version)?),
            ApiKey::FetchSnapshotKey => RequestBody::FetchSnapshot(decode(&mut bytes, version)?),
            ApiKey::DescribeProducersKey => {
                RequestBody::DescribeProducers(decode(&mut bytes, version)?)
            }
            ApiKey::BrokerRegistrationKey => {
                RequestBody::BrokerRegistration(decode(&mut bytes, version)?)
            }
            ApiKey::BrokerHeartbeatKey => {
                RequestBody::BrokerHeartbeat(decode(&mut bytes, version)?)
            }
            ApiKey::UnregisterBrokerKey => {
                RequestBody::UnregisterBroker(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeTransactionsKey => {
                RequestBody::DescribeTransactions(decode(&mut bytes, version)?)
            }
            ApiKey::ListTransactionsKey => {
                RequestBody::ListTransactions(decode(&mut bytes, version)?)
            }
            ApiKey::AllocateProducerIdsKey => {
                RequestBody::AllocateProducerIds(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeClusterKey => {
                RequestBody::DescribeCluster(decode(&mut bytes, version)?)
            }
        };

        Ok(KafkaFrame::Request { header, body })
    }

    fn parse_response(mut bytes: Bytes, request_header: CodecRequestHeader) -> Result<Self> {
        let header = ResponseHeader::decode(
            &mut bytes,
            request_header
                .api_key
                .response_header_version(request_header.version),
        )
        .context("Failed to decode response header")?;

        let version = request_header.version;
        let body = match request_header.api_key {
            ApiKey::ProduceKey => ResponseBody::Produce(decode(&mut bytes, version)?),
            ApiKey::FindCoordinatorKey => {
                ResponseBody::FindCoordinator(decode(&mut bytes, version)?)
            }
            ApiKey::FetchKey => ResponseBody::Fetch(decode(&mut bytes, version)?),
            ApiKey::OffsetFetchKey => ResponseBody::OffsetFetch(decode(&mut bytes, version)?),
            ApiKey::ListOffsetsKey => ResponseBody::ListOffsets(decode(&mut bytes, version)?),
            ApiKey::JoinGroupKey => ResponseBody::JoinGroup(decode(&mut bytes, version)?),
            ApiKey::SyncGroupKey => ResponseBody::SyncGroup(decode(&mut bytes, version)?),
            ApiKey::MetadataKey => ResponseBody::Metadata(decode(&mut bytes, version)?),
            ApiKey::DescribeClusterKey => {
                ResponseBody::DescribeCluster(decode(&mut bytes, version)?)
            }
            ApiKey::HeartbeatKey => ResponseBody::Heartbeat(decode(&mut bytes, version)?),
            ApiKey::SaslAuthenticateKey => {
                ResponseBody::SaslAuthenticate(decode(&mut bytes, version)?)
            }
            ApiKey::SaslHandshakeKey => ResponseBody::SaslHandshake(decode(&mut bytes, version)?),
            ApiKey::StopReplicaKey => ResponseBody::StopReplica(decode(&mut bytes, version)?),
            ApiKey::UpdateMetadataKey => ResponseBody::UpdateMetadata(decode(&mut bytes, version)?),
            ApiKey::ControlledShutdownKey => {
                ResponseBody::ControlledShutdown(decode(&mut bytes, version)?)
            }
            ApiKey::LeaderAndIsrKey => ResponseBody::LeaderAndIsr(decode(&mut bytes, version)?),
            ApiKey::OffsetCommitKey => ResponseBody::OffsetCommit(decode(&mut bytes, version)?),
            ApiKey::LeaveGroupKey => ResponseBody::LeaveGroup(decode(&mut bytes, version)?),
            ApiKey::DescribeGroupsKey => ResponseBody::DescribeGroups(decode(&mut bytes, version)?),
            ApiKey::ListGroupsKey => ResponseBody::ListGroups(decode(&mut bytes, version)?),
            ApiKey::ApiVersionsKey => ResponseBody::ApiVersions(decode(&mut bytes, version)?),
            ApiKey::CreateTopicsKey => ResponseBody::CreateTopics(decode(&mut bytes, version)?),
            ApiKey::DeleteTopicsKey => ResponseBody::DeleteTopics(decode(&mut bytes, version)?),
            ApiKey::DeleteRecordsKey => ResponseBody::DeleteRecords(decode(&mut bytes, version)?),
            ApiKey::InitProducerIdKey => ResponseBody::InitProducerId(decode(&mut bytes, version)?),
            ApiKey::OffsetForLeaderEpochKey => {
                ResponseBody::OffsetForLeaderEpoch(decode(&mut bytes, version)?)
            }
            ApiKey::AddPartitionsToTxnKey => {
                ResponseBody::AddPartitionsToTxn(decode(&mut bytes, version)?)
            }
            ApiKey::AddOffsetsToTxnKey => {
                ResponseBody::AddOffsetsToTxn(decode(&mut bytes, version)?)
            }
            ApiKey::EndTxnKey => ResponseBody::EndTxn(decode(&mut bytes, version)?),
            ApiKey::WriteTxnMarkersKey => {
                ResponseBody::WriteTxnMarkers(decode(&mut bytes, version)?)
            }

            ApiKey::TxnOffsetCommitKey => {
                ResponseBody::TxnOffsetCommit(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeAclsKey => ResponseBody::DescribeAcls(decode(&mut bytes, version)?),
            ApiKey::CreateAclsKey => ResponseBody::CreateAcls(decode(&mut bytes, version)?),
            ApiKey::DeleteAclsKey => ResponseBody::DeleteAcls(decode(&mut bytes, version)?),
            ApiKey::DescribeConfigsKey => {
                ResponseBody::DescribeConfigs(decode(&mut bytes, version)?)
            }
            ApiKey::AlterConfigsKey => ResponseBody::AlterConfigs(decode(&mut bytes, version)?),
            ApiKey::AlterReplicaLogDirsKey => {
                ResponseBody::AlterReplicaLogDirs(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeLogDirsKey => {
                ResponseBody::DescribeLogDirs(decode(&mut bytes, version)?)
            }
            ApiKey::CreatePartitionsKey => {
                ResponseBody::CreatePartitions(decode(&mut bytes, version)?)
            }
            ApiKey::CreateDelegationTokenKey => {
                ResponseBody::CreateDelegationToken(decode(&mut bytes, version)?)
            }
            ApiKey::RenewDelegationTokenKey => {
                ResponseBody::RenewDelegationToken(decode(&mut bytes, version)?)
            }
            ApiKey::ExpireDelegationTokenKey => {
                ResponseBody::ExpireDelegationToken(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeDelegationTokenKey => {
                ResponseBody::DescribeDelegationToken(decode(&mut bytes, version)?)
            }
            ApiKey::DeleteGroupsKey => ResponseBody::DeleteGroups(decode(&mut bytes, version)?),
            ApiKey::ElectLeadersKey => ResponseBody::ElectLeaders(decode(&mut bytes, version)?),
            ApiKey::IncrementalAlterConfigsKey => {
                ResponseBody::IncrementalAlterConfigs(decode(&mut bytes, version)?)
            }
            ApiKey::AlterPartitionReassignmentsKey => {
                ResponseBody::AlterPartitionReassignments(decode(&mut bytes, version)?)
            }
            ApiKey::ListPartitionReassignmentsKey => {
                ResponseBody::ListPartitionReassignments(decode(&mut bytes, version)?)
            }
            ApiKey::OffsetDeleteKey => ResponseBody::OffsetDelete(decode(&mut bytes, version)?),
            ApiKey::DescribeClientQuotasKey => {
                ResponseBody::DescribeClientQuotas(decode(&mut bytes, version)?)
            }
            ApiKey::AlterClientQuotasKey => {
                ResponseBody::AlterClientQuotas(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeUserScramCredentialsKey => {
                ResponseBody::DescribeUserScramCredentials(decode(&mut bytes, version)?)
            }
            ApiKey::AlterUserScramCredentialsKey => {
                ResponseBody::AlterUserScramCredentials(decode(&mut bytes, version)?)
            }
            ApiKey::VoteKey => ResponseBody::Vote(decode(&mut bytes, version)?),
            ApiKey::BeginQuorumEpochKey => {
                ResponseBody::BeginQuorumEpoch(decode(&mut bytes, version)?)
            }
            ApiKey::EndQuorumEpochKey => ResponseBody::EndQuorumEpoch(decode(&mut bytes, version)?),
            ApiKey::DescribeQuorumKey => ResponseBody::DescribeQuorum(decode(&mut bytes, version)?),
            ApiKey::AlterPartitionKey => ResponseBody::AlterPartition(decode(&mut bytes, version)?),
            ApiKey::UpdateFeaturesKey => ResponseBody::UpdateFeatures(decode(&mut bytes, version)?),
            ApiKey::EnvelopeKey => ResponseBody::Envelope(decode(&mut bytes, version)?),
            ApiKey::FetchSnapshotKey => ResponseBody::FetchSnapshot(decode(&mut bytes, version)?),
            ApiKey::DescribeProducersKey => {
                ResponseBody::DescribeProducers(decode(&mut bytes, version)?)
            }
            ApiKey::BrokerRegistrationKey => {
                ResponseBody::BrokerRegistration(decode(&mut bytes, version)?)
            }
            ApiKey::BrokerHeartbeatKey => {
                ResponseBody::BrokerHeartbeat(decode(&mut bytes, version)?)
            }
            ApiKey::UnregisterBrokerKey => {
                ResponseBody::UnregisterBroker(decode(&mut bytes, version)?)
            }
            ApiKey::DescribeTransactionsKey => {
                ResponseBody::DescribeTransactions(decode(&mut bytes, version)?)
            }
            ApiKey::ListTransactionsKey => {
                ResponseBody::ListTransactions(decode(&mut bytes, version)?)
            }
            ApiKey::AllocateProducerIdsKey => {
                ResponseBody::AllocateProducerIds(decode(&mut bytes, version)?)
            }
        };

        Ok(KafkaFrame::Response {
            version,
            header,
            body,
        })
    }

    pub fn encode(self, bytes: &mut BytesMut) -> Result<()> {
        // write dummy length
        let length_start = bytes.len();
        let bytes_start = length_start + 4;
        bytes.put_i32(0);

        // write message
        match self {
            KafkaFrame::Request { header, body } => {
                let header_version = ApiKey::try_from(header.request_api_key)
                    .map_err(|_| anyhow!("unknown api key {}", header.request_api_key))?
                    .request_header_version(header.request_api_version);
                header.encode(bytes, header_version)?;

                let version = header.request_api_version;
                match body {
                    RequestBody::Produce(x) => encode(x, bytes, version)?,
                    RequestBody::Fetch(x) => encode(x, bytes, version)?,
                    RequestBody::OffsetFetch(x) => encode(x, bytes, version)?,
                    RequestBody::ListOffsets(x) => encode(x, bytes, version)?,
                    RequestBody::JoinGroup(x) => encode(x, bytes, version)?,
                    RequestBody::SyncGroup(x) => encode(x, bytes, version)?,
                    RequestBody::Metadata(x) => encode(x, bytes, version)?,
                    RequestBody::FindCoordinator(x) => encode(x, bytes, version)?,
                    RequestBody::LeaderAndIsr(x) => encode(x, bytes, version)?,
                    RequestBody::Heartbeat(x) => encode(x, bytes, version)?,
                    RequestBody::CreateTopics(x) => encode(x, bytes, version)?,
                    RequestBody::DeleteTopics(x) => encode(x, bytes, version)?,
                    RequestBody::DeleteGroups(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeConfigs(x) => encode(x, bytes, version)?,
                    RequestBody::SaslAuthenticate(x) => encode(x, bytes, version)?,
                    RequestBody::SaslHandshake(x) => encode(x, bytes, version)?,
                    RequestBody::StopReplica(x) => encode(x, bytes, version)?,
                    RequestBody::UpdateMetadata(x) => encode(x, bytes, version)?,
                    RequestBody::ControlledShutdown(x) => encode(x, bytes, version)?,
                    RequestBody::OffsetCommit(x) => encode(x, bytes, version)?,
                    RequestBody::LeaveGroup(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeGroups(x) => encode(x, bytes, version)?,
                    RequestBody::ListGroups(x) => encode(x, bytes, version)?,
                    RequestBody::ApiVersions(x) => encode(x, bytes, version)?,
                    RequestBody::DeleteRecords(x) => encode(x, bytes, version)?,
                    RequestBody::InitProducerId(x) => encode(x, bytes, version)?,
                    RequestBody::OffsetForLeaderEpoch(x) => encode(x, bytes, version)?,
                    RequestBody::AddPartitionsToTxn(x) => encode(x, bytes, version)?,
                    RequestBody::AddOffsetsToTxn(x) => encode(x, bytes, version)?,
                    RequestBody::EndTxn(x) => encode(x, bytes, version)?,
                    RequestBody::WriteTxnMarkers(x) => encode(x, bytes, version)?,
                    RequestBody::TxnOffsetCommit(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeAcls(x) => encode(x, bytes, version)?,
                    RequestBody::CreateAcls(x) => encode(x, bytes, version)?,
                    RequestBody::DeleteAcls(x) => encode(x, bytes, version)?,
                    RequestBody::AlterConfigs(x) => encode(x, bytes, version)?,
                    RequestBody::AlterReplicaLogDirs(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeLogDirs(x) => encode(x, bytes, version)?,
                    RequestBody::CreatePartitions(x) => encode(x, bytes, version)?,
                    RequestBody::CreateDelegationToken(x) => encode(x, bytes, version)?,
                    RequestBody::RenewDelegationToken(x) => encode(x, bytes, version)?,
                    RequestBody::ExpireDelegationToken(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeDelegationToken(x) => encode(x, bytes, version)?,
                    RequestBody::ElectLeaders(x) => encode(x, bytes, version)?,
                    RequestBody::IncrementalAlterConfigs(x) => encode(x, bytes, version)?,
                    RequestBody::AlterPartitionReassignments(x) => encode(x, bytes, version)?,
                    RequestBody::ListPartitionReassignments(x) => encode(x, bytes, version)?,
                    RequestBody::OffsetDelete(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeClientQuotas(x) => encode(x, bytes, version)?,
                    RequestBody::AlterClientQuotas(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeUserScramCredentials(x) => encode(x, bytes, version)?,
                    RequestBody::AlterUserScramCredentials(x) => encode(x, bytes, version)?,
                    RequestBody::Vote(x) => encode(x, bytes, version)?,
                    RequestBody::BeginQuorumEpoch(x) => encode(x, bytes, version)?,
                    RequestBody::EndQuorumEpoch(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeQuorum(x) => encode(x, bytes, version)?,
                    RequestBody::AlterPartition(x) => encode(x, bytes, version)?,
                    RequestBody::UpdateFeatures(x) => encode(x, bytes, version)?,
                    RequestBody::Envelope(x) => encode(x, bytes, version)?,
                    RequestBody::FetchSnapshot(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeProducers(x) => encode(x, bytes, version)?,
                    RequestBody::BrokerRegistration(x) => encode(x, bytes, version)?,
                    RequestBody::BrokerHeartbeat(x) => encode(x, bytes, version)?,
                    RequestBody::UnregisterBroker(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeTransactions(x) => encode(x, bytes, version)?,
                    RequestBody::ListTransactions(x) => encode(x, bytes, version)?,
                    RequestBody::AllocateProducerIds(x) => encode(x, bytes, version)?,
                    RequestBody::DescribeCluster(x) => encode(x, bytes, version)?,
                    RequestBody::AlterScramCredentials(x) => encode(x, bytes, version)?,
                }
            }
            KafkaFrame::Response {
                version,
                header,
                body,
            } => {
                header.encode(bytes, body.header_version(version))?;
                match body {
                    ResponseBody::Produce(x) => encode(x, bytes, version)?,
                    ResponseBody::FindCoordinator(x) => encode(x, bytes, version)?,
                    ResponseBody::Fetch(x) => encode(x, bytes, version)?,
                    ResponseBody::OffsetFetch(x) => encode(x, bytes, version)?,
                    ResponseBody::ListOffsets(x) => encode(x, bytes, version)?,
                    ResponseBody::JoinGroup(x) => encode(x, bytes, version)?,
                    ResponseBody::SyncGroup(x) => encode(x, bytes, version)?,
                    ResponseBody::Metadata(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeCluster(x) => encode(x, bytes, version)?,
                    ResponseBody::Heartbeat(x) => encode(x, bytes, version)?,
                    ResponseBody::SaslAuthenticate(x) => encode(x, bytes, version)?,
                    ResponseBody::SaslHandshake(x) => encode(x, bytes, version)?,
                    ResponseBody::StopReplica(x) => encode(x, bytes, version)?,
                    ResponseBody::UpdateMetadata(x) => encode(x, bytes, version)?,
                    ResponseBody::ControlledShutdown(x) => encode(x, bytes, version)?,
                    ResponseBody::LeaderAndIsr(x) => encode(x, bytes, version)?,
                    ResponseBody::OffsetCommit(x) => encode(x, bytes, version)?,
                    ResponseBody::LeaveGroup(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeGroups(x) => encode(x, bytes, version)?,
                    ResponseBody::ListGroups(x) => encode(x, bytes, version)?,
                    ResponseBody::ApiVersions(x) => encode(x, bytes, version)?,
                    ResponseBody::CreateTopics(x) => encode(x, bytes, version)?,
                    ResponseBody::DeleteTopics(x) => encode(x, bytes, version)?,
                    ResponseBody::DeleteRecords(x) => encode(x, bytes, version)?,
                    ResponseBody::InitProducerId(x) => encode(x, bytes, version)?,
                    ResponseBody::OffsetForLeaderEpoch(x) => encode(x, bytes, version)?,
                    ResponseBody::AddPartitionsToTxn(x) => encode(x, bytes, version)?,
                    ResponseBody::AddOffsetsToTxn(x) => encode(x, bytes, version)?,
                    ResponseBody::EndTxn(x) => encode(x, bytes, version)?,
                    ResponseBody::WriteTxnMarkers(x) => encode(x, bytes, version)?,
                    ResponseBody::TxnOffsetCommit(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeAcls(x) => encode(x, bytes, version)?,
                    ResponseBody::CreateAcls(x) => encode(x, bytes, version)?,
                    ResponseBody::DeleteAcls(x) => encode(x, bytes, version)?,
                    ResponseBody::AlterConfigs(x) => encode(x, bytes, version)?,
                    ResponseBody::AlterReplicaLogDirs(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeLogDirs(x) => encode(x, bytes, version)?,
                    ResponseBody::CreatePartitions(x) => encode(x, bytes, version)?,
                    ResponseBody::CreateDelegationToken(x) => encode(x, bytes, version)?,
                    ResponseBody::RenewDelegationToken(x) => encode(x, bytes, version)?,
                    ResponseBody::ExpireDelegationToken(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeDelegationToken(x) => encode(x, bytes, version)?,
                    ResponseBody::ElectLeaders(x) => encode(x, bytes, version)?,
                    ResponseBody::IncrementalAlterConfigs(x) => encode(x, bytes, version)?,
                    ResponseBody::AlterPartitionReassignments(x) => encode(x, bytes, version)?,
                    ResponseBody::ListPartitionReassignments(x) => encode(x, bytes, version)?,
                    ResponseBody::OffsetDelete(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeClientQuotas(x) => encode(x, bytes, version)?,
                    ResponseBody::AlterClientQuotas(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeUserScramCredentials(x) => encode(x, bytes, version)?,
                    ResponseBody::AlterUserScramCredentials(x) => encode(x, bytes, version)?,
                    ResponseBody::Vote(x) => encode(x, bytes, version)?,
                    ResponseBody::BeginQuorumEpoch(x) => encode(x, bytes, version)?,
                    ResponseBody::EndQuorumEpoch(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeQuorum(x) => encode(x, bytes, version)?,
                    ResponseBody::AlterPartition(x) => encode(x, bytes, version)?,
                    ResponseBody::UpdateFeatures(x) => encode(x, bytes, version)?,
                    ResponseBody::Envelope(x) => encode(x, bytes, version)?,
                    ResponseBody::FetchSnapshot(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeProducers(x) => encode(x, bytes, version)?,
                    ResponseBody::BrokerRegistration(x) => encode(x, bytes, version)?,
                    ResponseBody::BrokerHeartbeat(x) => encode(x, bytes, version)?,
                    ResponseBody::UnregisterBroker(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeTransactions(x) => encode(x, bytes, version)?,
                    ResponseBody::ListTransactions(x) => encode(x, bytes, version)?,
                    ResponseBody::AllocateProducerIds(x) => encode(x, bytes, version)?,
                    ResponseBody::DescribeConfigs(x) => encode(x, bytes, version)?,
                    ResponseBody::DeleteGroups(x) => encode(x, bytes, version)?,
                }
            }
        }

        // overwrite dummy length with actual length of serialized bytes
        let bytes_len = bytes.len() - bytes_start;
        bytes[length_start..bytes_start].copy_from_slice(&(bytes_len as i32).to_be_bytes());

        Ok(())
    }
}

fn decode<T: Decodable>(bytes: &mut Bytes, version: i16) -> Result<T> {
    T::decode(bytes, version).with_context(|| {
        format!(
            "Failed to decode {} v{} body",
            std::any::type_name::<T>(),
            version
        )
    })
}

fn encode<T: Encodable>(encodable: T, bytes: &mut BytesMut, version: i16) -> Result<()> {
    encodable.encode(bytes, version).with_context(|| {
        format!(
            "Failed to encode {} v{} body",
            std::any::type_name::<T>(),
            version
        )
    })
}

/// This function is a helper to workaround a really degenerate rust compiler case.
/// The problem is that the string crate defines a TryFrom which collides with the stdlib TryFrom
/// and then naming the correct TryFrom becomes really annoying.
pub fn strbytes(str: &str) -> StrBytes {
    <StrBytes as string::TryFrom<Bytes>>::try_from(Bytes::copy_from_slice(str.as_bytes())).unwrap()
}

/// Allocationless version of kafka_strbytes
pub fn strbytes_static(str: &'static str) -> StrBytes {
    <StrBytes as string::TryFrom<Bytes>>::try_from(Bytes::from(str)).unwrap()
}
