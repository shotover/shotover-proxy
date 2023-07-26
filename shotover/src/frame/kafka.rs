use crate::codec::kafka::RequestHeader as CodecRequestHeader;
use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, DescribeClusterResponse, FetchRequest, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, LeaderAndIsrRequest, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, OffsetFetchRequest, OffsetFetchResponse, ProduceRequest,
    ProduceResponse, RequestHeader, ResponseHeader, SyncGroupRequest, SyncGroupResponse,
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
    Unknown { api_key: ApiKey, message: Bytes },
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
    Unknown { api_key: ApiKey, message: Bytes },
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
            ResponseBody::Unknown { api_key, .. } => api_key.response_header_version(version),
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
        let header_version = ApiKey::try_from(api_key)
            .unwrap()
            .request_header_version(api_version);
        let header = RequestHeader::decode(&mut bytes, header_version)
            .context("Failed to decode request header")?;

        let api_key = ApiKey::try_from(header.request_api_key)
            .map_err(|_| anyhow!("unknown api key {}", header.request_api_key))?;
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
            api_key => RequestBody::Unknown {
                api_key,
                message: bytes,
            },
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
            api_key => ResponseBody::Unknown {
                api_key,
                message: bytes,
            },
        };

        Ok(KafkaFrame::Response {
            version,
            header,
            body,
        })
    }

    pub fn bytes_len(&self) -> Result<usize> {
        Ok(match &self {
            KafkaFrame::Request { header, body } => {
                let header_version = ApiKey::try_from(header.request_api_key)
                    .map_err(|_| anyhow!("unknown api key {}", header.request_api_key))?
                    .request_header_version(header.request_api_version);

                let version = header.request_api_version;
                4 + header.compute_size(header_version)?
                    + match body {
                        RequestBody::Produce(x) => compute_size(x, version)?,
                        RequestBody::Fetch(x) => compute_size(x, version)?,
                        RequestBody::OffsetFetch(x) => compute_size(x, version)?,
                        RequestBody::ListOffsets(x) => compute_size(x, version)?,
                        RequestBody::JoinGroup(x) => compute_size(x, version)?,
                        RequestBody::SyncGroup(x) => compute_size(x, version)?,
                        RequestBody::Metadata(x) => compute_size(x, version)?,
                        RequestBody::FindCoordinator(x) => compute_size(x, version)?,
                        RequestBody::LeaderAndIsr(x) => compute_size(x, version)?,
                        RequestBody::Heartbeat(x) => compute_size(x, version)?,
                        RequestBody::Unknown { message, .. } => message.len(),
                    }
            }
            KafkaFrame::Response {
                version,
                header,
                body,
            } => {
                let version = *version;
                4 + header.compute_size(body.header_version(version))?
                    + match body {
                        ResponseBody::Produce(x) => compute_size(x, version)?,
                        ResponseBody::FindCoordinator(x) => compute_size(x, version)?,
                        ResponseBody::Fetch(x) => compute_size(x, version)?,
                        ResponseBody::OffsetFetch(x) => compute_size(x, version)?,
                        ResponseBody::ListOffsets(x) => compute_size(x, version)?,
                        ResponseBody::JoinGroup(x) => compute_size(x, version)?,
                        ResponseBody::SyncGroup(x) => compute_size(x, version)?,
                        ResponseBody::Metadata(x) => compute_size(x, version)?,
                        ResponseBody::DescribeCluster(x) => compute_size(x, version)?,
                        ResponseBody::Heartbeat(x) => compute_size(x, version)?,
                        ResponseBody::Unknown { message, .. } => message.len(),
                    }
            }
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
                    RequestBody::Unknown { message, .. } => bytes.extend_from_slice(&message),
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
                    ResponseBody::Unknown { message, .. } => bytes.extend_from_slice(&message),
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

fn compute_size<T: Encodable>(encodable: &T, version: i16) -> Result<usize> {
    encodable.compute_size(version).with_context(|| {
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
