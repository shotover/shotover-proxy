use crate::codec::kafka::KafkaCodecState;
use crate::codec::kafka::RequestHeader as CodecRequestHeader;
use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use std::fmt::{Display, Formatter, Result as FmtResult};

pub use kafka_protocol::messages::RequestKind as RequestBody;
pub use kafka_protocol::messages::ResponseKind as ResponseBody;
pub use kafka_protocol::protocol::StrBytes;

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

impl KafkaFrame {
    pub fn from_bytes(mut bytes: Bytes, codec_state: KafkaCodecState) -> Result<Self> {
        if codec_state.raw_sasl {
            match &codec_state.request_header {
                Some(_) => Ok(KafkaFrame::Response {
                    version: 0,
                    header: ResponseHeader::default(),
                    body: ResponseBody::SaslAuthenticate(
                        SaslAuthenticateResponse::default().with_auth_bytes(bytes),
                        // We dont set error_code field when the response contains a scram error, which sounds problematic.
                        // But in reality, at least for raw sasl mode, if kafka encounters an auth failure,
                        // it just kills the connection without sending any sasl response at all.
                        // So we never actually receive a scram response containing an error and
                        // so there would be no case where the error_code field would need to be set.
                    ),
                }),
                None => Ok(KafkaFrame::Request {
                    header: RequestHeader::default()
                        .with_request_api_key(ApiKey::SaslAuthenticateKey as i16),
                    body: RequestBody::SaslAuthenticate(
                        SaslAuthenticateRequest::default().with_auth_bytes(bytes),
                    ),
                }),
            }
        } else {
            // remove length header
            let _ = bytes.split_to(4);
            match &codec_state.request_header {
                Some(request_header) => KafkaFrame::parse_response(bytes, *request_header),
                None => KafkaFrame::parse_request(bytes),
            }
        }
    }

    fn parse_request(mut bytes: Bytes) -> Result<Self> {
        let api_key = i16::from_be_bytes(bytes[0..2].try_into().unwrap());
        let api_version = i16::from_be_bytes(bytes[2..4].try_into().unwrap());

        // We cannot parse unknown API keys into a RequestBody::Unknown(_) style variant since its impossible to
        // parse the request header without calling request_header_version.
        // Just an unfortunate limitation of the protocol.
        let api_key =
            ApiKey::try_from(api_key).map_err(|_| anyhow!("unknown api key {api_key}"))?;

        let header_version = api_key.request_header_version(api_version);
        let header = RequestHeader::decode(&mut bytes, header_version)
            .context("Failed to decode request header")?;

        let version = header.request_api_version;
        let body = RequestBody::decode(api_key, &mut bytes, version)?;

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
        let body = ResponseBody::decode(request_header.api_key, &mut bytes, version)?;

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
                body.encode(bytes, version)?
            }
            KafkaFrame::Response {
                version,
                header,
                body,
            } => {
                header.encode(bytes, body.header_version(version))?;
                body.encode(bytes, version)?
            }
        }

        // overwrite dummy length with actual length of serialized bytes
        let bytes_len = bytes.len() - bytes_start;
        bytes[length_start..bytes_start].copy_from_slice(&(bytes_len as i32).to_be_bytes());

        Ok(())
    }
}
