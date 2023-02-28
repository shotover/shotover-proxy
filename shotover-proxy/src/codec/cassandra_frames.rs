use bytes::{Buf, BufMut, Bytes, BytesMut};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::Version;
//use std::io;

/// Number of bytes in the header
const ENVELOPE_HEADER_LEN: usize = 9;
/// Number of stream bytes in accordance to protocol.
pub const STREAM_LEN: usize = 2;
/// Number of body length bytes in accordance to protocol.
pub const LENGTH_LEN: usize = 4;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CheckFrameSizeError {
    #[error("Not enough bytes!")]
    NotEnoughBytes,
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(u8),
    #[error("Unsupported opcode: {0}")]
    UnsupportedOpcode(u8),
}

#[derive(Copy, Clone)]
pub enum CassandraFrameCodec {
    Legacy(LegacyFrameCodec),
}

impl CassandraFrameCodec {
    pub fn new() -> Self {
        Self::Legacy(LegacyFrameCodec {})
    }

    pub fn check_size(&self, src: &mut BytesMut) -> Result<usize, CheckFrameSizeError> {
        match self {
            Self::Legacy(c) => c.check_size(src),
        }
    }

    pub fn decode_envelopes(
        &self,
        src: BytesMut,
        frame_compression: Compression,
    ) -> anyhow::Result<Vec<BytesMut>> {
        match self {
            Self::Legacy(c) => c.decode_envelopes(src, frame_compression),
        }
    }

    pub fn encode_envelopes(&self, dst: &mut BytesMut, envelopes: Vec<Bytes>) {
        match self {
            Self::Legacy(c) => c.encode_envelopes(dst, envelopes),
        }
    }
}

#[derive(Copy, Clone)]
pub struct LegacyFrameCodec {}

impl LegacyFrameCodec {
    pub fn check_size(&self, src: &mut BytesMut) -> Result<usize, CheckFrameSizeError> {
        if src.len() < ENVELOPE_HEADER_LEN {
            return Err(CheckFrameSizeError::NotEnoughBytes);
        }

        let body_len = i32::from_be_bytes(src[5..9].try_into().unwrap()) as usize;

        let envelope_len = ENVELOPE_HEADER_LEN + body_len;
        if src.len() < envelope_len {
            return Err(CheckFrameSizeError::NotEnoughBytes);
        }
        let _ = Version::try_from(src[0])
            .map_err(|_| CheckFrameSizeError::UnsupportedVersion(src[0] & 0x7f))?;

        Ok(envelope_len)
    }

    pub fn decode_envelopes(
        &self,
        src: BytesMut,
        _frame_compression: Compression,
    ) -> anyhow::Result<Vec<BytesMut>> {
        Ok(vec![src])
    }

    pub fn encode_envelopes(&self, dst: &mut BytesMut, envelopes: Vec<Bytes>) {
        for envelope in envelopes {
            dst.extend_from_slice(&envelope);
        }
    }
}
