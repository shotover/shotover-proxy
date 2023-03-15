use anyhow::anyhow;
use bytes::{Bytes, BytesMut};
use cassandra_protocol::compression::Compression;
use cassandra_protocol::frame::Version;
use std::fmt;

pub(self) const UNCOMPRESSED_FRAME_HEADER_LENGTH: usize = 6;
// pub(self) const COMPRESSED_FRAME_HEADER_LENGTH: usize = 8;
pub(self) const FRAME_TRAILER_LENGTH: usize = 4;

/// Number of bytes in the header
const ENVELOPE_HEADER_LEN: usize = 9;
/// Number of stream bytes in accordance to protocol.
// pub const STREAM_LEN: usize = 2;
/// Number of body length bytes in accordance to protocol.
// pub const LENGTH_LEN: usize = 4;

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
    Uncompressed(UncompressedFrameCodec),
}

impl fmt::Debug for CassandraFrameCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variant = match self {
            Self::Legacy(_) => "Legacy",
            Self::Uncompressed(_) => "Uncompressed",
        };

        write!(f, "CassandraFrameCodec::{}", variant)
    }
}

impl Default for CassandraFrameCodec {
    fn default() -> Self {
        Self::Legacy(LegacyFrameCodec {})
    }
}

impl CassandraFrameCodec {
    pub fn check_size(&self, src: &mut BytesMut) -> Result<usize, CheckFrameSizeError> {
        match self {
            Self::Legacy(c) => c.check_size(src),
            Self::Uncompressed(c) => c.check_size(src),
        }
    }

    pub fn decode_envelopes(
        &self,
        src: BytesMut,
        frame_compression: Compression,
    ) -> anyhow::Result<Vec<BytesMut>> {
        match self {
            Self::Legacy(c) => c.decode_envelopes(src, frame_compression),
            Self::Uncompressed(c) => c.decode_envelopes(src, frame_compression),
        }
    }

    pub fn encode_envelopes(&self, envelopes: Vec<Bytes>) -> Bytes {
        match self {
            Self::Legacy(c) => c.encode_envelopes(envelopes),
            Self::Uncompressed(c) => c.encode_envelopes(envelopes),
        }
    }
}

#[derive(Copy, Clone)]
pub struct UncompressedFrameCodec {}

impl UncompressedFrameCodec {
    pub fn check_size(&self, src: &mut BytesMut) -> Result<usize, CheckFrameSizeError> {
        let buffer_len = src.len();
        if buffer_len < UNCOMPRESSED_FRAME_HEADER_LENGTH {
            return Err(CheckFrameSizeError::NotEnoughBytes);
        }

        let header = if buffer_len >= 8 {
            i64::from_le_bytes(src[..8].try_into().unwrap()) & 0xffffffffffff
        } else {
            let mut header = 0;
            for (i, byte) in src[..UNCOMPRESSED_FRAME_HEADER_LENGTH].iter().enumerate() {
                header |= (*byte as i64) << (8 * i as i64);
            }

            header
        };

        let payload_length = (header & 0x1ffff) as usize;
        let payload_end = UNCOMPRESSED_FRAME_HEADER_LENGTH + payload_length;

        let frame_len = payload_end + FRAME_TRAILER_LENGTH;
        if buffer_len < frame_len {
            return Err(CheckFrameSizeError::NotEnoughBytes);
        }

        Ok(frame_len)
    }

    pub fn decode_envelopes(
        &self,
        src: BytesMut,
        _frame_compression: Compression,
    ) -> anyhow::Result<Vec<BytesMut>> {
        let buffer_len = src.len();

        let header = if buffer_len >= 8 {
            i64::from_le_bytes(src[..8].try_into().unwrap()) & 0xffffffffffff
        } else {
            let mut header = 0;
            for (i, byte) in src[..UNCOMPRESSED_FRAME_HEADER_LENGTH].iter().enumerate() {
                header |= (*byte as i64) << (8 * i as i64);
            }

            header
        };

        let header_crc24 = ((header >> 24) & 0xffffff) as i32;
        let computed_crc = cassandra_protocol::crc::crc24(&header.to_le_bytes()[..3]);

        if header_crc24 != computed_crc {
            return Err(anyhow!(format!(
                "Header CRC mismatch - expected {header_crc24}, found {computed_crc}."
            )));
        }

        let payload_length = (header & 0x1ffff) as usize;
        let payload_end = UNCOMPRESSED_FRAME_HEADER_LENGTH + payload_length;

        let frame_end = payload_end + FRAME_TRAILER_LENGTH;

        let payload_crc32 = u32::from_le_bytes(src[payload_end..frame_end].try_into().unwrap());

        let computed_crc =
            cassandra_protocol::crc::crc32(&src[UNCOMPRESSED_FRAME_HEADER_LENGTH..payload_end]);
        if payload_crc32 != computed_crc {
            return Err(anyhow!(format!(
                "Payload CRC mismatch - read {payload_crc32}, computed {computed_crc}."
            )));
        }

        let self_contained = (header & (1 << 17)) != 0;
        if !self_contained {
            unimplemented!("Cannot support non-self contained frames yet");
        }

        let payload = &src[UNCOMPRESSED_FRAME_HEADER_LENGTH..payload_end];

        let mut current_pos = 0;
        let mut envelopes: Vec<BytesMut> = vec![];

        loop {
            if payload[current_pos..].len() < ENVELOPE_HEADER_LEN {
                break;
            }

            let body_len = i32::from_be_bytes(
                payload[current_pos + 5..current_pos + 9]
                    .try_into()
                    .unwrap(),
            ) as usize;

            let envelope_len = ENVELOPE_HEADER_LEN + body_len;

            if current_pos + envelope_len > payload.len() {
                break;
            }

            let envelope = &payload[current_pos..current_pos + envelope_len];

            envelopes.push(envelope.into());
            current_pos += envelope_len;
        }

        Ok(envelopes)
    }

    pub fn encode_envelopes(&self, envelopes: Vec<Bytes>) -> Bytes {
        let mut buffer = BytesMut::new();
        buffer.resize(UNCOMPRESSED_FRAME_HEADER_LENGTH, 0);
        buffer.extend(envelopes.into_iter());

        let len = buffer.len();

        let mut len = (len - UNCOMPRESSED_FRAME_HEADER_LENGTH) as u64;
        if true {
            // TODO if self_contained
            len |= 1 << 17;
        }

        put3b(&mut buffer[..], len as i32);
        put3b(
            &mut buffer[3..],
            cassandra_protocol::crc::crc24(&len.to_le_bytes()[..3]),
        );

        add_trailer(&mut buffer, UNCOMPRESSED_FRAME_HEADER_LENGTH);

        buffer.freeze()
    }
}

#[inline]
fn put3b(buffer: &mut [u8], value: i32) {
    let value = value.to_le_bytes();
    buffer[0] = value[0];
    buffer[1] = value[1];
    buffer[2] = value[2];
}

#[inline]
fn add_trailer(buffer: &mut BytesMut, payload_start: usize) {
    buffer.reserve(4);

    let crc = cassandra_protocol::crc::crc32(&buffer[payload_start..]).to_le_bytes();

    buffer.extend_from_slice(&[crc[0], crc[1], crc[2], crc[3]]);
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

    pub fn encode_envelopes(&self, envelopes: Vec<Bytes>) -> Bytes {
        envelopes.into_iter().flatten().collect()
    }
}
