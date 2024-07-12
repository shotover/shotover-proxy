//! Taken from <https://github.com/scylladb/scylla-rust-driver/blob/4a4fd0e5e785031956f560ecf22cb8653eea122b/scylla/src/routing.rs>
//! We cant import it as that would bring the openssl dependency into shotover.

use bytes::Buf;
use cassandra_protocol::token::Murmur3Token;
use std::num::Wrapping;

pub struct Murmur3PartitionerHasher {
    total_len: usize,
    buf: [u8; Self::BUF_CAPACITY],
    h1: Wrapping<i64>,
    h2: Wrapping<i64>,
}

impl Murmur3PartitionerHasher {
    const BUF_CAPACITY: usize = 16;

    const C1: Wrapping<i64> = Wrapping(0x87c3_7b91_1142_53d5_u64 as i64);
    const C2: Wrapping<i64> = Wrapping(0x4cf5_ad43_2745_937f_u64 as i64);

    pub fn new() -> Self {
        Self {
            total_len: 0,
            buf: Default::default(),
            h1: Wrapping(0),
            h2: Wrapping(0),
        }
    }

    fn hash_16_bytes(&mut self, mut k1: Wrapping<i64>, mut k2: Wrapping<i64>) {
        k1 *= Self::C1;
        k1 = Self::rotl64(k1, 31);
        k1 *= Self::C2;
        self.h1 ^= k1;

        self.h1 = Self::rotl64(self.h1, 27);
        self.h1 += self.h2;
        self.h1 = self.h1 * Wrapping(5) + Wrapping(0x52dce729);

        k2 *= Self::C2;
        k2 = Self::rotl64(k2, 33);
        k2 *= Self::C1;
        self.h2 ^= k2;

        self.h2 = Self::rotl64(self.h2, 31);
        self.h2 += self.h1;
        self.h2 = self.h2 * Wrapping(5) + Wrapping(0x38495ab5);
    }

    fn fetch_16_bytes_from_buf(buf: &mut &[u8]) -> (Wrapping<i64>, Wrapping<i64>) {
        let k1 = Wrapping(buf.get_i64_le());
        let k2 = Wrapping(buf.get_i64_le());
        (k1, k2)
    }

    #[inline]
    fn rotl64(v: Wrapping<i64>, n: u32) -> Wrapping<i64> {
        Wrapping((v.0 << n) | (v.0 as u64 >> (64 - n)) as i64)
    }

    #[inline]
    fn fmix(mut k: Wrapping<i64>) -> Wrapping<i64> {
        k ^= Wrapping((k.0 as u64 >> 33) as i64);
        k *= Wrapping(0xff51afd7ed558ccd_u64 as i64);
        k ^= Wrapping((k.0 as u64 >> 33) as i64);
        k *= Wrapping(0xc4ceb9fe1a85ec53_u64 as i64);
        k ^= Wrapping((k.0 as u64 >> 33) as i64);

        k
    }
}

// The implemented Murmur3 algorithm is roughly as follows:
// 1. while there are at least 16 bytes given:
//      consume 16 bytes by parsing them into i64s, then
//      include them in h1, h2, k1, k2;
// 2. do some magic with remaining n < 16 bytes,
//      include them in h1, h2, k1, k2;
// 3. compute the token based on h1, h2, k1, k2.
//
// Therefore, the buffer of capacity 16 is used. As soon as it gets full,
// point 1. is executed. Points 2. and 3. are exclusively done in `finish()`,
// so they don't mutate the state.
impl Murmur3PartitionerHasher {
    pub fn write(&mut self, mut pk_part: &[u8]) {
        let mut buf_len = self.total_len % Self::BUF_CAPACITY;
        self.total_len += pk_part.len();

        // If the buffer is nonempty and can be filled completely, so that we can fetch two i64s from it,
        // fill it and hash its contents, then make it empty.
        if buf_len > 0 && Self::BUF_CAPACITY - buf_len <= pk_part.len() {
            // First phase: populate buffer until full, then consume two i64s.
            let to_write = Ord::min(Self::BUF_CAPACITY - buf_len, pk_part.len());
            self.buf[buf_len..buf_len + to_write].copy_from_slice(&pk_part[..to_write]);
            pk_part.advance(to_write);
            buf_len += to_write;

            debug_assert_eq!(buf_len, Self::BUF_CAPACITY);
            // consume 16 bytes from internal buf
            let mut buf_ptr = &self.buf[..];
            let (k1, k2) = Self::fetch_16_bytes_from_buf(&mut buf_ptr);
            debug_assert!(buf_ptr.is_empty());
            self.hash_16_bytes(k1, k2);
            buf_len = 0;
        }

        // If there were enough data, now we have an empty buffer. Further data, if enough, can be hence
        // hashed directly from the external buffer.
        if buf_len == 0 {
            // Second phase: fast path for big values.
            while pk_part.len() >= Self::BUF_CAPACITY {
                let (k1, k2) = Self::fetch_16_bytes_from_buf(&mut pk_part);
                self.hash_16_bytes(k1, k2);
            }
        }

        // Third phase: move remaining bytes to the buffer.
        debug_assert!(pk_part.len() < Self::BUF_CAPACITY - buf_len);
        let to_write = pk_part.len();
        self.buf[buf_len..buf_len + to_write].copy_from_slice(&pk_part[..to_write]);
        pk_part.advance(to_write);
        buf_len += to_write;
        debug_assert!(pk_part.is_empty());

        debug_assert!(buf_len < Self::BUF_CAPACITY);
    }

    pub fn finish(&self) -> Murmur3Token {
        let mut h1 = self.h1;
        let mut h2 = self.h2;

        let mut k1 = Wrapping(0_i64);
        let mut k2 = Wrapping(0_i64);

        let buf_len = self.total_len % Self::BUF_CAPACITY;

        if buf_len > 8 {
            for i in (8..buf_len).rev() {
                k2 ^= Wrapping(self.buf[i] as i8 as i64) << ((i - 8) * 8);
            }

            k2 *= Self::C2;
            k2 = Self::rotl64(k2, 33);
            k2 *= Self::C1;
            h2 ^= k2;
        }

        if buf_len > 0 {
            for i in (0..std::cmp::min(8, buf_len)).rev() {
                k1 ^= Wrapping(self.buf[i] as i8 as i64) << (i * 8);
            }

            k1 *= Self::C1;
            k1 = Self::rotl64(k1, 31);
            k1 *= Self::C2;
            h1 ^= k1;
        }

        h1 ^= Wrapping(self.total_len as i64);
        h2 ^= Wrapping(self.total_len as i64);

        h1 += h2;
        h2 += h1;

        h1 = Self::fmix(h1);
        h2 = Self::fmix(h2);

        h1 += h2;
        h2 += h1;

        Murmur3Token {
            value: (((h2.0 as i128) << 64) | h1.0 as i128) as i64,
        }
    }
}
