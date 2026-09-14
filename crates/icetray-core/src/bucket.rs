//! The Iceberg bucket transform (RFD 008): 32-bit murmur3 over the key's
//! bytes, computed here so a lookup routes to the partition the writer
//! assigned with arithmetic alone.

use crate::key::Key;

/// The bucket `key` falls in under Iceberg's `bucket[buckets]` transform:
/// `(murmur3_32(bytes) & i32::MAX) % buckets` on the signed hash, over the
/// bytes `Key::as_bytes` yields (spec appendix B). `buckets` is the
/// transform's count from the partition spec and is at least one.
pub fn bucket(key: &Key, buckets: u32) -> u32 {
    (murmur3_32(&key.as_bytes()) & i32::MAX).unsigned_abs() % buckets
}

/// The specification's 32-bit hash: murmur3, x86 variant, seed 0, read back
/// as the signed value Java produces, so the vectors of appendix B compare
/// directly.
fn murmur3_32(bytes: &[u8]) -> i32 {
    let mut source = bytes;
    let hash =
        murmur3::murmur3_32(&mut source, 0).expect("reading from an in-memory slice cannot fail");
    hash.cast_signed()
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{Datum, PrimitiveLiteral, Transform};
    use iceberg::transform::create_transform_function;
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
    use uuid::Uuid;

    use super::*;
    use crate::key::UUID_BYTES;

    /// Appendix B: `bucket` of long 34.
    const SPEC_LONG: i64 = 34;
    /// Appendix B: the hash of long 34.
    const SPEC_LONG_HASH: i32 = 2_017_239_379;
    /// Appendix B: the hash of string "iceberg".
    const SPEC_STRING_HASH: i32 = 1_210_000_089;
    /// Appendix B: the uuid vector, as iceberg-rust's own test states it.
    const SPEC_UUID: [u8; UUID_BYTES] = [
        0xf7, 0x9c, 0x3e, 0x09, 0x67, 0x7c, 0x4b, 0xbd, 0xa4, 0x79, 0x3f, 0x34, 0x9c, 0xb7, 0x85,
        0xe7,
    ];
    /// Appendix B: the hash of that uuid.
    const SPEC_UUID_HASH: i32 = 1_488_055_340;
    /// iceberg-rust's fixed and binary vector, the bytes 0 through 3.
    const SPEC_BYTES: [u8; 4] = [0x00, 0x01, 0x02, 0x03];
    /// The hash of those bytes; negative, so the sign-bit masking is exercised.
    const SPEC_BYTES_HASH: i32 = -188_683_207;
    /// The bucket count the spec's long and string examples use.
    const SPEC_BUCKETS_SMALL: u32 = 16;
    /// The bucket count iceberg-rust's uuid test uses.
    const SPEC_BUCKETS_UUID: u32 = 100;
    /// The bucket count iceberg-rust's binary and fixed tests use.
    const SPEC_BUCKETS_BYTES: u32 = 128;
    /// `2017239379 % 16`.
    const SPEC_LONG_BUCKET: u32 = 3;
    /// `1210000089 % 16`.
    const SPEC_STRING_BUCKET: u32 = 9;
    /// `1488055340 % 100`, as iceberg-rust's test asserts.
    const SPEC_UUID_BUCKET: u32 = 40;
    /// `(-188683207 & i32::MAX) % 128`, as iceberg-rust's test asserts.
    const SPEC_BYTES_BUCKET: u32 = 57;

    /// Keys each oracle test draws; every one is checked at every count.
    const ORACLE_KEYS: usize = 500;
    /// The bucket counts each key is checked at, spanning one bucket to a
    /// count larger than the hash's low byte.
    const BUCKET_COUNTS: [u32; 5] = [1, 2, 16, 128, 1_000];
    /// The longest random string or byte key drawn.
    const MAX_KEY_LEN: usize = 40;
    /// Random string keys draw code points from space up to this bound,
    /// which covers ASCII and two- and three-byte UTF-8 sequences.
    const CHAR_BOUND: u32 = 0x2FFF;
    /// The first code point drawn for string keys: space.
    const CHAR_START: u32 = 0x20;
    /// Fixed keys are drawn at this width.
    const FIXED_LEN: usize = 12;
    /// The seed of the long oracle sample; fixed so a failure reproduces.
    const LONG_SEED: u64 = 1;
    /// The seed of the string oracle sample.
    const STRING_SEED: u64 = 2;
    /// The seed of the binary oracle sample.
    const BINARY_SEED: u64 = 3;
    /// The seed of the fixed oracle sample.
    const FIXED_SEED: u64 = 4;
    /// The seed of the uuid oracle sample.
    const UUID_SEED: u64 = 5;

    /// The library's `bucket[n]` of a datum, the oracle every admitted type
    /// is checked against.
    fn oracle(datum: &Datum, buckets: u32) -> u32 {
        let function = create_transform_function(&Transform::Bucket(buckets)).unwrap();
        let result = function.transform_literal(datum).unwrap().unwrap();
        let PrimitiveLiteral::Int(bucket) = result.literal() else {
            panic!("bucket transform returned {result:?}");
        };
        u32::try_from(*bucket).unwrap()
    }

    /// Checks `bucket` against the oracle for every key at every count and
    /// returns how many checks ran.
    fn check_against_oracle(keys: &[(Key, Datum)]) -> usize {
        let mut checked = 0;
        for (key, datum) in keys {
            for buckets in BUCKET_COUNTS {
                assert_eq!(
                    bucket(key, buckets),
                    oracle(datum, buckets),
                    "key {key} in {buckets}"
                );
                checked += 1;
            }
        }
        checked
    }

    fn random_bytes(rng: &mut StdRng, len: usize) -> Vec<u8> {
        (0..len).map(|_| rng.random()).collect()
    }

    #[test]
    fn test_murmur3_32_spec_vectors() {
        assert_eq!(murmur3_32(&SPEC_LONG.to_le_bytes()), SPEC_LONG_HASH);
        assert_eq!(murmur3_32(b"iceberg"), SPEC_STRING_HASH);
        assert_eq!(murmur3_32(&SPEC_UUID), SPEC_UUID_HASH);
        assert_eq!(murmur3_32(&SPEC_BYTES), SPEC_BYTES_HASH);
    }

    #[test]
    fn test_bucket_spec_vectors() {
        assert_eq!(
            bucket(&Key::Long(SPEC_LONG), SPEC_BUCKETS_SMALL),
            SPEC_LONG_BUCKET
        );
        assert_eq!(
            bucket(&Key::Str("iceberg".into()), SPEC_BUCKETS_SMALL),
            SPEC_STRING_BUCKET
        );
        assert_eq!(
            bucket(&Key::Uuid(SPEC_UUID), SPEC_BUCKETS_UUID),
            SPEC_UUID_BUCKET
        );
        assert_eq!(
            bucket(&Key::Bytes(SPEC_BYTES.to_vec()), SPEC_BUCKETS_BYTES),
            SPEC_BYTES_BUCKET
        );
    }

    #[test]
    fn test_bucket_matches_library_for_long() {
        let mut rng = StdRng::seed_from_u64(LONG_SEED);
        let keys: Vec<(Key, Datum)> = (0..ORACLE_KEYS)
            .map(|_| {
                let value: i64 = rng.random();
                (Key::Long(value), Datum::long(value))
            })
            .collect();
        assert_eq!(
            check_against_oracle(&keys),
            ORACLE_KEYS * BUCKET_COUNTS.len()
        );
    }

    #[test]
    fn test_bucket_matches_library_for_string() {
        let mut rng = StdRng::seed_from_u64(STRING_SEED);
        let keys: Vec<(Key, Datum)> = (0..ORACLE_KEYS)
            .map(|_| {
                let len = rng.random_range(0..MAX_KEY_LEN);
                let text: String = (0..len)
                    .map(|_| char::from_u32(rng.random_range(CHAR_START..CHAR_BOUND)).unwrap())
                    .collect();
                (Key::Str(text.clone()), Datum::string(text))
            })
            .collect();
        assert_eq!(
            check_against_oracle(&keys),
            ORACLE_KEYS * BUCKET_COUNTS.len()
        );
    }

    #[test]
    fn test_bucket_matches_library_for_binary() {
        let mut rng = StdRng::seed_from_u64(BINARY_SEED);
        let keys: Vec<(Key, Datum)> = (0..ORACLE_KEYS)
            .map(|_| {
                let len = rng.random_range(0..MAX_KEY_LEN);
                let bytes = random_bytes(&mut rng, len);
                (Key::Bytes(bytes.clone()), Datum::binary(bytes))
            })
            .collect();
        assert_eq!(
            check_against_oracle(&keys),
            ORACLE_KEYS * BUCKET_COUNTS.len()
        );
    }

    #[test]
    fn test_bucket_matches_library_for_fixed() {
        let mut rng = StdRng::seed_from_u64(FIXED_SEED);
        let keys: Vec<(Key, Datum)> = (0..ORACLE_KEYS)
            .map(|_| {
                let bytes = random_bytes(&mut rng, FIXED_LEN);
                (Key::Bytes(bytes.clone()), Datum::fixed(bytes))
            })
            .collect();
        assert_eq!(
            check_against_oracle(&keys),
            ORACLE_KEYS * BUCKET_COUNTS.len()
        );
    }

    #[test]
    fn test_bucket_matches_library_for_uuid() {
        let mut rng = StdRng::seed_from_u64(UUID_SEED);
        let keys: Vec<(Key, Datum)> = (0..ORACLE_KEYS)
            .map(|_| {
                let bytes: [u8; UUID_BYTES] = rng.random();
                (Key::Uuid(bytes), Datum::uuid(Uuid::from_bytes(bytes)))
            })
            .collect();
        assert_eq!(
            check_against_oracle(&keys),
            ORACLE_KEYS * BUCKET_COUNTS.len()
        );
    }
}
