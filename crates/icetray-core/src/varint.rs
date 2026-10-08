//! The LEB128 varint that protobuf and thrift's compact protocol share: a
//! value in little-endian groups of seven bits, one a byte, the high bit
//! of each byte saying another follows. This is the one home of its
//! framing. The readers stay with the formats they read, since parquet's
//! page headers and prost's decoder refuse an overlong varint
//! differently.

/// The bit of a varint byte that says another byte follows.
pub const CONTINUES: u8 = 0x80;

/// The bits of a varint byte that carry its value.
pub const PAYLOAD: u8 = !CONTINUES;

/// The value bits each varint byte carries.
pub const PAYLOAD_BITS: u32 = PAYLOAD.count_ones();
