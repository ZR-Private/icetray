# Deletion vectors and roaring bitmaps from Java Iceberg

These files are copied unchanged from
[apache/iceberg](https://github.com/apache/iceberg) at commit
`43b2f7d007c26ad79ebdf60d37ccca144db1f08f`, path
`core/src/test/resources/org/apache/iceberg/deletes/`. Each file's git blob
id matches that commit's. Apache Iceberg is licensed under the Apache
License 2.0.

icetray-core's deletion vector decoder (`src/deletes/puffin.rs`), the
testkit's encoder (`icetray-testkit/src/delete_files.rs`), and the testkit's
oracle (`icetray-testkit/src/merge_oracle.rs`) all use the roaring and
crc32fast crates, so their agreement says nothing about agreement with the
Java writer. Their tests read these files with `include_bytes!`.

## Deletion vector blobs

Java Iceberg's `BitmapPositionDeleteIndex` serialized these, and its
`TestBitmapPositionDeleteIndex` checks them. Each is one whole
`deletion-vector-v1` blob: the four-byte big-endian length of the magic and
the vector, the magic `D1 D3 39 64`, the vector in roaring's portable 64-bit
format with runs encoded, and the four-byte big-endian CRC-32 of the magic
and the vector. In Java's test, `delete(start, end)` deletes `start` up to
but not including `end`, and `position(bitmap, container, value)` is
`bitmap * 2^32 + container * 2^16 + value`.

| File | SHA-256 | Positions |
| --- | --- | --- |
| `empty-position-index.bin` | `934731946f34f526bfa6b0d19c5144c189773a5f3f783f79891a8e990e9fe2ed` | none |
| `small-alternating-values-position-index.bin` | `c8237ab02ae9715deaf351114962ffc73fdfea63d8e74b7815de4e6f40993112` | 1, 3, 5, 7, 9 |
| `small-and-large-values-position-index.bin` | `45561868f486e4ebdba263e6ab5478f3eb52d372998883939fd64d385a68bf0e` | 100, 101, 2^31-1+100, 2^31-1+101 |
| `all-container-types-position-index.bin` | `98f569f1e0dfe39d38b83c8b3c852d3ba1e7fd8179bb4da19dc94f5318ec70ae` | see below |

`all-container-types-position-index.bin` holds, as `position(bitmap,
container, value)`, with `a..b` from `a` up to but not including `b`:

- bitmap 0: (0, 0, 5) and (0, 0, 7) in an array container, (0, 1, 1)..(0, 1,
  1000) and (0, 2, 1)..(0, 2, 65535) in run containers;
- bitmap 1: (1, 0, 10) and (1, 0, 20) in an array container, (1, 1, 10)..(1,
  1, 500) and (1, 2, 1)..(1, 2, 65535) in run containers.

## Portable 64-bit roaring bitmaps

These are bare bitmaps in roaring's portable 64-bit format, with no blob
framing. They come from the
[RoaringFormatSpec](https://github.com/RoaringBitmap/RoaringFormatSpec)
project, also licensed under the Apache License 2.0, and Iceberg's
`TestRoaringPositionBitmap` reads them. Java's test only checks that the
first three deserialize and that it rejects `64maphighvals.bin` for a key
above `Integer.MAX_VALUE`. The RoaringFormatSpec testdata README does not
describe these files, so the contents below are read from each file's
layout.

| File | SHA-256 | Contents |
| --- | --- | --- |
| `64mapempty.bin` | `af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc` | no bitmaps |
| `64map32bitvals.bin` | `3e767bc7ca1016f1f1f757da3d650adb997d7ecff9195ee7306dc7029ef739a7` | one bitmap, key 0, holding 0 through 9 |
| `64mapspreadvals.bin` | `1d40bfe6531faa13217d3df4301a1e6a6392416ee91628520d63ded1f3babc0e` | ten bitmaps, keys 0 through 9, each holding 0 through 9 |
| `64maphighvals.bin` | `11ec16f5fe8ce50181d7c22e9409bb89445af61b927928c3a145420b454316c6` | eleven bitmaps by its count, but not decodable, see below |

In Iceberg's copy of `64maphighvals.bin`, every byte at or above 0x80
appears as `EF BF BD`, the UTF-8 encoding of U+FFFD, 308 times. Its first
key reads as 0xEFBDBFEF, above `Integer.MAX_VALUE`, which is what Java
rejects, and the bytes after that key are not a 32-bit roaring bitmap, so
icetray's decoder rejects the file as a malformed vector.
