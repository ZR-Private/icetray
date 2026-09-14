# Resident index bytes per key, by key shape

Measured 2026-09-10 on synthetic files. The resident index is mostly the
compressed key column, and how well sorted keys compress depends on their
shape, not their declared type: sequential integers cost about a byte per
key, random 64-bit integers about eight, a UUID stored as sixteen bytes about
fourteen, and the same UUID as text about eighteen. The method reproduces the
two figures measured on real tables in
[prototype findings, finding 2](2026-09-10-prototype-findings.md#2-resident-index-size)
to within 3 percent, which is what makes the other rows usable for sizing.

## Method

One Parquet file per key shape, one million sorted keys, one row group,
written with the contract's layout: dictionary encoding off, 1,000-row pages
(a write batch per page), zstd, page index on. The number reported is the
key column chunk's compressed size divided by keys, which is what stays
resident per key apart from a fraction of a byte for footers, page indexes,
and fences. Keys are sampled the way a bucketed file sees them: a file holds
a hash-selected subset of the key space, so "sequential" here means a random
quarter of a sequence, sorted.

```python
import os, random, uuid, pyarrow as pa, pyarrow.parquet as pq
N = 1_000_000
rng = random.Random(7)
def measure(name, arr, encoding=None):
    t = pa.table({"key": arr})
    path = f"/tmp/keysize_{name}.parquet"
    kw = dict(compression="zstd", use_dictionary=False, write_batch_size=1000,
              data_page_size=1, data_page_version="1.0", row_group_size=N, write_page_index=True)
    if encoding: kw["column_encoding"] = {"key": encoding}
    pq.write_table(t, path, **kw)
    c = pq.ParquetFile(path).metadata.row_group(0).column(0)
    print(f"{name:32s} {','.join(c.encodings):22s} compressed {c.total_compressed_size/N:6.2f} B/key   uncompressed {c.total_uncompressed_size/N:6.2f} B/key")
ids = sorted(rng.sample(range(4_000_000), N))
measure("long_sequential_quarter", pa.array([3*i for i in ids], pa.int64()))
measure("long_random64", pa.array(sorted(rng.getrandbits(63) for _ in range(N)), pa.int64()))
b16 = sorted(rng.getrandbits(128).to_bytes(16, "big") for _ in range(N))
measure("bin16_random_uuid", pa.array(b16, pa.binary()))
measure("bin16_random_uuid_delta", pa.array(b16, pa.binary()), "DELTA_BYTE_ARRAY")
measure("str_uuid36", pa.array(sorted(str(uuid.UUID(bytes=b)) for b in b16), pa.string()))
measure("str_structured91", pa.array(["p"*70 + "user:" + f"{3*i:016d}" for i in ids], pa.string()))
comp = sorted(rng.randrange(100).to_bytes(8,"big") + rng.getrandbits(128).to_bytes(16,"big") for _ in range(N))
measure("bin24_tenant8_random16", pa.array(comp, pa.binary()))
```

## Results

Compressed key column bytes per key. Encoding is PLAIN unless noted.

| key shape | uncompressed | compressed | real-table figure, if measured |
|---|---|---|---|
| 64-bit integer, a random quarter of a sequence | 8.0 | 1.19 | 1.20 (2M-key table on S3) |
| 64-bit integer, random (a hashed id) | 8.0 | 7.57 | |
| UUID as 16 raw bytes | 20.0 (4-byte length prefix) | 14.43 | |
| UUID as 16 raw bytes, DELTA_BYTE_ARRAY | 14.7 | 14.69 | |
| UUID as 36-character text | 40.0 | 18.32 | |
| 91-byte string, 75-byte shared prefix, zero-padded number | 95.0 | 2.88 | 2.97 (2M-key table on S3) |
| 8-byte tenant id (100 tenants) then 16 random bytes | 28.0 | 15.97 | |

Observations:

- Sorted random bytes barely compress. Adjacent keys share only as many
  leading bytes as the file's key count allows (about two and a half for a
  million keys), below zstd's three-byte match minimum, so the compressor
  removes the length prefixes and little else. Prefix encoding
  (DELTA_BYTE_ARRAY) does no better here.
- A random 64-bit id costs about six times a sequential one. Hashed or
  snowflake-style identifiers are in the random class.
- Text costs more than the bytes it renders. The 36-character UUID is 18 bytes
  per key against 14 for the same value as binary; hex digits carry four bits
  each, and zstd recovers about half of the waste.
- A shared prefix is nearly free, whether it is a 75-character string
  constant or an 8-byte tenant id; the random tail sets the price.

## Sizing rule

Process memory per replica is about 40 MiB plus 1.2 times the index
([prototype findings, finding 2](2026-09-10-prototype-findings.md#2-resident-index-size)),
and the index is the compressed key bytes above plus about a tenth of a byte
per key for page indexes and fences at contract file sizes. For a table of
`N` keys:

| key shape | index bytes per key | process GB per billion keys |
|---|---|---|
| sequential 64-bit | about 1.3 | about 1.6 |
| random 64-bit | about 7.7 | about 9 |
| UUID as 16 bytes | about 14.5 | about 17.5 |
| UUID as text | about 18.4 | about 22 |

The bloom-filter fallback replaces the chunks with about 1.2 bytes per key
regardless of shape, at two GETs per hit instead of one.

## Not measured

Files written by Spark rather than the Arrow writer for the random shapes
(the two calibration rows suggest the difference is small); key counts per
file other than one million (shared prefix length grows with the logarithm
of the count, so larger files compress slightly better); the memory the
serving process adds per key for the fence array of long binary keys beyond
the estimate above.
