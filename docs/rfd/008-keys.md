# Keys

## Problem

A lookup hashes its key to a bucket, narrows to files by key range, and
binary-searches a fence array (RFD 004), so key types, order, hashed bytes,
and length each need a rule. The Iceberg bucket transform takes one source
column, while the most detailed workload shape surveyed keys its values by
a high-cardinality string or composite id
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).
Memory per key in the resident index grows with key length
([prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size)),
and neither Parquet nor Iceberg bounds key length
([finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length)).

## Decision

A table will have one key column of Iceberg type long, string, binary, or
uuid, with `fixed` columns served as binary; a composite key is the caller's
encoding into that column and icetray moves the bytes in and out unchanged;
keys over 4,096 bytes are refused at load and keys over 128 bytes draw
advice.

Comparison follows the type: long keys numerically, string keys as their
UTF-8 bytes, binary and `fixed` keys bytewise, and uuid keys in the order
the Iceberg specification defines for that type. The uuid comparator is
conformance-tested against the reference implementation. Each file's key
range and fence array (RFD 004) are built and searched under that order.

Routing hashes the key with the Iceberg bucket transform's murmur3,
conformance-tested against the Iceberg library for every admitted type.

Both thresholds are tiers under RFD 007 (Serving tiers): the 4,096-byte cap
is a refusal, and the 128-byte threshold is advice because its price is
memory, which none of RFD 007's penalties measure. Both numbers may move
with the measured bytes per key.

Guidance, documented and not enforced: encode composite keys with
fixed-width fields, which prevent collisions and preserve field order under
bytewise comparison, or with length-prefixed fields, which prevent
collisions only. A client-side encoding helper ships when a second consumer
needs one. RFD 010 (Transport and client contract) carries a key as one of
three wire variants; the bytes variant covers binary, `fixed`, and uuid.

## Alternatives Considered

Multi-column keys, with the bucket transform on a leading column. Rejected:
the transform takes one source column, so a composite key is one column at
the storage layer regardless.

A composite-key encoding shipped in the client. Deferred until a second
consumer would otherwise write the same one.

A UUID rendered as text instead of a uuid or binary column. Rejected: the
same value costs a quarter more resident bytes per key as thirty-six
characters than as sixteen bytes
([key bytes by shape, results](../analysis/2026-09-10-key-bytes-by-shape.md#results)),
and a text column cannot be ordered by the specification's uuid comparator.

No length cap. Rejected: resident bytes per key scale with key length
([finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size)),
so an unbounded key is an unbounded index.

Refusing keys over 128 bytes. Rejected: such keys serve at one GET per hit
([finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length));
the price is resident memory, which none of RFD 007's penalties measure.

## Consequences

Long keys move a table toward the disk or bloom residency of RFD 017
(Residency tiers); the 128-byte advice is the early warning. Shape matters
as much as length: sorted random bytes barely compress, so a hashed 64-bit
id costs several times a sequential one
([key bytes by shape, results](../analysis/2026-09-10-key-bytes-by-shape.md#results)).
Prefer integers or binary to their text renderings, and put a composite
key's shared part first and its random part last.

A delimiter-joined key whose fields contain the delimiter collides, and
icetray cannot tell.

Two order assumptions were not exercised in writer output
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)):
string comparison as UTF-8 bytes against non-ASCII keys, and the uuid
comparator against a uuid column. A writer that sorts another way produces
files that read as unsorted, a refusal under RFD 007; a golden table for
each is the check.

One question is left to the table owner: whether a dataset's versions share
one table under a version prefix in the key or get one table each. A prefix
means one table to configure and follow, and one batch read can mix
versions, but every retained version's keys sit in one resident index and
retiring a version rewrites the table. Separate tables keep each version's
index and refresh independent and retire a version by dropping its
configuration, at the price of one more table to run.
