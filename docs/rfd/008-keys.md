# Keys

## Problem

A lookup hashes its key to a bucket, narrows to files by exact key range,
and binary-searches a fence array (RFD 004); every step rests on which key
types are allowed, how keys compare, which bytes the hash sees, and how long
a key may be. The Iceberg bucket transform takes one source column, so the
storage layer sees one key column, while the most detailed workload shape
surveyed keys its values by a high-cardinality string or composite id
([workload archetypes, archetype A](../analysis/2026-09-09-workload-archetypes.md#archetype-a-batch-computed-per-user-vectors)).
Memory per key in the resident index grows with key length
([prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size)),
and neither Parquet nor Iceberg bounds key length on its own
([prototype findings, finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length)).
Any disagreement between the writer's order or hash and the reader's turns a
present key into a miss.

## Decision

A table will have one key column of Iceberg type long, string, binary, or
uuid, with `fixed` columns served as binary; a composite key is the caller's
encoding into that column and icetray moves the bytes in and out unchanged;
keys over 4,096 bytes are refused at load and keys over 128 bytes draw
advice.

Comparison follows the type: long keys numerically, string keys as their
UTF-8 bytes, binary and `fixed` keys bytewise, and uuid keys in the order
the Iceberg specification defines for that type. Uuid is its own type rather
than sixteen opaque bytes because its order is the one a reader cannot
assume: a library's natural ordering of a UUID need not be the ordering of
its bytes, so the comparator is conformance-tested against the reference
implementation and never inferred. Each file's exact key range and fence
array (RFD 004 (Resident key-chunk index)) are built and searched under that
order, the order the writer sorted by.

Routing hashes the key with the Iceberg bucket transform's murmur3,
conformance-tested against the Iceberg library for every admitted type, so a
key is looked up in the bucket the writer assigned it.

Both thresholds are tiers under RFD 007 (Serving tiers). The 4,096-byte cap
is a refusal because resident bytes per key scale with key length and the
cap bounds the index. The 128-byte threshold is advice because the price is
memory, which none of RFD 007's penalties measure. Both numbers are sizing
judgments and may move with the measured bytes per key.

A key's value is opaque to icetray; only its type and order are not.
Guidance, documented and not enforced:
encode composite keys with fixed-width fields, which keep distinct composites
from colliding and preserve field order under bytewise comparison, or with
length-prefixed fields, which prevent collisions only. No
client-side encoding helper ships until a second consumer needs one. The
client contract, RFD 010 (Transport and client contract), carries a key as
one of three wire variants, whose bytes variant covers binary, `fixed`, and
uuid keys.

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
([prototype findings, finding 2](../analysis/2026-09-10-prototype-findings.md#2-resident-index-size)),
so an unbounded key is an unbounded index.

Refusing keys over 128 bytes. Rejected: such keys serve at one GET per hit
([prototype findings, finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length));
the price is resident memory, which none of RFD 007's penalties measure, so
the finding is advice.

## Consequences

Long keys move a table toward the disk or bloom residency of RFD 017
(Residency tiers): as bytes per key grow, the resident key chunks stop
fitting the memory budget. The 128-byte advice is the early warning of
that drift. Shape matters as much as length: sorted random bytes barely
compress, so a hashed 64-bit id costs several times a sequential one and a
UUID as text costs more than the same UUID as sixteen bytes
([key bytes by shape, results](../analysis/2026-09-10-key-bytes-by-shape.md#results)).
Prefer integers or binary to their text renderings, and put a composite
key's shared part first and its random part last.

The caller owns its encoding; a delimiter-joined key whose fields contain
the delimiter collides, and icetray cannot tell.

Two order assumptions rest on the writer and neither was exercised in
writer output
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)):
string comparison as UTF-8 bytes against non-ASCII keys, and the uuid
comparator against a uuid column. In both cases a writer that sorts another
way produces files that read as unsorted, a refusal under RFD 007 (Serving
tiers), so a golden table for each is the way the assumption stops being
one.

One question is left to the table owner: whether a dataset's versions share
one table under a version prefix in the key or get one table each. A prefix
means one table to configure and follow, and one batch read can mix
versions, but every retained version's keys sit in one resident index and
retiring a version rewrites the table. Separate tables keep each version's
index and refresh independent and retire a version by dropping its
configuration, at the price of one more table to run per version.
