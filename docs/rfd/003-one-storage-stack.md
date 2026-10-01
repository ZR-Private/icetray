# One storage stack

## Problem

icetray reads five kinds of bytes from object storage: table metadata,
manifest lists, manifests, Parquet footers, and pages. The Iceberg library
fetches metadata through its own file I/O; the Parquet reader wants
object-store-shaped range reads. That is two request paths and two
credential configurations, with no single home for location rewriting
(Iceberg metadata records absolute paths), request counting (RFD 004
(Resident key-chunk index) states its cost promises in GETs per lookup), or
credentials (profiles, single sign-on, and external credential helpers on
laptops; pod identity on clusters; static keys for S3-compatible endpoints
and tests). Bridging the SDK's chain without its cache spawned the
credential helper on every GET
([prototype findings, finding 11](../analysis/2026-09-10-prototype-findings.md#11-without-caller-side-caching-the-sdk-credential-chain-spawns-the-credential-helper-on-every-get)).

## Decision

Every byte icetray reads from object storage (table metadata, manifest
lists, manifests, Parquet footers, pages) will pass through one
object-store layer that owns location rewriting, request counting, and
credentials from the AWS SDK's chain, with the Iceberg library used only to
parse metadata and to talk to catalogs, and reads from the local files of
RFD 017 (Residency tiers) will be counted in the same accounting, beside
GETs.

- Location rewriting is a prefix map applied to every path the metadata
  yields before a request leaves the process.
- The request counter sits in the same layer, so its count is exact by
  construction; the test oracle of RFD 014 (Test layering) and the GET
  metrics of RFD 012 (Deployment contract) read the same number. One
  counter carries GETs, bytes, hedges, local reads, and local bytes
  ([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#the-request-counter-is-the-oracle)).
  The hedges of RFD 013 (Tail latency) are issued and counted there.
- Credentials come from the SDK's default chain (profiles, single sign-on,
  external credential helpers, pod identity), bridged into the object-store
  client. The bridge caches what the chain resolves until shortly before
  expiry. Explicit static keys in configuration win over the chain; that is
  the path for S3-compatible endpoints and tests.

## Alternatives Considered

Two stacks, the Iceberg library's file I/O for metadata and the
object-store layer for data. Rejected: it doubles the credential and
configuration surface, and the library's I/O has no hook for location
rewriting.

The Iceberg library's I/O for everything. Rejected: no rewriting hook, and
the data path would grow an adapter that is the object-store layer under
another name.

The object-store library's environment-only credentials. Rejected: they
read static keys only, so profiles and external credential helpers are out
of reach.

## Consequences

The bridge must cache. Without one the chain is resolved on every GET, and
a helper process spawn lands on every cold hit
([prototype findings, finding 11](../analysis/2026-09-10-prototype-findings.md#11-without-caller-side-caching-the-sdk-credential-chain-spawns-the-credential-helper-on-every-get)).

The counter counts logical range requests; coalesced adjacent ranges make
it overstate what crossed the network. The cost promises are stated in
logical GETs, for which it is exact.

Credentials vended by a catalog (per-table temporary credentials) are
unsupported until a bridge for them exists; whether to build one is open.
For: some catalogs grant table access only that way. Against: per-table
credentials split the one credential surface into one per table, and a
credential-vending catalog was never exercised
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).

How a deployer supplies credentials is RFD 012 (Deployment contract); this
RFD adds no deployer-facing surface.
