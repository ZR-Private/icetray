# One storage stack

## Problem

icetray reads five kinds of bytes: table metadata, manifest lists,
manifests, Parquet footers, and pages. The two libraries involved each
arrive with their own way of fetching them. The Iceberg library carries a
file I/O abstraction for metadata, while the Parquet reader wants
object-store-shaped range reads for footers and pages. Used as they come,
they give two request paths and two credential configurations, and leave
no single place for three things the design depends on.

Location rewriting is the first. Iceberg metadata records absolute paths,
and a mirrored or relocated table lives under a different prefix. Request
counting is the second, because RFD 004 (Resident key-chunk index) states
its cost promises in GETs per lookup, and a promise about GETs can only be
tested and reported where every GET is issued. Credentials are the third.
Laptops authenticate through profiles, single sign-on, and external
credential helpers, clusters through pod identity, and S3-compatible
endpoints and tests through static keys. A careless credential bridge is
also a latency bug: bridged without a cache, the SDK's chain spawns the
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

Sending every read through one layer gives each of these concerns a single
home. Location rewriting is a prefix map, applied to every path the
metadata yields before a request leaves the process. The request counter
sits in the same layer, so its count is exact by construction, and the
test oracle of RFD 014 (Test layering) and the GET metrics of RFD 012
(Deployment contract) read the same number. One counter carries GETs,
bytes, hedges, local reads, and local bytes, so a lookup's whole cost is
one structure whatever tier the table runs
([prototype findings, method](../analysis/2026-09-10-prototype-findings.md#the-request-counter-is-the-oracle)).
The hedges of RFD 013 (Tail latency) are issued and counted there as well.

Credentials come from the SDK's default chain (profiles, single sign-on,
external credential helpers, pod identity), bridged into the object-store
client. The bridge caches what the chain resolves until shortly before it
expires, so the chain is consulted once per credential lifetime. Explicit
static keys in configuration take precedence over the chain, and they are
the path for S3-compatible endpoints and for tests.

## Alternatives Considered

Two stacks, with the Iceberg library's own file I/O for metadata and the
object-store layer for data. Rejected because it doubles the credential
and configuration surface. The library's I/O also has no hook for location
rewriting, so a relocated table would resolve its metadata under one
prefix and its data under another.

The Iceberg library's I/O for everything. It lacks the rewriting hook
here too, and since the Parquet reader wants object-store-shaped range
reads, the data path would grow an adapter that is the object-store layer
under another name.

The object-store library's environment-only credentials. These read
static keys from the environment and nothing else. That rules out
profiles and external credential helpers, which is how laptops and many
clusters authenticate.

## Consequences

The bridge must cache. Without a cache the chain is resolved on every GET,
and when resolving means spawning a helper process, the cost lands on
every cold hit
([prototype findings, finding 11](../analysis/2026-09-10-prototype-findings.md#11-without-caller-side-caching-the-sdk-credential-chain-spawns-the-credential-helper-on-every-get)).

The counter counts logical range requests. When adjacent ranges coalesce
into one wire request, the count overstates what crossed the network. This
imprecision is accepted because the design states its cost promises in
logical GETs, and for those the counter is exact.

This layer does not take credentials vended by a catalog (per-table
temporary credentials). Serving them needs a bridge of its own, and
whether to build one is an open question. In favor: some catalogs grant
table access only that way, and without the bridge those tables cannot be
served at all. Against: per-table credentials split the one credential
surface into one per table, and what a credential-vending catalog would
demand of such a bridge is itself unknown
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)).

How a deployer supplies credentials to this layer is the domain of RFD 012
(Deployment contract). This RFD adds no deployer-facing surface of its own.
