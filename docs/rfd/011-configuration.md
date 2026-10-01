# Configuration

## Problem

icetray owns three kinds of bytes: configuration a person edits,
configuration a pipeline renders, and state it persists for itself. The
person wants comments and unambiguous types; most pipelines emit JSON. A
format chosen for one is a translation step for the other. The first
persisted state is the header of a chunk file under the disk residency of
RFD 017 (Residency tiers); choosing its envelope beforehand is cheap.

## Decision

User-editable configuration will be TOML, the same schema will be accepted
as JSON so that machine-produced configuration can be consumed without a
translation step, and any state icetray persists for itself will be a
length-prefixed protobuf behind a magic byte and a version.

One schema, two syntaxes, one parser: both are read into the same
structure and validated once, and a test loads one configuration through
both. The syntax might be chosen by file extension or declared alongside
the path; RFD 012 (Deployment contract) says where the path comes from.

The configuration covers:

- Server: listen addresses, the ref poll interval of RFD 005 (Generations
  and refresh), and the page cache budget.
- Storage: endpoint, region, the prefix map for location rewriting, the
  hedging percentile, window, and ceiling of RFD 013 (Tail latency), the
  static keys of RFD 003 (One storage stack), and the optional local
  directory of RFD 012.
- Tables: a list, each with a name, a source, a ref, a key column, a value
  column, `strict` (RFD 007 (Serving tiers)), and the `residency` and
  resident-bytes budget of RFD 017.

A running server applies a change to the tables and refuses one to the
rest; a restart applies it (RFD 018 (Configuration reload)). The prototype
ran on this shape in TOML
([prototype findings, what was built](../analysis/2026-09-10-prototype-findings.md#what-was-built)).

In the envelope, the magic byte identifies a file as icetray's before it
is parsed, the version lets a newer build refuse or convert an older file,
and the length prefix detects truncation and lets several messages share
one file. Protobuf is already in the build for RFD 010 (Transport and
client contract).

## Alternatives Considered

YAML. Rejected: an unquoted value can parse as a number or a boolean when
a string was meant.

Environment variables only. Rejected: a list of tables is structured data,
and flattening it into variable names is a format of its own.

JSON only. Rejected: humans edit this file, and JSON has no comments and
no tolerance for a trailing comma.

Serializing the in-memory structure for persisted state. Rejected: it
changes whenever the structure does and carries no version.

## Consequences

Pipelines that emit JSON hand icetray the file directly; the repository
holds no converter between the syntaxes.

The chunk file of RFD 017 is the first artifact under the envelope: magic
byte, version byte, and a length-prefixed `ChunkFileHeader` message, read
and written by the core crate's chunk file format. A later artifact uses a
magic byte of its own.

Open question: whether individual settings may also be set from the
environment, above the file. For: a container often varies one or two
values per environment, the twelve-factor habit. Against: two sources for
one setting need a precedence rule and a second place to look, and a
JSON-emitting pipeline can already vary a value in the file.
