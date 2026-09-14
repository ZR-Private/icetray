# Configuration

## Problem

icetray owns the format of three kinds of bytes: configuration a person
edits, configuration a pipeline renders, and state it may one day persist
for itself. The person wants comments and unambiguous types. The pipeline
renders the same settings from templates and orchestrator values, and most
such pipelines emit JSON. A format chosen for one author is a translation
step for the other, where copies drift and a setting can be valid in one
and refused in the other. The settings are
structured either way: a list of tables, each with several fields, beside
server and storage settings that other RFDs define.

The first thing icetray persists for itself is the header of a chunk file
under the disk residency of RFD 017 (Residency tiers), and an artifact
chosen under shipping pressure picks whatever format is at hand. Choosing
the envelope beforehand is cheap.

## Decision

User-editable configuration will be TOML, the same schema will be accepted
as JSON so that machine-produced configuration can be consumed without a
translation step, and any state icetray persists for itself will be a
length-prefixed protobuf behind a magic byte and a version.

One schema, two syntaxes, one parser behind both. Both syntaxes are read
into the same structure and validated once, so no setting can be accepted
in one and refused in the other, and a test holds the two equivalent by
loading one configuration through both. The syntax might be chosen by file
extension or declared alongside the path; RFD 012 (Deployment contract)
says where the path comes from.

The configuration covers the server (listen addresses, the interval at
which RFD 005 (Generations and refresh) polls the ref, the page cache
budget), storage (endpoint, region, the prefix map for location rewriting,
the hedging percentile, window, and ceiling of RFD 013 (Tail latency), the
static keys RFD 003 (One storage stack) lets configuration supply, and the
optional local directory of RFD 012 (Deployment contract)), and tables (a
list, each with a name, a source, a ref, a key column, a value column,
`strict`, whose meaning is RFD 007 (Serving tiers), and the `residency` and
resident-bytes budget of RFD 017 (Residency tiers), the budget also settable
for the replica). The
prototype ran on this shape in TOML
([prototype findings, what was built](../analysis/2026-09-10-prototype-findings.md#what-was-built)).

The envelope rule exists so that the first persisted artifact does not
invent a format. The magic byte identifies a file as icetray's before any
of it is parsed; the version lets a newer build refuse or convert an older
file instead of misreading it; the length prefix frames the message, so
truncation is detected and several messages can share one file. Protobuf
is already in the build for RFD 010 (Transport and client contract), so
the rule adds no dependency.

## Alternatives Considered

YAML for the user-editable file. Rejected: an unquoted value can parse as
a number or a boolean when a string was meant, and configuration is where
that surprise costs the most.

Environment variables only. Rejected: a list of tables is structured data,
and flattening records into variable names is a format of its own, without
comments or types.

JSON only. Rejected: humans edit this file, and JSON has no comments and
no tolerance for a trailing comma. Accepting JSON serves the pipeline;
requiring it would tax the person.

Serializing the in-memory structure as it stands for persisted state.
Rejected: it changes whenever the structure does and carries no version to
say which build wrote it, so a file cannot be read across builds.

## Consequences

Deployers whose pipelines emit JSON hand icetray the file directly, under
the surface RFD 012 (Deployment contract) defines; nothing in this
repository converts between the syntaxes.

The envelope is a rule with no artifact behind it yet. Whichever persisted
thing comes first pays for its reader and writer; anything persisted later
pays nothing.

Open question: whether individual settings may also be set from the
environment, above the file. For: a container often varies one or two
values per environment, a listen address or a region, and setting them
without rendering a file is the twelve-factor habit. Against: two sources
for one setting need a precedence rule and give an operator a second place
to look when a value is wrong, and a pipeline that emits JSON can already
vary a value inside the file.
