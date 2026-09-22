# Doctor

## Problem

RFD 002 (Serving layout contract) leaves what the doctor checks and how it
reports to this RFD, and RFD 007 (Serving tiers) leaves it the exit code a
build pipeline can gate on. Checking is necessary because a writer's output
cannot be inferred from its properties: the prototype found writer properties
ignored without error
([prototype findings, finding 7](../analysis/2026-09-10-prototype-findings.md#7-two-iceberg-1101-writer-properties-are-no-ops)),
a row limit that left value pages of many megabytes
([finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)),
and a batch writer at default settings splitting a bucket into overlapping
sorted files
([finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
A table builder needs the verdict before a table ships. The server needs it
at every generation load, because `strict` turns a cost finding into a
refusal, and a refusal needs a named reason. One checker has to answer both,
or the two answers drift.

## Decision

A `doctor` command, and the same check at every generation load, will report
a table's serviceability, its cost findings with their penalties, and its
advice with the writer property to change, against thresholds that are
measured defaults kept in one documented table.

The refusal reasons and cost findings are RFD 007's lists, grouped by its
tiers, and the doctor reports each cost finding with the penalty RFD 007
assigns it. The advice: missing or truncated manifest bounds, value pages
over 1 MiB, keys over 128 bytes, data files over 128 MiB, a truncated
column index, more files per bucket than the table's size and file-size
target explain, and a key bloom filter absent, oversized, or undersized for
its row group's key count, reported as the filter's bits per key. The
value-page line is about bytes fetched per hit, a cost that is linear in
the page's size with no step anywhere, so it is drawn where the transfer
itself reaches the order of a round trip rather than at the contract's
64 KiB bound; the bound stays the recipe for large values, where a row limit
alone leaves pages of megabytes, and the doctor reports the mean value size
beside the mean page size so an owner sees how many values a hit fetches for
the one it wanted. An advice names the Iceberg or Parquet writer property
that fixes it when one does; long keys and many files per bucket are the
owner's data and its compaction, which no writer property sets, so those
two name none. The summary also carries each residency tier's resident
bytes computed from footers alone, so an owner chooses a tier (RFD 017
(Residency tiers)) from one run
([bloom residency tier, results](../analysis/2026-09-10-bloom-residency-tier.md#results)).

The command exits 0 for a table with no cost finding (advice does not change
the code), 2 when a cost finding is present, and a different non-zero code
with the refusal reason when the table cannot be served. At load the same
findings decide the tier, honor `strict`, and are reported under the names
RFD 007 fixes. The command is a subcommand of the serving binary (RFD 012
(Deployment contract)).

The thresholds live in one table, shared by the documentation and the code,
and each row cites the analysis behind it: the key-length rows rest on
[finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length),
the file-size row on
[range-GET latency, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies),
which leaves the point inside an unmeasured band to judgment. The fixture
that exercises each finding is RFD 014's (Test layering).

## Alternatives Considered

Checks only in the subcommand. Rejected: the server must know the
tier at load to honor `strict`, and only the check at load sees the snapshot
actually served.

A pass/fail checker. Rejected by RFD 007: off-contract layouts serve
correctly at the same GET count
([finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)),
so one bit would either refuse tables that work or hide the price of tables
that are slow.

Thresholds as configuration. Rejected for v1: the thresholds are measured
facts about object-store latency and Parquet decoding, and a deployer who
moved them would make a finding's name mean something different at each
site.

## Consequences

Table builders get a verdict and a fix before a table ships, and a pipeline
gets a loud failure from the exit code before the table reaches a server,
where `strict` would refuse it at load. Key pages between the contract's 1,000
rows and the 2,000-row cost threshold draw no finding: the slack is a chosen
margin, so a writer whose page rounding overshoots the contract does not
trip the doctor.

The thresholds rest on a few measurements from limited vantage points
([range-GET latency, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish)).
One cited table makes a moved threshold a one-row edit with its evidence
beside it, and makes a wrong threshold wrong everywhere at once.

The doctor needs a full load today, because sort order, key length, and
duplicate keys across overlapping files are read from the decoded key
chunks. Open question, listed as unverified
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)):
a metadata-only mode that reads footers and manifests only. For it: on a
very large table a full load is a heavy way to answer a layout question, and
footers and manifests carry most clauses. Against it:
the clauses that decide refusal live in the key chunks, so a metadata-only
verdict could pass a table the load then refuses, and two verdicts under one
name is the confusion a single checker exists to prevent.
