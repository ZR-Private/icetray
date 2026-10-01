# Doctor

## Problem

RFD 002 (Serving layout contract) leaves what the doctor checks and how it
reports to this RFD; RFD 007 (Serving tiers) leaves it the exit code a
pipeline gates on. Writer properties do not predict output: the prototype
found properties ignored
([prototype findings, finding 7](../analysis/2026-09-10-prototype-findings.md#7-two-iceberg-1101-writer-properties-are-no-ops)),
a row limit leaving value pages of many megabytes
([finding 6](../analysis/2026-09-10-prototype-findings.md#6-a-row-limit-alone-does-not-bound-page-size-for-large-values-and-both-writers-check-the-byte-limit-lazily)),
and a default batch writer splitting a bucket into overlapping sorted files
([finding 9](../analysis/2026-09-10-prototype-findings.md#9-spark-writes-several-sorted-files-per-bucket-sometimes-overlapping)).
The table builder needs the verdict before a table ships and the server at
every load, where `strict` turns a cost finding into a named refusal.

## Decision

A `doctor` command, and the same check at every generation load, will report
a table's serviceability, its cost findings with their penalties, and its
advice with the writer property to change, against thresholds that are
measured defaults kept in one documented table.

Refusal reasons and cost findings are RFD 007's lists, grouped by tier,
each cost finding carrying its penalty. The advice lines:

- missing or truncated manifest bounds;
- value pages over 1 MiB;
- value pages over the lookup's 64 MiB value page ceiling (RFD 004
  (Resident key-chunk index));
- keys over 128 bytes;
- data files over 128 MiB;
- a truncated column index;
- more files per bucket than the table's size and file-size target explain;
- a key bloom filter absent, oversized, or undersized for its row group's
  key count, reported as the filter's bits per key.

The 1 MiB line sits where a page's transfer reaches the order of a round
trip; the cost is linear in page size, with no step at the contract's
64 KiB bound. The doctor reports the mean value size beside the mean page
size.

The ceiling line warns of lookups refused with a file fault. It is advice:
RFD 007's penalty units price answers served, and its trigger, the file's
mean value page size with the dictionary page included, can miss one
oversized page or pass on a large value dictionary when no page is over.

An advice line names the Iceberg or Parquet writer property that fixes it;
long keys and many files per bucket have none.

The summary carries each residency tier's resident bytes, computed from
footers alone (RFD 017 (Residency tiers))
([bloom residency tier, results](../analysis/2026-09-10-bloom-residency-tier.md#results)).

Exit codes: 0 with no cost finding (advice does not change the code), 2
with a cost finding, and 1 with the refusal reason when the table cannot
be served.

The doctor alone checks every key's bucket: after the load it hashes every
key of every bucketed file under the file's own bucketing and refuses the
table under RFD 007's `key_outside_bucket` for a key outside the bucket its
manifest entry records. The load hashes only each file's fences and first
and last key (RFD 007); every row would add tens of nanoseconds per key per
load. At load the same findings decide the tier, honor `strict`, and carry
RFD 007's names. The command is a subcommand of the serving binary (RFD 012
(Deployment contract)).

The thresholds live in one table shared by the documentation and the code,
each row citing its analysis: key length rests on
[finding 13](../analysis/2026-09-10-prototype-findings.md#13-key-length),
file size on
[range-GET latency, what the evidence implies](../analysis/2026-09-10-s3-range-get-latency.md#what-the-evidence-implies),
which leaves the point inside an unmeasured band to judgment. RFD 014 (Test
layering) holds the fixture for each finding.

## Alternatives Considered

Checks only in the subcommand. Rejected: the server must know the tier at
load to honor `strict`, and only that check sees the snapshot served.

A pass/fail checker. Rejected by RFD 007: off-contract layouts serve
correctly at the same GET count
([finding 1](../analysis/2026-09-10-prototype-findings.md#1-one-get-per-cold-hit-none-per-miss-on-every-layout)),
so one bit would refuse working tables or hide slow ones' price.

Thresholds as configuration. Rejected for v1: a deployer who moved measured
thresholds would make a finding's name mean something different at each
site.

## Consequences

A pipeline gets the verdict, fix, and exit code before a table reaches a
server and its `strict` gate. Key pages between the
contract's 1,000 rows and the 2,000-row cost threshold draw no finding; the
slack is a margin for a writer whose page rounding overshoots the contract.

The thresholds rest on a few measurements from limited vantage points
([range-GET latency, what this does and does not establish](../analysis/2026-09-10-s3-range-get-latency.md#what-this-does-and-does-not-establish));
one cited table makes a moved threshold a one-row edit, and a wrong one
wrong everywhere.

The doctor needs a full load today: sort order, key length, and duplicate
keys across overlapping files come from the decoded key chunks. Open
question, listed as unverified
([prototype findings, what was not verified](../analysis/2026-09-10-prototype-findings.md#what-was-not-verified)):
a metadata-only mode over footers and manifests. For it: footers and
manifests carry most clauses, and a full load is heavy on a very large
table. Against it: the clauses that decide
refusal live in the key chunks, so a metadata-only verdict could pass a
table the load then refuses.
