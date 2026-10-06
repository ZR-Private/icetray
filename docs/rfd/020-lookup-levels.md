# Lookup levels

## Problem

RFD 007 (Serving tiers) sorts what the doctor finds into refusals,
inefficient findings, and advice, and a table's `strict` setting decides
whether an inefficient table is refused. That is one switch for a
trade-off owners make in opposite directions. A team behind a
latency-sensitive page wants every lookup at its floor and wants to be told
when a writer breaks that. A team that wants no compaction job and no
write discipline will accept slower lookups for easier operations. `strict`
expresses only the first stance. With it off, the findings are counted per
code, but no single series says a table has become slower than its owner
accepts.

The trade-off sharpens once a table's cost can change between commits.
Today every inefficient finding is a property of how the table was
written. Writers that append small files, and merge-on-read writers once
RFD 021 serves them, add cost with every commit until a compaction removes
it, so a table can be served correctly while its lookups grow more
expensive week by week, and nothing in the current vocabulary says so.

## Decision

Each table will declare the lookup cost it accepts as one of three levels,
`optimal`, `balanced`, or `permissive`; icetray will measure the level each
table achieves, flag a table that falls below its level, and, when the
table is `strict`, refuse the snapshot whose layout put it there, with
every condition an operator alerts on exposed as a single series.

### The levels

A level is stated in what a lookup costs, so it means the same on every
table:

- `optimal`: a hit costs one object-store GET, a miss costs none, a lookup
  probes one file, decodes one key page of ordinary size, and reads its
  key chunks from memory or the kernel's page cache, never from the
  device.
- `balanced`: a lookup still probes one file, with a fixed extra cost:
  extra GETs per hit, GETs on a miss at a filter's false-positive rate,
  larger key pages, or device reads of key chunks.
- `permissive`: any snapshot icetray can serve correctly, including one
  where a lookup probes more than one file.

The line between `balanced` and `permissive` is the one that matters for
operations: costs within one file follow from how files are written,
while extra files probed come from how the table is committed to and grow
until a compaction removes them. The definitions above are the rule, and
the doctor's findings apply it. Each finding that bears on cost caps the
level, and a table's level is the lowest cap any of its files, buckets, or
conditions sets:

| Cap | Finding or condition |
|---|---|
| `balanced` | `bloom` residency |
| `balanced` | `dictionary_encoded_value` |
| `balanced` | `large_key_pages` |
| `balanced` | a `disk` table's key chunks out of the page cache |
| `permissive` | `overlapping_files` |
| `permissive` | `no_bucket_partition` |

Advice findings cap nothing. A key page counts as large where the doctor's
`large_key_pages` finding says so, not at the contract's recommended size,
so the table and the finding cannot disagree. RFD 021 decides where
merge-on-read snapshots fall; a deletion vector may cost no extra file.

Refusals stay outside the levels. A snapshot that could answer wrongly, or
that exceeds its resident or index budget (RFD 017), is refused at every
level.

### Settings

A table has three settings:

```toml
residency = "disk"
lookups   = "optimal"
strict    = false
```

Residency stays the choice of what a table pays in memory and local disk,
and it bounds the level a table can reach: `full` and `disk` can reach
`optimal`, and `bloom` reaches `balanced` at best. `lookups` defaults to
the best level the table's residency can reach, `optimal` for `full` and
`disk` and `balanced` for `bloom`, so the default reads as "tell me when
this table gets worse than its residency allows". A configuration whose
`lookups` its residency cannot reach is refused when the file is read.

`strict` defaults to false. That reverses RFD 007's default, and it closes
RFD 007's open question about it: by default no table is refused for its
cost, and every table that falls short is flagged from the moment it does.
`strict` now means that falling below `lookups` for a reason in the
snapshot's layout is an error rather than a warning. A strict table whose
snapshot is below its level at start is refused and the server does not
start. A refresh to such a snapshot is refused, the last snapshot that met
the level keeps serving, and the table is `held`. The refusal is
deterministic for its snapshot and is remembered as RFD 005 (Generations
and refresh) describes, until the source moves or a reload changes the
table's entry. The level is judged from the snapshot's manifests and
footers before any key chunk is built, so a refused snapshot writes
nothing to the local directory.

### What `strict` acts on

One rule decides it. A shortfall that comes from the snapshot's own layout
belongs to the snapshot, and `strict` refuses it. A shortfall that depends
on the server (its memory, its other tables, the kernel's cache) belongs
to the server and is flagged at every setting, because holding an older
snapshot would not reliably cure it, and refusing on it would make the
outcome depend on load order and on neighbours. The page cache is the one
server condition a level depends on today:

- While a `disk` table serves, icetray samples which of its chunk-file
  pages are resident. The table falls below `optimal` when the resident
  fraction drops below a fixed threshold and recovers when it rises above
  a higher one, so the flag does not flap. The sample decides; on a
  platform that cannot sample, this condition is not judged and the table
  reads the level of its layout.
- At load, icetray compares what every `disk` table's chunk files and the
  page cache's disk level need with the room under the memory limit (the
  cgroup's, or the host's) less the process's resident set. A shortfall
  is logged as a warning naming the bytes missing, so an undersized
  deployment is reported at start, before the sample sees it. Each chunk
  file is counted once, however many generations or reloaded entries
  share it.

### States and alerts

Freshness and cost are reported separately. `icetray_table_state` keeps
its states (`loading`, `serving`, `degraded`, where degraded means the
last load or refresh failed for a fault) and gains `held`: the newest
snapshot was refused under `strict` for its level, and the last snapshot
that met the level is serving. A held table is not degraded, so an alert
on `degraded` pages for faults in storage and catalogs, not for a writer's
layout. Cost is judged against the newest snapshot icetray has measured,
served or held, so the owner's alert does not change when `strict` is
turned on or off. Every alert is one series compared with a constant:

- `icetray_table_below_level{table}`: 1 while the newest measured snapshot
  is below the table's level for its layout, or the table is below it for
  the page cache.
- `icetray_table_state{table,state="held"}`: 1 while a strict table is
  held.
- `icetray_table_state{table,state="degraded"}`: 1 while the last load or
  refresh failed for a fault, as today.

For dashboards, `icetray_table_level{table}` is the level achieved and
`icetray_table_declared_level{table}` the level declared, each 0 for
optimal, 1 for balanced, 2 for permissive. The level series appear with
the table's first swap, like the other per-generation gauges (RFD 018). A
snapshot refused for a reason outside the levels, such as delete files,
leaves them as they were.

The measurements behind a level are per-table gauges, so a dashboard shows
cost rising before a table changes level: the GETs per hit and per miss
and the files probed per lookup that the layout predicts for a cold
lookup, and the resident fraction of chunk files. Observed traffic stays
in `icetray_lookups_total`, where warm hits make the GETs paid per hit
read below the predicted cost.

### Doctor

The doctor reports, per table, the declared level (`lookups`), the level
its layout achieves (`level`, replacing `serviceability`), and for each
finding the level it caps. For a chosen level it names the residencies
that can reach it and what each costs in memory and disk. Its exit codes
follow the level: 0 for a table at or above its declared level, 2 for one
below it that would be served, 1 for one that would be refused, whether
for a refusal or for its level under `strict`. A refusal for the level
carries the reason `below_level`, which replaces `inefficient` in the
output contract and in `icetray_refresh_errors_total`. The doctor judges
the layout only. The page cache is a condition of the serving process,
which the doctor runs apart from, and it never decides a refusal, so the
doctor still never passes a table that `serve` refuses.

### Reload

A reload that changes only a table's `lookups` or `strict` applies to the
serving generation in place, with no load, since the level is judged from
the layout the generation already holds. If the new settings make a strict
table's serving snapshot fall below its level, the reload is refused whole,
as starting with that configuration would be (RFD 018).

## Alternatives Considered

**Numeric limits per table**, such as a maximum of GETs per hit or files
probed. Rejected because owners would be choosing numbers they cannot judge
without knowing icetray's internals; three named stances carry the
decision they can make, and the numbers stay in the gauges and the
doctor's explanation.

**Levels relative to residency**, where `bloom` at its own floor counts as
optimal. Rejected because a level would then mean different costs on
different tables. Only the default follows residency.

**Two levels**, best possible and everything else. Rejected because a fixed
cost accepted once and a cost that grows with every commit call for
different responses: the first needs nothing, the second a compaction
schedule.

**Refusing under `strict` when the chunk files will not fit the page
cache.** Rejected because whether a snapshot fits depends on the other
tables and on load order: a lenient table that grows would hold a strict
neighbour at every later commit, and two replicas with one configuration
could refuse different tables. The index budget is treated the same way
for the same reason.

**A `degraded` series that includes held tables.** Rejected because one
series would then fire for a storage outage and for a writer's layout,
and routing the page to the right owner would need a join.

**Returning errors for a strict table's lookups** when its newest snapshot
falls below its level. Deferred: it is louder to clients, but it turns a
writer's mistake into an outage for every reader. Holding the last snapshot
that met the level keeps the cost promise and lets freshness slip, which
the held state shows. A later setting could add the error behavior without
changing the default.

**Keeping `strict` with its RFD 007 meaning** beside the levels. Rejected
because two switches over one question need a rule for which wins, and
nothing shipped depends on the old meaning.

## Consequences

RFD 007 keeps its refusal list and its findings; its strict gate is
replaced, its inefficient findings become the caps on a level, and its
default for `strict` changes from on to off. RFD 016 (Doctor) reports
levels, and exit 0 now means a table meets its declared level rather than
that it has no inefficient finding, which a pipeline gating on the exit
code should know. RFD 011 (Configuration) gains `lookups` and the new
`strict`. The doctor's `serviceability` field, the `inefficient` finding
tier, and the `inefficient` refusal reason give way to the level and
`below_level`. RFD 005's table states gain `held`.

The level check moves ahead of building key chunks. Today the strict gate
runs on a built generation, so a refused snapshot has already written its
chunk files and evicted other tables' pages from the page cache.

Measuring the level at load costs little, since the findings exist. The
page cache adds a sum at load and a periodic residency sample.
Merge-on-read adds measurements that RFD 021 defines.

The default flags any table worse than its residency allows. A deployment
serving `full` or `disk` tables it knows to be balanced should say so in
their entries, or its alerts fire on the first day.

Open questions:

- The two resident fractions between which a `disk` table falls out of
  and back into `optimal`, and how often it is sampled. The values are
  fixed by icetray rather than configured, and are chosen after measuring
  how lookup latency tracks residency.
