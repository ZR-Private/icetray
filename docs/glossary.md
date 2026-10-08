# Glossary

The terms the other pages use, each defined once.

**Advice.** A finding that names a writer property worth changing for
memory, load time, or tuning. Advice never changes whether a table serves.
See [the table contract](table-contract.md).

**Degraded.** The state of a table whose last refresh failed. The last
good generation keeps serving, `icetray_table_state{state="degraded"}`
reads 1, and the next poll tries again, except after a failure of the
snapshot itself, a refusal or a fault of its metadata or data files, which
is tried again once the table's metadata location moves. Readiness does
not change. A strict table refused for its level is held instead. See
[Refresh and reload](operating.md#refresh-and-reload).

**Delete state.** The rows a generation's deletion vectors and position
delete files remove from each data file it holds, kept in memory for
every residency and checked before a lookup reads a value page. Its bytes
count toward `icetray_index_bytes`, and a `full` table's resident budget
counts it at a bound taken from the manifests. See
[RFD 021](rfd/021-merge-on-read.md).

**Deletion vector.** A bitmap of the deleted row positions of one data
file, stored as a blob in a Puffin file, which Iceberg format v3 writers
use in place of position delete files. icetray serves them.

**Fence array.** The first key of every key page of a data file, held in
memory for every residency. A lookup binary-searches it to pick the one key
page that can hold its key.

**Generation.** One snapshot of a table as icetray serves it: the
snapshot's data files with their resident index. A new snapshot is built
as a new generation beside the serving one and swapped in whole, so every
answer comes from exactly one snapshot, whose id it carries.

**Held.** The state of a strict table that refused its newest snapshot
for its level while the last snapshot that met the level keeps serving.
`icetray_table_state{state="held"}` reads 1, and the table is not
degraded, since a writer's layout put it there and no fault did. It
ends when a snapshot that meets the level swaps in, when the ref returns
to the serving snapshot, or when a reload relaxes `lookups` to a level the
refused snapshot meets or turns `strict` off. See
[Refresh and reload](operating.md#refresh-and-reload).

**Key chunk.** The compressed bytes of a data file's key column in one row
group. A load reads every key chunk once to verify the keys, and the
`full` residency keeps them in memory.

**Level.** The lookup cost a table declares it accepts, its `lookups`,
and the cost its snapshot's layout achieves, each one of `optimal`,
`balanced`, or `permissive`. `optimal` is one GET per column a hit reads
and none per miss, probing one file; `balanced` probes one file at a
fixed extra cost; `permissive` is any snapshot icetray serves correctly,
including one where a lookup probes several files. Residency and the
doctor's capping findings set the level achieved. Each value column has a
level, the table's is its dearest column's, and a table below its
declared level is flagged.
See [Choosing a level](configuration.md#choosing-a-level).

**One-shot.** A subcommand that opens tables, prints JSON lines, and exits:
`doctor`, `get`, and `info`. Each loads its tables the way `serve` does.
See [the command line](configuration.md#the-command-line).

**Projection.** Trimming a value of a column declared protobuf to the
top-level fields a lookup names, by field number, after the page cache;
it saves the bytes on the wire and the caller's decode, not a GET. See
[Protobuf columns](table-contract.md#protobuf-columns).

**Refused.** A snapshot icetray will not serve, for a named reason: an
answer could be wrong (unsorted files, duplicate keys, equality deletes,
and the like), the table cannot be held the way its residency requires,
or, under `strict`, its layout is below the table's level
(`below_level`).

**Residency.** Where a table keeps its key chunks: `full` (in memory),
`disk` (in a file in the local directory, with footers, page indexes,
and the fence array in memory), or `bloom` (bloom filters in memory instead). See
[Choosing a residency](configuration.md#choosing-a-residency).

**Strict.** A per-table setting, off by default, that refuses a snapshot
whose layout puts the table below its level instead of serving it
flagged. A strict table holds its last snapshot that met the level, and
fails to start when its first snapshot is below it.

**Value column.** A column a table serves as bytes, listed in its
`value_columns` and read only when a lookup names it; a table may list
none, to answer only whether a key is present. Each column a cold hit
names costs a GET of its own, so one packed column performs best. See
[Value columns](table-contract.md#value-columns).
