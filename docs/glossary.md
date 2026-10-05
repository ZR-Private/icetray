# Glossary

The terms the other pages use, each defined once.

**Advice.** A finding that names a writer property worth changing for
memory, load time, or tuning. Advice never changes whether a table serves.
See [the table contract](table-contract.md).

**Degraded.** The state of a table whose last refresh failed. The last
good generation keeps serving, `icetray_table_state{state="degraded"}`
reads 1, and the next poll tries again, except after a refusal of the
snapshot itself, which is tried again once the table's metadata location
moves. Readiness does not change. See
[Refresh and reload](operating.md#refresh-and-reload).

**Fence array.** The first key of every key page of a data file, held in
memory for every residency. A lookup binary-searches it to pick the one key
page that can hold its key.

**Generation.** One snapshot of a table as icetray serves it: the
snapshot's data files with their resident index. A new snapshot is built
as a new generation beside the serving one and swapped in whole, so every
answer comes from exactly one snapshot, whose id it carries.

**Inefficient.** A table that icetray serves correctly but with more work
per lookup than the contract layout: an extra GET per hit, more keys
decoded, or more files probed. The doctor reports each such finding with
its measured penalty. With `strict` on, the default, an inefficient table
is refused. See [the table contract](table-contract.md).

**Key chunk.** The compressed bytes of a data file's key column in one row
group. A load reads every key chunk once to verify the keys, and the
`full` residency keeps them in memory.

**One-shot.** A subcommand that opens tables, prints JSON lines, and exits:
`doctor`, `get`, and `info`. Each loads its tables the way `serve` does.
See [the command line](configuration.md#the-command-line).

**Refused.** A table icetray will not serve, for a named reason: an answer
could be wrong (unsorted files, duplicate keys, delete files, and the
like), the table cannot be held the way its residency requires, or it is
inefficient while `strict` is on.

**Residency.** Where a table keeps its key chunks: `full` (in memory),
`disk` (in a file in the local directory, with footers, page indexes,
and the fence array in memory), or `bloom` (bloom filters in memory instead). See
[Choosing a residency](configuration.md#choosing-a-residency).

**Strict.** A per-table setting, on by default, that refuses an
inefficient table instead of serving it with its penalty.
