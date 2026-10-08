# Value columns

## Problem

RFD 002 (Serving layout contract) gives a table one value column and
recommends packing a row into it, as a serialized protobuf, so that a
cold hit costs one GET. That serves a table read by one kind of caller.
It serves a wide row read by several kinds of caller badly. A row that
carries an embedding beside a thousand properties, read by one team for
the embedding and by another for a few properties, makes every caller
fetch, receive, and decode the whole row, and a value page holds fewer
rows, so the page cache holds fewer of the hot set.

Two entries in the configuration over the same table, each naming a
different column, serve today. They share the page cache and, in the
disk residency, one chunk file per data file. Each entry still builds
its own key index, polls and loads on its own, and refreshes on its own
schedule, so for up to a poll interval the two can answer from
different snapshots.

RFD 002 rejected several value columns for the first version, because
each column read adds a range GET to a cold hit and typed per-column
values amount to a query API. The first cost is real but falls only on
a caller that asks for more than one column. The second does not follow
if every column stays bytes. And a lookup already takes two separate
steps: the key index finds a row, and the value column's offset index
finds that row's page. A second column costs one more offset index, not
a second key index.

## Decision

Rows read by several kinds of caller will be served by letting a table
name several value columns, or none, each read only when a lookup names
it, all from one key index and one generation.

- **Configuration.** `value_columns` lists the columns a table serves
  and replaces `value_column`. The list may be empty, for a table that
  answers only whether a key is present. A name listed twice, or the
  key column listed as a value, is a configuration error, and so is an
  empty name. Order carries no meaning: a generation holds the columns
  in name order, names compared as their bytes, so a reordered list is
  not a change and keeps the generation and its indexes. A change to the
  set reloads the whole table under RFD 018 (Configuration reload),
  accepted because a table's columns change rarely.
- **Types.** Each column is `binary`, `fixed`, or `string`, served as
  bytes unchanged. A column of any other type refuses the table as
  `value_type_unsupported`, naming the column. How a column is found in
  each data file is RFD 023 (Column resolution).
- **Wire.** Get and MultiGet carry one list of columns, applied to
  every key of the call, with no default: an empty list answers
  presence alone. A name the table does not serve, or a name given
  twice, is `INVALID_ARGUMENT`. A result says whether the key is
  present and, when it is, carries one value per requested column in
  the order requested, each its bytes or null, in a message of its own
  since a proto3 list of bytes cannot hold a null. Presence and null
  are separate answers, so RFD 010's (Transport and client contract)
  combined presence enum splits in two. TableInfo lists the columns in
  name order, each with its type as an enum and a `fixed` column's
  width, and the doctor lists them in the same order. The operator
  subcommands name columns the same way: `get --column`, repeated, and
  `--value-column`, repeated, for a table opened by `--location`.
- **Cost.** A hit costs finding the row in its residency (RFD 017
  (Residency tiers)), then one GET per column it names, two for a
  dictionary-encoded column, issued concurrently, except a column RFD
  023 answers by its default, which costs none. A presence-only lookup
  costs finding the row: nothing under `full`, a local read under
  `disk`, and the key page under `bloom`. Deleted rows (RFD 021
  (Merge-on-read)) are checked once per row. The page cache is keyed by
  file and offset, so columns share it unchanged.
- **Bounds.** `server.max_multi_get_keys` becomes
  `server.max_multi_get_values` and counts keys times columns, a
  presence-only key counting one, and the batch fan-out bounds GETs in
  flight rather than lookups: each GET a lookup issues, a key page, a
  value page, or a dictionary page, holds one slot until it ends, and
  none waits for a slot while holding one, so a batch holds no more
  GETs open than today whatever its keys and columns. Each GET is hedged
  on its own, in the table's one hedge window, and the hedge-rate
  ceiling counts GETs.
- **Index.** A file keeps resident the offset index of the key and of
  each configured column and drops the rest at build, the resident set
  RFD 004 (Resident key-chunk index) describes.
- **Findings and levels.** RFD 016's (Doctor) value-column findings,
  `dictionary_encoded_value`, `large_value_pages`,
  `value_pages_over_ceiling`, and the advice `column_default`, are made
  per column and name it, in the doctor's report, in `BelowLevel`, and
  in a `column` label that `icetray_findings` carries for those codes
  alone; the doctor's per-file report lists each column's facts. The
  type refusal, `value_type_unsupported`, names its column in its
  message. Each column has a level under RFD 020 (Lookup levels),
  published as `icetray_table_column_level`, measured on the same
  snapshot as the table's level, so while a table is held its column
  series describe the refused snapshot. The table's level is the
  dearest column's, since any column may be asked for, and a table of
  no columns has the level of finding the row. RFD 020's alert series
  stay one per table.
- **Metrics.** `icetray_value_page_gets_total` counts the value-page
  and dictionary-page GETs issued, by table and column, each once where
  it is issued, hedges left to `icetray_hedges_total`; it is what a
  caller budgets with. `icetray_table_gets_per_hit` predicts finding the
  row and reading the dearest column, and
  `icetray_table_column_gets_per_hit` each column's own GETs, so a
  caller can add up a request. A hit is cold when a value or dictionary
  page of a column it names came from object storage, a fetch another
  lookup started included, and warm otherwise, so a presence-only hit
  is warm. Naming several columns costs several columns by the caller's
  choice, which is not a finding.

One packed column performs best, and RFD 002's recipe stays the
recommendation for a table read by one kind of caller. A table read by
several kinds of caller splits its row along how it is read: fields
read together share a column, and a large part read on its own, such as
an embedding, gets a column of its own. A caller that needs most of a
row is better served by one packed column than by naming several.

## Alternatives Considered

Two configuration entries over one table. It works without a change, as
a test shows. Rejected as the pattern to recommend, because it builds
and holds the key index once per entry and loads and polls once per
entry. Its answers name their snapshots, so a caller reading both can
tell when they diverge, but it cannot make them agree.

Every column of the table servable, with none named in configuration.
Rejected because findings, levels, and resident offset indexes would
then scale with the table's width rather than with what is served, and
a column the owner never meant to expose would be readable.

A default column, read by a request that names none. Rejected because
an empty request would then mean whatever the configuration lists
first, a behavior the caller never stated.

A change of columns applied in place, reading only the footers.
Rejected for now, since columns change rarely and a whole-table reload
is what RFD 018 already does.

Typed columns, such as a long or a double read by name. Rejected because
such a value has no "bytes unchanged" form, so the wire would need typed
values, which is the query API RFD 002 declined.

Projection inside one packed column, trimming a serialized protobuf to
the field numbers a caller asks for, which RFD 002 left to RFD 007
(Serving tiers). Not chosen here: it saves bytes on the wire and in the
caller's decode, but not the GET, and can be decided on its own.

Several requested columns fetched as one range read. Rejected because a
row's columns lie in column chunks at unrelated offsets, so one read
spanning them fetches the other columns' bytes in between.

## Consequences

The wire contract and the configuration break, which before 1.0 is
allowed in place (`CONTRIBUTING.md`, Stability).

Earlier RFDs are revised to match: RFD 002 (one or more value columns,
one performing best); RFD 004 and RFD 017 (resident offset indexes);
RFD 005 and RFD 006 (a generation's layout and a lookup's steps name
columns); RFD 007 (the type refusal names its column); RFD 010 (the
result, the request's columns, TableInfo, and the renamed limit); RFD
011 (`value_columns` and its validation); RFD 013 (fan-out and the hedge
ceiling count GETs); RFD 016 (findings and the per-file report by
column, a change to the doctor's output contract); RFD 018 (a change of
columns reloads the table); RFD 020 (`optimal` and the cost gauges
describe one column read, and the level is the dearest column's); and
RFD 021 (a live row's value pages are those of the columns named). The
table contract, the configuration reference, and the performance guide
carry the guidance on when to split a row and when to pack it.

A multi-column cold hit waits on the slowest of its GETs, so its tail is
wider than a single-column hit's even with every GET hedged.

Under `strict`, one column whose layout falls below the declared level
holds the whole table, for callers that never read that column too.

Adding a column reaches the configuration before the clients that ask
for it, and removing one reaches the clients first, since a column the
table does not serve is `INVALID_ARGUMENT`.
