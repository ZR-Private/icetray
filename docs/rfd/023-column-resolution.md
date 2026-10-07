# Column resolution

## Problem

A table's configuration names its key column and its value columns (RFD
022 (Value columns)), and each name is resolved against the snapshot's
own schema (RFD 005 (Generations and refresh)). Inside each data file,
icetray then finds the column by its top-level name in the Parquet
schema, and a file without a column of that name is a file fault that
fails the load or refresh.

Iceberg finds a file's columns by field id, not by name, and matching
by name alone leaves two cases wrong. A swap of two columns' names,
done as renames, serves each column's bytes under the other's name,
with no error. And a column added to the schema cannot be served until
every file written before it is rewritten, though Iceberg reads such a
file as holding the column's default. A rename, the third case, fails
loudly by name, which is the right outcome for a table whose owner and
configuration reason by name.

## Decision

Finding a configured column in each data file will be solved by matching
its name among the file's top-level columns, checked against the field
id the snapshot's schema gives that name when the file records field
ids, with a column the file provably lacks answering its Iceberg
default and every other mismatch refused.

A file records field ids when every top-level column of its Parquet
schema carries one, as Iceberg's writers write them. A file where only
some do is treated as recording none. What each file holds for a
configured column decides the answer:

| The file holds | The answer |
|---|---|
| A column of that name, with the schema's field id | Served |
| A column of that name, with another field id | Refused |
| No column of that name, but the schema's field id under another name | Refused |
| Neither the name nor the field id, and records field ids | The column's default |
| No field ids, and a column of that name | Served, unchecked |
| No field ids, and no column of that name | Refused |

- **Names are top-level.** Names and field ids are matched among the
  top-level fields of the snapshot's schema and the top-level columns of
  the file. A configured name that is not a top-level field of the
  snapshot's schema, such as a nested field's dotted name, refuses the
  snapshot, and a name the schema lacks altogether stays the fault of
  the snapshot that RFD 005 makes it.
- **The check covers every file of every snapshot,** a file reused
  from the previous generation included, since a snapshot that drops a
  column and adds a new one under the same name changes what the old
  files mean without changing the files.
- **A swap or a reused name is refused,** since the column found by
  name is not the column the schema means. **A rename is refused,**
  since the file holds the column under a name the configuration does
  not use, and nothing in a file without field ids says whether a
  missing column was added or renamed.
- **A column added later answers its default.** A file that records
  field ids and holds neither the name nor the id was written before
  the column existed. Its rows answer the `initial-default` the
  snapshot's schema gives the column, as the iceberg crate parses it,
  or null when there is none. A default stated in the metadata that
  cannot be read as the column's type refuses the snapshot rather than
  answering null. Such a column costs no value-page GET and makes no
  layout finding in that file. The doctor reports how many files
  answer each column by its default, as the advice `column_default`.
- **A `fixed` column** whose width in a file differs from the schema's
  is refused.
- **The key column** follows the same check but has no default: a file
  without its key column is refused.
- **A refusal is of the snapshot,** under the named reason
  `column_mismatch` (RFD 007 (Serving tiers)), naming the file and the
  column. The last good generation keeps serving, the refusal is
  remembered against the metadata location (RFD 009 (Writes between
  builds)), and at boot RFD 005's start-or-fail rule applies.

The table contract follows. A column can be added to a served table
without a rewrite. A served column is not renamed: a new column is
added beside it, written, and moved to by the callers, and the old one
is dropped, first from the clients, then from the configuration, then
from the schema.

## Alternatives Considered

Resolution by field id, as Iceberg's readers do, with the name used only
to find the id. Rejected because the name is how the owner and the
configuration reason about a column. A rename would keep serving a
configured name the schema no longer holds, and a swap would serve a
different column under the name the caller asked for than before it.
Both are correct by Iceberg's model and a surprise by the owner's.

Matching by name alone, as icetray does today. Rejected because a swap
serves wrong bytes with no error, and an added column cannot be served
until every older file is rewritten.

A file that lacks a column always answering its default. Rejected
because a file without field ids cannot tell a column added after it
from one renamed after it, and answering the default for a renamed
column serves null for data the file holds.

Consulting the table's name mapping (`schema.name-mapping.default`) for
files without field ids. Not adopted, since tables built to be served
are written by Iceberg's writers, which record field ids.

## Consequences

The rule applies to every table, one-column tables included. A file
that lacks its configured value column, which today fails as the file
fault `MissingColumn`, now answers the default when its field ids show
the column was added after it, and is refused under `column_mismatch`
otherwise. A swap of names, which today serves wrong bytes with no
error, is refused.

Rows that answer a default cost no GET for that column, so a table
mid-evolution costs less than the worst case its level and gauges
predict, until its files are rewritten.

The check runs against every file at every refresh, reused files
included. It compares field ids the resident footer already holds, so
it reads nothing.

Earlier RFDs and documents are revised to match: RFD 002 and the table
contract (columns may be added, served columns are not renamed); RFD
005 (resolution and the check on reused files); RFD 007 (the reason
`column_mismatch`, which replaces the file fault `MissingColumn`); RFD 016 (the advice `column_default`); and the
operating guide's list of refresh-error reasons.

Fixtures pin each row of the table above under RFD 014 (Test layering),
with a reused file under a dropped and re-added name, a file with field
ids on only some columns, a nested name, and an unreadable default.
