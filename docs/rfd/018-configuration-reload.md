# Configuration reload

## Problem

The tables a server serves are fixed when it starts. Changing them means
a restart, and a restart reloads every table: the unchanged ones pay a
first load or, under the disk tier of RFD 017 (Residency tiers), a restart
from trusted chunk files. A server that follows its tables' refs (RFD 005
(Generations and refresh)) can load a table beside the ones serving.

A management API makes the running process the record of what it serves;
a reload makes the file that record. The choice decides where the list of
tables lives, so it is expensive to reverse.

## Decision

A change to the configuration file's tables will be applied to a running
server when the file's contents change or the process receives SIGHUP, by
loading every added or changed table beside the tables serving and
swapping the whole set at once; a reload that cannot be applied in full
will change nothing.

- The server reads the file on its poll interval (RFD 005) and compares a
  digest of its bytes with the digest of the file it applied; SIGHUP reads
  it at once.
- A change that fails to parse or validate (RFD 011 (Configuration)) is
  refused as a whole. A poll passes over a refused file until its bytes
  change; SIGHUP tries it again at once.
- A reload applies the tables only. A file whose server, storage, or log
  settings differ from the running ones is refused with a reason that
  names the setting; a restart applies it.
- Tables are matched by name. A table whose entry is equal in every field
  keeps its generation, its refresh, and its reservations untouched.
- An added table loads under the load deadline, as at start. A changed
  table loads beside the one serving under its name.
- A removed table stops refreshing at the swap. A call in flight finishes
  on the generation it holds; a later call for the name is `NOT_FOUND`.
  The local directory's retention (RFD 017) then deletes the chunk files
  no serving generation references.
- The swap waits until every added and changed table serves. If any
  fails, the reload is refused, the tables it loaded are dropped, and the
  set that was serving serves on: the start-or-fail rule of RFD 005,
  carried to a running server.
- One reload runs at a time. A change that arrives during a reload is
  read when it ends.
- Each reload is logged with the digest of the file it read and, for a
  refusal, the reason. Metrics count reloads by result, and a gauge is 1
  while the file on disk differs from the configuration applied; the
  digest itself as a gauge would lose digits
  ([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)).
- Readiness does not change on a reload.
- A table loading for a reload writes no per-table gauges until the swap.
  The metrics recorder cannot remove a series, so a removed table's state
  series all read 0 and its index bytes 0, and its other series keep their
  last values until the process restarts.

## Alternatives Considered

A management API that loads and unloads tables. Rejected: a call reaches
one replica, so icetray would have to persist and replicate it, a second
source for the list of tables that drifts from the file. It would also
need authentication and access policy, which RFD 012 (Deployment contract)
leaves to the platform.

Watching the file for filesystem events. Rejected as the trigger: a
symlink swap or a save by rename replaces the file under the watch. A
digest on the poll interval sees every change within one interval, at the
cost of one small local read per poll.

SIGHUP alone. Rejected as the only trigger: an orchestrator that updates a
mounted file does not signal the process. It stays as the way to apply a
change at once.

Applying the tables that loaded and reporting the rest. Rejected: a table
missing from a running server is the silent failure RFD 005 refuses at
start.

Reloading server and storage settings as well. Rejected for now: each is
its own problem (rebinding a listener, resizing a page cache in use,
replacing the storage client under loads in flight).

## Consequences

Adding or changing a table reloads that table only. A restart is needed
for server, storage, and log settings.

A replica must have the memory for a reload's new tables beside the
serving ones, the headroom RFD 012 sizes for a refresh that rewrites a
table. A reload that exceeds the memory limit ends the process, and since
every replica reads the same file, an oversized table crash-loops the
fleet. The doctor's predicted resident bytes (RFD 016 (Doctor)) are the
check before the change.

The file a replica applies can lag the file on disk by up to one poll
interval, for as long as a load takes, or until the file is fixed after a
refusal. The gauge and the log make the difference visible; they do not
close it. A file refused on reload would also fail at the next start.

Replicas apply a change at different moments, within the orchestrator's
delay in updating mounted files plus one poll interval; a client can see a
table from one replica and `NOT_FOUND` from another in that window.

A single-file mount fixed at start gets no reload; icetray does not detect
the mount.

The file must be replaced whole, by a rename or a symlink swap. A read
during an in-place write can see a prefix that ends between two tables and
parses as a configuration without the rest: a poll would drop those tables
and the next would load them again.
