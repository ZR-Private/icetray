# Configuration reload

## Problem

The tables a server serves are fixed when it starts. Adding one, removing
one, or changing one's settings means editing the configuration file and
restarting the process, and a restart reloads every table, not the one that
changed: the tables that did not change pay a first load, or a restart's
trusted chunk files under the disk residency of RFD 017 (Residency tiers),
while a replica with no local directory pays a first load of all of them.
A server that follows its tables' refs already (RFD 005 (Generations and
refresh)) has everything it needs to load a table beside the ones serving.

Two mechanisms are common for this. A management API that loads and
unloads tables makes the running process the record of what it serves. A
reload makes the file that record and has the process follow it. The choice
decides where the list of tables lives, and so it is expensive to reverse.

## Decision

A change to the configuration file's tables will be applied to a running
server when the file's contents change or the process receives SIGHUP, by
loading every added or changed table beside the tables serving and
swapping the whole set at once; a reload that cannot be applied in full
will change nothing.

The server reads the file on its poll interval (RFD 005) and compares a
digest of its bytes with the digest of the file it applied; SIGHUP reads
it at once. A read that finds the bytes unchanged costs nothing further. A
change that fails to parse or validate (RFD 011 (Configuration)) is refused
as a whole.

A reload applies the tables only. A file whose server, storage, or log
settings differ from the running ones is refused as a whole with a reason
that names the setting and says a restart applies it, since a listener,
the page cache, and the storage client are built once at start.

Tables are matched by name. A table whose entry is equal in every field
keeps its generation, its refresh, and its reservations untouched. An
added table loads under the load deadline, as at start. A changed table
loads as a new table beside the one serving under its name, which serves
until the swap. A removed table stops refreshing at the swap; a call in
flight finishes on the generation it holds, and a later call for the name
is `NOT_FOUND`, as for any name not served. After the swap the local
directory's retention (RFD 017) deletes the chunk files no serving
generation references, as it does after a refresh.

The swap waits until every added and changed table serves its first
generation. If any of them fails to load, the reload is refused, the tables
it loaded are dropped, and the set that was serving serves on. This is the
start-or-fail rule of RFD 005 carried to a running server: at any moment
the server serves exactly the tables of one configuration it applied, never
a partial set.

One reload runs at a time. A change that arrives during a reload is read
when the reload ends, so the next reload starts from the newest file and
the changes in between are never applied on their own.

Each reload's outcome is logged with the digest of the file it read and,
for a refusal, the reason. Metrics count reloads by result, and a gauge is
1 while the file on disk differs from the configuration applied, so an
operator can see that a replica serves a configuration other than its file
without comparing digests, which as a gauge would lose digits
([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)).
Readiness does not change: a refused reload leaves the replica serving the
configuration it applied. A removed table's per-table series leave the
metrics output.

## Alternatives Considered

A management API that loads and unloads tables. Rejected. A call reaches
one replica, and every other replica, and every replica started later,
knows nothing of it unless icetray persists and replicates what the call
did. That record is a second source for the list of tables beside the
file, and it drifts from the file. A call that makes a replica fetch and
hold tens of gigabytes also needs authentication and access policy, which
RFD 012 (Deployment contract) leaves to the platform. Search engines that
work this way own the data they serve; icetray serves tables whose record
is the catalog.

Watching the file for filesystem events. Rejected as the trigger. An
orchestrator that updates a mounted file by swapping a symlink, and an
editor that saves by rename, both replace the file under a watch on it. A
digest compared on the poll interval sees every change within one interval,
whatever wrote it, at the cost of one small local read per poll.

SIGHUP alone. Rejected as the only trigger: an orchestrator that updates a
mounted file does not signal the process, so every deployer would need a
component that does. It stays as the way to apply a change at once.

Applying the tables that loaded and reporting the rest. Rejected: a table
missing from a running server is the silent failure RFD 005 refuses at
start.

Reloading server and storage settings as well. Rejected for now: each is
its own problem (rebinding a listener, resizing a page cache in use,
replacing the storage client under loads in flight), and a restart applies
them.

## Consequences

Adding or changing a table no longer reloads the others, and a restart is
needed only for server, storage, and log settings.

A replica must have the memory for a reload's new tables beside the
serving ones, the same headroom RFD 012 sizes for a refresh that rewrites a
table. A reload that exceeds the memory limit ends the process, and since
every replica reads the same file, every replica reloads at once and a
restart loads the same file: an oversized table crash-loops the fleet as it
would have on a restart. The doctor's predicted resident bytes (RFD 016
(Doctor)) are the check before the change.

The file a replica applies can lag or differ from the file on disk, by up
to one poll interval while it waits, for as long as a load takes, or until
the file is fixed after a refusal. The gauge and the log make the
difference visible; they do not close it. A file that is refused on reload
would also fail at the next start, so a refusal is a warning that the next
restart will fail.

Every replica reads its own copy of the file, so replicas apply a change at
different moments, within the orchestrator's delay in updating mounted
files plus one poll interval. A client can see a table from one replica
and `NOT_FOUND` from another in that window.

A deployer that mounts the file where the orchestrator never updates it,
such as a single-file mount that is fixed at start, gets no reload from a
change and must restart as before; that is the deployer's choice of mount,
not a mode icetray detects.
