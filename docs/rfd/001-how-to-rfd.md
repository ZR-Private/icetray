# How to RFD

> Updated 2026-09-30: an RFD states its evidence as the conclusion it
> supports; exact measurements stay in analyses and benchmarks.

> Updated 2026-10-04: a plan is deleted once its work has landed, so
> `docs/plans/` is often absent.

## Problem

Decisions in a small project get made in conversation, often with an AI
partner, and then evaporate. Months later the code shows what was decided
but not why. The same question gets argued again, and the reasoning that
would have made the next decision easier is gone. A record of decisions
only helps if it is cheap enough to write at the moment of deciding.

## Decision

Decisions in this project will be recorded as short RFDs in `docs/rfd/`,
one decision per file, each stating its decision as a single affirmative
sentence of the form "problem A will be solved with approach X."

An RFD records a decision once it has been made. The discussion behind it
happens live, in a conversation or a working session, and is usually over
by the time anyone writes the RFD. The measurements behind a decision live
in `docs/analysis/`, and the step-by-step work of carrying it out lives in
`docs/plans/`. The RFD says what the evidence showed and what was decided
because of it; the measurements themselves and the plan stay in their own
documents.

## Structure

```markdown
# Short Title

## Problem

What is broken, missing, or unclear, and why it needs a decision now.

## Decision

One affirmative sentence: problem A will be solved with approach X.
Then the key details, as briefly as they can be stated.

## Alternatives Considered

What else was on the table and why it lost. A sentence or two each.

## Consequences

What follows from the decision, including the costs being accepted.
```

A section can be a single sentence, and a section that does not apply is
left out. An RFD has only its title and these sections: frontmatter,
status fields, author lines, approval workflows, and date headers are all
left out, because the file's git history records when it was written and
by whom.

## House rules

**The decision is one affirmative sentence.** "The primary-key index will
be the Parquet page index over sorted, bucket-partitioned data; there will
be no sidecar index in v1" is a decision. "We should consider using the
page index" is not one, and a list of options is not one either. If the
sentence cannot be written, the decision has not been made yet and the RFD
is not ready.

**One decision per RFD.** Three decisions belong in three files that
cross-reference each other. An RFD should take under five minutes to read.
One that takes longer is either several decisions or an implementation
plan under the wrong name.

**The number identifies an RFD and implies nothing about order.** Files
are named `NNN-short-name.md`, with three sequential digits, in
`docs/rfd/`. The number exists so that "See RFD 004" points at one file,
and that is all it is for.

**Update in place.** When a decision changes, edit its RFD. A material
change gets a note at the top:

```markdown
> Updated 2026-11-02: the sidecar index is now in scope for v1. See RFD 009.
```

If an RFD no longer describes reality and nobody will update it, mark it
superseded and point to its replacement. A stale RFD misleads the reader
who trusts it, which makes it worse than a missing one.

**Evidence appears as the conclusion it supports.** An RFD says what was
measured and what it showed: "the column reader was faster on every
layout tried", "load is bound by CPU once enough files are in flight".
Where the size of something is the argument, an order of magnitude makes
it: a cold range GET costs tens of milliseconds and a resident lookup
microseconds. A number that is part of the decision is stated exactly,
with the reason that fixes it. Exact figures, machines, and dates belong
in an analysis, and published benchmarks in documents of
their own. An RFD may link to them where a reader would want the detail,
and makes its claims without depending on the link.

**A setting's default is not a decision.** An RFD names a setting and
what it bounds, "`server.batch_fan_out` bounds the GETs one batch holds
in flight", and leaves its value to the configuration reference. It
states a value only where the value is itself the decision, with the
reason that fixes it: the response bound defaults to a gRPC client's
default receive limit, and a cache holds at least one value page per
shard. A value that could change without anyone revisiting the decision
lives in its named constant in the code and in the configuration
reference, so changing it needs no update note. The user guides name a
setting and link the reference rather than repeat its value.

**An RFD says what was decided and leaves the how to a plan.** It names
its dependencies without versioning them, since versions come from the
registry at implementation time. Build steps, file layouts, and sequencing
belong in a plan.

## Where things go

| | `docs/rfd/` | `docs/analysis/` | `docs/plans/` |
|---|---|---|---|
| Answers | What was decided, and why | What is true, with evidence | How to carry it out |
| Lifespan | Long-lived reference | Long-lived evidence | Consumed during implementation |
| Written | At the moment of deciding | Before deciding, while researching | After the RFD, before building |
| Naming | `NNN-short-name.md` | `YYYY-MM-DD-topic.md` | `YYYY-MM-DD-topic.md`, with a paired `-design.md` when one exists |
| Audience | Anyone, including future maintainers | Whoever is about to decide | The person or agent implementing |

An analysis can exist without an RFD, because a survey that leads nowhere
is still worth keeping. An RFD that rests on evidence states what it
concluded from it and may link the analysis behind it. A plan cites the
RFD it implements and is deleted once the work has landed, while the RFD
outlives it, so `docs/plans/` is often absent.

## When to write one

Write an RFD when there was more than one reasonable approach, when the
choice would be expensive to reverse, or when a future reader is likely to
ask why. Skip it for choices that are obvious, trivially reversible, or
only matter while building. If the problem and the approach cannot be
explained in a page, reconsider the work instead of writing a longer
document.

## Alternatives Considered

**ADR format with status metadata.** Status fields (proposed, accepted,
deprecated) and frontmatter add ceremony that a one-person project never
uses, and git history already records dates and authorship.

**Discussion-first RFDs.** Processes that make the document the venue for
debate suit teams with many stakeholders. Here the discussion has usually
happened before anything is written, so a document shaped for it would
mostly sit empty.

**A measured figure beside every claim.** Citing the exact numbers behind
each claim ties a decision to one machine and one day, so every
re-measurement edits the RFD, and the reasoning gets lost among the
figures. The analysis keeps the figures; the RFD keeps what they meant.

**No records, with the commits left to speak for themselves.** A commit
message says what changed, but leaves out what was rejected and why, and
the reasons for rejecting an alternative are exactly what gets lost.

## Consequences

Some decisions will be written down after the fact, and the record will
hold the reasoning without the debate that produced it. Readers get the
reasons rather than the argument, and in exchange the record is cheap
enough to exist at all. What keeps the system honest is the affirmative
sentence: when it cannot be written, the work is not ready to start.
