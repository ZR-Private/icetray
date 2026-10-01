# How to RFD

## Problem

Decisions made in conversation are lost; the code shows what was decided
but not why, and the question is argued again. The record has to be cheap
enough to be written at the moment of deciding.

## Decision

Decisions in this project will be recorded as short RFDs in `docs/rfd/`,
one decision per file, each stating its decision as a single affirmative
sentence of the form "problem A will be solved with approach X."

Discussion happens live and is usually over before the RFD is written.
Evidence lives in `docs/analysis/`, implementation steps in `docs/plans/`;
the RFD cites both and contains neither.

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

Sections can be a sentence long. A section that does not apply is omitted.
No frontmatter, status field, author line, approval workflow, or date
header; git history carries date and author.

## House rules

**The decision is a sentence, and it is affirmative.** "The primary-key
index will be the Parquet page index over sorted, bucket-partitioned
data; there will be no sidecar index in v1" is a decision. "We should
consider using the page index" is not, and neither is a list of options.
If the sentence cannot be written, the RFD is not ready.

**One decision per RFD.** Three decisions are three files that
cross-reference each other. An RFD reads in under five minutes.

**Numbering is identity, not order.** Files are `NNN-short-name.md`,
three digits, sequential, in `docs/rfd/`. The number exists so that "See
RFD 004" resolves.

**Update in place.** When a decision changes, edit the RFD. A material
change gets a note at the top:

```markdown
> Updated 2026-11-02: the sidecar index is now in scope for v1. See RFD 009.
```

An RFD that no longer describes reality and will not be updated is marked
superseded with a pointer to its replacement.

**RFDs say what, not how.** Dependencies are named; versions come from the
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

An analysis can exist without an RFD. An RFD cites its analysis. A plan
cites its RFD and is deletable once the work has landed.

## When to write one

Write an RFD when there was more than one reasonable approach, when the
choice would be expensive to reverse, or when a future reader will ask
why. Skip it for choices that are obvious, trivially reversible, or only
matter while building. If problem and approach do not fit on a page,
reconsider the work.

## Alternatives Considered

**ADR format with status metadata.** Ceremony a one-person project never
uses; git history records dates and authorship.

**Discussion-first RFDs.** The discussion has usually already happened;
the document would stay empty.

**No records; let the commits speak.** Commits say what changed and lose
what was rejected and why.

## Consequences

Some decisions are written down after the fact, so the record holds the
reasoning without the debate. That is the trade for a record cheap enough
to exist.
