# How to RFD

## Problem

Decisions in a small project get made in conversation, often with an AI
partner, and then evaporate. Months later the code shows what was decided
but not why, the same question gets re-litigated, and the reasoning that
would have made the next decision easier is gone. The record has to be
cheap enough that it actually gets written at the moment of deciding.

## Decision

Decisions in this project will be recorded as short RFDs in `docs/rfd/`,
one decision per file, each stating its decision as a single affirmative
sentence of the form "problem A will be solved with approach X."

The RFD memorializes the decision. It is not where the discussion happens.
Discussion happens live, in a conversation or a working session, and is
usually over by the time the RFD is written. Evidence that grounded the
decision lives in `docs/analysis/`; the step-by-step of carrying it out
lives in `docs/plans/`. The RFD cites both and contains neither.

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
There is no frontmatter, no status field, no author line, no approval
workflow, and no date header; the file's git history carries when it was
written and by whom.

## House rules

**The decision is a sentence, and it is affirmative.** "The primary-key
index will be the Parquet page index over sorted, bucket-partitioned
data; there will be no sidecar index in v1" is a decision. "We should
consider using the page index" is not, and neither is a list of options.
If the sentence cannot be written, the decision has not been made yet and
the RFD is not ready.

**One decision per RFD.** Three decisions are three files that
cross-reference each other. An RFD reads in under five minutes; a longer
one is either several decisions or an implementation plan wearing the
wrong name.

**Numbering is identity, not order.** Files are `NNN-short-name.md`,
three digits, sequential, in `docs/rfd/`. "See RFD 004" is the only thing
the number is for.

**Update in place.** When a decision changes, edit the RFD. A material
change gets a note at the top:

```markdown
> Updated 2026-11-02: the sidecar index is now in scope for v1. See RFD 009.
```

If an RFD no longer describes reality and nobody will update it, mark it
superseded with a pointer to its replacement. A stale RFD is worse than a
missing one.

**RFDs say what, not how.** Dependencies are named, not versioned;
versions come from the registry at implementation time. Build steps,
file layouts, and sequencing belong in a plan.

## Where things go

| | `docs/rfd/` | `docs/analysis/` | `docs/plans/` |
|---|---|---|---|
| Answers | What was decided, and why | What is true, with evidence | How to carry it out |
| Lifespan | Long-lived reference | Long-lived evidence | Consumed during implementation |
| Written | At the moment of deciding | Before deciding, while researching | After the RFD, before building |
| Naming | `NNN-short-name.md` | `YYYY-MM-DD-topic.md` | `YYYY-MM-DD-topic.md`, with a paired `-design.md` when one exists |
| Audience | Anyone, including future maintainers | Whoever is about to decide | The person or agent implementing |

An analysis can exist without an RFD; a survey that leads nowhere is still
worth keeping. An RFD that rests on evidence cites the analysis rather than
repeating it. A plan cites the RFD it implements and is deletable once the
work has landed; the RFD outlives it.

## When to write one

Write an RFD when there was more than one reasonable approach, when the
choice would be expensive to reverse, or when a future reader will
plausibly ask why. Do not write one for choices that are obvious,
trivially reversible, or only matter while building. If the problem and
the approach cannot be explained in a page, that is a signal to reconsider
the work, not to write a longer document.

## Alternatives Considered

**ADR format with status metadata.** Status fields (proposed, accepted,
deprecated) and frontmatter add ceremony that a one-person project never
uses; git history already records dates and authorship.

**Discussion-first RFDs.** Processes where the document is the venue for
debate suit teams with many stakeholders. Here the discussion has usually
already happened, so a document shaped for it would mostly stay empty.

**No records; let the commits speak.** Commit messages say what changed,
not what was rejected or why. The "why not" is exactly the part that
gets lost.

## Consequences

Some decisions will be written down after the fact, and the record will
hold the reasoning without the debate that produced it. That is the
trade: readers get the why, not the argument, in exchange for a record
that is cheap enough to exist. The discipline that keeps the system
honest is the affirmative sentence; when it cannot be written, the work
is not ready to start.
