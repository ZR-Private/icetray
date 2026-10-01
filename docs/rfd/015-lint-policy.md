# Lint policy

## Problem

Denying the pedantic group is easy to declare and easy to hollow out: each
allow granted at the point of friction is precedent for the next. This
project has met the three usual cases. Generated protobuf code cannot be
edited to satisfy a lint. Integers become floats wherever a count turns
into a ratio or a gauge, and the prototype exported a snapshot id as a
floating-point gauge and lost its low digits
([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)).
Tests would be shorter under looser rules. The rules are in
[CONTRIBUTING.md](../../CONTRIBUTING.md); the reasons are here.

## Decision

The workspace will deny clippy's pedantic group everywhere, tests included,
with four named lints allowed in the one workspace lint table and nothing
else, deny `unwrap_used` outside tests, fail CI on a missing doc comment, and
admit exactly two scoped carve-outs: inner allows on the module that
includes the generated protobuf code, and `cast_precision_loss` inside one
small module of named integer-to-float helpers; identifiers (ids, offsets)
never become floats.

- Deny means a violation fails the build. `missing_docs` is a warning
  locally and a failure in CI, which runs with warnings denied.
- The four workspace-wide allows are pedantic lints whose advice the
  project rejects for every crate: repeating the module name in a type
  name, marking every candidate `must_use`, and boilerplate error and
  panic sections in doc comments. Adding a fifth is a commit that says
  why.
- When any other lint fires in hand-written code, the code changes.
- The generated-code module (from the protobuf service of RFD 010
  (Transport and client contract)) is the only place for an inner allow.
  Its allow list grows one lint at a time, after the generator's output
  has tripped it. The attribute stays on the module; the crate stays under
  the workspace table.
- Integers become floats only through the helper module, each helper
  named for the quantity it yields (a ratio, a gauge reading, a count, a
  length). Precision loss on very large values is accepted there.
- Ids and offsets stay integers. The snapshot id goes to logs and the
  table-info reply; metrics report the snapshot's sequence number, a count
  of commits (RFD 012 (Deployment contract)).

## Alternatives Considered

Allowing the pedantic group workspace-wide, or allowing each lint as it
fires. A rule that yields at every point of friction constrains nothing.

Opting the crate that carries the generated code out of the workspace lint
table. The same crate has a hand-written half.

Laundering casts through integer halves. Splitting a wide integer into
pieces that each convert without a warning silences the lint and keeps the
precision loss.

## Consequences

Test bodies pay the pedantic tax too, and a feature typically takes a few
rounds of lint fixing before it merges. That cost buys a mechanical
reviewer on every build.

An integer that needs to be a float picks the helper that names its
purpose. A value that fits none of the helpers is probably an identifier,
and the right fix is to keep it an integer.
