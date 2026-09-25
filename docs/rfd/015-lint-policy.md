# Lint policy

## Problem

Denying the pedantic group across a workspace is easy to declare and easy
to hollow out, because the exceptions granted afterward decide what the
denial constrains. A generated file trips a lint, a metric needs an
integer as a float, a test would be shorter under looser rules; each case
argues for an allow, and every allow granted at the point of friction is
precedent for the next. This project has met all three cases. Generated
protobuf code cannot be edited to satisfy a lint. Integers have to become
floats wherever a count turns into a ratio or a gauge reading, and the
lint that catches the dangerous conversions catches the harmless ones
too. The prototype exported a snapshot id as a floating-point gauge and
lost its low digits
([prototype findings, finding 14](../analysis/2026-09-10-prototype-findings.md#14-metrics)),
which is the conversion the lint exists to stop.

The rules that settle these cases are stated in
[CONTRIBUTING.md](../../CONTRIBUTING.md); the reasons for the shape are
recorded here. The lint table is set once for the whole workspace, so a
policy loosened later is loosened for every crate at once, and the
reasons need to be on record before the next allow is requested.

## Decision

The workspace will deny clippy's pedantic group everywhere, tests included,
with four named lints allowed in the one workspace lint table and nothing
else, deny `unwrap_used` outside tests, fail CI on a missing doc comment, and
admit exactly two scoped carve-outs: inner allows on the module that
includes the generated protobuf code, and `cast_precision_loss` inside one
small module of named integer-to-float helpers; identifiers (ids, offsets)
never become floats.

Deny means a violation fails the build; `missing_docs` is a warning locally
and a failure in CI, which runs with warnings denied. The four workspace-wide
allows are pedantic lints whose advice the project rejects for every crate
(repeating the module name in a type name, marking every candidate
`must_use`, and boilerplate error and panic sections in doc comments); they
are listed in the lint table, and adding a fifth is a commit that says why.
Nothing else earns an allow; when a lint fires in hand-written code, the code
changes.

The generated-code module is the only place an inner allow is acceptable.
The generated code comes from the checked-in protobuf service of RFD 010
(Transport and client contract), and the module's allow list grows one
lint at a time, each entry added only after the generator's output has
tripped it. The attribute never widens to the file or the crate, and the
crate stays under the workspace table.

The helper module exists because the precision-loss lint cannot tell a
ratio from an identifier. Integers become floats only through its helpers,
each named for the kind of quantity it yields (a ratio, a gauge reading, a
count, a length), so every call site says what the float is for, and the
loss of precision on very large values is accepted there by design. Ids
and offsets never pass through them; the snapshot id that motivated the
rule is left to logs and the table-info reply, and metrics report the
snapshot's sequence number, a count of commits, instead (RFD 012
(Deployment contract)).

## Alternatives Considered

Allowing the pedantic group workspace-wide, or allowing each lint as it
fires. The policy would exist in name only; a rule that yields at every
point of friction constrains nothing.

Opting the crate that carries the generated code out of the workspace lint
table. The same crate has a hand-written half, and the exemption would
have covered it as well.

Laundering casts through integer halves. Splitting a wide integer into
narrower pieces that each convert to a float without a warning, then
recombining them, silences the lint while keeping the precision loss; it
hides the conversion the lint is there to surface.

## Consequences

Test bodies pay the pedantic tax too. A test cannot cast freely or skip
the conventions the code it exercises follows, and a feature typically
takes a few rounds of lint fixing before it merges. That is the accepted
cost; it buys a mechanical reviewer that runs on every build.

Any integer that needs to be a float has to reach for a helper and pick
the one that names its purpose. That friction is deliberate: a value that fits
none of the helpers is probably an identifier, and the right fix is to
keep it an integer.
