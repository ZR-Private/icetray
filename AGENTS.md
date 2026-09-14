# Agent instructions

Read `CONTRIBUTING.md` first. It is the full set of conventions and it
applies to you exactly as it applies to a human contributor. This file only
adds what an agent gets wrong more often than a person does.

- **Versions come from the registry, never from memory.** `cargo add <crate>`
  and keep what it writes. Do not edit a version string by hand.
- **Never add `#[allow(...)]`** to get past clippy. Fix the code. If you
  believe the lint is wrong, say so and stop; changing the workspace lint
  table is a decision for a person.
- **Gate at the batch, not the change.** `cargo test -p <crate>` while you
  work. `dev/ci-checks.sh` and `cargo test --workspace` once, when the
  batch is done, before reporting. Run them to completion and read the
  output; do not pipe them through `head` or `tail`.
- **Claims need evidence.** "Tests pass" means you ran them in this session
  and saw the result. Report failures with the output, and say plainly which
  checks you did not run.
- **A failing test you did not cause is still yours to handle.** Fix it or
  write it down with file, line, and suspected cause. Never report green
  around it.
- **Comments state constraints.** No narration of what you did, when, or why
  it was hard. Put that in the commit message.
- **Commits**: Conventional Commits as in `CONTRIBUTING.md`. No co-author or
  generated-by trailers.
- **Do not push.** Commit locally and report the SHA. A push is a separate
  decision made by a person each time.
- **Decisions live in `docs/rfd/`.** Read the RFDs that touch your area
  before proposing a change, and follow `docs/rfd/001-how-to-rfd.md` when a
  decision needs recording. Writing an RFD is a decision for a person; draft
  one when asked, never on your own initiative.
