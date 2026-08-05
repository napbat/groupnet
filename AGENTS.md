# AGENTS.md

Instructions for any coding agent working in this repository.

## Orientation

groupnet is a deterministic, leaderless-by-default coordination fabric for
sharded distributed systems. Read `README.md` for the architecture,
`docs/technical.md` for pinned design contracts, and
`docs/consistency-modes.md` for the consistency-modes design — that document
is the **contract of record** for the Hosted-mode/consistency work and its
Section 6 is the build order.

Downstream consumers that must not break: **docres/shardstore** (uses
membership, TTL'd entries, placement — liveness only) and **s3cache** (uses
the `consistency` + `acks` tiers deeply). Their needs are documented in
`docs/consistency-modes.md` Section 2.

## Hard invariants (violating any of these is a defect, not a style choice)

- **Sans-IO core.** `groupnet-core` and `groupnet-sim` never touch a clock,
  a socket, or tokio — the engine consumes events and returns effects. New
  protocol logic goes in the engine so the deterministic simulator can drive
  it.
- **Thin dependencies.** tokio (narrow feature slice) is the only external
  runtime dependency, and only runtime-layer crates may see it.
  `groupnet-core`'s test graph stays tokio-free. Bench/dev-only deps (divan)
  never enter the published graph.
- **The derived coordinator is never authoritative.** Authority exists only
  in the opt-in Hosted mode's epoch-fenced host.
- **`groupnet-testkit` is internal**: `publish = false`, consumed only as a
  **path-only** dev-dependency (never add a `version` key; never add it to
  `[workspace.dependencies]` or any `[dependencies]`).
- **Wire compatibility.** New protocol features add new frame kinds inside
  the current `FRAME_VERSION` (unknown kinds are dropped by old nodes).
  Changing an existing frame body forces a version bump — avoid it.

## Engineering rules (owner-set, 2026-08-05)

1. **No Rust source file over 1000 lines.** Split modules before they get
   there. (Current largest file is ~670 lines; keep headroom.)
2. **Clippy `all` + `pedantic`** are workspace lints; CI treats warnings as
   errors. Verify with
   `cargo clippy --workspace --all-targets -- -D warnings`. Any
   `#[expect]`/`#[allow]` needs a reason (`reason = "..."` or an adjacent
   comment); prefer `#[expect]` so dead exceptions surface.
3. **Every contract or feature ships with tests that prove it** — unit
   and/or integration:
   - engine/protocol logic: deterministic simulation tests in
     `groupnet-sim` (seeded RNG, partitions, virtual time) for safety and
     liveness properties;
   - async runtime paths: integration tests over the in-memory transport
     using `groupnet-testkit` (`MemCluster`, `eventually` — never bare
     sleeps);
   - wire changes: codec round-trip tests;
   - untested code is unfinished code.

## Testing conventions

- Unit tests: inline `#[cfg(test)] mod tests` at the bottom of the file they
  test. Integration tests: `tests/*.rs`, noun-named by behavior. Shared
  helpers: `groupnet-testkit` (never `tests/common/mod.rs`).
- Workspace lints also enforce `unsafe_code = "forbid"`, `missing_docs`,
  `missing_debug_implementations` — document every public item.
- Bounded polling via `groupnet_testkit::cluster::eventually` /
  `eventually_within`; a site that needs a tighter failure-report bound
  declares its own `SETTLE` constant.

## Verification (all must be green before a change is done)

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
cargo check -p groupnet --no-default-features --all-targets
cargo check -p groupnet-transport --no-default-features
cargo test -p groupnet --features tcp-msg
cargo test -p groupnet-consistency --features acks
```

Benches (dev-only): `cargo bench -p groupnet-core` (smoke: `-- --test`).

## Process

- Architectural work is design-doc-first: agree the contract in `docs/`
  before code. Implementation proceeds in slices; the workspace is green
  (all commands above) at the end of every slice.
- Commit messages follow the existing `feat:`/`fix:`/`test:`/`docs:` style.
