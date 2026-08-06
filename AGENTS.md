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
   there. (Largest today, all within ~50 lines of the limit, so the next
   addition to any of them splits it *first*:
   `groupnet-sim/tests/election_external_failover.rs` ~990;
   `groupnet-consistency`'s `tests/hosted_dst.rs` ~975, `src/hosted/writes.rs`
   ~970 and `tests/hosted_dst_migrate.rs` ~960; `groupnet-core`'s `src/wire.rs`
   ~960 and `tests/election_quorum.rs` ~950;
   `groupnet-consistency/tests/lease_dst.rs` ~950. With room still:
   `groupnet-sim`'s `tests/election_external_skew.rs` ~910 and
   `src/simulation.rs` ~895, `groupnet-consistency/src/lease/shell.rs` ~890,
   `groupnet-core`'s `engine/election/mod.rs` ~875 and `src/config.rs` ~860, the
   rest of the `groupnet-sim` `election_external*` suites at ~785–790,
   `groupnet-runtime`'s `tests/external.rs` ~705 / `tests/external_faults.rs`
   ~780, and M6's handoff files — `groupnet-consistency`'s
   `src/hosted/handoff/stream.rs` ~780, `tests/handoff_fence.rs` ~760,
   `tests/handoff_migration.rs` ~755, `src/hosted/handoff/wire.rs` ~720 and
   `tests/handoff_resync.rs` ~700.
   `simulation.rs` has now absorbed three subsystems' event kinds —
   **the next addition to it splits the probe/liveness dispatch out** rather
   than growing it again.) Two splits worth copying: a **shell** splits from
   its sans-IO core (`hosted/reads.rs` drives, `hosted/lineage.rs` decides and
   is unit-tested without a runtime), and a **DST harness** splits by *schedule
   family*, each file carrying its own copy of the harness and asserting the
   floors its own schedule earns — the house pattern `groupnet-sim`'s
   `election_quorum*` and this crate's `hosted_dst*` suites both follow, and
   `groupnet-runtime`'s `external.rs` / `external_faults.rs` (the tier, and the
   same tier with its store broken) applies to an integration suite.
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
cargo test -p groupnet-consistency --features leases
cargo test -p groupnet-consistency --features hosted
cargo test -p groupnet-consistency --features handoff
cargo test -p groupnet --features consistency-leases
cargo test -p groupnet --features consistency-hosted
cargo test -p groupnet --features consistency-handoff
cargo clippy -p groupnet-consistency --all-targets --features leases -- -D warnings
cargo clippy -p groupnet-consistency --all-targets --features hosted -- -D warnings
cargo clippy -p groupnet-consistency --all-targets --features handoff -- -D warnings
```

The last three are not redundant: no crate in the workspace turns `leases`,
`hosted` or `handoff` on by default, so the workspace clippy above never sees
those tiers' code, their tests, or their DST at all. `handoff` is not covered by
the `hosted` runs either — it is the only consistency feature that pulls in the
data plane, so it is the only one whose build graph differs from the rest.

Benches (dev-only): `cargo bench -p groupnet-core` (smoke: `-- --test`) — the
seventeenth command, and the only one that is not a correctness gate.

## Process

- Architectural work is design-doc-first: agree the contract in `docs/`
  before code. Implementation proceeds in slices; the workspace is green
  (all commands above) at the end of every slice.
- Commit messages follow the existing `feat:`/`fix:`/`test:`/`docs:` style.
