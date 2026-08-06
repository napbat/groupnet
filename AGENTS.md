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
   there. (Largest today, and the only one still within ~50 lines of the limit,
   so the next addition to it splits it *first*:
   `groupnet-consistency/tests/lease_dst.rs` ~948. The band under it, with
   ~90–150 lines of room: `groupnet-sim`'s `tests/election_external_skew.rs`
   ~910, `tests/election_quorum.rs` ~896 and `src/simulation.rs` ~894;
   `groupnet-core`'s `tests/election.rs` ~909, `src/engine/election/mod.rs`
   ~873, `src/config.rs` ~861 and `src/engine/election/quorum.rs` ~859;
   `groupnet-consistency`'s `src/lease/shell.rs` ~889,
   `tests/hosted_dst_liveness.rs` ~875 and `tests/lease_dst_liveness.rs` ~853.
   With room still: `groupnet-runtime`'s `src/node.rs` ~803,
   `tests/external_faults.rs` ~781, `src/anchor.rs` ~773, `tests/quorum.rs`
   ~754 and `tests/external.rs` ~706; `groupnet-consistency`'s
   `src/hosted/writes/mod.rs` ~794, `src/hosted/handoff/stream.rs` ~782,
   `tests/handoff_fence.rs` ~761, `src/hosted/lineage.rs` ~759,
   `tests/handoff_migration.rs` ~757, `tests/hosted_migration.rs` ~752,
   `src/hosted/ledger.rs` ~725, `src/hosted/handoff/wire.rs` ~720 and
   `tests/handoff_resync.rs` ~702; `groupnet-sim/tests/election_external.rs`
   ~786; `groupnet-core`'s `tests/state.rs` ~774 and `src/wire/mod.rs` ~705.
   `simulation.rs` has now absorbed three subsystems' event kinds —
   **the next addition to it splits the probe/liveness dispatch out** rather
   than growing it again.) Four splits worth copying:
   - a **shell** splits from its sans-IO core (`hosted/reads.rs` drives,
     `hosted/lineage.rs` decides and is unit-tested without a runtime);
   - a **DST harness** splits by *schedule family*, each file carrying its own
     copy of the harness and asserting the floors its own schedule earns — the
     house pattern `groupnet-sim`'s `election_quorum*` and this crate's
     `hosted_dst*` suites both follow, and `groupnet-runtime`'s `external.rs` /
     `external_faults.rs` (the tier, and the same tier with its store broken)
     applies to an integration suite;
   - a **shaped-scenario suite** splits by the *rule* each scenario prices, not
     by size: `election_external_failover.rs` keeps the availability-axis
     scenarios and the failover budget, `election_external_rank.rs` takes the
     three the rank gate pays for; `election_quorum.rs` keeps the voter ledger
     and the grant round, `election_quorum_renewal.rs` takes renewal, fencing
     and the recovered-grant posture. Every test is self-contained, so the
     move costs nothing;
   - a **big inline `#[cfg(test)] mod tests`** moves to a sibling file —
     `wire.rs` becomes `wire/mod.rs` + `wire/tests.rs` behind
     `#[cfg(test)] mod tests;` (likewise `hosted/writes/`), which keeps every
     `wire::tests::…` path byte-identical. And when a DST file is *already*
     one schedule family (one `#[test]`, one seed loop) it cannot split by
     family without moving seeds and floors, so it splits its harness into a
     `#[path]`-included child instead: `tests/hosted_dst.rs` keeps the model
     and the property suite, `tests/hosted_dst/harness.rs` holds the cluster
     harness and the schedule. The child sees the parent's private items, so
     only the scenario entry point needs `pub(crate)` and the binary's output
     does not move a byte.
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
