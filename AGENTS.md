# AGENTS.md

Instructions for any coding agent working in this repository.

## Orientation

groupnet is a deterministic, leaderless-by-default coordination fabric for
sharded distributed systems. Read `README.md` for usage,
`docs/README.md` for the architecture and network-lifecycle guides, and
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
- **Workspace dependency and lint policy.** Dependency versions belong in
  `[workspace.dependencies]`; crates inherit them with `workspace = true`
  and select their own features and optionality. The path-only testkit rule
  above is the exception. Every crate uses `[lints] workspace = true`; shared
  lint levels belong in the workspace root.
- **Peers upgrade together.** Every node in a cluster runs the same groupnet
  build; groupnet keeps no compatibility with any other peer version (older
  or newer). Frame bodies, frame kinds and bulk/handoff codecs may change
  freely — never add capability negotiation, version gates or fallbacks for
  another peer version. `FRAME_VERSION` (and each codec's own version byte)
  stays only as a guard that rejects a mis-deployed node's frames instead of
  misparsing them; bump it whenever a kind or body changes. Decoders still
  fail closed on malformed or unknown bytes. Persisted formats keep their
  migration paths.

## Engineering rules (owner-set, 2026-08-05)

1. **No Rust source file over 1000 lines.** Split modules before they get
   there. (Largest today, and the only ones still within ~50 lines of the
   limit, so the next addition to them splits them *first*:
   `groupnet-consistency`'s `src/replication/shell/driver.rs` ~967,
   `tests/lease_dst.rs` ~971, `tests/replication.rs` ~944 and
   `tests/volatile_bootstrap_runtime/scenarios.rs` ~951 (new runtime
   scenarios go in their own `volatile_bootstrap_runtime/` children, as
   `follower_progress.rs` does; the parent, ~711 since `PeerDonor` moved to
   `peer_donor.rs`, keeps the shared fakes);
   `groupnet-runtime`'s `src/driver.rs` ~960 and `src/group.rs` ~960. The
   band under them, with ~60–150 lines of room: `groupnet-core`'s
   `src/volatile_recovery/tests.rs` ~930 (the next lapse-arm test goes in a
   sibling file, as `tests_peer.rs` and `tests_seal.rs` did),
   `tests/election.rs` ~909,
   `src/volatile_bootstrap/transfer/tests.rs` ~904,
   `src/volatile_bootstrap/engine.rs` ~931 (the next recapture state goes in
   `engine/recapture.rs`, ~426),
   `src/volatile_bootstrap/engine/participation.rs` ~883,
   `src/engine/election/mod.rs` ~873,
   `src/replication/session/subscription.rs` ~863, `src/config.rs` ~861,
   `src/engine/election/quorum.rs` ~859 and
   `src/volatile_recovery/engine.rs` ~865 (its fallback paths moved to
   `engine/fallback.rs`, its head evidence to `engine/evidence.rs`);
   `groupnet-sim`'s
   `tests/election_external_skew.rs` ~910, `tests/election_quorum.rs` ~896
   and `src/simulation.rs` ~894; `groupnet-consistency`'s
   `src/replication/shell.rs` ~914,
   `tests/hosted_dst_liveness.rs` ~875, `tests/volatile_bootstrap_bulk_adapter.rs`
   ~864, `src/volatile_recovery/bootstrap/session/run.rs` ~862,
   `tests/lease_dst_liveness.rs` ~876,
   `src/replication/shell/driver/snapshot.rs` ~844 and
   `tests/replication_snapshot.rs` ~840.
   With room still: `groupnet-runtime`'s `src/node.rs` ~803,
   `tests/external_faults.rs` ~781, `src/anchor.rs` ~773, `tests/quorum.rs`
   ~754 and `tests/external.rs` ~706; `groupnet-consistency`'s
   `src/hosted/writes/mod.rs` ~795, `src/hosted/handoff/stream.rs` ~775,
   `src/lease/core.rs` ~827, `src/lease/shell.rs` ~794 (a coherent write's
   wait lives in `lease/shell/writes.rs`),
   `tests/handoff_fence.rs` ~761, `src/hosted/lineage.rs` ~800,
   `tests/handoff_migration.rs` ~757, `tests/hosted_migration.rs` ~752,
   `src/hosted/ledger.rs` ~725, `src/hosted/handoff/wire.rs` ~717 and
   `tests/handoff_resync.rs` ~702; `groupnet-sim/tests/election_external.rs`
   ~786; `groupnet-core`'s `tests/state.rs` ~774 and `src/wire/mod.rs` ~701.
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

- Follow [Cargo's project layout](https://doc.rust-lang.org/cargo/guide/project-layout.html):
  libraries in `src/lib.rs`, binaries in `src/main.rs` or `src/bin/`,
  and targets in `examples/`, `benches/`, and `tests/`. Multi-file targets use
  `<target-name>/main.rs` plus target-local modules. New target names use
  kebab-case; Rust module names use snake_case.
- Unit tests: inline `#[cfg(test)] mod tests` at the bottom of the file they
  test. Integration tests are noun-named by behavior. Reusable cross-suite
  helpers belong in `groupnet-testkit` (never `tests/common/mod.rs`).
- Keep public configuration and lifecycle APIs separate from adapter workers,
  routing state transitions, and wire codecs. Split by responsibility rather
  than adding wrappers or weakening lints to accommodate oversized functions.
- Routing is intrinsic to managed networks, not a selectable link implementation.
  Keep `LinkProvider`/`BoundLink`/`LinkLifecycle` in `groupnet-transport::link`.
  Protocol crates own their typed configuration and binding and must not depend on
  the concrete router in production. Never add protocol-kind or shutdown enums to
  the router; all links register through the shared contract.
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
cargo test -p groupnet-runtime --features dns
cargo clippy -p groupnet-runtime -p groupnet --all-targets --features groupnet-runtime/dns,groupnet/dns,groupnet/udp -- -D warnings
cargo test -p groupnet-transport-router -p groupnet-runtime --features groupnet-runtime/router
cargo clippy -p groupnet-transport-router -p groupnet-runtime -p groupnet --all-targets --features groupnet/router,groupnet-runtime/router -- -D warnings
cargo test --workspace --features groupnet/ipc,groupnet/punch,groupnet/udp,groupnet/tcp-msg
cargo clippy --workspace --all-targets --features groupnet/ipc,groupnet/punch,groupnet/udp,groupnet/tcp-msg -- -D warnings
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
cargo test -p groupnet-consistency --features replication
cargo test -p groupnet --features consistency-replication
cargo clippy -p groupnet-consistency --all-targets --features replication -- -D warnings
cargo clippy -p groupnet --all-targets --features consistency-replication -- -D warnings
cargo test -p groupnet-consistency --features volatile-recovery
cargo test -p groupnet --features consistency-volatile-recovery
cargo clippy -p groupnet-consistency --all-targets --features volatile-recovery -- -D warnings
cargo clippy -p groupnet --all-targets --features consistency-volatile-recovery -- -D warnings
cargo check -p groupnet --no-default-features --features consistency-volatile-recovery --all-targets
RUSTDOCFLAGS='-D warnings' cargo doc -p groupnet-consistency --features volatile-recovery --no-deps
cargo test -p groupnet-transport --features bulk
cargo test -p groupnet-transport-mem --features bulk
cargo test -p groupnet-consistency --features volatile-bootstrap-bulk
cargo test -p groupnet --features consistency-volatile-bootstrap-bulk
cargo clippy -p groupnet-consistency -p groupnet-transport -p groupnet-transport-mem -p groupnet --all-targets --features groupnet-consistency/volatile-bootstrap-bulk,groupnet/consistency-volatile-bootstrap-bulk,groupnet-transport-mem/bulk -- -D warnings
cargo check -p groupnet --no-default-features --features consistency-volatile-bootstrap-bulk --all-targets
RUSTDOCFLAGS='-D warnings' cargo doc -p groupnet-consistency --features volatile-bootstrap-bulk --no-deps
cargo test -p groupnet --features rpc,tcp
cargo clippy -p groupnet --all-targets --features rpc,tcp -- -D warnings
cargo check -p groupnet --no-default-features --features rpc --all-targets
RUSTDOCFLAGS='-D warnings' cargo doc -p groupnet-rpc --no-deps
```

The feature-specific Clippy runs are not redundant: no crate in the workspace turns `leases`,
`hosted`, `handoff`, `replication`, `volatile-recovery`, or `volatile-bootstrap-bulk` on by default, so the workspace clippy above never sees
those tiers' code, their tests, or their DST at all. `handoff` is not covered by
the `hosted` runs either. `Handoff` and `volatile-bootstrap-bulk` each pull in
the data plane, so both have distinct feature graphs that need explicit gates.
`groupnet-rpc` itself is a plain workspace member (the workspace runs cover
it); the facade's `rpc` feature is off by default, so its re-export and the
real-TCP facade test need the `rpc,tcp` runs.

Benches (dev-only): `cargo bench -p groupnet-core` (smoke: `-- --test`) — the
optional performance command; it is not a correctness gate.

## Process

- Architectural work is design-doc-first: agree the contract in `docs/`
  before code. Implementation proceeds in slices; the workspace is green
  (all commands above) at the end of every slice.
- Commit messages follow the existing `feat:`/`fix:`/`test:`/`docs:` style.
