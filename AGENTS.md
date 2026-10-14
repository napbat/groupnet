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
   `tests/lease-dst.rs` ~971, `tests/replication.rs` ~944 and
   `tests/volatile-bootstrap-runtime/scenarios.rs` ~951 (new runtime
   scenarios go in their own `volatile-bootstrap-runtime/` children, as
   `follower_progress.rs` does; the parent `main.rs`, ~705 since `PeerDonor`
   moved to `peer_donor.rs`, keeps the shared fakes);
   `groupnet-runtime`'s `src/driver.rs` ~960 and `src/group.rs` ~960. The
   band under them, with ~60–150 lines of room: `groupnet-core`'s
   `src/volatile_recovery/tests.rs` ~930 (the next lapse-arm test goes in a
   sibling file, as `peer_tests.rs` and `seal_tests.rs` did),
   `tests/election.rs` ~909,
   `src/volatile_bootstrap/transfer/tests.rs` ~904,
   `src/volatile_bootstrap/engine.rs` ~931 (the next recapture state goes in
   `engine/recapture.rs`, ~426),
   `src/volatile_bootstrap/engine/participation.rs` ~883,
   `src/engine/election.rs` ~873,
   `src/replication/session/subscription.rs` ~863, `src/config.rs` ~861,
   `src/engine/election/quorum.rs` ~859 and
   `src/volatile_recovery/engine.rs` ~865 (its fallback paths moved to
   `engine/fallback.rs`, its head evidence to `engine/evidence.rs`);
   `groupnet-sim`'s
   `tests/election-external-skew.rs` ~910, `tests/election-quorum.rs` ~896
   and `src/simulation.rs` ~894; `groupnet-consistency`'s
   `src/replication/shell.rs` ~914,
   `tests/hosted-dst-liveness.rs` ~875, `tests/volatile-bootstrap-bulk-adapter.rs`
   ~864, `src/volatile_recovery/bootstrap/session/run.rs` ~862,
   `tests/lease-dst-liveness.rs` ~876,
   `src/replication/shell/driver/snapshot.rs` ~844 and
   `tests/replication-snapshot.rs` ~840.
   With room still: `groupnet-runtime`'s `src/node.rs` ~803,
   `tests/external-faults.rs` ~781, `src/anchor.rs` ~773, `tests/quorum.rs`
   ~754 and `tests/external.rs` ~706; `groupnet-consistency`'s
   `src/hosted/writes.rs` ~795, `src/hosted/handoff/stream.rs` ~775,
   `src/lease/core.rs` ~827, `src/lease/shell.rs` ~794 (a coherent write's
   wait lives in `lease/shell/writes.rs`),
   `tests/handoff-fence.rs` ~761, `src/hosted/lineage.rs` ~800,
   `tests/handoff-migration.rs` ~757, `tests/hosted-migration.rs` ~752,
   `src/hosted/ledger.rs` ~725, `src/hosted/handoff/wire.rs` ~717 and
   `tests/handoff-resync.rs` ~702; `groupnet-sim/tests/election-external.rs`
   ~786; `groupnet-core`'s `tests/state.rs` ~774 and `src/wire.rs` ~701.
   `simulation.rs` has now absorbed three subsystems' event kinds —
   **the next addition to it splits the probe/liveness dispatch out** rather
   than growing it again.) Four splits worth copying:
   - a **shell** splits from its sans-IO core (`hosted/reads.rs` drives,
     `hosted/lineage.rs` decides and is unit-tested without a runtime);
   - a **DST harness** splits by *schedule family*, each file carrying its own
     copy of the harness and asserting the floors its own schedule earns — the
     house pattern `groupnet-sim`'s `election-quorum*` and this crate's
     `hosted-dst*` suites both follow, and `groupnet-runtime`'s `external.rs` /
     `external-faults.rs` (the tier, and the same tier with its store broken)
     applies to an integration suite;
   - a **shaped-scenario suite** splits by the *rule* each scenario prices, not
     by size: `election-external-failover.rs` keeps the availability-axis
     scenarios and the failover budget, `election-external-rank.rs` takes the
     three the rank gate pays for; `election-quorum.rs` keeps the voter ledger
     and the grant round, `election-quorum-renewal.rs` takes renewal, fencing
     and the recovered-grant posture. Every test is self-contained, so the
     move costs nothing;
   - a **big inline `#[cfg(test)] mod tests`** moves to a child file —
     `wire.rs` keeps `#[cfg(test)] mod tests;` and the body moves to
     `wire/tests.rs` (likewise `hosted/writes.rs`), which keeps every
     `wire::tests::…` path byte-identical. And when a DST file is *already*
     one schedule family (one `#[test]`, one seed loop) it cannot split by
     family without moving seeds and floors, so it splits its harness into a
     target-local child instead: `tests/hosted-dst/main.rs` keeps the model
     and the property suite, `tests/hosted-dst/harness.rs` holds the cluster
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

4. **Separate functions with a blank line**, including methods and trait method
   declarations. Put the separator before the next function's doc comments and
   attributes, not between those comments/attributes and the function.

## Testing conventions

- Follow [Cargo's project layout](https://doc.rust-lang.org/cargo/guide/project-layout.html):
  libraries in `src/lib.rs`, binaries in `src/main.rs` or `src/bin/`,
  and targets in `examples/`, `benches/`, and `tests/`. Binary, example, bench
  and integration-test names are kebab-case (`tests/hosted-dst-liveness.rs`,
  `examples/fenced-ownership.rs`); a multi-file target is `<target-name>/main.rs`
  plus snake_case target-local modules (`tests/hosted-dst/harness.rs`).
- Library modules are `foo.rs` with their children in `foo/`, never
  `foo/mod.rs`. Module files and directories are snake_case and do not repeat
  their parent's name (`msg/endpoint.rs`, not `msg_endpoint.rs`). `#[path]` is
  reserved for platform-selected modules (`ipc/platform.rs`) and fixtures
  shared between unit and integration tests; never use it to flatten a module
  tree or stitch a target together.
- Unit tests: inline `#[cfg(test)] mod tests` at the bottom of the file they
  test; a large one moves to `foo/tests.rs`. Further suites for the same module
  are behavior-named `<topic>_tests.rs` children declared by that module
  (`punch/endpoint/candidate_state_tests.rs`), sharing helpers through
  `pub(super)` items in its `tests.rs`. Integration tests are noun-named by
  behavior. Reusable cross-suite helpers belong in `groupnet-testkit` (never
  `tests/common/mod.rs`).
- Keep public configuration and lifecycle APIs separate from adapter workers,
  routing state transitions, and wire codecs. Split by responsibility rather
  than adding wrappers or weakening lints to accommodate oversized functions.
- Routing is intrinsic to managed networks, not a selectable link implementation.
  Keep `LinkProvider`/`BoundLink`/`LinkLifecycle` in `groupnet-transport::link`.
  Protocol crates own their typed configuration and binding and must not depend on
  the concrete router in production. Never add protocol-kind or shutdown enums to
  the router; all links register through the shared contract.
- Native punching is adjacent connection establishment and ongoing path management,
  not a transport or independently registered link. TCP/UDP protocol crates consume
  owned connections and register their own links. Logical heterogeneous forwarding
  stays in the router; group/route discovery never grants adjacent-peer admission.
- Application messaging follows `groupnet-runtime -> groupnet-messaging ->
  groupnet-network -> groupnet-transport`. `groupnet-messaging` owns application
  codecs, endpoint receipts, retry and deduplication state; the router carries
  opaque packets through bounded protocol-ID queues and must not interpret ACKs.
  `groupnet-streams` owns static session protocols: ordered TLS bytes and encrypted
  unordered messages with explicit reliable/unreliable policy. Runtime owns the
  single public `Endpoint<P>` and `Peer<P>` handle families, shared protocol state,
  node/group callbacks and fanout reports. `Messages`, `Ordered`, and `Unordered`
  are descriptors: `node.endpoint(implementation)` and
  `node.peer(id, implementation)` resolve shared node resources internally and
  bind per-operation options once, without opening a connection. Keep node-wide
  capacity/configuration separate from descriptor delivery/setup options.
  Public unsealed binding and message/session traits support custom static
  implementations; do not introduce a protocol enum or boxed per-call futures.
  Managed message receives always use the existing single-owner node/group inbox,
  never the raw messaging worker queue. Ordered/unordered setup queues remain
  isolated, and unordered accepts must respect the selected policy.
  Keep the lossless buffer/context and frame APIs equivalent, with explicit
  `BestEffort` (default), `Delivered`, and `Applied` boundaries.
- Packet hot paths use typed `zerocopy` headers with explicit byte order, not
  manual field offsets or serialization of native Rust layout. Preserve owned
  `Bytes`/`PacketBuffer` storage through routing and adapters; borrowed APIs may
  copy at their ownership boundary, but do not add repeated whole-payload copies.
  Operational capacities and timers belong to validated typed configuration;
  wire representability, replay protection and finite memory admission remain
  invariants. Control-rate protection must not impose a fixed packet-rate ceiling
  on admitted relay data.
- Workspace lints also enforce `unsafe_code = "forbid"`, `missing_docs`,
  `missing_debug_implementations` — document every public item.
- Bounded polling via `groupnet_testkit::cluster::eventually` /
  `eventually_within`; a site that needs a tighter failure-report bound
  declares its own `SETTLE` constant.

## Verification (all must be green before a change is done)

Install the test runner with `cargo install cargo-nextest --locked` if
`cargo nextest --version` is unavailable. Repository settings are in
`.config/nextest.toml`: retries are disabled; profile `ci` runs all tests even
after failures. Use `cargo nextest run --profile ci --workspace --all-features`
for a complete binary-test run in automation.

Nextest does not run doctests; keep the separate `cargo test --doc` gates.
The feature-isolated `cargo test` commands below also retain their doctests.

```bash
cargo nextest run --workspace
cargo test --workspace --doc
cargo nextest run --workspace --all-features
cargo test --workspace --all-features --doc
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
cargo check -p groupnet --no-default-features --all-targets
cargo check -p groupnet-transport --no-default-features
cargo test -p groupnet --features tcp-msg
cargo test -p groupnet-runtime --features dns
cargo clippy -p groupnet-runtime -p groupnet --all-targets --features groupnet-runtime/dns,groupnet/dns,groupnet/udp -- -D warnings
cargo test -p groupnet-streams -p groupnet-messaging -p groupnet-network -p groupnet-runtime
cargo clippy -p groupnet-streams -p groupnet-messaging -p groupnet-network -p groupnet-runtime -p groupnet --all-targets -- -D warnings
cargo test --workspace --features groupnet/ipc,groupnet/connectivity,groupnet/udp,groupnet/tcp-msg
cargo clippy --workspace --all-targets --features groupnet/ipc,groupnet/connectivity,groupnet/udp,groupnet/tcp-msg -- -D warnings
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
