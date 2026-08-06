//! Voter durability: the driver-side store that turns the engine's write-ahead
//! [`Effect::PersistGrant`] contract into a real one.
//!
//! Only a [`Quorum`](groupnet_core::Activation::Quorum) group has anything to
//! persist, and only a node inside its voter roster. Everything here is inert
//! for every other group — configuring a store on an
//! [`Eventual`](groupnet_core::GroupMode::Eventual) group is legal and simply
//! never called.
//!
//! [`Effect::PersistGrant`]: groupnet_core::Effect::PersistGrant

use std::fmt;
use std::sync::Arc;

use groupnet_core::{NodeId, RecoveredGrant};

/// Durable storage for one group's voter ledger: the `(epoch, claimant)` pair
/// this node last granted.
///
/// A voter that grants, crashes, and restarts inside a claim window could
/// otherwise grant a *second* claimant the same epoch — the classic
/// persistent-vote problem, and the one way two majorities of one roster can
/// both be collected for a single epoch. Implementing this trait is how a
/// deployment closes it; a deployment with nowhere to write falls back on the
/// engine's boot blackout instead, which is a timing rule rather than a
/// durability one (see [`RecoveredGrant`]).
///
/// # The write-ahead rule
///
/// The driver **completes [`persist`](Self::persist) before the election frame
/// the grant licenses leaves this node**, and — if `persist` returns an error —
/// **drops that frame outright, and every later re-offer of the same pair**,
/// until a subsequent persist succeeds and supersedes it. A grant that reached
/// the wire but not the store is precisely the double-grant a crash-restart
/// turns into two hosts for one epoch, so the driver fails closed: no
/// durability, no grant. The cost of a persistently failing store is therefore
/// availability — this voter stops closing epochs, and (with the caveat below)
/// stops being able to become host itself, so a roster that needs it stalls
/// until it is fixed.
///
/// # The one grant the drop cannot withhold
///
/// A claimant's **own** grant is counted straight into its round rather than
/// sent, so there is not always a frame left to withhold. Row Q4b re-attempts
/// that self-grant on every tick the round is open, long after the claim went
/// out; a roster of *one* closes its round on the self-grant before the claim is
/// broadcast at all. In both shapes a persist that fails still leaves the round
/// closed, and **the activation's `LeadState` is not withheld** — this node
/// hosts on a grant its disk refused. On a roster of two or more that lasts one
/// lease (the renewal round's claim *is* withheld, so no voter re-grants and the
/// host demotes); on a roster of one it lasts indefinitely, because a solo
/// voter's renewal closes in-engine with no frame to drop.
///
/// This costs **S1-strict across an amnesiac restart** — the epoch may stop
/// being a unique name for a hostship — and nothing else. S4c-global (at most
/// one unexpired lease per group) is carried by the grant promise and the boot
/// blackout, which are timing rules that never consult the store, so it holds
/// whatever the disk does.
///
/// The drop is **silent by design**. Nothing in [`NetStats`] counts it: those
/// counters are produced by the sans-IO engine, which cannot see a driver's
/// disk, and a frame this node decided not to send is not traffic. The error
/// this method returned is the operator's signal — log it, count it, page on it
/// inside the implementation, where the actual failure is visible.
///
/// # Shape of an implementation
///
/// * **One store per group.** A store is configured on the
///   [`GroupProfile`](crate::GroupProfile) a group is joined under, so it is
///   never told which group it is writing for. A process hosting several Quorum
///   groups gives each its own store (or one store keyed by the group it was
///   built for).
/// * **Blocking is expected.** The driver calls this on Tokio's blocking pool,
///   so a synchronous `write` + `fsync` is the intended shape rather than
///   something to work around. Take the durability seriously: a write that is
///   only in the page cache when the machine loses power did not happen.
/// * **Last write wins.** The ledger is one slot, not a log. Each call
///   supersedes the previous one; only a *new* pair is ever persisted, because
///   an idempotent re-grant (how a host renews) decides nothing new.
/// * **A panic counts as a failure.** The driver treats a panicking store
///   exactly like an error return — neither proves the pair reached disk.
///
/// On boot, the deployment reads its store back and hands the result to
/// [`GroupProfile::with_voter_storage`](crate::GroupProfile::with_voter_storage)
/// as a [`RecoveredGrant`]. Read it *before* joining: the join path is
/// synchronous and holds a lock, so it can perform no I/O of its own.
///
/// [`NetStats`]: groupnet_core::NetStats
pub trait GrantStore: Send + Sync + 'static {
    /// Durably record that this node has granted `epoch` to `claimant`,
    /// replacing whatever pair was recorded before.
    ///
    /// Returning `Ok(())` is a promise that a restart will read this pair back.
    /// The driver takes it literally: it sends the grant only once this has
    /// returned `Ok`.
    ///
    /// # Errors
    /// Whatever the underlying storage reports. Any error (and any panic) makes
    /// the driver drop the grant this call was write-ahead of — see the trait
    /// doc.
    fn persist(&self, epoch: u64, claimant: &NodeId) -> std::io::Result<()>;
}

/// The voter-durability wiring one group is joined with: what storage said on
/// boot, and where to write what happens next.
///
/// Built by [`GroupProfile::with_voter_storage`](crate::GroupProfile::with_voter_storage);
/// the two halves travel together because either alone is a half-measure — a
/// store with no recovery re-arms the boot blackout it was meant to replace,
/// and a recovery with no store recovers a pair that will never be updated.
#[derive(Clone)]
pub(crate) struct VoterStorage {
    /// What this voter's storage says it had granted before the restart.
    pub recovered: RecoveredGrant,
    /// Where grants are written before they reach the wire.
    pub store: Arc<dyn GrantStore>,
}

impl fmt::Debug for VoterStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The store is a trait object with no `Debug` bound — deliberately, so
        // an implementation is free to hold a file handle and nothing else.
        f.debug_struct("VoterStorage")
            .field("recovered", &self.recovered)
            .finish_non_exhaustive()
    }
}
