//! The write-ahead half of the Quorum voter contract on the driver side: the
//! grant persist the actor waits for, the bound on that wait once the node
//! closes, and the guard that withholds the frames of a grant its store
//! refused.

use std::sync::Arc;
use std::time::Duration;

use groupnet_core::{GroupId, NodeId, wire};
use tokio_util::sync::CancellationToken;

use crate::store::GrantStore;

/// The `(epoch, claimant)` a group actor's engine has recorded as granted but
/// its store has not accepted — `None` while everything the engine believes is
/// also on disk.
type Undurable = Option<(u64, NodeId)>;

/// A voter's grant store and its close wait (see
/// [`VoterStorage::with_close_wait`](crate::VoterStorage::with_close_wait)).
pub(crate) type Grants = (Arc<dyn GrantStore>, Duration);

/// What [`Node::close`](crate::Node::close) reports once every group actor of
/// the node has stopped: the grant writes it stopped waiting for.
///
/// Empty in the common case: no grant was being written, or each write ended
/// within its close wait, so no grant of this node can become durable after
/// the close. Each [`UnfinishedGrant`] is a write still running in a voter's
/// store, which can still land.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CloseReport {
    /// The grant writes still running when their close wait ran out.
    pub unfinished: Vec<UnfinishedGrant>,
}

impl CloseReport {
    /// Whether no grant of the closed node can become durable any more.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.unfinished.is_empty()
    }
}

/// A grant write that [`Node::close`](crate::Node::close) stopped waiting for:
/// it was still running in the voter's store when its
/// [close wait](crate::VoterStorage::with_close_wait) ran out, so the pair can
/// still become durable **after** the close returned.
///
/// The driver counted the pair as not durable and withheld the grant frames
/// that carry it. What is at stake is the store that the node's next life
/// loads. Before that life starts, wait until the store has finished the
/// write, or fence the store so that the late write cannot land.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnfinishedGrant {
    /// The group whose voter wrote the grant.
    pub group: GroupId,
    /// The epoch of the grant.
    pub epoch: u64,
    /// The claimant the grant names.
    pub claimant: NodeId,
}

/// One group actor's voter durability: the store, the close wait, the guard
/// state, and the write that the close abandoned, if any.
pub(super) struct Durability {
    /// `None` is the **blackout posture**: [`persist`](Self::persist) is a
    /// no-op and the engine's post-restart grant blackout stands in for
    /// durability.
    store: Option<Arc<dyn GrantStore>>,
    /// How long a write still running when the node closes is waited for.
    close_wait: Duration,
    /// Cancelled when the node closes.
    closed: CancellationToken,
    /// The grant this engine believes it made but whose durability the store
    /// refused. It outlives the batch it was armed in, deliberately: the
    /// engine re-answers an already-recorded pair without re-persisting it (a
    /// re-grant decides nothing new), so a guard scoped to one batch would
    /// swallow the first frame and let the very next round leak the same
    /// undurable grant. Disarmed by the next persist that *succeeds* — which
    /// supersedes the pair on disk, making everything at or below it safe to
    /// answer again.
    undurable: Undurable,
    /// The write the close stopped waiting for.
    abandoned: Option<(u64, NodeId)>,
}

/// How one grant write ended, as the actor saw it.
enum Write {
    /// The store answered: `true` when the pair is on disk.
    Done(bool),
    /// The node closed and the write outran the close wait.
    Abandoned,
}

impl Durability {
    pub(super) fn new(store: Option<Grants>, closed: CancellationToken) -> Self {
        let (store, close_wait) = match store {
            Some((store, close_wait)) => (Some(store), close_wait),
            None => (None, Duration::ZERO),
        };
        Self {
            store,
            close_wait,
            closed,
            undurable: None,
            abandoned: None,
        }
    }

    /// Completes one grant's write-ahead persist, and arms or disarms the
    /// guard by whether the pair is now **durable**.
    ///
    /// Runs on Tokio's blocking pool: a store is real I/O (a write and an
    /// `fsync`, if it means what it says), and a group actor holds no lock but
    /// would otherwise stall a runtime worker for the length of a disk round
    /// trip. The actor still waits for the answer — that wait is the
    /// write-ahead contract.
    ///
    /// A store error and a *panicking* store count as not durable, because
    /// neither proves the pair reached disk. Once the node has closed, no new
    /// write starts, and a write that is running is waited for one close wait
    /// at most; past that it is recorded as abandoned (see
    /// [`UnfinishedGrant`]) and counts as not durable too.
    pub(super) async fn persist(&mut self, epoch: u64, claimant: &NodeId) {
        let Some(store) = &self.store else {
            return;
        };
        let durable = if self.closed.is_cancelled() {
            false // nothing is written down after the close
        } else {
            match write(store, epoch, claimant, &self.closed, self.close_wait).await {
                Write::Done(durable) => durable,
                Write::Abandoned => {
                    self.abandoned = Some((epoch, claimant.clone()));
                    false
                }
            }
        };
        // On disk, a successful write supersedes whatever was there — so any
        // older pair the guard was withholding is covered by this one and the
        // node may answer again.
        self.undurable = (!durable).then(|| (epoch, claimant.clone()));
    }

    /// Whether `wire` must not be sent: it publishes the grant the store did
    /// not accept. See [`undurable_frame`].
    pub(super) fn withholds(&self, wire: &[u8], local: &NodeId) -> bool {
        self.undurable
            .as_ref()
            .is_some_and(|grant| undurable_frame(wire, grant, local))
    }

    /// The write the close stopped waiting for, named for `group`.
    pub(super) fn into_unfinished(self, group: &GroupId) -> Option<UnfinishedGrant> {
        self.abandoned.map(|(epoch, claimant)| UnfinishedGrant {
            group: group.clone(),
            epoch,
            claimant,
        })
    }
}

/// Runs one write on the blocking pool, and gives up on it `close_wait` after
/// the node closed. The write's own answer wins a tie.
async fn write(
    store: &Arc<dyn GrantStore>,
    epoch: u64,
    claimant: &NodeId,
    closed: &CancellationToken,
    close_wait: Duration,
) -> Write {
    let store = store.clone();
    let claimant = claimant.clone();
    let mut task = tokio::task::spawn_blocking(move || store.persist(epoch, &claimant));
    tokio::select! {
        biased;
        done = &mut task => Write::Done(matches!(done, Ok(Ok(())))),
        () = async {
            closed.cancelled().await;
            tokio::time::sleep(close_wait).await;
        } => Write::Abandoned,
    }
}

/// Whether `wire` is a frame that would publish the undurable `grant` — the one
/// the driver must therefore swallow.
///
/// The engine emits [`Effect::PersistGrant`] before any frame the grant it
/// records licenses — and only two of that effect's four emission shapes (see
/// [`Effect::PersistGrant`] for all of them) put such a frame in the batch at
/// all:
///
/// * **A peer's claim answered** — the `LeadGrant` naming this node as granter.
///   Dropping it means the claimant never counts us toward its majority. A
///   floor answer (row Q3a: the granted pair re-sent to a claimant refused at
///   or below its epoch) has the same shape, so the same arm withholds the
///   floor answer of an undurable pair.
/// * **Our own claim opened** — the voter's own grant is counted straight into
///   the round rather than sent, so what the persist precedes is the `LeadClaim`
///   broadcast itself. Dropping it means nobody answers a round whose first
///   grant is not durable, so it cannot reach a majority either. The same arm
///   swallows a *host's* renewal claim once the guard is armed at the pair it is
///   renewing, which is what bounds the case below.
///
/// # What this guard bounds rather than prevents
///
/// Row Q4b re-attempts a claimant's self-grant on every tick its round is open,
/// and that retry's persist precedes no frame at all — the claim went out at
/// round open, before the ledger had anything to write down. If the retry both
/// fails to persist *and* completes the majority, the activation's `LeadState`
/// is not withheld and this node hosts on a grant its disk refused. The same
/// goes for a **roster of one**, whose majority the self-grant alone satisfies:
/// row Q4's round is closed before the claim it opened is ever broadcast.
///
/// How long that hostship lasts depends on whether renewing costs a frame:
///
/// * **A roster of two or more.** One lease and no further. The guard stays
///   armed, so the renewal round's `LeadClaim` is swallowed by the arm above,
///   no voter re-grants, and the host demotes when the lease lapses.
/// * **A roster of one.** Indefinitely. A solo voter's renewal round closes
///   in-engine on its own re-grant — which persists nothing (row Q2 writes
///   nothing new) and sends nothing (there is no peer to claim from) — so the
///   guard never sees a frame to swallow and the lease is extended forever on a
///   grant the disk refused.
///
/// Both are the same risk class, because neither touches the clock: S4c-global
/// is untouched — the lease is a real one, anchored to the send instant like any
/// other, and nothing outside this node ever believed the grant. What is at risk
/// is S1-strict across an amnesiac restart, which is the property a failing
/// store has already forfeited.
///
/// Matched by **content, not by position**, for two reasons. A batch whose
/// claim fan-out is empty leaves the `PersistGrant` adjacent to unrelated
/// gossip traffic, so "swallow the next `Send`" would eat a digest. And the
/// engine re-offers both shapes on the anti-entropy cadence *without* a fresh
/// `PersistGrant` — a re-grant of a pair already in the ledger writes nothing
/// new — so positional matching would withhold the first frame and ship the
/// same undurable grant a round later, which is the whole hole this closes.
/// The decode is paid only while the guard is armed, i.e. only after a store
/// has actually failed.
///
/// [`Effect::PersistGrant`]: groupnet_core::Effect::PersistGrant
fn undurable_frame(wire: &[u8], (epoch, claimant): &(u64, NodeId), local: &NodeId) -> bool {
    let Some(frame) = wire::decode(wire) else {
        return false;
    };
    match frame.lead {
        Some(wire::LeadBody::Grant {
            epoch: sent,
            claimant: to,
            granter,
        }) => sent == *epoch && to == *claimant && granter == *local,
        // A self-grant's claim: the round it opens already counts the grant
        // this node could not write down, so the claim is what must not fly.
        Some(wire::LeadBody::Claim {
            epoch: sent,
            claimant: bidder,
        }) => sent == *epoch && bidder == *claimant && bidder == *local,
        Some(wire::LeadBody::State { .. }) | None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::undurable_frame;
    use groupnet_core::{GroupId, NodeId, wire};

    fn n(id: &str) -> NodeId {
        NodeId::new(id)
    }

    /// One encoded election frame, kind derived from the body exactly as the
    /// engine derives it.
    fn lead(body: wire::LeadBody) -> Vec<u8> {
        let kind = match body {
            wire::LeadBody::Claim { .. } => wire::Kind::LeadClaim,
            wire::LeadBody::Grant { .. } => wire::Kind::LeadGrant,
            wire::LeadBody::State { .. } => wire::Kind::LeadState,
        };
        frame(kind, Some(body))
    }

    fn frame(kind: wire::Kind, lead: Option<wire::LeadBody>) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind,
            group: GroupId::new("g"),
            target: None,
            digest: Vec::new(),
            wants: Vec::new(),
            members: Vec::new(),
            metadata: Vec::new(),
            lead,
        })
    }

    fn grant(epoch: u64, claimant: &str, granter: &str) -> Vec<u8> {
        lead(wire::LeadBody::Grant {
            epoch,
            claimant: n(claimant),
            granter: n(granter),
        })
    }

    fn claim(epoch: u64, claimant: &str) -> Vec<u8> {
        lead(wire::LeadBody::Claim {
            epoch,
            claimant: n(claimant),
        })
    }

    /// The armed guard: this node ("me") could not write down its grant of
    /// epoch 7 to "them".
    fn withheld(wire: &[u8]) -> bool {
        undurable_frame(wire, &(7, n("them")), &n("me"))
    }

    /// The first shape: a peer's claim answered. The frame that carries the
    /// undurable grant is exactly the one this node must not send.
    #[test]
    fn our_own_grant_of_the_undurable_pair_is_withheld() {
        assert!(withheld(&grant(7, "them", "me")));
    }

    /// A grant is withheld only if it is *ours* and *this* pair. Anything else
    /// is somebody else's business, or a pair the store already accepted.
    #[test]
    fn a_grant_that_is_not_this_pair_from_this_node_flies() {
        assert!(!withheld(&grant(7, "them", "other")), "another granter");
        assert!(!withheld(&grant(8, "them", "me")), "another epoch");
        assert!(!withheld(&grant(7, "someone", "me")), "another claimant");
        assert!(!withheld(&grant(6, "them", "me")), "an earlier epoch");
    }

    /// The second shape: our own claim. A claimant's self-grant is counted
    /// into the round rather than sent, so the claim is what publishes it —
    /// and this is also the arm that swallows a host's *renewal* claim once
    /// the guard is armed at the pair it is renewing.
    #[test]
    fn our_own_claim_for_the_undurable_pair_is_withheld() {
        assert!(undurable_frame(&claim(7, "me"), &(7, n("me")), &n("me")));
    }

    /// A claim is only ever withheld when this node is bidding for itself: a
    /// peer's claim is not ours to drop, and a claim whose claimant is not the
    /// pair's claimant publishes nothing we failed to write down.
    #[test]
    fn somebody_elses_claim_flies() {
        assert!(!undurable_frame(
            &claim(7, "them"),
            &(7, n("them")),
            &n("me")
        ));
        assert!(!withheld(&claim(7, "me")), "our claim, another pair");
        assert!(!undurable_frame(&claim(8, "me"), &(7, n("me")), &n("me")));
    }

    /// The documented hole, pinned so it cannot be closed by accident: an
    /// activation's `LeadState` is **not** withheld. A row Q4b retry (or a
    /// roster of one) can close a round on the very grant the store refused,
    /// and by then this node is already host — see the guard's doc for how far
    /// that reaches.
    #[test]
    fn the_activation_state_is_not_withheld() {
        assert!(!withheld(&lead(wire::LeadBody::State {
            epoch: 7,
            host: Some(n("me")),
        })));
        assert!(!withheld(&lead(wire::LeadBody::State {
            epoch: 7,
            host: None,
        })));
    }

    /// Matched by content, not by position: an armed guard must not eat the
    /// gossip traffic that happens to share the batch, and must not choke on
    /// bytes it cannot decode.
    #[test]
    fn non_election_traffic_and_garbage_fly() {
        assert!(!withheld(&frame(wire::Kind::Digest, None)));
        assert!(!withheld(&frame(wire::Kind::Ping, None)));
        assert!(!withheld(&[]), "an empty frame decodes to nothing");
        assert!(!withheld(&[0xff, 0xff, 0xff, 0xff]), "garbage");
    }

    /// A store that counts the writes it is asked for.
    #[derive(Default)]
    struct Counting(std::sync::atomic::AtomicUsize);

    impl crate::store::GrantStore for Counting {
        fn persist(&self, _epoch: u64, _claimant: &NodeId) -> std::io::Result<()> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// Once the node has closed, no grant write starts: the store is not
    /// asked, the pair counts as not durable (its frames are withheld), and
    /// nothing is reported unfinished, because nothing is in flight.
    #[tokio::test]
    async fn no_grant_write_starts_after_the_close() {
        let store = std::sync::Arc::new(Counting::default());
        let closed = tokio_util::sync::CancellationToken::new();
        let mut durability = super::Durability::new(
            Some((store.clone(), std::time::Duration::from_secs(3600))),
            closed.clone(),
        );
        durability.persist(4, &n("them")).await;
        assert_eq!(store.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(!durability.withholds(&grant(4, "them", "me"), &n("me")));

        closed.cancel();
        durability.persist(5, &n("them")).await;
        assert_eq!(
            store.0.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a write started after the close"
        );
        assert!(
            durability.withholds(&grant(5, "them", "me"), &n("me")),
            "a pair the store never saw must not be sent"
        );
        assert_eq!(durability.into_unfinished(&GroupId::new("g")), None);
    }
}
