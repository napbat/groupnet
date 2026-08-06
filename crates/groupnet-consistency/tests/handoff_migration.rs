//! Snapshot handoff (feature `handoff`) where the Hosted tier's **recovery**
//! runs out of road: a successor whose own applied state fell past the end of
//! the ring, elected, unable to prove leader completeness, and completed by a
//! covering transfer instead.
//!
//! This is the M4 ring-bound passage made concrete and then walked out of. The
//! heir stops applying, the host writes past the ring, and the heir's `Gap` is
//! one it cannot fill — so it applies the visible tail and **claims none of it**,
//! and its published watermark stays where it was. Then the host dies. The heir
//! is host at a new epoch and refuses service with [`HostedError::Recovering`],
//! naming a target no replay will ever produce. A handoff from the voter that
//! kept up is what ends that: `donors()` leaves the requester out, the transfer
//! lands, `seed` publishes the claim, the latch fires — `fence()` answers,
//! `publish` is admitted — and a `QuorumApplied` round behind it resolves.
//!
//! Split by harness family from its siblings, the house pattern
//! (`groupnet-sim`'s `election_quorum*`, this crate's `hosted_dst*`), each file
//! carrying its own copy: `handoff.rs` and `handoff_fence.rs` drive the protocol
//! itself, and `handoff_resync.rs` runs the laggard's story over **one** steady
//! hostship. This file is the only one that kills a node and closes a second
//! epoch, and every helper here exists for that.
//!
//! # The consumer this file models
//!
//! Deliberately the one the module's honesty box is written for: a consumer
//! whose state **is** the groupnet-carried state, with no store of its own to
//! rebuild from. That shows up in one place, [`Voter::follow`]'s `Gap` arm:
//!
//! * a `Gap` naming writes this node never applied (`missed_through.seq > 0`) is
//!   a **hole**, and while a hole is open the node applies what arrives and
//!   **records nothing**. Recording the top of a lineage asserts coverage of
//!   everything below it, and this consumer cannot honour that assertion;
//! * the lineage-opening `Gap` at `(epoch, 0)` missed nothing of its own
//!   lineage, so it is recorded like any other event — the same rule, reading a
//!   gap that covers no write.
//!
//! `hosted_migration.rs`'s consumer takes the other branch (it rebuilds from a
//! store and records the gap's watermark), which is exactly why nothing there
//! ever needs a handoff, and why the recovery it shows always completes on its
//! own.
//!
//! Every wait is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep. The group runs the **storage-free** Quorum posture, so every
//! election is charged the engine's boot blackout — one `LEASE_MS` — which is
//! what makes `SETTLE` as loose as it is.

#![cfg(feature = "handoff")]

use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use groupnet_consistency::{
    CAP_HOSTED, Commit, CommitLedger, CommitOutcome, Completeness, Frontier, FrontierView, Handoff,
    HandoffReceipt, HostedError, HostedRead, HostedReads, HostedWrites, Snapshot, SnapshotChunks,
    SnapshotSink, SnapshotSource, Watermarks, WriteToken,
};
use groupnet_core::{Activation, HostedConfig, NodeId, VoterRoster, placement};
use groupnet_runtime::{Group, GroupProfile, Leadership, Node, Role};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::{MemBulkNet, MemBulkTransport, MemTransport, Network};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The poll budget for every assertion here. The longest chain in this file is a
/// whole migration *plus* a recovery: detect the dead host, burn the voters'
/// grant promise, close a new epoch, read a fresh majority out of gossip.
const SETTLE: Duration = Duration::from_secs(10);

/// A brisk gossip cadence, so grant rounds, renewals and ledger republishes all
/// happen in wall-clock milliseconds.
const GOSSIP_MS: u64 = 15;

/// A host's authority after its last confirmed renewal round, and — storage-free
/// — also the boot blackout a voter sits out before it will grant a claimant.
const LEASE_MS: u64 = 600;

/// The hosted feed's ring. **Tiny on purpose**: the burst below has to turn it
/// over several times, so what falls off is genuinely unreachable by replay.
const RING: usize = 3;

/// The deadline a healthy committed write is given. A bound, not an expectation.
const PATIENT: Duration = Duration::from_secs(2);

const fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("nonzero")
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

/// Rendezvous ranking of `ids` for `group`, best first — the order the engine's
/// claim guard reads, so `ranked(..)[0]` is the node that will bid and
/// `ranked(..)[1]` is the one that inherits when it dies.
fn ranked(group: &str, ids: &[&str]) -> Vec<NodeId> {
    let members: Vec<(NodeId, u32)> = ids.iter().map(|id| (NodeId::new(*id), 1)).collect();
    placement::owners(group, &members, ids.len())
}

/// A Quorum profile over `voters`, storage-free (see the module docs).
fn quorum_profile(voters: &[&str]) -> GroupProfile {
    GroupProfile::hosted(HostedConfig {
        activation: Activation::Quorum {
            voters: VoterRoster::new(voters.iter().map(|v| NodeId::new(*v))),
        },
        lease_ms: LEASE_MS,
    })
}

// ---------------------------------------------------------------------------
// The consumer's state, and the two halves of the data contract over it.
// ---------------------------------------------------------------------------

/// One node's applied replica: the ownership index a hosted write stream builds,
/// held in memory and **nowhere else**. There is no store behind this to rebuild
/// from, which is precisely the consumer the handoff exists for.
#[derive(Debug, Default)]
struct Replica {
    /// key -> the token that last wrote it. Monotone per key, so re-applying a
    /// write is a no-op — the idempotence this crate's standing contract asks of
    /// every apply, and what makes an overlapping snapshot safe.
    keys: BTreeMap<String, WriteToken>,
    /// Per-writer applied watermarks, read under the **same lock** as `keys`.
    /// That is the whole of the source's half of the contract: `covers` must
    /// describe the image, and it can only do that if the two are read together.
    marks: Watermarks,
    /// A `Gap` naming writes this node never applied. While it is set the node
    /// keeps applying and **claims nothing** — see the module docs.
    hole: bool,
}

impl Replica {
    /// Applies one delivered write. Monotone in both folds.
    fn apply(&mut self, writer: &NodeId, token: WriteToken, key: String) {
        let slot = self.keys.entry(key).or_insert(token);
        *slot = (*slot).max(token);
        let mark = self.marks.entry(writer.clone()).or_insert(token);
        *mark = (*mark).max(token);
    }

    /// Whether a `Gap` through `missed_through` leaves this consumer whole.
    ///
    /// `seq == 0` is a lineage opening that missed no write **of that lineage**;
    /// anything above it names writes gone from the ring and gone from here, and
    /// no amount of local remediation invents them.
    fn absorbs(missed_through: WriteToken) -> bool {
        missed_through.seq == 0
    }

    /// The image and what it covers, taken together under one lock.
    fn image(&self) -> (Watermarks, Vec<Bytes>) {
        let chunks = self
            .keys
            .iter()
            .map(|(key, token)| Bytes::from(format!("{key}\t{}\t{}\n", token.epoch, token.seq)))
            .collect();
        (self.marks.clone(), chunks)
    }

    /// Merges a staged image in — the `finish` swap, written as a merge because
    /// this consumer's apply loop keeps running across the transfer and a
    /// wholesale overwrite would drop whatever landed while the image was in
    /// flight. Monotone per key, so it is idempotent under a retry too.
    fn install(&mut self, staged: &[u8]) -> io::Result<()> {
        let text = std::str::from_utf8(staged).map_err(io::Error::other)?;
        for line in text.lines() {
            let mut parts = line.split('\t');
            let (Some(key), Some(epoch), Some(seq), None) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "malformed snapshot record",
                ));
            };
            let token = WriteToken {
                epoch: epoch.parse().map_err(io::Error::other)?,
                seq: seq.parse().map_err(io::Error::other)?,
            };
            let slot = self.keys.entry(key.to_owned()).or_insert(token);
            *slot = (*slot).max(token);
        }
        Ok(())
    }
}

/// The donor half: opens an image of one node's replica.
struct ReplicaSource(Arc<Mutex<Replica>>);

impl SnapshotSource for ReplicaSource {
    type Chunks = ImageChunks;

    async fn open(&self) -> io::Result<Snapshot<ImageChunks>> {
        let (covers, chunks) = self.0.lock().expect("replica").image();
        Ok(Snapshot {
            covers,
            chunks: ImageChunks(chunks.into_iter()),
        })
    }
}

/// One opened image's chunks.
struct ImageChunks(std::vec::IntoIter<Bytes>);

impl SnapshotChunks for ImageChunks {
    async fn next(&mut self) -> io::Result<Option<Bytes>> {
        Ok(self.0.next())
    }
}

/// The requester half: stages into a scratch buffer of its own and touches the
/// replica only from `finish`, so a dropped, unfinished sink is observably a
/// no-op.
struct ReplicaSink {
    replica: Arc<Mutex<Replica>>,
    staged: Vec<u8>,
}

impl SnapshotSink for ReplicaSink {
    async fn apply(&mut self, chunk: Bytes) -> io::Result<()> {
        self.staged.extend_from_slice(&chunk);
        Ok(())
    }

    async fn finish(self) -> io::Result<()> {
        let ReplicaSink { replica, staged } = self;
        replica.lock().expect("replica").install(&staged)
    }
}

// ---------------------------------------------------------------------------
// One node's whole participation in the tier, plus its data-plane endpoint.
// ---------------------------------------------------------------------------

/// One node: the group, its commit ledger, its frontier, its replica, its gated
/// follower loop, its write path, and its data-plane endpoint.
///
/// The follower loop runs behind a **gate** rather than being spawned and
/// aborted, because a restarted [`HostedReads`] would start at every peer feed's
/// current end — so aborting it is "this node lost its subscription", while
/// closing the gate is "this node stopped applying", which is the failure this
/// file is about.
struct Voter {
    id: NodeId,
    net: Network,
    node: Option<Node<MemTransport>>,
    group: Option<Group>,
    ledger: Option<Arc<CommitLedger>>,
    writes: Option<HostedWrites<String>>,
    replica: Arc<Mutex<Replica>>,
    frontier: Arc<Frontier>,
    _view: FrontierView,
    plane: Option<DataPlane<MemBulkTransport>>,
    gate: watch::Sender<bool>,
    apply: Option<JoinHandle<()>>,
    serving: Option<JoinHandle<()>>,
}

impl Voter {
    /// Brings one node up, joined to `group` under the Quorum profile over
    /// `voters`, advertising [`CAP_HOSTED`] — but not yet following.
    fn spawn(net: &Network, group: &str, id: &str, seeds: &[&str], voters: &[&str]) -> Self {
        let opts = NodeOpts::new(group)
            .gossip_interval_ms(GOSSIP_MS)
            .group_profile(quorum_profile(voters));
        let (id, node, handle) = spawn_mem_node(net, id, seeds, &opts);
        handle
            .advertise_capabilities([CAP_HOSTED])
            .expect("the advertisement is enqueued");
        let ledger = Arc::new(CommitLedger::new(handle.clone()));
        let writes = HostedWrites::committed(
            handle.clone(),
            id.clone(),
            cap(RING),
            |key: &String| key.clone().into_bytes(),
            Arc::clone(&ledger),
        )
        .expect("a Quorum group supports the committed regime");
        let (frontier, view) = Frontier::new();
        Self {
            id,
            net: net.clone(),
            node: Some(node),
            group: Some(handle),
            ledger: Some(ledger),
            writes: Some(writes),
            replica: Arc::default(),
            frontier: Arc::new(frontier),
            _view: view,
            plane: None,
            gate: watch::channel(true).0,
            apply: None,
            serving: None,
        }
    }

    fn group(&self) -> &Group {
        self.group.as_ref().expect("this voter is alive")
    }

    fn writes(&self) -> &HostedWrites<String> {
        self.writes.as_ref().expect("this voter is alive")
    }

    fn ledger(&self) -> &Arc<CommitLedger> {
        self.ledger.as_ref().expect("this voter is alive")
    }

    fn plane(&self) -> &DataPlane<MemBulkTransport> {
        self.plane
            .as_ref()
            .expect("this voter joined the data plane")
    }

    fn is_host(&self) -> bool {
        self.group().leadership().role == Role::Host
    }

    /// The follower loop the deployment contract asks of every voter, in the
    /// shape a consumer **without a store** must write it: apply, then record —
    /// and while a hole is open, apply and record *nothing*.
    fn follow(&mut self) {
        assert!(self.apply.is_none(), "already following");
        let mut reads = HostedReads::new(self.group().clone(), self.id.clone(), decode);
        // The builder step the contracted loop asks for: when this node is
        // admitted to serve, its own lineage is cut there.
        self.writes().bind(&mut reads);
        let ledger = Arc::clone(self.ledger());
        let replica = Arc::clone(&self.replica);
        let frontier = Arc::clone(&self.frontier);
        let mut gate = self.gate.subscribe();
        self.apply = Some(tokio::spawn(async move {
            let mut host: Option<NodeId> = None;
            loop {
                while !*gate.borrow_and_update() {
                    if gate.changed().await.is_err() {
                        return;
                    }
                }
                // `HostedReads::next` is cancel-safe, so closing the gate can
                // pre-empt an in-flight poll without losing anything.
                let event = tokio::select! {
                    biased;
                    _ = gate.changed() => continue,
                    event = reads.next() => event,
                };
                let Some(event) = event else { return };
                match event {
                    HostedRead::Wrote {
                        host: writer,
                        token,
                        key,
                    } => {
                        let whole = {
                            let mut replica = replica.lock().expect("replica");
                            replica.apply(&writer, token, key);
                            !replica.hole
                        };
                        // The apply happened above; the claim follows it only
                        // when there is no hole underneath. A watermark asserts
                        // coverage of everything below it, and a node with a
                        // hole cannot make that assertion honestly.
                        if whole {
                            frontier.advance(&writer, token);
                            ledger.record(&writer, token).await;
                        }
                    }
                    HostedRead::Gap { missed_through } => {
                        // No store to rebuild from: the only honest remediation
                        // is a covering transfer, and until one lands this node
                        // stops publishing claims it cannot back.
                        if Replica::absorbs(missed_through) {
                            if let Some(host) = &host {
                                frontier.advance(host, missed_through);
                                ledger.record(host, missed_through).await;
                            }
                        } else {
                            replica.lock().expect("replica").hole = true;
                        }
                    }
                    HostedRead::Migrated {
                        host: adopted,
                        epoch: _,
                    } => {
                        host = adopted;
                        ledger.refresh().await;
                    }
                }
            }
        }));
    }

    /// Registers this node's data-plane endpoint **once** and starts serving
    /// handoff requests from its own replica.
    ///
    /// One endpoint per node: re-registering an id on a [`MemBulkNet`] replaces
    /// its accept queue, so the requester's `connect` and the donor's `accept`
    /// share the [`DataPlane`] this builds rather than each making their own.
    fn join_data_plane(&mut self, bulk: &MemBulkNet) {
        assert!(self.plane.is_none(), "already on the data plane");
        let plane = DataPlane::new(bulk.endpoint(self.id.clone()));
        let accepting = plane.clone();
        let handoff = Handoff::new(self.group().clone(), self.id.clone());
        let source = ReplicaSource(Arc::clone(&self.replica));
        self.serving = Some(tokio::spawn(async move {
            // One transfer at a time: `offer` owns no concurrency policy, which
            // is exactly so the consumer can pick this one.
            while let Ok((_from, mut stream)) = accepting.accept().await {
                let _ = handoff.offer(&mut stream, &source).await;
            }
        }));
        self.plane = Some(plane);
    }

    /// Publishes one key at [`Commit::QuorumApplied`] and applies it locally.
    ///
    /// The local apply is the caller's job and not an oversight: a host's own
    /// subscriber excludes its own feed, so nothing ever delivers a host its own
    /// writes — and `publish` has already recorded the token into this node's
    /// ledger on the understanding that they were applied.
    async fn author(&self, key: &str) -> WriteToken {
        let receipt = self
            .writes()
            .publish_committed(&key.to_owned(), Commit::QuorumApplied, PATIENT)
            .await
            .expect("the host serves");
        assert_eq!(receipt.outcome, CommitOutcome::Committed);
        self.replica
            .lock()
            .expect("replica")
            .apply(&self.id, receipt.token, key.to_owned());
        self.frontier.advance(&self.id, receipt.token);
        receipt.token
    }

    /// Folds a completed transfer's receipt into everything that speaks for this
    /// node: the replica's own marks, its commit ledger, and its frontier.
    ///
    /// The sink installed the bytes; this is the **claim** about them, and it is
    /// what closes the hole. Nothing here is a cursor: the `Gap` already
    /// positioned the subscriber, and this moves only state and evidence.
    async fn adopt(&self, receipt: &HandoffReceipt) {
        {
            let mut replica = self.replica.lock().expect("replica");
            for (writer, token) in &receipt.covers {
                let mark = replica.marks.entry(writer.clone()).or_insert(*token);
                *mark = (*mark).max(*token);
            }
            replica.hole = false;
        }
        Handoff::seed(receipt, self.ledger(), &self.frontier).await;
    }

    /// Stops applying without losing the subscription, and resumes.
    fn hold(&self, applying: bool) {
        self.gate.send_replace(applying);
    }

    /// This node's replica as a comparable image: key -> the token that wrote it.
    fn keys(&self) -> BTreeMap<String, WriteToken> {
        self.replica.lock().expect("replica").keys.clone()
    }

    fn has_hole(&self) -> bool {
        self.replica.lock().expect("replica").hole
    }

    /// Kills the node outright, endpoint and all. Dropping the handles is not
    /// enough on its own: the receive loop owns an `Arc` of the same inner
    /// state, so the actors keep ticking until the endpoint is evicted from the
    /// `Network`.
    async fn kill(&mut self) {
        for task in [self.apply.take(), self.serving.take()]
            .into_iter()
            .flatten()
        {
            task.abort();
            let _ = task.await;
        }
        self.plane = None;
        self.writes = None;
        self.ledger = None;
        self.group = None;
        self.node = None;
        drop(self.net.endpoint(self.id.clone()));
    }
}

/// Brings `ids` up as an all-to-all Quorum cluster on `net`, each node a voter,
/// none of them following yet.
fn spawn_roster(net: &Network, group: &str, ids: &[&str]) -> Vec<Voter> {
    ids.iter()
        .map(|id| {
            let seeds: Vec<&str> = ids.iter().copied().filter(|other| other != id).collect();
            Voter::spawn(net, group, id, &seeds, ids)
        })
        .collect()
}

/// The live members' group handles, for the convergence helpers.
fn live(voters: &[Voter]) -> Vec<&Group> {
    voters.iter().filter_map(|v| v.group.as_ref()).collect()
}

/// The leadership every live node agrees on, or `None` while it is still
/// settling — asserted as one indivisible predicate so a poll cannot catch half
/// of it.
fn agreed(groups: &[&Group]) -> Option<Leadership> {
    let first = groups.first()?.leadership();
    first.host.as_ref()?;
    let all: Vec<Leadership> = groups.iter().map(|g| g.leadership()).collect();
    if all
        .iter()
        .any(|l| l.epoch != first.epoch || l.host != first.host)
    {
        return None;
    }
    (all.iter().filter(|l| l.role == Role::Host).count() == 1).then_some(first)
}

/// Brings the roster up, starts every follower loop, and waits for an epoch to
/// close and its host to finish recovering.
async fn elected(voters: &mut [Voter]) -> (Leadership, usize) {
    for voter in voters.iter_mut() {
        voter.follow();
    }
    converged_within(&live(voters), SETTLE).await;
    eventually_within("every voter to see the whole hosted roster", SETTLE, || {
        live(voters)
            .iter()
            .all(|g| g.members_with_capability(CAP_HOSTED).len() == voters.len())
    })
    .await;
    eventually_within("the roster to close an epoch", SETTLE, || {
        agreed(&live(voters)).is_some()
    })
    .await;
    let lead = agreed(&live(voters)).expect("agreed just above");
    let host = lead.host.clone().expect("agreement requires a named host");
    let index = voters
        .iter()
        .position(|v| v.id == host)
        .expect("the host is one of ours");
    eventually_within("the host to finish recovering", SETTLE, || {
        voters[index].writes().fence().is_some()
    })
    .await;
    (lead, index)
}

// ---------------------------------------------------------------------------
// The scenario.
// ---------------------------------------------------------------------------

/// A host-elect beyond the ring cannot recover on its own, and a handoff is what
/// lets it. `Recovering` → transfer → `seed` → latch → a committed write.
#[tokio::test]
async fn a_recovering_host_completes_through_a_handoff() {
    const GROUP: &str = "handoff-recover";
    const IDS: [&str; 3] = ["hv-a", "hv-b", "hv-c"];
    /// Three times the ring, committed at the level that makes every one of
    /// them the successor's problem.
    const BURST: usize = 10;
    const { assert!(BURST > RING, "the burst must turn the ring over") };

    let net = Network::new();
    let bulk = MemBulkNet::new();
    // Rendezvous order: index 0 bids, index 1 inherits when it dies.
    let rank = ranked(GROUP, &IDS);
    let order: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    let mut voters = spawn_roster(&net, GROUP, &order);
    let (first, host) = elected(&mut voters).await;
    assert_eq!(host, 0);
    for voter in &mut voters {
        voter.join_data_plane(&bulk);
    }
    let old_host = voters[0].id.clone();
    let (heir, witness) = (1usize, 2usize);
    let head = strand_the_heir(&voters, heir, witness, &old_host, BURST).await;

    // --- the host dies, and the heir inherits a debt it cannot pay ---
    voters[0].kill().await;
    let (second, need) = stalled_successor(&voters, heir, &old_host, head).await;
    assert!(
        second.epoch > first.epoch,
        "a migration takes a strictly higher epoch: {second:?} after {first:?}"
    );
    let receipt = rescue(&voters, heir, witness, &need, second.epoch).await;
    voters[heir].adopt(&receipt).await;

    // --- the latch fires ---
    eventually_within("the recovery latch to fire", SETTLE, || {
        voters[heir].writes().fence().is_some()
    })
    .await;
    let fence = voters[heir].writes().fence().expect("recovered");
    assert_eq!((fence.epoch, &fence.host), (second.epoch, &voters[heir].id));
    assert_eq!(
        voters[heir].writes().recovery(),
        Some(Completeness::Complete),
        "the rule never learned a handoff happened — only that watermarks moved"
    );
    assert_eq!(
        voters[heir].keys(),
        voters[witness].keys(),
        "and its replica is the donor's"
    );

    // …and the strong path resolves behind it: the heir and the witness are a
    // majority of the roster, and the witness is applying throughout.
    assert_eq!(
        voters[heir].author("served").await,
        WriteToken {
            epoch: second.epoch,
            seq: 1
        },
        "a fresh feed life at the new leadership epoch, sequencing from one"
    );
}

/// One committed prefix everyone applies, then a burst the heir sleeps through.
/// Returns the head of the dead host's feed — the debt the successor inherits.
async fn strand_the_heir(
    voters: &[Voter],
    heir: usize,
    witness: usize,
    old_host: &NodeId,
    burst: usize,
) -> WriteToken {
    let prefix = voters[0].author("prefix").await;
    for index in [heir, witness] {
        let ledger = Arc::clone(voters[index].ledger());
        eventually_within("both followers to apply the prefix", SETTLE, || {
            ledger.applied(old_host) == Some(prefix)
        })
        .await;
    }

    // The heir stops applying and the ring turns over on top of it. Every write
    // still commits: the host and the witness are a majority of the roster.
    voters[heir].hold(false);
    let mut head = prefix;
    for n in 0..burst {
        head = voters[0].author(&format!("past-{n}")).await;
    }
    voters[heir].hold(true);
    eventually_within("the heir to be told history is gone", SETTLE, || {
        voters[heir].has_hole()
    })
    .await;
    assert_eq!(
        voters[heir].ledger().applied(old_host),
        Some(prefix),
        "it drained everything the ring still held and claimed none of it"
    );
    head
}

/// Waits for the heir to take the group, and pins what it is: host, refusing
/// service, holding no fence, and naming exactly the target it cannot reach.
async fn stalled_successor(
    voters: &[Voter],
    heir: usize,
    old_host: &NodeId,
    head: WriteToken,
) -> (Leadership, Watermarks) {
    eventually_within("the heir to take the group", SETTLE, || {
        voters[heir].is_host()
    })
    .await;
    let second = voters[heir].group().leadership();
    eventually_within("the recovery rule to read a fresh majority", SETTLE, || {
        matches!(
            voters[heir].writes().recovery(),
            Some(Completeness::Recovering { needed }) if !needed.is_empty()
        )
    })
    .await;
    let Some(Completeness::Recovering { needed }) = voters[heir].writes().recovery() else {
        panic!("a fresh majority was just read");
    };
    assert_eq!(
        needed,
        vec![(old_host.clone(), head)],
        "the target is the dead host's whole feed, and the ring holds three of it"
    );
    assert_eq!(
        voters[heir]
            .writes()
            .publish(&"too-soon".to_owned())
            .await
            .expect_err("an unrecovered host must not serialize anything"),
        HostedError::Recovering
    );
    assert_eq!(voters[heir].writes().fence(), None);
    // The recovery rule's own target *is* the handoff's `need`. There is no
    // translation step and no new vocabulary: one names a debt, the other pays
    // it.
    (second, needed.into_iter().collect())
}

/// The transfer that discharges it, from the voter that kept up.
async fn rescue(
    voters: &[Voter],
    heir: usize,
    witness: usize,
    need: &Watermarks,
    epoch: u64,
) -> HandoffReceipt {
    // The donor stamps its offer with what *it* has adopted, so a donor that has
    // not yet learned the new epoch would be refused as stale — correctly, and
    // uselessly. Waiting for it is the deployment's business, not the protocol's.
    eventually_within("the donor to adopt the new epoch", SETTLE, || {
        voters[witness].group().leadership().epoch >= epoch
    })
    .await;
    let handoff = Handoff::new(voters[heir].group().clone(), voters[heir].id.clone());
    eventually_within("a covering donor to be visible in gossip", SETTLE, || {
        !handoff.donors(need).is_empty()
    })
    .await;
    let donors = handoff.donors(need);
    assert!(
        !donors.contains(&voters[heir].id),
        "the adopted host is the requester itself, and it is not offered as its \
         own donor — the self-filter removes it before coverage is even asked, \
         because a node that asked itself would connect to its own endpoint and \
         wait for an offer only its own accept loop could write — {donors:?}"
    );
    assert_eq!(
        donors.first(),
        Some(&voters[witness].id),
        "so the caught-up voter is next, and a follower donor is what the \
         honesty box calls a best-effort second choice — {donors:?}"
    );
    let sink = ReplicaSink {
        replica: Arc::clone(&voters[heir].replica),
        staged: Vec::new(),
    };
    let receipt = handoff
        .fetch(voters[heir].plane(), &donors[0], need, sink)
        .await
        .expect("a covering donor at an agreeing fence");
    assert_eq!(receipt.fence_epoch, epoch);
    assert_eq!(
        receipt.fence_host,
        Some(voters[heir].id.clone()),
        "the donor stamps the hostship it has adopted — which is the requester's \
         own, and not provably stale"
    );
    receipt
}
