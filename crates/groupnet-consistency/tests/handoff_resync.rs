//! Snapshot handoff (feature `handoff`) as a **laggard's way back**: a node
//! whose history has fallen off the end of the ring, remediated by a covering
//! transfer, and then resuming the live stream where it stands.
//!
//! The protocol's own surface is proved next door — `handoff.rs` drives the
//! ordered exchange over a plain group, `handoff_fence.rs` the two staleness
//! re-verifications and the donor choice. Neither has a ring or a hosted write
//! path in it. This file supplies both, over one steady hostship: nothing is
//! killed here and no second epoch is ever closed, which is exactly the split
//! from its sibling `handoff_migration.rs` (a whole migration, and a successor
//! that cannot serve). Each file carries its own copy of the harness — the house
//! pattern `groupnet-sim`'s `election_quorum*` and this crate's `hosted_dst*`
//! both follow.
//!
//! The one property, in full: a fourth node joins after the host has written
//! well past a small ring; it is told so honestly — **one** `Gap`, whose
//! `missed_through` names writes the ring no longer holds; `donors()` picks the
//! serving host; the snapshot crosses and installs; `seed` folds the receipt
//! into the ledger and the frontier; and then the live writes behind it arrive
//! **in order, with no second `Gap`**, leaving its state equal to the donor's.
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
//! store and records the gap's watermark), which is why nothing there ever needs
//! a handoff. The two are the two halves of one contract.
//!
//! Every wait is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep. The group runs the **storage-free** Quorum posture, so its first
//! election is charged the engine's boot blackout — one `LEASE_MS` — which is
//! what makes `SETTLE` as loose as it is.

#![cfg(feature = "handoff")]

use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use groupnet_consistency::hosted::hosted_feed_name;
use groupnet_consistency::{
    CAP_HOSTED, Commit, CommitLedger, CommitOutcome, Frontier, FrontierView, Handoff,
    HandoffReceipt, HostedRead, HostedReads, HostedWrites, Snapshot, SnapshotChunks, SnapshotSink,
    SnapshotSource, Watermarks, WriteToken, advertised_head_named,
};
use groupnet_core::{Activation, HostedConfig, NodeId, VoterRoster, placement};
use groupnet_runtime::{Group, GroupProfile, Leadership, Node, Role};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::{MemBulkNet, MemBulkTransport, MemTransport, Network};
use tokio::task::JoinHandle;

/// The poll budget for every assertion here: a storage-free first election plus
/// a gossip round or two. A genuine regression still reports in seconds.
const SETTLE: Duration = Duration::from_secs(10);

/// A brisk gossip cadence, so grant rounds and ledger republishes happen in
/// wall-clock milliseconds.
const GOSSIP_MS: u64 = 15;

/// A host's authority after its last confirmed renewal, and — storage-free —
/// also the boot blackout before any first epoch can close.
const LEASE_MS: u64 = 600;

/// The hosted feed's ring. **Tiny on purpose**: it is the whole subject of the
/// file, and the burst below has to turn it over several times so that what
/// falls off is genuinely unreachable by replay.
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
/// claim guard reads, so the first entry is the node that will bid and the
/// **last** is one that can never claim while anything above it lives.
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

/// A single-writer target, in the map shape both ends of a handoff speak.
fn need_of(writer: &NodeId, token: WriteToken) -> Watermarks {
    [(writer.clone(), token)].into_iter().collect()
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
    ///
    /// One chunk per key: small, so the transfer is genuinely multi-frame, and
    /// boundaries that mean nothing on the wire — which is the point.
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

/// What a follower loop was handed, in arrival order — enough to assert that a
/// resumed stream is contiguous and that nothing gapped twice.
#[derive(Debug, Default)]
struct Trace {
    gaps: Vec<WriteToken>,
    wrote: Vec<WriteToken>,
}

/// One node: the group, its commit ledger, its frontier, its replica, its
/// follower loop, its write path, and its data-plane endpoint.
struct Voter {
    id: NodeId,
    _node: Node<MemTransport>,
    group: Group,
    ledger: Arc<CommitLedger>,
    writes: HostedWrites<String>,
    replica: Arc<Mutex<Replica>>,
    frontier: Arc<Frontier>,
    view: FrontierView,
    plane: Option<DataPlane<MemBulkTransport>>,
    apply: Option<JoinHandle<()>>,
    serving: Option<JoinHandle<()>>,
    trace: Arc<Mutex<Trace>>,
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
            _node: node,
            group: handle,
            ledger,
            writes,
            replica: Arc::default(),
            frontier: Arc::new(frontier),
            view,
            plane: None,
            apply: None,
            serving: None,
            trace: Arc::default(),
        }
    }

    fn plane(&self) -> &DataPlane<MemBulkTransport> {
        self.plane
            .as_ref()
            .expect("this voter joined the data plane")
    }

    /// The follower loop the deployment contract asks of every voter, in the
    /// shape a consumer **without a store** must write it: apply, then record —
    /// and while a hole is open, apply and record *nothing*.
    fn follow(&mut self) {
        assert!(self.apply.is_none(), "already following");
        let mut reads = HostedReads::new(self.group.clone(), self.id.clone(), decode);
        self.writes.bind(&mut reads);
        let ledger = Arc::clone(&self.ledger);
        let replica = Arc::clone(&self.replica);
        let frontier = Arc::clone(&self.frontier);
        let trace = Arc::clone(&self.trace);
        self.apply = Some(tokio::spawn(async move {
            let mut host: Option<NodeId> = None;
            while let Some(event) = reads.next().await {
                match event {
                    HostedRead::Wrote {
                        host: writer,
                        token,
                        key,
                    } => {
                        trace.lock().expect("trace").wrote.push(token);
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
                        trace.lock().expect("trace").gaps.push(missed_through);
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
        let handoff = Handoff::new(self.group.clone(), self.id.clone());
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
            .writes
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
        Handoff::seed(receipt, &self.ledger, &self.frontier).await;
    }

    /// This node's replica as a comparable image: key -> the token that wrote it.
    fn keys(&self) -> BTreeMap<String, WriteToken> {
        self.replica.lock().expect("replica").keys.clone()
    }

    fn has_hole(&self) -> bool {
        self.replica.lock().expect("replica").hole
    }

    fn trace(&self) -> Trace {
        let trace = self.trace.lock().expect("trace");
        Trace {
            gaps: trace.gaps.clone(),
            wrote: trace.wrote.clone(),
        }
    }
}

/// Brings `ids` up as an all-to-all cluster on `net`, each under the Quorum
/// profile over `voters`, none of them following yet.
fn spawn_roster(net: &Network, group: &str, ids: &[&str], voters: &[&str]) -> Vec<Voter> {
    ids.iter()
        .map(|id| {
            let seeds: Vec<&str> = ids.iter().copied().filter(|other| other != id).collect();
            Voter::spawn(net, group, id, &seeds, voters)
        })
        .collect()
}

/// The leadership every node agrees on, or `None` while it is still settling —
/// asserted as one indivisible predicate so a poll cannot catch half of it.
fn agreed(voters: &[Voter]) -> Option<Leadership> {
    let first = voters.first()?.group.leadership();
    first.host.as_ref()?;
    let all: Vec<Leadership> = voters.iter().map(|v| v.group.leadership()).collect();
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
    let groups: Vec<&Group> = voters.iter().map(|v| &v.group).collect();
    converged_within(&groups, SETTLE).await;
    let count = voters.len();
    eventually_within("every voter to see the whole hosted roster", SETTLE, || {
        groups
            .iter()
            .all(|g| g.members_with_capability(CAP_HOSTED).len() == count)
    })
    .await;
    drop(groups);
    eventually_within("the roster to close an epoch", SETTLE, || {
        agreed(voters).is_some()
    })
    .await;
    let lead = agreed(voters).expect("agreed just above");
    let host = lead.host.clone().expect("agreement requires a named host");
    let index = voters
        .iter()
        .position(|v| v.id == host)
        .expect("the host is one of ours");
    eventually_within("the host to finish recovering", SETTLE, || {
        voters[index].writes.fence().is_some()
    })
    .await;
    (lead, index)
}

// ---------------------------------------------------------------------------
// The scenario.
// ---------------------------------------------------------------------------

/// A node that arrives after the ring has turned over is told the truth — one
/// `Gap` naming history that is simply gone — pulls a covering snapshot from the
/// host it names, and then **resumes**: the live writes behind it arrive in
/// order, with no second `Gap`, and its final state is the donor's key for key.
#[tokio::test]
async fn a_laggard_beyond_the_ring_resumes_after_a_handoff() {
    const GROUP: &str = "handoff-resync";
    const IDS: [&str; 4] = ["hr-a", "hr-b", "hr-c", "hr-d"];
    /// Four times the ring: whatever window the joiner ends up seeing, most of
    /// the history is unreachable by replay.
    const BURST: usize = 12;
    /// The live writes the resume is asserted over. Each is awaited before the
    /// next is authored, so the ring cannot overflow underneath them — a second
    /// gap here would be a regression, never a sizing artefact.
    const LIVE: u64 = 3;
    const { assert!(BURST > RING, "the burst must turn the ring over") };

    let net = Network::new();
    let bulk = MemBulkNet::new();
    // Rendezvous order: the three voters take the top ranks and the joiner the
    // last, so its arrival can never move the hostship this test is written
    // around.
    let rank = ranked(GROUP, &IDS);
    let order: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    let (voter_ids, joiner_id) = (&order[..3], order[3]);
    let mut voters = spawn_roster(&net, GROUP, voter_ids, voter_ids);
    let (lead, host) = elected(&mut voters).await;
    assert_eq!(
        host, 0,
        "the top-ranked live candidate is the one that bids"
    );
    for voter in &mut voters {
        voter.join_data_plane(&bulk);
    }
    let host_id = voters[host].id.clone();
    let mut head = WriteToken { epoch: 0, seq: 0 };
    for n in 0..BURST {
        head = voters[host].author(&format!("pre-{n}")).await;
    }

    // --- the late joiner, and the honest Gap it is met with ---
    let mut joiner = Voter::spawn(&net, GROUP, joiner_id, voter_ids, voter_ids);
    joiner.follow();
    joiner.join_data_plane(&bulk);
    arrive_late(&joiner, &host_id, lead.epoch, head, BURST).await;

    // --- the remediation: donors(), fetch, seed ---
    let receipt = resync(&joiner, &host_id, head).await;
    assert_eq!(receipt.fence_epoch, lead.epoch);
    assert_eq!(receipt.fence_host, Some(host_id.clone()));
    joiner.adopt(&receipt).await;
    assert_eq!(
        joiner.keys(),
        voters[host].keys(),
        "the sink installed the donor's image, key for key"
    );
    assert_eq!(joiner.ledger.applied(&host_id), Some(head));
    assert!(
        tokio::time::timeout(SETTLE, joiner.view.reached(&host_id, head))
            .await
            .expect("the frontier is already past the covered head"),
        "a barrier on the covered head holds the moment the receipt is folded"
    );

    // --- and now it resumes ---
    let last = resume(&joiner, &voters[host], head, LIVE).await;
    assert_eq!(
        joiner.keys(),
        voters[host].keys(),
        "and it ends where the host is"
    );
    assert_eq!(joiner.ledger.applied(&host_id), Some(last));
}

/// What a node that missed the burst is met with: exactly one `Gap`, naming
/// history the ring no longer holds, and a replica it will not publish claims
/// about.
async fn arrive_late(joiner: &Voter, host_id: &NodeId, epoch: u64, head: WriteToken, burst: usize) {
    eventually_within("the joiner to be told history is gone", SETTLE, || {
        joiner.has_hole()
    })
    .await;
    let gaps = joiner.trace().gaps;
    assert_eq!(gaps.len(), 1, "exactly one, and it is the lineage opening");
    let gap = gaps[0];
    assert_eq!(gap.epoch, epoch, "the gap belongs to the live lineage");
    assert!(
        gap.seq > 0 && gap.seq < head.seq,
        "the ring holds a window and the gap names everything below it: {gap:?} \
         against a head of {head:?}"
    );
    assert_eq!(
        joiner.ledger.applied(host_id),
        None,
        "it has applied the visible tail and claims none of it — the top of a \
         lineage asserts coverage of everything below it"
    );
    assert!(joiner.keys().len() < burst, "and its replica is short");
}

/// Picks a donor the way a consumer would — `donors()` over the target the
/// laggard is short of, read out of its own gossiped view of the host's feed —
/// and pulls the snapshot from it.
async fn resync(joiner: &Voter, host_id: &NodeId, head: WriteToken) -> HandoffReceipt {
    eventually_within(
        "the joiner to see the host's advertised head",
        SETTLE,
        || advertised_head_named(&hosted_feed_name(""), &joiner.group, host_id) == Some(head),
    )
    .await;
    let need = need_of(host_id, head);
    let handoff = Handoff::new(joiner.group.clone(), joiner.id.clone());
    eventually_within("a covering donor to be visible in gossip", SETTLE, || {
        !handoff.donors(&need).is_empty()
    })
    .await;
    let donors = handoff.donors(&need);
    assert_eq!(
        donors.first(),
        Some(host_id),
        "the serving host is asked first: its state is the one state \
         definitionally survivable — {donors:?}"
    );
    assert!(
        !donors.contains(&joiner.id),
        "and the requester is not among them: the self-filter takes it out by \
         construction, so nobody ever connects to their own endpoint to wait \
         for an offer only their own accept loop could write"
    );
    let sink = ReplicaSink {
        replica: Arc::clone(&joiner.replica),
        staged: Vec::new(),
    };
    let receipt = handoff
        .fetch(joiner.plane(), &donors[0], &need, sink)
        .await
        .expect("a covering donor at an agreeing fence");
    assert!(
        receipt
            .covers
            .get(host_id)
            .is_some_and(|covered| *covered >= head),
        "the receipt covers what was needed: {:?}",
        receipt.covers
    );
    receipt
}

/// The live stream behind the transfer: `live` writes, each awaited, arriving
/// **in order and without a second `Gap`**. Returns the last token.
async fn resume(joiner: &Voter, host: &Voter, head: WriteToken, live: u64) -> WriteToken {
    let mark = joiner.trace().wrote.len();
    let mut last = head;
    for n in 0..live {
        last = host.author(&format!("live-{n}")).await;
        eventually_within(
            "the joiner to apply and claim the live write",
            SETTLE,
            || joiner.ledger.applied(&host.id) == Some(last),
        )
        .await;
    }
    // The **total**, not a delta taken at this function's entry: a count that
    // only has to match a snapshot taken after the transfer would pass on a
    // build that gapped again *during* it, which is the window this whole file
    // holds open. One gap for the whole run, and it is the arrival gap.
    assert_eq!(
        joiner.trace().gaps.len(),
        1,
        "the arrival `Gap` is still the only one this node has ever been \
         handed: it had already positioned the cursor and the handoff filled \
         the state behind it — there was never anything left to miss"
    );
    let expected: Vec<WriteToken> = (1..=live)
        .map(|n| WriteToken {
            epoch: head.epoch,
            seq: head.seq + n,
        })
        .collect();
    assert_eq!(
        joiner.trace().wrote[mark..],
        expected[..],
        "contiguous, in order, from where it stood"
    );
    last
}
