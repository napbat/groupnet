//! Snapshot handoff (feature `handoff`) where it meets the **fence**: the two
//! staleness re-verifications, and the donor choice that reads the group's
//! published ledgers.
//!
//! The sibling file `handoff.rs` drives the same protocol over a plain group,
//! where both ends stamp `(0, None)` and no election is ever waited on. Split
//! from it by *harness family*, the way `groupnet-sim`'s `election_quorum*` and
//! this crate's `hosted_dst*` suites split — each file carrying its own copy of
//! the source and sink, and asserting the properties its own harness earns.
//! This one earns four:
//!
//! * a donor whose fence is behind ours is refused at the **offer**, before a
//!   chunk is applied;
//! * a donor whose fence falls behind **while the snapshot streams** is refused
//!   at the **terminator** — the second re-verification, and the reason there is
//!   one;
//! * a donor whose *own* leadership moves mid-stream stamps the **new** pair
//!   into that terminator rather than the one it offered under, and is refused
//!   on it — the same re-read seen from the donor's side;
//! * `donors()` asks the serving host first, leaves out whoever's published
//!   reading is short, and never names the caller.
//!
//! # How a donor is held still — or moved on its own
//!
//! The requester runs in a real `Quorum` group that closes real epochs. The
//! donor sits on its **own control-plane fabric**, with nobody else on it,
//! joined to a group of the same name under the same roster — a majority of
//! which it can never reach, so it adopts nothing and stamps `(0, None)` for as
//! long as it lives. One shared `MemBulkNet` carries the transfer between them,
//! which is exactly the shape worth testing: the two planes ride different
//! sockets, and a donor can be perfectly reachable for bulk while its gossip is
//! gone.
//!
//! The third test wants the opposite — a donor whose view *moves* while it
//! streams — and gets it from the same device with one knob turned. The same
//! lone node under `Activation::Settle` is the engine's legitimate
//! **side-of-one**: nothing challenges its claim, so it activates a settle
//! window later and takes a hostship the requester's cluster will never agree
//! with. Same fabric-of-one, opposite behaviour, no freezing and no killing.
//!
//! Freezing an *in-cluster* node instead does not work, and it is worth saying
//! why rather than leaving the next reader to find out. Evicting a node's
//! endpoint stops it receiving but not sending, so it goes on probing, suspects
//! everyone, decides it is the top-ranked live candidate and starts claiming —
//! pushing the very epochs the test is trying to hold still. Killing it outright
//! takes its `Group` handle, and its beliefs, with it. A node that can reach
//! nobody is the quiet, honest version of both, and it is a real deployment
//! shape: the minority side of a partition that has lasted since boot.
//!
//! Every wait is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep. The groups run the **storage-free** Quorum posture, so every
//! first election is charged the engine's boot blackout — which is why `SETTLE`
//! is as loose as it is, and is also the window one test below opens a transfer
//! inside of.

#![cfg(feature = "handoff")]

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncWrite};
use groupnet_consistency::hosted::commit_reading;
use groupnet_consistency::{
    CommitLedger, Handoff, HandoffError, Snapshot, SnapshotChunks, SnapshotSink, SnapshotSource,
    Watermarks, WriteToken,
};
use groupnet_core::{Activation, HostedConfig, NodeId, VoterRoster};
use groupnet_runtime::{Group, GroupProfile, Leadership, Node, Role};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
use groupnet_transport::bulk::{DataPlane, DataStream};
use groupnet_transport_mem::{MemBulkNet, MemTransport, Network};
use tokio::sync::watch;

/// The poll budget for every assertion here. As loose as `hosted_migration`'s
/// and for the same reason: the longest chain in this file is a storage-free
/// first election — a boot blackout, a grant round, an agreed pair. A genuine
/// regression still reports in seconds.
const SETTLE: Duration = Duration::from_secs(10);

/// A brisk gossip cadence, so convergence and election happen in wall-clock
/// milliseconds.
const GOSSIP_MS: u64 = 15;

/// A host's authority after its last confirmed renewal, and — storage-free —
/// also the boot blackout before any first epoch can close. The transfer in
/// [`a_donor_whose_fence_falls_behind_mid_stream_is_refused_at_the_terminator`]
/// is opened inside that window, and opening it takes microseconds.
const LEASE_MS: u64 = 600;

/// How long a `Settle` claim must stand before its claimant activates — used
/// only by [`lone_settle_profile`], and sized to be comfortably longer than
/// opening a stream and sending one chunk takes.
const CLAIM_SETTLE_MS: u64 = 400;

/// The `(writer, token)` map spelling every test here uses.
fn marks(pairs: &[(&str, u64, u64)]) -> Watermarks {
    pairs
        .iter()
        .map(|(writer, epoch, seq)| {
            (
                NodeId::new(*writer),
                WriteToken {
                    epoch: *epoch,
                    seq: *seq,
                },
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The consumer's two halves. `handoff.rs` carries its own copy, with the
// counters that file's assertions need and this one's do not.
// ---------------------------------------------------------------------------

/// A snapshot source over a fixed list of chunks, with the knob these tests
/// need: a **gate** the chunk stream parks on partway through, so the fence can
/// be moved with a transfer held open across it.
struct TestSource {
    covers: Watermarks,
    chunks: Vec<Bytes>,
    /// Park once this many chunks have been handed over, until the gate opens.
    /// `None` never parks.
    gate: Option<(usize, watch::Receiver<bool>)>,
}

impl TestSource {
    fn new(covers: Watermarks, chunks: Vec<Bytes>) -> Self {
        Self {
            covers,
            chunks,
            gate: None,
        }
    }

    /// Parks the chunk stream after `after` chunks until `gate` goes `true`.
    fn gated_after(mut self, after: usize, gate: watch::Receiver<bool>) -> Self {
        self.gate = Some((after, gate));
        self
    }
}

impl SnapshotSource for TestSource {
    type Chunks = TestChunks;

    async fn open(&self) -> io::Result<Snapshot<TestChunks>> {
        Ok(Snapshot {
            covers: self.covers.clone(),
            chunks: TestChunks {
                remaining: self.chunks.clone().into_iter(),
                handed: 0,
                gate: self.gate.clone(),
            },
        })
    }
}

/// One opened image's chunks.
struct TestChunks {
    remaining: std::vec::IntoIter<Bytes>,
    handed: usize,
    gate: Option<(usize, watch::Receiver<bool>)>,
}

impl SnapshotChunks for TestChunks {
    async fn next(&mut self) -> io::Result<Option<Bytes>> {
        if let Some((after, gate)) = &mut self.gate {
            if self.handed == *after {
                // The borrow of the watch ends with the condition, so nothing is
                // held across the await — `hosted_migration.rs`'s gate, verbatim.
                while !*gate.borrow_and_update() {
                    if gate.changed().await.is_err() {
                        return Err(io::Error::new(io::ErrorKind::BrokenPipe, "gate dropped"));
                    }
                }
            }
        }
        self.handed += 1;
        Ok(self.remaining.next())
    }
}

/// What a [`TestSink`] did, as the test that handed it over can still see.
#[derive(Debug, Default)]
struct SinkState {
    /// Chunks handed to `apply`, whether or not they were ever installed.
    staged: usize,
    /// Whether `finish` adopted anything. `false` is **nothing installed**,
    /// which is exactly what a dropped, unfinished sink must leave behind.
    installed: bool,
}

/// The handle a test keeps after handing its sink to the driver — the sink
/// itself is consumed, and that is the point.
#[derive(Clone, Debug, Default)]
struct SinkProbe(Arc<Mutex<SinkState>>);

impl SinkProbe {
    fn sink(&self) -> TestSink {
        TestSink {
            state: Arc::clone(&self.0),
            pending: Vec::new(),
        }
    }

    fn staged(&self) -> usize {
        self.0.lock().expect("sink state").staged
    }

    /// Nothing adopted: the state **every** failure path must leave behind,
    /// whatever crossed the link first.
    fn installed_nothing(&self) -> bool {
        !self.0.lock().expect("sink state").installed
    }
}

/// Stages into a scratch buffer of its own and publishes it only from `finish`
/// — the consumer contract this tier documents, written the way the docs say to
/// write it, so "dropped without finishing" is observably a no-op.
struct TestSink {
    state: Arc<Mutex<SinkState>>,
    pending: Vec<u8>,
}

impl SnapshotSink for TestSink {
    async fn apply(&mut self, chunk: Bytes) -> io::Result<()> {
        self.pending.extend_from_slice(&chunk);
        self.state.lock().expect("sink state").staged += 1;
        Ok(())
    }

    async fn finish(self) -> io::Result<()> {
        self.state.lock().expect("sink state").installed = true;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The harness: a real Quorum group, and a donor outside every epoch it closes.
// ---------------------------------------------------------------------------

/// One connected pair of framed streams on `net`: `(at from, at to)`.
async fn linked(
    net: &MemBulkNet,
    from: &NodeId,
    to: &NodeId,
) -> (
    DataStream<impl AsyncRead + AsyncWrite + Send + Unpin + 'static>,
    DataStream<impl AsyncRead + AsyncWrite + Send + Unpin + 'static>,
) {
    let out = DataPlane::new(net.endpoint(from.clone()));
    let inbound = DataPlane::new(net.endpoint(to.clone()));
    let opened = out.connect(to).await.expect("the data plane connects");
    let (who, accepted) = inbound.accept().await.expect("the peer accepts");
    assert_eq!(&who, from, "the accept attributes the connector");
    (opened, accepted)
}

/// A Quorum profile over `voters`, storage-free — so every first election is
/// charged the boot blackout.
fn quorum_profile(voters: &[&str]) -> GroupProfile {
    GroupProfile::hosted(HostedConfig {
        activation: Activation::Quorum {
            voters: VoterRoster::new(voters.iter().map(|v| NodeId::new(*v))),
        },
        lease_ms: LEASE_MS,
    })
}

/// A `Settle` profile for a node that is **alone** on its own fabric: the
/// legitimate side-of-one the election's own docs describe. It is the
/// top-ranked live candidate of a group of one, so it claims `highest_seen + 1`
/// and — with nobody to challenge it — activates a settle window later, taking
/// a hostship no other cluster's view can agree with.
///
/// `CLAIM_SETTLE_MS` is wide on purpose: one boot guard plus one settle window
/// is the margin
/// [`a_donor_that_takes_a_hostship_mid_stream_stamps_it_into_the_terminator`]
/// gets between spawning this node and taking its offer, and that offer must be
/// stamped from the hostless view it starts in.
fn lone_settle_profile() -> GroupProfile {
    GroupProfile::hosted(HostedConfig {
        activation: Activation::Settle {
            claim_settle_ms: CLAIM_SETTLE_MS,
        },
        lease_ms: LEASE_MS,
    })
}

/// One node of a Quorum-activated hosted group: as much of
/// `hosted_migration.rs`'s `Voter` as these tests need, and no more — they want
/// epochs, not a write path.
struct Cell {
    id: NodeId,
    _node: Node<MemTransport>,
    group: Group,
}

impl Cell {
    fn epoch(&self) -> u64 {
        self.group.leadership().epoch
    }
}

/// Brings `ids` up as an all-to-all Quorum cluster on `net`. **Nothing is
/// elected yet**: the storage-free posture charges the first epoch a boot
/// blackout, and one test below opens a transfer inside that window on purpose.
fn quorum_cluster(net: &Network, group: &str, ids: &[&str]) -> Vec<Cell> {
    ids.iter()
        .map(|id| {
            let seeds: Vec<&str> = ids.iter().copied().filter(|other| other != id).collect();
            let opts = NodeOpts::new(group)
                .gossip_interval_ms(GOSSIP_MS)
                .group_profile(quorum_profile(ids));
            let (id, node, group) = spawn_mem_node(net, id, &seeds, &opts);
            Cell {
                id,
                _node: node,
                group,
            }
        })
        .collect()
}

/// The leadership every node agrees on, or `None` while it is still settling:
/// the same `(epoch, host)` everywhere and exactly one [`Role::Host`], asserted
/// as one indivisible predicate so a poll cannot catch half of it.
fn agreed(cells: &[Cell]) -> Option<Leadership> {
    let first = cells.first()?.group.leadership();
    first.host.as_ref()?;
    let all: Vec<Leadership> = cells.iter().map(|cell| cell.group.leadership()).collect();
    if all
        .iter()
        .any(|lead| lead.epoch != first.epoch || lead.host != first.host)
    {
        return None;
    }
    (all.iter().filter(|lead| lead.role == Role::Host).count() == 1).then_some(first)
}

/// Waits for one epoch to close with everybody agreeing on it, and reports it
/// with the host's index.
async fn elected(cells: &[Cell]) -> (Leadership, usize) {
    let groups: Vec<&Group> = cells.iter().map(|cell| &cell.group).collect();
    converged_within(&groups, SETTLE).await;
    drop(groups);
    eventually_within("the roster to close an epoch", SETTLE, || {
        agreed(cells).is_some()
    })
    .await;
    let lead = agreed(cells).expect("agreed just above");
    let host = lead.host.clone().expect("agreement requires a named host");
    let index = cells
        .iter()
        .position(|cell| cell.id == host)
        .expect("the host is one of ours");
    (lead, index)
}

/// A node alone on a control-plane fabric of its own, sharing only the group's
/// *name* with the requester's cluster — see the module docs for why it is built
/// this way and not by freezing a member.
///
/// What its view then does is the `profile`'s business, and both answers are
/// used here: under [`quorum_profile`] it can never reach a majority, so it is
/// **pinned** at `(0, None)` for as long as it lives; under
/// [`lone_settle_profile`] it is a side-of-one that closes its own epoch and
/// takes a hostship the requester's cluster will never agree with.
struct Stranger {
    _net: Network,
    _node: Node<MemTransport>,
    id: NodeId,
    group: Group,
}

impl Stranger {
    fn leadership(&self) -> (u64, Option<NodeId>) {
        let lead = self.group.leadership();
        (lead.epoch, lead.host)
    }
}

fn stranger(group: &str, id: &str, profile: GroupProfile) -> Stranger {
    let net = Network::new();
    let opts = NodeOpts::new(group)
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(profile);
    let (id, node, handle) = spawn_mem_node(&net, id, &[], &opts);
    assert_eq!(
        handle.leadership().epoch,
        0,
        "a node that has just booted has adopted nothing"
    );
    Stranger {
        _net: net,
        _node: node,
        id,
        group: handle,
    }
}

// ---------------------------------------------------------------------------
// 1. A donor whose fence is behind, refused at the offer.
// ---------------------------------------------------------------------------

/// This donor holds a perfectly good, fully covering snapshot, and the only
/// thing wrong with it is *which view its state belongs to*. The offer's fence
/// stamp is what says so, and it says so before a chunk is applied.
#[tokio::test]
async fn a_donor_whose_fence_is_behind_is_refused_at_the_offer() {
    const GROUP: &str = "handoff-stale";
    const IDS: [&str; 3] = ["hs-a", "hs-b", "hs-c"];

    let net = Network::new();
    let bulk = MemBulkNet::new();
    let cells = quorum_cluster(&net, GROUP, &IDS);
    let (lead, _host) = elected(&cells).await;
    assert!(lead.epoch >= 1, "an epoch closed: {lead:?}");
    let donor = stranger(GROUP, "hs-stranger", quorum_profile(&IDS));
    let requester = &cells[0];

    // Covering outright: nothing about this donor's *state* is short.
    let need = marks(&[("w-a", 9, 9)]);
    let source = TestSource::new(
        need.clone(),
        vec![Bytes::from_static(b"state from a view nobody survives")],
    );
    let sink = SinkProbe::default();
    let (mut to_donor, mut at_donor) = linked(&bulk, &requester.id, &donor.id).await;
    let serving = Handoff::new(donor.group.clone(), donor.id.clone());
    let served = tokio::spawn(async move { serving.offer(&mut at_donor, &source).await });

    let err = Handoff::new(requester.group.clone(), requester.id.clone())
        .fetch_on(&mut to_donor, &need, sink.sink())
        .await
        .expect_err("a donor behind our fence must not be adopted");
    match err {
        HandoffError::StaleDonor {
            donor_epoch,
            donor_host,
            adopted_epoch,
            ..
        } => {
            assert_eq!(donor_epoch, 0, "the donor stamped what it had adopted");
            assert_eq!(donor_host, None, "hostless, and honestly so");
            assert_eq!(adopted_epoch, requester.epoch());
            assert!(
                adopted_epoch > donor_epoch,
                "we hold a strictly higher fence: {adopted_epoch} vs {donor_epoch}"
            );
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        sink.staged(),
        0,
        "refused at the offer: nothing was applied"
    );
    assert!(sink.installed_nothing());
    // Whether the donor got its whole snapshot into the pipe before we hung up
    // is a race, not a property; only the requester's verdict is one.
    served.abort();
}

// ---------------------------------------------------------------------------
// 2. A donor whose fence falls behind mid-stream, refused at the terminator.
// ---------------------------------------------------------------------------

/// The re-verification the whole design turns on. Both ends agree on the fence
/// when the offer is taken — the requester's group has closed no epoch yet — and
/// the requester's view moves past the donor's **while the snapshot is still
/// streaming**. Nothing about the offer was wrong, and checking only there would
/// install exactly the state a surviving host has already ruled out. The `Done`
/// frame's re-read stamp, checked against a re-read `leadership()`, is what
/// refuses it.
///
/// That the offer really was accepted at the shared fence needs no separate
/// assertion: a chunk is only ever staged behind an offer that passed both
/// checks, so the first staged chunk *is* the proof — and if an epoch had beaten
/// the transfer, that wait would fail loudly rather than pass for the wrong
/// reason.
#[tokio::test]
async fn a_donor_whose_fence_falls_behind_mid_stream_is_refused_at_the_terminator() {
    const GROUP: &str = "handoff-deposed";
    const IDS: [&str; 3] = ["hd-a", "hd-b", "hd-c"];

    let net = Network::new();
    let bulk = MemBulkNet::new();
    // Not awaited: the transfer below opens inside the boot blackout, while the
    // requester's group is still hostless at epoch 0.
    let cells = quorum_cluster(&net, GROUP, &IDS);
    let donor = stranger(GROUP, "hd-stranger", quorum_profile(&IDS));
    let requester = &cells[0];
    assert_eq!(requester.epoch(), 0, "no epoch has closed yet");

    let gate = watch::channel(false).0;
    let need = marks(&[("w-a", 9, 9)]);
    let source = TestSource::new(
        need.clone(),
        (0..4u8).map(|n| Bytes::from(vec![n; 32])).collect(),
    )
    .gated_after(1, gate.subscribe());
    let sink = SinkProbe::default();

    let (mut to_donor, mut at_donor) = linked(&bulk, &requester.id, &donor.id).await;
    let serving = Handoff::new(donor.group.clone(), donor.id.clone());
    let served = tokio::spawn(async move { serving.offer(&mut at_donor, &source).await });
    let puller = Handoff::new(requester.group.clone(), requester.id.clone());
    let staging = sink.clone();
    let fetched =
        tokio::spawn(async move { puller.fetch_on(&mut to_donor, &need, staging.sink()).await });

    eventually_within("the first chunk to be staged", SETTLE, || {
        sink.staged() == 1
    })
    .await;

    // --- and now the fence moves, with the transfer held open across it ---
    let (lead, _host) = elected(&cells).await;
    assert!(lead.epoch >= 1);
    gate.send_replace(true);

    let err = fetched
        .await
        .expect("the requester task")
        .expect_err("a snapshot stamped under a superseded view must not install");
    match err {
        HandoffError::StaleDonor {
            donor_epoch,
            adopted_epoch,
            ..
        } => {
            assert_eq!(
                donor_epoch, 0,
                "the terminator carries what the donor believed at the *end*"
            );
            assert!(adopted_epoch >= lead.epoch, "and we have moved past it");
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        sink.staged(),
        4,
        "the whole snapshot crossed and was staged..."
    );
    assert!(
        sink.installed_nothing(),
        "...and none of it was installed: the sink was dropped unfinished"
    );
    let _ = served.await;
}

// ---------------------------------------------------------------------------
// 3. The donor's own view moves mid-stream, and it says so in the terminator.
// ---------------------------------------------------------------------------

/// The other half of the same re-verification, and the half only the **donor**
/// can get wrong: the `Done` frame's stamp is re-read at the end of the
/// transfer, so a donor whose leadership moved while it was streaming puts the
/// **new** pair on the wire rather than the one it offered under. The requester
/// then refuses it.
///
/// The device is the mid-stream test's, pointed the other way. Both ends start
/// hostless at epoch 0 and agree about it, which is what lets the offer pass;
/// then, with a gated chunk stream holding the transfer open, the *donor* — a
/// side-of-one on its own fabric — closes its own epoch and takes itself as
/// host, while the requester's cluster closes its first epoch under a host of
/// its own.
///
/// The requester must move too, and that is not incidental: a donor stamped
/// *above* us is `Ok` by design (it is the better-informed party), so the only
/// way a donor's own advance can be refused at all is for the requester to hold
/// a pair at or above it — here the same epoch under a different named host,
/// which is the one same-epoch contradiction the core refuses. What this test
/// pins that its sibling cannot is the *source* of the offending stamp: it is
/// the donor's re-read, not the offer's, and the two differ.
#[tokio::test]
async fn a_donor_that_takes_a_hostship_mid_stream_stamps_it_into_the_terminator() {
    const GROUP: &str = "handoff-restamp";
    const IDS: [&str; 3] = ["hb-a", "hb-b", "hb-c"];

    let net = Network::new();
    let bulk = MemBulkNet::new();
    // Not awaited: the transfer below opens inside the boot blackout, while the
    // requester's group is still hostless at epoch 0.
    let cells = quorum_cluster(&net, GROUP, &IDS);
    let donor = stranger(GROUP, "hb-donor", lone_settle_profile());
    let requester = &cells[0];
    assert_eq!(requester.epoch(), 0, "no epoch has closed yet");

    let gate = watch::channel(false).0;
    let need = marks(&[("w-a", 9, 9)]);
    let source = TestSource::new(
        need.clone(),
        (0..4u8).map(|n| Bytes::from(vec![n; 32])).collect(),
    )
    .gated_after(1, gate.subscribe());
    let sink = SinkProbe::default();

    let (mut to_donor, mut at_donor) = linked(&bulk, &requester.id, &donor.id).await;
    let serving = Handoff::new(donor.group.clone(), donor.id.clone());
    let served = tokio::spawn(async move { serving.offer(&mut at_donor, &source).await });
    let puller = Handoff::new(requester.group.clone(), requester.id.clone());
    let staging = sink.clone();
    let fetched =
        tokio::spawn(async move { puller.fetch_on(&mut to_donor, &need, staging.sink()).await });

    eventually_within("the first chunk to be staged", SETTLE, || {
        sink.staged() == 1
    })
    .await;
    // A chunk is only ever staged behind an offer that passed both checks, and
    // leadership is monotone — so reading the donor hostless at epoch 0 *now* is
    // proof that is what its `Offer` carried, and that the requester agreed.
    assert_eq!(
        donor.leadership(),
        (0, None),
        "the offer was stamped from a view the requester shared"
    );

    // --- and now the *donor's* view moves, with the transfer held open ---
    eventually_within(
        "the donor's side-of-one to take its own group",
        SETTLE,
        || donor.leadership().1.as_ref() == Some(&donor.id),
    )
    .await;
    let (lead, _host) = elected(&cells).await;
    assert!(lead.epoch >= 1);
    gate.send_replace(true);

    let err = fetched
        .await
        .expect("the requester task")
        .expect_err("a donor whose fence our own view contradicts must not install");
    match err {
        HandoffError::StaleDonor {
            donor_epoch,
            donor_host,
            adopted_epoch,
            adopted_host,
        } => {
            assert_eq!(
                donor_host.as_ref(),
                Some(&donor.id),
                "the terminator carries the pair the donor took *during* the \
                 transfer, not the hostless stamp it offered under"
            );
            assert!(donor_epoch >= 1, "and that pair closed a real epoch");
            assert!(
                donor_epoch < adopted_epoch || donor_host != adopted_host,
                "refused because the two views cannot both be right: donor \
                 ({donor_epoch}, {donor_host:?}) against adopted \
                 ({adopted_epoch}, {adopted_host:?})"
            );
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(
        sink.staged(),
        4,
        "the whole snapshot crossed and was staged..."
    );
    assert!(
        sink.installed_nothing(),
        "...and none of it was installed: the sink was dropped unfinished"
    );
    let _ = served.await;
}

// ---------------------------------------------------------------------------
// 4. Choosing a donor.
// ---------------------------------------------------------------------------

/// `donors()` reads the group's **published** commit ledgers: the serving host
/// comes first, a member whose reading does not clear the target is not offered
/// at all, the **caller is never on the list even when its own reading covers**,
/// and a target nobody covers is an empty list rather than a guess.
///
/// The host-first half of the rule is pinned by the unit tests beside the code,
/// against hosts a *healthy* cluster cannot produce — the engine elects the
/// rendezvous-top candidate, so the two orders coincide here by construction.
/// What this adds is that the rule runs against real gossiped readings — and the
/// self-exclusion is asserted here in the one arrangement that can tell it from
/// "a node whose reading covered the target would not be asking": the observer's
/// published reading *does* cover, and it is still not offered itself.
#[tokio::test]
async fn donors_asks_the_host_first_and_leaves_out_whoever_is_short() {
    const GROUP: &str = "handoff-donors";
    const IDS: [&str; 3] = ["dn-a", "dn-b", "dn-c"];

    let net = Network::new();
    let cells = quorum_cluster(&net, GROUP, &IDS);
    let (lead, host) = elected(&cells).await;
    let host_id = lead.host.clone().expect("agreement names a host");
    let short = (0..cells.len())
        .find(|index| *index != host)
        .expect("a follower");
    let observer = (0..cells.len())
        .find(|index| *index != host && *index != short)
        .expect("three nodes");

    // Every member publishes a reading; one of them is short of the target.
    let writer = NodeId::new("w-a");
    let need = marks(&[("w-a", 4, 10)]);
    for (index, cell) in cells.iter().enumerate() {
        let ledger = CommitLedger::new(cell.group.clone());
        let token = if index == short {
            WriteToken { epoch: 4, seq: 1 }
        } else {
            WriteToken { epoch: 4, seq: 10 }
        };
        ledger.record(&writer, token).await;
    }
    let watcher = cells[observer].group.clone();
    eventually_within("every reading to reach the observer's view", SETTLE, || {
        cells
            .iter()
            .all(|cell| commit_reading(&watcher, &cell.id).is_some())
    })
    .await;

    let me = cells[observer].id.clone();
    let handoff = Handoff::new(cells[observer].group.clone(), me.clone());
    // The observer is one of the two members whose reading clears `need` — it
    // published `(4, 10)` like the host did — and it is still not listed. This
    // is the caller asking for something it already has, which is exactly the
    // shape "it would not be asking" does not cover, and the shape that would
    // otherwise send it connecting to its own endpoint.
    assert!(
        commit_reading(&cells[observer].group, &me)
            .is_some_and(|reading| reading.applied == marks(&[("w-a", 4, 10)])),
        "the caller's own published reading covers the target"
    );
    assert_eq!(
        handoff.donors(&need),
        vec![host_id.clone()],
        "the serving host, and neither the short member nor the caller itself"
    );
    assert!(
        !handoff.donors(&need).contains(&cells[short].id),
        "a published-but-short reading is not a donor"
    );
    // A target nobody reaches is nobody, stated plainly rather than by handing
    // back the least-bad option.
    assert!(
        handoff.donors(&marks(&[("w-a", 9, 1)])).is_empty(),
        "nothing covers a target past every reading"
    );
    // ...while asking for nothing is answered by anyone who publishes at all —
    // still host-first, and still not the caller.
    let anyone = handoff.donors(&Watermarks::new());
    assert_eq!(anyone.len(), IDS.len() - 1, "{anyone:?}");
    assert_eq!(anyone.first(), Some(&host_id));
    assert!(
        !anyone.contains(&me),
        "not even for an empty need: {anyone:?}"
    );
}
