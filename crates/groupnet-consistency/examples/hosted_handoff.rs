//! Late-joiner resync: a node arrives after the host has written past its own
//! ring, is told so honestly, and pulls a **covering snapshot** over the data
//! plane instead of guessing.
//!
//! Three nodes form a `Hosted` group with `Activation::Quorum` over all three as
//! voters, and publish a stream of records through [`HostedWrites`] at
//! [`Commit::QuorumApplied`]. The feed's ring is deliberately tiny, so most of
//! that history is gone from gossip within seconds of being written. Then a
//! fourth node joins.
//!
//! What it is met with is the whole point:
//!
//! * **The `Gap` is honest, not apologetic.** The ring cannot replay history it
//!   no longer holds, and the subscriber says exactly that — one `Gap`, naming
//!   the last write it will never see.
//! * **A consumer with no store of its own cannot shrug it off.** This one's
//!   state *is* the groupnet-carried state, so it applies what arrives and
//!   **publishes no watermark at all**: claiming the top of the lineage would
//!   assert coverage of everything below it.
//! * **The remediation is a fenced artifact.** [`Handoff::fetch`] pulls a
//!   covering image from a donor [`Handoff::donors`] chose — the serving host
//!   first — verified against the donor's fence stamp at the offer *and* at the
//!   terminator, and installed only then. [`Handoff::seed`] turns the receipt
//!   into evidence the rest of the group reads.
//! * **And then the stream resumes.** The `Gap` had already positioned the
//!   cursor; the handoff filled the state behind it. The live writes that follow
//!   arrive in order, with no second `Gap`.
//!
//! Two planes, both in-process: `Network` carries gossip, `MemBulkNet` carries
//! the snapshot. Swap them for `groupnet-transport-udp` and
//! `groupnet-transport-tcp` and nothing here changes.
//!
//! ```text
//! cargo run -p groupnet-consistency --example hosted_handoff --features handoff
//! ```

use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use groupnet_consistency::hosted::hosted_feed_name;
use groupnet_consistency::{
    Commit, CommitLedger, Frontier, Handoff, HandoffReceipt, HostedRead, HostedReads, HostedWrites,
    Offered, Snapshot, SnapshotChunks, SnapshotSink, SnapshotSource, Watermarks, WriteToken,
    advertised_head_named,
};
use groupnet_core::{Activation, HostedConfig, NodeId, VoterRoster, placement};
use groupnet_runtime::{Group, GroupProfile, Node, Role};
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::{MemBulkNet, MemBulkTransport, MemTransport, Network};
use tokio::sync::mpsc;

/// The hosted group: one shard's worth of records.
const GROUP: &str = "records";

/// Four machines. The first three by rendezvous rank are the voters; the last is
/// the late joiner, and ranking it last is what keeps it out of the election.
const IDS: [&str; 4] = ["rec-a", "rec-b", "rec-c", "rec-d"];

/// Brisk gossip, so a demo does not spend its life waiting for a grant round.
const GOSSIP_MS: u64 = 15;

/// A host's authority after its last confirmed renewal — and, storage-free, also
/// the blackout a rebooted voter sits out before it will grant a new claimant.
const LEASE_MS: u64 = 600;

/// The hosted feed's ring. **Four writes**, which is absurd for a deployment and
/// exactly right for a demo: it makes the overrun happen in one act instead of
/// after an hour of traffic.
const RING: usize = 4;

/// Records the host writes before the fourth node shows up. Four times the ring,
/// so three quarters of the history is unreachable by replay.
const RECORDS: usize = 16;

/// Records written after the handoff, to show the stream resuming.
const LIVE: usize = 3;

/// The deadline a committed write is given. A bound, not an expectation.
const PATIENT: Duration = Duration::from_secs(2);

/// Polling budget for [`settle`]: 2000 × 5 ms, generous enough for a whole
/// storage-free election.
const POLL: Duration = Duration::from_millis(5);
const POLLS: usize = 2000;

// ---------------------------------------------------------------------------
// The consumer's state: an in-memory index, and nothing behind it.
// ---------------------------------------------------------------------------

/// One node's applied replica. There is no store under this — which is the whole
/// reason the handoff exists, and the reason a `Gap` here is not survivable on
/// its own.
#[derive(Debug, Default)]
struct Replica {
    /// record -> the token that wrote it.
    records: BTreeMap<String, WriteToken>,
    /// Per-writer applied watermarks, read under the **same lock** as `records`:
    /// a snapshot's `covers` must describe the image it ships with, and that is
    /// only true if the two are read together.
    marks: Watermarks,
    /// A `Gap` naming writes this node never applied. While it is set, the node
    /// applies what arrives and claims nothing.
    hole: bool,
}

impl Replica {
    fn apply(&mut self, writer: &NodeId, token: WriteToken, key: String) {
        let slot = self.records.entry(key).or_insert(token);
        *slot = (*slot).max(token);
        let mark = self.marks.entry(writer.clone()).or_insert(token);
        *mark = (*mark).max(token);
    }

    /// The image and what it covers, taken together under one lock. One chunk
    /// per record: chunk boundaries mean nothing on the wire, and sizing them
    /// for the link rather than for the data is the source's business.
    fn image(&self) -> (Watermarks, Vec<Bytes>) {
        let chunks = self
            .records
            .iter()
            .map(|(key, token)| Bytes::from(format!("{key}\t{}\t{}\n", token.epoch, token.seq)))
            .collect();
        (self.marks.clone(), chunks)
    }

    /// Merges a staged image in — the `finish` swap. A merge rather than an
    /// overwrite because the apply loop keeps running across the transfer, and
    /// monotone per record, so an overlapping snapshot changes nothing twice.
    fn install(&mut self, staged: &[u8]) -> io::Result<()> {
        let text = std::str::from_utf8(staged).map_err(io::Error::other)?;
        for line in text.lines() {
            let mut parts = line.split('\t');
            let (Some(key), Some(epoch), Some(seq), None) =
                (parts.next(), parts.next(), parts.next(), parts.next())
            else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad record"));
            };
            let token = WriteToken {
                epoch: epoch.parse().map_err(io::Error::other)?,
                seq: seq.parse().map_err(io::Error::other)?,
            };
            let slot = self.records.entry(key.to_owned()).or_insert(token);
            *slot = (*slot).max(token);
        }
        Ok(())
    }
}

/// The donor half of the data contract.
struct ReplicaSource(Arc<Mutex<Replica>>);

impl SnapshotSource for ReplicaSource {
    type Chunks = ImageChunks;

    async fn open(&self) -> io::Result<Snapshot<ImageChunks>> {
        let (covers, chunks) = self.0.lock().expect("lock").image();
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

/// The requester half: stages into a buffer of its own and touches the replica
/// only from `finish`, so a dropped, unfinished sink discards — which is what
/// every verification failure in this protocol does.
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
        replica.lock().expect("lock").install(&staged)
    }
}

// ---------------------------------------------------------------------------
// One node's whole participation: control plane, data plane, and the loop.
// ---------------------------------------------------------------------------

/// One participant.
struct Member {
    id: NodeId,
    _node: Node<MemTransport>,
    group: Group,
    ledger: Arc<CommitLedger>,
    writes: HostedWrites<String>,
    replica: Arc<Mutex<Replica>>,
    frontier: Arc<Frontier>,
    plane: DataPlane<MemBulkTransport>,
    /// Gaps this node's follower loop has been handed, in arrival order.
    gaps: Arc<Mutex<Vec<WriteToken>>>,
}

impl Member {
    /// Brings one node up on both planes, joined to [`GROUP`] under the Quorum
    /// profile over `voters`, following and serving.
    fn spawn(net: &Network, bulk: &MemBulkNet, id: &str, voters: &[&str]) -> Self {
        let me = NodeId::new(id);
        let mut builder =
            Node::builder(me.clone(), net.endpoint(me.clone())).gossip_interval_ms(GOSSIP_MS);
        for seed in IDS.iter().filter(|other| **other != id) {
            builder = builder.seed(NodeId::new(*seed));
        }
        let node = builder.spawn();
        let roster = VoterRoster::new(voters.iter().map(|v| NodeId::new(*v)));
        let group = node.join_group_with(
            GROUP,
            GroupProfile::hosted(HostedConfig {
                activation: Activation::Quorum { voters: roster },
                lease_ms: LEASE_MS,
            }),
        );
        let ledger = Arc::new(CommitLedger::new(group.clone()));
        let writes = HostedWrites::committed(
            group.clone(),
            me.clone(),
            NonZeroUsize::new(RING).expect("nonzero"),
            |key: &String| key.clone().into_bytes(),
            Arc::clone(&ledger),
        )
        .expect("a Quorum group supports the committed regime");
        let member = Self {
            id: me.clone(),
            _node: node,
            group: group.clone(),
            ledger,
            writes,
            replica: Arc::default(),
            frontier: Arc::new(Frontier::new().0),
            plane: DataPlane::new(bulk.endpoint(me)),
            gaps: Arc::default(),
        };
        member.follow();
        member
    }

    /// The follower loop, in the shape a consumer **without a store** must write
    /// it: apply, then record — and while a hole is open, apply and record
    /// nothing. A watermark asserts coverage of everything below it, and this
    /// node cannot honour that assertion until a transfer lands.
    fn follow(&self) {
        let mut reads = HostedReads::new(self.group.clone(), self.id.clone(), |bytes: &[u8]| {
            String::from_utf8(bytes.to_vec()).ok()
        });
        self.writes.bind(&mut reads);
        let (ledger, replica) = (Arc::clone(&self.ledger), Arc::clone(&self.replica));
        let (frontier, gaps) = (Arc::clone(&self.frontier), Arc::clone(&self.gaps));
        tokio::spawn(async move {
            let mut host: Option<NodeId> = None;
            while let Some(event) = reads.next().await {
                match event {
                    HostedRead::Wrote {
                        host: writer,
                        token,
                        key,
                    } => {
                        let whole = {
                            let mut replica = replica.lock().expect("lock");
                            replica.apply(&writer, token, key);
                            !replica.hole
                        };
                        if whole {
                            frontier.advance(&writer, token);
                            ledger.record(&writer, token).await;
                        }
                    }
                    HostedRead::Gap { missed_through } => {
                        gaps.lock().expect("lock").push(missed_through);
                        // `seq == 0` opens a lineage and missed nothing of it;
                        // anything above names writes the ring no longer holds.
                        if missed_through.seq == 0 {
                            if let Some(host) = &host {
                                frontier.advance(host, missed_through);
                                ledger.record(host, missed_through).await;
                            }
                        } else {
                            replica.lock().expect("lock").hole = true;
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
        });
    }

    /// Answers handoff requests from this node's own replica, reporting each
    /// completed transfer on `served` so the narrative can print it in order.
    fn serve(&self, served: mpsc::UnboundedSender<Offered>) {
        let plane = self.plane.clone();
        let handoff = Handoff::new(self.group.clone(), self.id.clone());
        let source = ReplicaSource(Arc::clone(&self.replica));
        tokio::spawn(async move {
            while let Ok((_from, mut stream)) = plane.accept().await {
                if let Ok(offered) = handoff.offer(&mut stream, &source).await {
                    let _ = served.send(offered);
                }
            }
        });
    }

    /// Publishes one record and applies it locally — a host's own subscriber
    /// excludes its own feed, so nothing ever delivers it its own writes.
    async fn author(&self, key: &str) -> WriteToken {
        let receipt = self
            .writes
            .publish_committed(&key.to_owned(), Commit::QuorumApplied, PATIENT)
            .await
            .expect("the host serves");
        self.replica
            .lock()
            .expect("lock")
            .apply(&self.id, receipt.token, key.to_owned());
        self.frontier.advance(&self.id, receipt.token);
        receipt.token
    }

    fn held(&self) -> usize {
        self.replica.lock().expect("lock").records.len()
    }

    fn gaps(&self) -> Vec<WriteToken> {
        self.gaps.lock().expect("lock").clone()
    }
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let net = Network::new();
    let bulk = MemBulkNet::new();
    // Rendezvous ranking decides who bids; taking the joiner from the bottom of
    // it keeps a late arrival from disturbing a healthy hostship.
    let weighted: Vec<(NodeId, u32)> = IDS.iter().map(|id| (NodeId::new(*id), 1)).collect();
    let rank = placement::owners(GROUP, &weighted, IDS.len());
    let order: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    let (voters, joiner_id) = (&order[..3], order[3]);

    let members: Vec<Member> = voters
        .iter()
        .map(|id| Member::spawn(&net, &bulk, id, voters))
        .collect();
    let (served_tx, mut served_rx) = mpsc::unbounded_channel();
    for member in &members {
        member.serve(served_tx.clone());
    }

    let host = act1_election(&members).await;
    let head = act2_overrun(&members[host]).await;
    let joiner = act3_late(&net, &bulk, joiner_id, voters, &members[host], head).await;
    act4_handoff(&joiner, &members[host], head, &mut served_rx).await;
    act5_resume(&joiner, &members[host], head).await;
    moral();
}

/// An election, and the recovery that gates *service* rather than election.
async fn act1_election(members: &[Member]) -> usize {
    println!("== three voters, one hosted group \"{GROUP}\" ==");
    println!("  activation: Quorum over all three; commits at QuorumApplied");
    println!("  the feed's ring holds {RING} writes — small on purpose");
    let host = settle("an epoch to close", || {
        members
            .iter()
            .position(|m| m.group.leadership().role == Role::Host)
    })
    .await;
    let fence = settle("the host to finish recovering", || {
        members[host].writes.fence()
    })
    .await;
    println!(
        "  {} is host, serving under fence {fence}",
        members[host].id
    );
    host
}

/// The overrun: more writes than the ring can hold, every one of them committed.
async fn act2_overrun(host: &Member) -> WriteToken {
    println!("\n== act 1: the host writes past its own ring ==");
    let mut head = WriteToken { epoch: 0, seq: 0 };
    for n in 0..RECORDS {
        head = host.author(&format!("rec-{n:02}")).await;
    }
    println!(
        "  {RECORDS} records committed at QuorumApplied, up to token {}",
        tok(head)
    );
    println!(
        "  the ring now advertises the last {RING} of them; the first {} are \
         gone from gossip entirely",
        RECORDS - RING
    );
    head
}

/// The fourth node, and the honest `Gap` it is met with.
async fn act3_late(
    net: &Network,
    bulk: &MemBulkNet,
    id: &str,
    voters: &[&str],
    host: &Member,
    head: WriteToken,
) -> Member {
    println!("\n== act 2: a fourth node joins, late ==");
    let joiner = Member::spawn(net, bulk, id, voters);
    let gap = settle("the joiner to be told what it missed", || {
        joiner.gaps().first().copied()
    })
    .await;
    println!("  {} joined and adopted the live lineage", joiner.id);
    println!(
        "  Gap: writes of this lineage through {} were not delivered.",
        tok(gap)
    );
    println!("       The ring cannot replay history it no longer holds.");
    println!(
        "  {} holds {} of {RECORDS} records — and publishes no watermark at all:",
        joiner.id,
        joiner.held()
    );
    println!(
        "       claiming {} would assert coverage of the {} below it,",
        tok(head),
        gap.seq
    );
    println!("       and this consumer has no store to rebuild them from.");
    assert_eq!(joiner.ledger.applied(&host.id), None, "nothing claimed");
    joiner
}

/// The remediation: choose a donor, verify it, install it, and publish the claim.
async fn act4_handoff(
    joiner: &Member,
    host: &Member,
    head: WriteToken,
    served: &mut mpsc::UnboundedReceiver<Offered>,
) {
    println!("\n== act 3: the handoff ==");
    settle("the joiner to see the host's advertised head", || {
        (advertised_head_named(&hosted_feed_name(""), &joiner.group, &host.id) == Some(head))
            .then_some(())
    })
    .await;
    let need: Watermarks = [(host.id.clone(), head)].into_iter().collect();
    let handoff = Handoff::new(joiner.group.clone(), joiner.id.clone());
    let donors = settle("a covering donor", || {
        let donors = handoff.donors(&need);
        (!donors.is_empty()).then_some(donors)
    })
    .await;
    let names: Vec<&str> = donors.iter().map(NodeId::as_str).collect();
    println!("  donors(need = {}) -> {names:?}", marks(&need));
    println!("       the serving host first: its state is the one state a");
    println!(
        "       surviving hostship definitionally holds — and never {},",
        joiner.id
    );
    println!("       because a node that asked itself would connect to its own");
    println!("       endpoint and wait for an offer nobody is there to write.");

    let sink = ReplicaSink {
        replica: Arc::clone(&joiner.replica),
        staged: Vec::new(),
    };
    let receipt: HandoffReceipt = handoff
        .fetch(&joiner.plane, &donors[0], &need, sink)
        .await
        .expect("a covering donor at an agreeing fence");
    let offered = served.recv().await.expect("the donor reports what it sent");
    let stamped = receipt
        .fence_host
        .as_ref()
        .map_or_else(|| "hostless".to_owned(), ToString::to_string);
    println!(
        "  offer:  stamped ({}, {stamped}), covers {}",
        receipt.fence_epoch,
        marks(&receipt.covers)
    );
    println!(
        "  stream: {} chunks, {} payload bytes, then a Done frame — because a",
        offered.chunks, offered.bytes
    );
    println!("          clean end of stream is indistinguishable from a finished");
    println!("          snapshot, and silence is never success on this protocol.");
    println!("  verify: at the offer, the donor's stamp against a freshly re-read");
    println!("          leadership, then its covers against the need; at the");
    println!("          terminator, the counts first and then the *re-read* stamp");
    println!("          again — and only after all four was the sink finished.");

    Handoff::seed(&receipt, &joiner.ledger, &joiner.frontier).await;
    joiner.replica.lock().expect("lock").hole = false;
    println!(
        "  seeded: {} now holds {} of {RECORDS} records, and publishes {}={}",
        joiner.id,
        joiner.held(),
        host.id,
        joiner
            .ledger
            .applied(&host.id)
            .map_or_else(|| "nothing".to_owned(), tok)
    );
}

/// And the stream picks up where it stood.
async fn act5_resume(joiner: &Member, host: &Member, head: WriteToken) {
    println!("\n== act 4: the stream resumes ==");
    let before = joiner.gaps().len();
    for n in 0..LIVE {
        let token = host.author(&format!("live-{n}")).await;
        settle("the joiner to apply and claim it", || {
            (joiner.ledger.applied(&host.id) == Some(token)).then_some(())
        })
        .await;
        println!("  {} delivered and claimed, in order", tok(token));
    }
    println!(
        "  gaps since the handoff: {} — the Gap had already positioned the",
        joiner.gaps().len() - before
    );
    println!("  cursor, and the transfer filled the state behind it. There was");
    println!("  never anything left to miss.");
    assert_eq!(
        joiner.ledger.applied(&host.id),
        Some(WriteToken {
            epoch: head.epoch,
            seq: head.seq + LIVE as u64
        })
    );
}

fn moral() {
    println!("\n== the moral ==");
    println!("  A `Gap` is not a shrug. It is a bounded, named statement about");
    println!("  what was missed — and a consumer that cannot rebuild from its own");
    println!("  store now has something to do about it: pull a covering image");
    println!("  from a peer, over the data plane, fenced by the same epoch the");
    println!("  write path is fenced by, and adopt it only after it verifies.");
    println!("  Nothing new was added to the cursor: the Gap positioned it, the");
    println!("  handoff filled the state, and re-applying overlap is a no-op.");
    println!();
    println!("  Sizing, honestly: the transfer has to outrun the write rate. If");
    println!("  the writers advance past what a snapshot covers before it lands,");
    println!("  the requester finishes and is short again — and if that is");
    println!("  durably true it is a livelock. Pick a ring deep enough for the");
    println!("  worst migration lag you accept, or a snapshot small enough to");
    println!("  land inside it. The fix is capacity, not cleverness; a caller");
    println!("  that wants the failure loud bounds its own retries.");
}

/// A token in the compact `epoch:seq` spelling, so a narrative line reads like
/// one — the same shape [`Fence`](groupnet_consistency::Fence) prints in.
fn tok(token: WriteToken) -> String {
    format!("{}:{}", token.epoch, token.seq)
}

/// A watermark map in the same spelling: `{writer=epoch:seq, …}`.
fn marks(watermarks: &Watermarks) -> String {
    let pairs: Vec<String> = watermarks
        .iter()
        .map(|(writer, token)| format!("{writer}={}", tok(*token)))
        .collect();
    format!("{{{}}}", pairs.join(", "))
}

/// Polls `probe` until it yields, panicking after a generous deadline — a demo
/// that cannot converge should fail loudly, not hang on a later await.
async fn settle<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    for _ in 0..POLLS {
        if let Some(ready) = probe() {
            return ready;
        }
        tokio::time::sleep(POLL).await;
    }
    panic!("timed out waiting for {what}");
}
