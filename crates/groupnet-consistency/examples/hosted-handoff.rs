//! Late-joiner resync: a node arrives after the host has written past its own
//! ring, is told so honestly, and pulls a **covering snapshot** over the data
//! plane instead of guessing.
//!
//! Three voters form a `Hosted` group with `Activation::Quorum` and commit a
//! stream of records at [`Commit::QuorumApplied`] through a deliberately tiny
//! ring. Then a fourth node joins, and what it meets is the point of the run:
//!
//! * **The `Gap` is honest.** The ring cannot replay history it no longer
//!   holds; the joiner's subscriber says so with one `Gap` naming the last write
//!   it will never see.
//! * **A consumer with no store of its own claims nothing while short.** It
//!   applies what arrives but publishes no watermark: claiming the top of the
//!   lineage would assert coverage of everything below it.
//! * **The remediation is a verified install.** [`Handoff::fetch`] pulls a
//!   covering image from the donor [`Handoff::donors`] ranks first, verifies the
//!   donor's fence stamp at the offer and again at the terminator, and only then
//!   calls the sink's `finish` — the application's atomic install.
//!   [`Handoff::seed`] turns the receipt into the watermark the group reads.
//! * **Then the stream resumes**, in order, with no second `Gap`.
//!
//! Two in-process planes: `Network` carries gossip, `MemBulkNet` carries the
//! snapshot.
//!
//! ```text
//! cargo run -p groupnet-consistency --example hosted-handoff --features handoff
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::hosted::handoff::{Bytes, DataPlane};
use groupnet_consistency::{
    ChunkLimit, Commit, CommitLedger, Frontier, Group, Handoff, HostedRead, HostedReads,
    HostedWrites, Snapshot, SnapshotChunks, SnapshotSink, SnapshotSource, WriteToken,
};
use groupnet_core::{Activation, HostedConfig, NodeId, VoterRoster, placement};
use groupnet_runtime::{GroupProfile, Node};
use groupnet_transport_mem::{MemBulkNet, MemBulkTransport, MemLink, Network};

const GROUP: &str = "records";

/// Four machines: the first three by rendezvous rank vote; the last joins late.
const IDS: [&str; 4] = ["rec-a", "rec-b", "rec-c", "rec-d"];

/// Four slots: absurd for a deployment, right for a demo of an overrun.
const RING: usize = 4;

/// Records written before the joiner arrives — most of them gone from gossip.
const RECORDS: usize = 16;

/// Records written after the handoff, to show the stream resuming.
const LIVE: usize = 3;

/// Per-writer applied watermarks.
type Marks = BTreeMap<NodeId, WriteToken>;

/// One node's applied state, with no store under it — which is why a `Gap` is
/// not survivable here without a handoff.
#[derive(Debug, Default)]
struct Replica {
    records: BTreeSet<String>,
    /// What `records` covers, read under the same lock so a snapshot's `covers`
    /// describes the image it ships with.
    marks: Marks,
    /// The `Gap` this node has not yet remediated. While set, it applies what
    /// arrives and claims nothing.
    hole: Option<WriteToken>,
}

impl Replica {
    /// Applies one write; returns whether it may be claimed (no open hole).
    fn apply(&mut self, writer: &NodeId, token: WriteToken, key: String) -> bool {
        self.records.insert(key);
        if self.hole.is_some() {
            return false;
        }
        self.raise(writer, token);
        true
    }

    /// Closes the hole once a verified snapshot covering `covers` is installed.
    fn heal(&mut self, covers: &Marks) {
        self.hole = None;
        for (writer, token) in covers {
            self.raise(writer, *token);
        }
    }

    fn raise(&mut self, writer: &NodeId, token: WriteToken) {
        let mark = self.marks.entry(writer.clone()).or_insert(token);
        *mark = (*mark).max(token);
    }
}

/// The donor half: the image and its coverage, taken together under one lock,
/// cut into chunks no longer than the donor's limit.
struct ReplicaSource(Arc<Mutex<Replica>>);

impl SnapshotSource for ReplicaSource {
    type Chunks = ImageChunks;

    fn open(
        &self,
        limit: ChunkLimit,
    ) -> impl Future<Output = io::Result<Snapshot<Self::Chunks>>> + Send {
        let replica = self.0.lock().expect("lock");
        let image: Vec<&str> = replica.records.iter().map(String::as_str).collect();
        let image = Bytes::from(image.join("\n"));
        let chunks: Vec<Bytes> = (0..image.len())
            .step_by(limit.get())
            .map(|at| image.slice(at..image.len().min(at + limit.get())))
            .collect();
        std::future::ready(Ok(Snapshot {
            covers: replica.marks.clone(),
            chunks: ImageChunks(chunks.into_iter()),
        }))
    }
}

/// One opened image's chunks.
struct ImageChunks(std::vec::IntoIter<Bytes>);

impl SnapshotChunks for ImageChunks {
    fn next(&mut self) -> impl Future<Output = io::Result<Option<Bytes>>> + Send {
        std::future::ready(Ok(self.0.next()))
    }
}

/// The requester half: stages into its own buffer and touches the replica only
/// in `finish`, so a dropped, unfinished sink — every verification failure —
/// installs nothing.
struct ReplicaSink {
    replica: Arc<Mutex<Replica>>,
    staged: Vec<u8>,
}

impl SnapshotSink for ReplicaSink {
    fn apply(&mut self, chunk: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        self.staged.extend_from_slice(&chunk);
        std::future::ready(Ok(()))
    }

    /// The atomic install: decode everything first, then merge under one lock
    /// (a merge, because the apply loop keeps running meanwhile).
    fn finish(self) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(std::str::from_utf8(&self.staged).map_or_else(
            |err| Err(io::Error::new(io::ErrorKind::InvalidData, err)),
            |image| {
                let mut replica = self.replica.lock().expect("lock");
                replica.records.extend(image.lines().map(str::to_owned));
                Ok(())
            },
        ))
    }
}

/// One participant on both planes.
struct Member {
    id: NodeId,
    _node: Node,
    group: Group,
    ledger: Arc<CommitLedger>,
    writes: HostedWrites<String>,
    replica: Arc<Mutex<Replica>>,
    frontier: Arc<Frontier>,
    plane: DataPlane<MemBulkTransport>,
}

impl Member {
    async fn spawn(net: &Network, bulk: &MemBulkNet, me: &NodeId, voters: &[NodeId]) -> Self {
        let peers: Vec<NodeId> = IDS
            .iter()
            .map(|id| NodeId::new(*id))
            .filter(|peer| peer != me)
            .collect();
        let mut builder = Node::builder(me.clone())
            .link(MemLink::new(net.endpoint(me.clone()), peers.clone()))
            .gossip_interval_ms(15);
        for peer in peers {
            builder = builder.seed(peer);
        }
        let node = builder.start().await.expect("the memory link binds");
        let group = node.join_group_with(
            GROUP,
            GroupProfile::hosted(HostedConfig {
                activation: Activation::Quorum {
                    voters: VoterRoster::new(voters.iter().cloned()),
                },
                lease_ms: 600,
            }),
        );
        let ledger = Arc::new(CommitLedger::new(group.clone()));
        let writes = HostedWrites::builder(
            group.clone(),
            me.clone(),
            NonZeroUsize::new(RING).expect("nonzero"),
            |key: &String| key.clone().into_bytes(),
        )
        .committed(Arc::clone(&ledger))
        .build()
        .expect("a Quorum group supports the committed regime");
        let member = Self {
            id: me.clone(),
            _node: node,
            group,
            ledger,
            writes,
            replica: Arc::default(),
            frontier: Arc::new(Frontier::new().0),
            plane: DataPlane::new(bulk.endpoint(me.clone())),
        };
        member.follow();
        member
    }

    /// The follower loop of a consumer **without a store**: apply, then record —
    /// and while a hole is open, record nothing.
    fn follow(&self) {
        let mut reads = HostedReads::new(self.group.clone(), self.id.clone(), |bytes: &[u8]| {
            String::from_utf8(bytes.to_vec()).ok()
        });
        self.writes.bind(&mut reads);
        let (ledger, replica) = (Arc::clone(&self.ledger), Arc::clone(&self.replica));
        let frontier = Arc::clone(&self.frontier);
        tokio::spawn(async move {
            let mut host: Option<NodeId> = None;
            while let Some(event) = reads.next().await {
                let claim = match event {
                    HostedRead::Wrote {
                        host: writer,
                        token,
                        key,
                    } => replica
                        .lock()
                        .expect("lock")
                        .apply(&writer, token, key)
                        .then_some((writer, token)),
                    // `seq == 0` opens a lineage and missed nothing of it;
                    // anything above names writes the ring no longer holds.
                    HostedRead::Gap { missed_through } if missed_through.seq == 0 => {
                        host.clone().map(|host| (host, missed_through))
                    }
                    HostedRead::Gap { missed_through } => {
                        replica.lock().expect("lock").hole = Some(missed_through);
                        None
                    }
                    HostedRead::Migrated { host: adopted, .. } => {
                        host = adopted;
                        ledger
                            .refresh()
                            .await
                            .expect("the group actor accepts the ledger");
                        None
                    }
                };
                if let Some((writer, token)) = claim {
                    frontier.advance(&writer, token);
                    ledger
                        .record(&writer, token)
                        .await
                        .expect("the group actor accepts the ledger");
                }
            }
        });
    }

    /// Answers handoff requests from this node's replica.
    fn serve(&self) {
        let plane = self.plane.clone();
        let handoff = Handoff::new(self.group.clone(), self.id.clone());
        let (id, source) = (self.id.clone(), ReplicaSource(Arc::clone(&self.replica)));
        tokio::spawn(async move {
            while let Ok((from, mut stream)) = plane.accept().await {
                match handoff.offer(&mut stream, &source).await {
                    Ok(offered) => println!(
                        "  [{id}] served {from}: {} chunk(s), {} bytes",
                        offered.chunks, offered.bytes
                    ),
                    Err(err) => println!("  [{id}] refused {from}: {err}"),
                }
            }
        });
    }

    /// Commits one record and applies it locally — a host's own subscriber never
    /// delivers it its own writes.
    async fn author(&self, key: String) -> WriteToken {
        let receipt = self
            .writes
            .publish_committed(&key, Commit::QuorumApplied, Duration::from_secs(2))
            .await
            .expect("the host serves");
        assert!(receipt.is_committed(), "{key}: {:?}", receipt.outcome);
        let mut replica = self.replica.lock().expect("lock");
        replica.apply(&self.id, receipt.token, key);
        self.frontier.advance(&self.id, receipt.token);
        receipt.token
    }

    fn held(&self) -> usize {
        self.replica.lock().expect("lock").records.len()
    }
}

#[tokio::main]
async fn main() {
    let (net, bulk) = (Network::new(), MemBulkNet::new());
    // Rendezvous rank decides who bids; the joiner comes from the bottom of it
    // so a late arrival does not disturb a healthy hostship.
    let weighted: Vec<(NodeId, u32)> = IDS.iter().map(|id| (NodeId::new(*id), 1)).collect();
    let rank = placement::owners(GROUP, &weighted, IDS.len());
    let (voters, joiner) = (&rank[..3], &rank[3]);
    let mut members = Vec::new();
    for id in voters {
        let member = Member::spawn(&net, &bulk, id, voters).await;
        member.serve();
        members.push(member);
    }
    let host = settle("a host to activate and recover", || {
        members
            .iter()
            .find(|member| member.writes.fence().is_some())
    })
    .await;

    println!("== {} hosts; the ring holds {RING} writes ==", host.id);
    let mut head = WriteToken { epoch: 0, seq: 0 };
    for n in 0..RECORDS {
        head = host.author(format!("rec-{n:02}")).await;
    }
    println!("  {RECORDS} records committed, up to {head:?}");

    println!("\n== {joiner} joins late ==");
    let joiner = Member::spawn(&net, &bulk, joiner, voters).await;
    let missed = settle("the joiner's Gap", || {
        joiner.replica.lock().expect("lock").hole
    })
    .await;
    println!(
        "  Gap through {missed:?}: holds {} of {RECORDS} records, claims {:?}",
        joiner.held(),
        joiner.ledger.applied(&host.id)
    );

    println!("\n== the handoff ==");
    let need: Marks = [(host.id.clone(), missed)].into();
    let handoff = Handoff::new(joiner.group.clone(), joiner.id.clone());
    let donors = settle("a covering donor", || {
        Some(handoff.donors(&need)).filter(|donors| !donors.is_empty())
    })
    .await;
    let sink = ReplicaSink {
        replica: Arc::clone(&joiner.replica),
        staged: Vec::new(),
    };
    let receipt = handoff
        .fetch(&joiner.plane, &donors[0], &need, sink)
        .await
        .expect("a covering donor at an agreeing fence");
    println!(
        "  verified and installed from {} at fence epoch {}, covering {:?}",
        donors[0], receipt.fence_epoch, receipt.covers
    );
    joiner.replica.lock().expect("lock").heal(&receipt.covers);
    Handoff::seed(&receipt, &joiner.ledger, &joiner.frontier)
        .await
        .expect("the group actor accepts the ledger");
    println!(
        "  holds {} of {RECORDS} records, claims {:?}",
        joiner.held(),
        joiner.ledger.applied(&host.id)
    );

    println!("\n== the stream resumes ==");
    for n in 0..LIVE {
        let token = host.author(format!("live-{n}")).await;
        settle("the joiner to apply and claim it", || {
            (joiner.ledger.applied(&host.id) == Some(token)).then_some(())
        })
        .await;
    }
    assert!(
        joiner.replica.lock().expect("lock").hole.is_none(),
        "no second Gap"
    );
    println!(
        "  {LIVE} live writes applied and claimed in order; no second Gap, up to {:?}",
        joiner.ledger.applied(&host.id)
    );
}

/// Polls `probe` until it yields, panicking after a generous deadline.
async fn settle<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    for _ in 0..2000 {
        if let Some(ready) = probe() {
            return ready;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}
