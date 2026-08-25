//! Snapshot handoff (feature `handoff`) at the **protocol** level: two real
//! nodes, one in-process data plane, and the whole request/offer/chunks/done
//! exchange driven end to end.
//!
//! The three verdicts and the phase table are unit-tested next door without a
//! runtime, and this file deliberately does not re-test them. What it holds is
//! what those pure tests cannot reach — the *order* the driver takes them in,
//! and what is left behind when one refuses:
//!
//! * a covering snapshot crosses whole, byte for byte, and seeds the receiver's
//!   commit ledger and frontier;
//! * a donor short of the target refuses **before** it reads a byte of its own
//!   image, so nothing crosses the link to be thrown away;
//! * a donor that dies mid-stream is a truncation, and what did cross is
//!   discarded rather than half-installed;
//! * `is_request` tells a handoff opener from anything else on a shared plane.
//!
//! Everything here runs over a plain (`Eventual`) group, and that is the point
//! of the split: a hostless group stamps `(0, None)` at both ends, the staleness
//! rule passes that, and no test in this file pays for an election it is not
//! about. The fence-sensitive half — the two staleness checks and `donors()` —
//! needs real epochs and lives in `handoff_fence.rs`, which carries its own copy
//! of the source and sink below. Duplicated helpers across sibling test files is
//! the house pattern (`groupnet-sim`'s `election_quorum*`, this crate's
//! `hosted_dst*`).
//!
//! Every wait is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep.

#![cfg(feature = "handoff")]

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncWrite};
use groupnet_consistency::hosted::handoff::is_request;
use groupnet_consistency::{
    CommitLedger, Frontier, Handoff, HandoffError, Offered, Snapshot, SnapshotChunks, SnapshotSink,
    SnapshotSource, Watermarks, WriteToken,
};
use groupnet_core::NodeId;
use groupnet_testkit::cluster::{MemCluster, eventually_within};
use groupnet_transport::bulk::{DataPlane, DataStream};
use groupnet_transport_mem::MemBulkNet;
use tokio::sync::watch;

/// The poll budget for every assertion here. Nothing in this file waits on
/// gossip, so this is a deadlock bound rather than a convergence one.
const SETTLE: Duration = Duration::from_secs(10);

/// A brisk gossip cadence, so the two nodes find each other in wall-clock
/// milliseconds. Nothing here depends on their having done so.
const GOSSIP_MS: u64 = 15;

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
// The consumer's two halves, as a test can watch them.
// ---------------------------------------------------------------------------

/// What a [`TestSource`] was asked to do, shared with the test that built it.
#[derive(Debug, Default)]
struct SourceProbe {
    opens: AtomicUsize,
    polls: AtomicUsize,
}

impl SourceProbe {
    fn opens(&self) -> usize {
        self.opens.load(Ordering::Relaxed)
    }

    /// How many times the chunk stream was polled. `0` proves the image was
    /// never *read*, which is the whole assertion behind an early refusal.
    fn polls(&self) -> usize {
        self.polls.load(Ordering::Relaxed)
    }
}

/// A snapshot source over a fixed list of chunks, with the one knob the failure
/// tests need: a **gate** the chunk stream parks on partway through, so a test
/// can cut the link in a known place rather than wherever the scheduler happens
/// to be.
struct TestSource {
    covers: Watermarks,
    chunks: Vec<Bytes>,
    /// Park once this many chunks have been handed over, until the gate opens.
    /// `None` never parks.
    gate: Option<(usize, watch::Receiver<bool>)>,
    probe: Arc<SourceProbe>,
}

impl TestSource {
    fn new(covers: Watermarks, chunks: Vec<Bytes>) -> Self {
        Self {
            covers,
            chunks,
            gate: None,
            probe: Arc::default(),
        }
    }

    /// Parks the chunk stream after `after` chunks until `gate` goes `true`.
    fn gated_after(mut self, after: usize, gate: watch::Receiver<bool>) -> Self {
        self.gate = Some((after, gate));
        self
    }

    fn probe(&self) -> Arc<SourceProbe> {
        Arc::clone(&self.probe)
    }
}

impl SnapshotSource for TestSource {
    type Chunks = TestChunks;

    fn open(&self) -> impl std::future::Future<Output = io::Result<Snapshot<TestChunks>>> + Send {
        self.probe.opens.fetch_add(1, Ordering::Relaxed);
        std::future::ready(Ok(Snapshot {
            covers: self.covers.clone(),
            chunks: TestChunks {
                remaining: self.chunks.clone().into_iter(),
                handed: 0,
                gate: self.gate.clone(),
                probe: Arc::clone(&self.probe),
            },
        }))
    }
}

/// One opened image's chunks.
struct TestChunks {
    remaining: std::vec::IntoIter<Bytes>,
    handed: usize,
    gate: Option<(usize, watch::Receiver<bool>)>,
    probe: Arc<SourceProbe>,
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
        self.probe.polls.fetch_add(1, Ordering::Relaxed);
        self.handed += 1;
        Ok(self.remaining.next())
    }
}

/// What a [`TestSink`] did, as the test that handed it over can still see.
#[derive(Debug, Default)]
struct SinkState {
    /// Chunks handed to `apply`, whether or not they were ever installed.
    staged: usize,
    /// The bytes `finish` adopted. `None` is **nothing installed**, which is
    /// exactly what a dropped, unfinished sink must leave behind.
    installed: Option<Vec<u8>>,
    /// How many times `finish` returned.
    finishes: usize,
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

    fn read<T>(&self, f: impl FnOnce(&SinkState) -> T) -> T {
        f(&self.0.lock().expect("sink state"))
    }

    fn staged(&self) -> usize {
        self.read(|state| state.staged)
    }

    fn installed(&self) -> Option<Vec<u8>> {
        self.read(|state| state.installed.clone())
    }

    fn finishes(&self) -> usize {
        self.read(|state| state.finishes)
    }

    /// Nothing adopted and nothing finished: the state **every** failure path
    /// must leave behind, whatever crossed the link first.
    fn installed_nothing(&self) -> bool {
        self.read(|state| state.installed.is_none() && state.finishes == 0)
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
    fn apply(&mut self, chunk: Bytes) -> impl std::future::Future<Output = io::Result<()>> + Send {
        self.pending.extend_from_slice(&chunk);
        self.state.lock().expect("sink state").staged += 1;
        std::future::ready(Ok(()))
    }

    fn finish(self) -> impl std::future::Future<Output = io::Result<()>> + Send {
        let TestSink { state, pending } = self;
        let mut state = state.lock().expect("sink state");
        state.installed = Some(pending);
        state.finishes += 1;
        std::future::ready(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// The harness: one plain group, one data plane, two framed streams.
// ---------------------------------------------------------------------------

/// A requester and a donor on one plain group, plus the data-plane fabric they
/// talk over.
struct Wire {
    cluster: MemCluster,
    bulk: MemBulkNet,
}

impl Wire {
    fn build(group: &str, ids: [&str; 2]) -> Self {
        Self {
            cluster: MemCluster::builder(&ids)
                .group(group)
                .gossip_interval_ms(GOSSIP_MS)
                .spawn(),
            bulk: MemBulkNet::new(),
        }
    }

    fn requester(&self) -> Handoff {
        Handoff::new(self.cluster.groups[0].clone(), self.cluster.ids[0].clone())
    }

    fn donor(&self) -> Handoff {
        Handoff::new(self.cluster.groups[1].clone(), self.cluster.ids[1].clone())
    }

    /// One connected pair of framed streams, `(at the requester, at the donor)`.
    async fn linked(
        &self,
    ) -> (
        DataStream<impl AsyncRead + AsyncWrite + Send + Unpin + 'static>,
        DataStream<impl AsyncRead + AsyncWrite + Send + Unpin + 'static>,
    ) {
        let (from, to) = (&self.cluster.ids[0], &self.cluster.ids[1]);
        let out = DataPlane::new(self.bulk.endpoint(from.clone()));
        let inbound = DataPlane::new(self.bulk.endpoint(to.clone()));
        let opened = out.connect(to).await.expect("the data plane connects");
        let (who, accepted) = inbound.accept().await.expect("the donor accepts");
        assert_eq!(&who, from, "the accept attributes the connector");
        (opened, accepted)
    }
}

// ---------------------------------------------------------------------------
// 1. The happy path, end to end.
// ---------------------------------------------------------------------------

/// A covering snapshot crosses whole and installs exactly once, and the receipt
/// it hands back is what seeds this node's evidence: the commit ledger its peers
/// read, and the frontier its own readers barrier on.
#[tokio::test]
async fn a_covering_snapshot_crosses_whole_and_seeds_the_receiving_node() {
    let wire = Wire::build("handoff-happy", ["hh-req", "hh-donor"]);
    let chunks = vec![
        Bytes::from_static(b"the first piece of somebody's state"),
        // An empty chunk is a chunk: boundaries are the source's business, and
        // the sink must see exactly the ones it produced.
        Bytes::from_static(b""),
        Bytes::from_static(b"...and the last"),
    ];
    let whole: Vec<u8> = chunks
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect();
    let covers = marks(&[("w-a", 2, 7), ("w-b", 1, 3)]);
    let source = TestSource::new(covers.clone(), chunks);
    let source_probe = source.probe();
    let sink = SinkProbe::default();
    let need = marks(&[("w-a", 2, 5)]);

    let (mut to_donor, mut at_donor) = wire.linked().await;
    let donor = wire.donor();
    let served = tokio::spawn(async move { donor.offer(&mut at_donor, &source).await });

    let receipt = wire
        .requester()
        .fetch_on(&mut to_donor, &need, sink.sink())
        .await
        .expect("a covering donor at an agreeing fence");
    let offered = served
        .await
        .expect("the donor task")
        .expect("the donor served");

    // What the requester learned...
    assert_eq!(
        receipt.covers, covers,
        "the receipt carries the image's claim"
    );
    assert_eq!(
        (receipt.fence_epoch, receipt.fence_host.clone()),
        (0, None),
        "a hostless group stamps hostlessly, and that is a first-class stamp"
    );
    // ...agrees with what the donor thinks it sent, counter for counter.
    assert_eq!(
        offered,
        Offered {
            covers: covers.clone(),
            chunks: 3,
            bytes: u64::try_from(whole.len()).expect("a test-sized snapshot"),
        }
    );
    assert_eq!(source_probe.opens(), 1, "one image per served request");

    // ...and the bytes are the bytes.
    assert_eq!(sink.staged(), 3, "every chunk applied, in its own call");
    assert_eq!(sink.installed(), Some(whole));
    assert_eq!(sink.finishes(), 1, "finished exactly once");

    // The fold that turns a transfer into a recovery.
    let ledger = CommitLedger::new(wire.cluster.groups[0].clone());
    let (frontier, view) = Frontier::new();
    Handoff::seed(&receipt, &ledger, &frontier).await;
    assert_eq!(
        ledger.watermarks(),
        covers,
        "the ledger now publishes what the snapshot brought"
    );
    for (writer, token) in &covers {
        assert!(
            tokio::time::timeout(SETTLE, view.reached(writer, *token))
                .await
                .expect("the frontier is already past every covered token"),
            "barrier on {writer:?}"
        );
    }
    // Both folds are monotone, so a retried handoff is a no-op, never a
    // regression.
    Handoff::seed(&receipt, &ledger, &frontier).await;
    assert_eq!(ledger.watermarks(), covers);
}

// ---------------------------------------------------------------------------
// 2. The early refusal.
// ---------------------------------------------------------------------------

/// A donor that cannot reach the target refuses at the offer, carrying its own
/// map so the requester learns *how far behind* rather than merely that it is —
/// and it refuses without reading a byte of its own image, which is the whole
/// reason the coverage check sits where it does.
#[tokio::test]
async fn a_donor_short_of_the_target_refuses_before_a_byte_is_read() {
    let wire = Wire::build("handoff-short", ["hn-req", "hn-donor"]);
    let covers = marks(&[("w-a", 1, 5)]);
    let source = TestSource::new(
        covers.clone(),
        vec![Bytes::from_static(b"state nobody will ever see")],
    );
    let source_probe = source.probe();
    let sink = SinkProbe::default();
    let need = marks(&[("w-a", 1, 9)]);

    let (mut to_donor, mut at_donor) = wire.linked().await;
    let donor = wire.donor();
    let served = tokio::spawn(async move { donor.offer(&mut at_donor, &source).await });

    let err = wire
        .requester()
        .fetch_on(&mut to_donor, &need, sink.sink())
        .await
        .expect_err("a donor that does not cover must not be adopted");
    match err {
        HandoffError::NotCovered { have } => assert_eq!(
            have, covers,
            "the refusal carries the donor's own map, not the request's"
        ),
        other => panic!("unexpected {other:?}"),
    }
    // The donor's own caller sees the same fact in the same words.
    match served.await.expect("the donor task") {
        Err(HandoffError::NotCovered { have }) => assert_eq!(have, covers),
        other => panic!("unexpected {other:?}"),
    }

    assert_eq!(
        source_probe.opens(),
        1,
        "the image was opened, to be measured"
    );
    assert_eq!(
        source_probe.polls(),
        0,
        "...and never read: no chunk crossed the link to be thrown away"
    );
    assert_eq!(sink.staged(), 0, "the sink was never touched");
    assert!(sink.installed_nothing());
}

// ---------------------------------------------------------------------------
// 3. The truncation.
// ---------------------------------------------------------------------------

/// A donor that dies mid-stream is a **truncation**, never a success: the
/// framing layer reports its disappearance as a clean end of stream, and only
/// the protocol's own terminator can tell that apart from a finished snapshot.
/// What had already crossed goes with the sink.
#[tokio::test]
async fn a_donor_that_dies_mid_stream_installs_nothing() {
    let wire = Wire::build("handoff-cut", ["hc-req", "hc-donor"]);
    // Held open after two chunks, so the cut lands in a known place.
    let gate = watch::channel(false).0;
    let source = TestSource::new(
        marks(&[("w-a", 1, 9)]),
        (0..4u8).map(|n| Bytes::from(vec![n; 64])).collect(),
    )
    .gated_after(2, gate.subscribe());
    let sink = SinkProbe::default();
    let need = marks(&[("w-a", 1, 9)]);

    let (mut to_donor, mut at_donor) = wire.linked().await;
    let donor = wire.donor();
    let served = tokio::spawn(async move { donor.offer(&mut at_donor, &source).await });
    let requester = wire.requester();
    let staging = sink.clone();
    let fetched = tokio::spawn(async move {
        requester
            .fetch_on(&mut to_donor, &need, staging.sink())
            .await
    });

    eventually_within("two chunks to be staged", SETTLE, || sink.staged() == 2).await;
    // The donor goes away mid-snapshot: its task drops the stream, which is a
    // clean EOF at a frame boundary and says nothing whatever about being done.
    served.abort();

    let err = fetched
        .await
        .expect("the requester task")
        .expect_err("silence is never success on this protocol");
    assert!(matches!(err, HandoffError::Truncated), "{err:?}");
    assert_eq!(sink.staged(), 2, "two chunks really did cross...");
    assert!(
        sink.installed_nothing(),
        "...and not one of them was adopted"
    );
}

// ---------------------------------------------------------------------------
// 4. Demultiplexing a shared data plane.
// ---------------------------------------------------------------------------

/// A bulk transport carries whatever its consumer puts on it, so a responder has
/// to decide whether an accepted stream is a handoff before it commits to
/// reading one. `is_request` is that test: cheap, prefix-only, conclusive in the
/// negative — and the frames it runs against here are real ones, taken off the
/// wire rather than hand-built.
#[tokio::test]
async fn is_request_tells_a_handoff_opener_from_anything_else() {
    let wire = Wire::build("handoff-demux", ["hx-req", "hx-donor"]);
    let need = marks(&[("w-a", 1, 1)]);

    // --- a genuine opener, captured from a real fetch ---
    let (mut to_donor, mut at_donor) = wire.linked().await;
    let requester = wire.requester();
    let sink = SinkProbe::default();
    let staging = sink.clone();
    let wanted = need.clone();
    let fetched = tokio::spawn(async move {
        requester
            .fetch_on(&mut to_donor, &wanted, staging.sink())
            .await
    });
    let opener = at_donor
        .recv()
        .await
        .expect("a frame")
        .expect("the requester opens the exchange");
    assert!(is_request(&opener), "a real request demuxes as one");
    // The fixed prefix alone is enough to dispatch on, and one byte less is not.
    assert!(is_request(&opener[..6]), "magic, version and kind");
    assert!(!is_request(&opener[..5]));
    drop(at_donor);
    assert!(
        matches!(
            fetched.await.expect("the requester task"),
            Err(HandoffError::Truncated)
        ),
        "the far end went away without answering"
    );
    assert!(sink.installed_nothing());

    // --- a genuine *answer*, which is this protocol and still not an opener ---
    let (mut raw, mut at_server) = wire.linked().await;
    let donor = wire.donor();
    let source = TestSource::new(need, vec![Bytes::from_static(b"whatever")]);
    let served = tokio::spawn(async move { donor.offer(&mut at_server, &source).await });
    raw.send(opener.clone())
        .await
        .expect("replaying the captured request");
    let answer = raw
        .recv()
        .await
        .expect("a frame")
        .expect("the donor answers the request it was replayed");
    assert!(!is_request(&answer), "an offer is not an opener");
    drop(raw);
    let _ = served.await;

    // --- and everything that is not this protocol at all ---
    for foreign in [
        &b""[..],
        &b"GNH"[..],
        &b"GNHX\x01\x01"[..],
        &b"GNHO\x02\x01"[..],
        &b"hello, world"[..],
        &[0u8; 32][..],
    ] {
        assert!(!is_request(foreign), "{foreign:?} is not a handoff opener");
    }
}
