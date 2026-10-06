//! The snapshot-handoff tier reached through the umbrella facade (feature
//! `consistency-handoff`).
//!
//! A smoke test, deliberately: the tier's behaviour is proved next door in
//! `groupnet-consistency` (four suites of it). What this pins is that the
//! **feature wires up**, and that is a taller claim here than for any other
//! consistency tier, because this is the only one that spans both planes:
//!
//! * `consistency-handoff` turns on the underlying crate's `handoff` (and the
//!   `hosted` tier it is built on), so `consistency::hosted::handoff` exists and
//!   its vocabulary is reachable at the facade's crate root like every other
//!   tier's;
//! * it turns on the facade's own `bulk`, so `transport::bulk` — the
//!   `DataPlane` a fetch is driven over — is there to drive it with;
//! * and it reaches **into the in-memory binding**: `transport::mem` must expose
//!   `MemBulkNet`, not only the control plane's `Network`. That is the
//!   `groupnet-transport-mem?/bulk` edge in the facade's manifest, and this file
//!   is what fails if somebody removes it.
//!
//! Then it does one real transfer over all three, because a type-check would
//! pass on a build where the two planes could not actually be held by the same
//! process.

#![cfg(all(feature = "consistency-handoff", feature = "mem"))]

use std::io;

use bytes::Bytes;
use groupnet::consistency::{
    CommitLedger, Frontier, Handoff, Offered, Snapshot, SnapshotChunks, SnapshotSink,
    SnapshotSource, Watermarks, WriteToken,
};
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::bulk::DataPlane;
use groupnet::transport::mem::{MemBulkNet, MemLink, Network};

/// The `(writer, token)` map used for coverage and installed frontiers.
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

/// A snapshot over a fixed list of chunks.
struct FixedSource(Watermarks, Vec<Bytes>);

impl SnapshotSource for FixedSource {
    type Chunks = FixedChunks;

    fn open(&self) -> impl Future<Output = io::Result<Snapshot<FixedChunks>>> {
        std::future::ready(Ok(Snapshot {
            covers: self.0.clone(),
            chunks: FixedChunks(self.1.clone().into_iter()),
        }))
    }
}

struct FixedChunks(std::vec::IntoIter<Bytes>);

impl SnapshotChunks for FixedChunks {
    fn next(&mut self) -> impl Future<Output = io::Result<Option<Bytes>>> {
        std::future::ready(Ok(self.0.next()))
    }
}

/// Stages into a buffer of its own and hands it back only from `finish`, so a
/// dropped, unfinished sink is observably a no-op.
struct VecSink(tokio::sync::mpsc::UnboundedSender<Vec<u8>>, Vec<u8>);

impl SnapshotSink for VecSink {
    fn apply(&mut self, chunk: Bytes) -> impl Future<Output = io::Result<()>> {
        self.1.extend_from_slice(&chunk);
        std::future::ready(Ok(()))
    }

    fn finish(self) -> impl Future<Output = io::Result<()>> {
        let VecSink(installed, staged) = self;
        std::future::ready(installed.send(staged).map_err(io::Error::other))
    }
}

/// …and one real transfer runs on it: a group over the facade's in-memory
/// **control** plane, a snapshot over the facade's in-memory **data** plane, and
/// the receipt seeded into a commit ledger. Both planes, one process, one
/// feature flag.
#[tokio::test]
async fn a_handoff_runs_over_the_facades_two_planes() {
    let net = Network::new();
    let bulk = MemBulkNet::new();
    let (from, to) = (NodeId::new("fh-req"), NodeId::new("fh-donor"));
    let mut groups = Vec::with_capacity(2);
    for (id, peer) in [(&from, &to), (&to, &from)] {
        let node = Node::builder(id.clone())
            .link(MemLink::new(net.endpoint(id.clone()), vec![peer.clone()]))
            .start()
            .await
            .expect("start node");
        let group = node.join_group("shard-42");
        groups.push((node, group));
    }

    let covers = marks(&[("w-a", 2, 7)]);
    let source = FixedSource(
        covers.clone(),
        vec![
            Bytes::from_static(b"half of it, "),
            Bytes::from_static(b"and the rest"),
        ],
    );
    let donor = Handoff::new(groups[1].1.clone(), to.clone());
    let inbound = DataPlane::new(bulk.endpoint(to.clone()));
    let served = tokio::spawn(async move {
        let (_who, mut stream) = inbound.accept().await.expect("the donor accepts");
        donor.offer(&mut stream, &source).await
    });

    let (installed, mut arrived) = tokio::sync::mpsc::unbounded_channel();
    let receipt = Handoff::new(groups[0].1.clone(), from.clone())
        .fetch(
            &DataPlane::new(bulk.endpoint(from.clone())),
            &to,
            &marks(&[("w-a", 2, 5)]),
            VecSink(installed, Vec::new()),
        )
        .await
        .expect("a covering donor on a hostless group stamps (0, None) at both ends");

    assert_eq!(receipt.covers, covers, "the receipt carries the claim");
    assert_eq!((receipt.fence_epoch, &receipt.fence_host), (0, &None));
    assert_eq!(
        arrived.recv().await.as_deref(),
        Some(&b"half of it, and the rest"[..]),
        "every chunk, in the source's order, installed exactly once"
    );
    assert_eq!(
        served.await.expect("the donor task").expect("it served"),
        Offered {
            covers: covers.clone(),
            chunks: 2,
            bytes: 24,
        },
        "and the donor's own accounting agrees, counter for counter"
    );

    // The fold that turns a transfer into evidence, reached from the facade too.
    let ledger = CommitLedger::new(groups[0].1.clone());
    let (frontier, view) = Frontier::new();
    Handoff::seed(&receipt, &ledger, &frontier).await;
    assert_eq!(ledger.watermarks(), covers);
    for (writer, token) in &covers {
        assert!(view.reached(writer, *token).await, "barrier on {writer}");
    }
}
