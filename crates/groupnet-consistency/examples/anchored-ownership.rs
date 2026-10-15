//! Anchored ownership: three nodes elect a host by **winning one conditional
//! write against an object store**, and the host stamps every ownership record
//! with the fence that write bought it.
//!
//! This is `fenced-ownership` with the election replaced: the epoch is closed by
//! `Activation::External` — an `If-None-Match` / `If-Match` `PUT` against a
//! single anchor object — instead of by a voter majority. Everything downstream
//! of the epoch is unchanged, because a fence is a fence whoever allocated it.
//! What the run shows:
//!
//! * **No quorum, no votes, no roster — one conditional PUT.** The rendezvous
//!   top-ranked node creates the anchor record, and that node is host.
//! * **Ownership records carry the fence** the anchor awarded.
//! * **Succession is a steal at a strictly higher epoch.** Kill the host and its
//!   record stops being renewed; a survivor supersedes it once it is
//!   `lease_ms + steal_margin_ms` stale, at epoch + 1, and the dead host's
//!   in-flight write is refused by the **store**.
//!
//! The group runs the **Local** regime ([`HostedWrites::new`]): no voter roster
//! and no recovery gate. Nothing in the fabric agreed to anything — the anchor
//! allocates the epoch and the store holds the truth — so a successor serves
//! the instant the anchor says it holds the epoch. Connectivity to the anchor,
//! not to peers, is therefore the availability axis: a host cut off from every
//! peer but still reaching the object keeps hosting, and one that loses the
//! object lapses.
//!
//! Three `Node`s over the in-memory transport stand in for three machines, and
//! `MemAnchor` stands in for one S3 key.
//!
//! ```text
//! cargo run -p groupnet-consistency --example anchored-ownership --features hosted
//! ```

use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::Group;
use groupnet_consistency::hosted::HostedWrites;
use groupnet_core::anchor::AnchorRecord;
use groupnet_core::{NodeId, Role};
use groupnet_runtime::{
    Anchor, AnchorCas, AnchorFuture, AnchorToken, AnchorWriteIf, GroupProfile, Node,
};
use groupnet_transport_mem::{MemLink, Network};

const IDS: [&str; 3] = ["own-a", "own-b", "own-c"];

/// The anchor record's TTL (judged by a successor's clock) and the engine lease
/// the host steps down on (judged by its own) — one number, two hats.
const LEASE_MS: u64 = 600;

/// How far past a record's expiry a claimant waits before superseding it: the
/// margin that absorbs wall-clock disagreement between holder and claimant.
const STEAL_MARGIN_MS: u64 = 200;

/// The external CAS anchor, in memory — and this is **all** S3 needs to be: a
/// `GET` returning the record with its version marker, and a `PUT` conditional
/// on that marker. Swap the [`Mutex`] for an S3 client, the etag for the
/// response's `ETag`, [`AnchorWriteIf::Absent`] for `If-None-Match: *` and
/// [`AnchorWriteIf::Matches`] for `If-Match: <etag>`, and nothing else changes.
/// One object per group: two groups sharing a key would fight undetectably.
#[derive(Debug, Default)]
struct MemAnchor {
    object: Mutex<Option<(AnchorRecord, String)>>,
    /// A counter, not a hash: two writes of identical bytes still get distinct
    /// etags, which a renewal's precondition depends on.
    next_etag: AtomicU64,
}

impl MemAnchor {
    /// What the object holds right now — ground truth, readable without asking
    /// any node.
    fn record(&self) -> Option<AnchorRecord> {
        let object = self.object.lock().expect("lock");
        object.as_ref().map(|(record, _)| record.clone())
    }
}

impl Anchor for MemAnchor {
    fn load(&self) -> AnchorFuture<'_, io::Result<Option<(AnchorRecord, AnchorToken)>>> {
        // A 404 is `Ok(None)`: an absent anchor is a state, not an error.
        let object = self.object.lock().expect("lock");
        let loaded = object
            .as_ref()
            .map(|(record, etag)| (record.clone(), AnchorToken::new(etag.clone())));
        Box::pin(std::future::ready(Ok(loaded)))
    }

    fn store(
        &self,
        pre: AnchorWriteIf,
        record: AnchorRecord,
    ) -> AnchorFuture<'_, io::Result<AnchorCas>> {
        let mut object = self.object.lock().expect("lock");
        let allowed = match (&pre, object.as_ref()) {
            (AnchorWriteIf::Absent, None) => true,
            (AnchorWriteIf::Matches(token), Some((_, etag))) => token.as_str() == etag,
            (AnchorWriteIf::Absent, Some(_)) | (AnchorWriteIf::Matches(_), None) => false,
        };
        let outcome = if allowed {
            let etag = self.next_etag.fetch_add(1, Ordering::Relaxed).to_string();
            *object = Some((record, etag.clone()));
            AnchorCas::Stored(AnchorToken::new(etag))
        } else {
            AnchorCas::Mismatch // a 412: definite, and always "not you"
        };
        Box::pin(std::future::ready(Ok(outcome)))
    }
}

/// A store with fenced writes: `key -> (fence epoch, owner)`. It never learns an
/// anchor exists, and would evaluate a `Quorum`-allocated epoch identically.
#[derive(Debug, Default)]
struct CasStore(Mutex<BTreeMap<String, (u64, String)>>);

impl CasStore {
    /// Writes `owner` at `key` unless a higher fence epoch already wrote it.
    fn put_if_fence_ge(&self, key: &str, epoch: u64, owner: &str) -> Result<(), u64> {
        let mut objects = self.0.lock().expect("lock");
        match objects.get(key) {
            Some((stored, _)) if *stored > epoch => Err(*stored),
            _ => {
                objects.insert(key.to_owned(), (epoch, owner.to_owned()));
                Ok(())
            }
        }
    }
}

/// One participant: its node and its write path.
struct Member {
    id: NodeId,
    node: Node,
    group: Group,
    writes: HostedWrites<String>,
}

impl Member {
    async fn spawn(net: &Network, id: &str, anchor: &Arc<MemAnchor>) -> Self {
        let me = NodeId::new(id);
        let peers: Vec<NodeId> = IDS
            .iter()
            .filter(|peer| **peer != id)
            .map(|peer| NodeId::new(*peer))
            .collect();
        let mut builder = Node::builder(me.clone())
            .link(MemLink::new(net.endpoint(me.clone()), peers.clone()))
            .gossip_interval(Duration::from_millis(15));
        for peer in peers {
            builder = builder.seed(peer);
        }
        let node = builder.start().await.expect("the memory link binds");
        let anchor: Arc<dyn Anchor> = anchor.clone();
        let group = node.join_group_with(
            "docs",
            GroupProfile::External {
                lease_ms: LEASE_MS,
                steal_margin_ms: STEAL_MARGIN_MS,
                anchor: Some(anchor),
            },
        );
        // The Local regime: no `CommitLedger`, no roster, no recovery gate.
        let writes = HostedWrites::new(
            group.clone(),
            me.clone(),
            NonZeroUsize::new(32).expect("nonzero"),
            |doc: &String| doc.clone().into_bytes(),
        );
        Self {
            id: me,
            node,
            group,
            writes,
        }
    }

    fn is_host(&self) -> bool {
        self.group.leadership().role == Role::Host
    }

    /// A process death: the record is not released, it simply stops being
    /// renewed.
    async fn kill(self) {
        self.node.close().await;
    }
}

#[tokio::main]
async fn main() {
    let net = Network::new();
    let anchor = Arc::new(MemAnchor::default());
    let store = CasStore::default();
    let mut members = Vec::new();
    for id in IDS {
        members.push(Member::spawn(&net, id, &anchor).await);
    }

    let host = settle("the anchor to award an epoch", || {
        members.iter().position(Member::is_host)
    })
    .await;
    let held = anchor
        .record()
        .expect("a host activated, so a record exists");
    println!(
        "== {} won the anchor: epoch {}, one conditional PUT, no votes ==",
        held.host, held.epoch
    );
    for doc in ["doc:alpha", "doc:beta"] {
        claim(&members[host], &store, doc).await;
    }

    println!("\n== the host dies, and its record ages out ==");
    // A worker on the host captures its fence, then stalls before its store
    // write lands.
    let zombie = members[host]
        .writes
        .fence()
        .expect("the Local regime serves the instant the anchor says so");
    let dead = members.swap_remove(host);
    println!("  {} captured fence {zombie}, stalled, and died", dead.id);
    dead.kill().await;

    let successor = settle("a survivor to supersede the stale record", || {
        members.iter().find(|member| member.is_host())
    })
    .await;
    let record = anchor.record().expect("the successor wrote one");
    assert_eq!(
        record.epoch,
        held.epoch + 1,
        "a steal allocates exactly once"
    );
    assert_eq!(record.host, successor.id);
    println!(
        "  {} superseded it at epoch {}: If-Match on the stale record's etag",
        record.host, record.epoch
    );
    claim(successor, &store, "doc:gamma").await;
    match store.put_if_fence_ge("doc:gamma", zombie.epoch, zombie.host.as_str()) {
        Ok(()) => println!("  !! the zombie's write landed: the fence was not enforced"),
        Err(owner) => println!(
            "  the zombie's write under fence {zombie} is REFUSED: doc:gamma is owned at epoch {owner}"
        ),
    }

    println!("\n== the bucket ==");
    for (doc, (epoch, owner)) in store.0.lock().expect("lock").iter() {
        println!("  {doc} owned by {owner} under fence epoch {epoch}");
    }
}

/// Publish the claim into the host's serialized feed, then write it to the
/// store under the fence that authorized it — identical to `fenced-ownership`,
/// because the fence does not remember which activation allocated it.
async fn claim(host: &Member, store: &CasStore, doc: &str) {
    host.writes
        .publish(&doc.to_owned())
        .await
        .expect("the host serves");
    let fence = host.writes.fence().expect("still hosting");
    match store.put_if_fence_ge(doc, fence.epoch, host.id.as_str()) {
        Ok(()) => println!("  [{}] {doc} stored under fence {fence}", host.id),
        Err(owner) => println!(
            "  [{}] {doc}: store refused, owned at epoch {owner}",
            host.id
        ),
    }
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
