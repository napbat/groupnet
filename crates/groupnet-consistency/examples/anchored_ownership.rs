//! Anchored ownership: three nodes elect a host by **winning one conditional
//! write against an object store**, and the host stamps every ownership record
//! it writes with the fence that write bought it.
//!
//! This is [`fenced_ownership`]'s run with the election replaced. There the
//! epoch was closed by a majority of a static voter roster; here it is closed by
//! [`Activation::External`] — an `If-None-Match` / `If-Match` `PUT` against a
//! single object, and nothing in the fabric at all. Everything downstream of the
//! epoch is unchanged, which is the point: a fence is a fence whoever allocated
//! it, so the store-side rule that stops a zombie is the same rule.
//!
//! What the run shows, in order:
//!
//! * **No quorum, no votes, no roster — one conditional PUT.** The rendezvous
//!   top-ranked node prompts its driver, the driver creates the anchor record,
//!   and that node is host. Nothing was broadcast to decide it and nothing was
//!   persisted anywhere: the object *is* the ledger.
//! * **Ownership records carry the fence.** Each claim goes through the host's
//!   serialized feed and is then written to the `CasStore` under the epoch the
//!   anchor awarded — the docres shape, unchanged from `fenced_ownership`.
//! * **Succession is a steal, at a strictly higher epoch.** Kill the host and
//!   its record stops being renewed. A survivor supersedes it once it is
//!   `lease_ms + steal_margin_ms` stale, at epoch + 1 — and the dead host's
//!   in-flight write, still carrying the old fence, is refused by the **store**.
//! * **Connectivity to the anchor is the availability axis.** Stated in the
//!   closing lines, because it is what buys docres the *guaranteed-and-fast*
//!   quadrant: a host cut off from every peer but still able to reach the object
//!   keeps hosting, correctly, and a host that keeps every peer but loses the
//!   object lapses. Partitions of the fabric stop being leadership events.
//!
//! # Why this group runs [`HostedWrites::new`] and not [`HostedWrites::committed`]
//!
//! Deliberately the **Local** regime: no voter roster, and no leader-completeness
//! recovery gate before the successor may serve. Under `Quorum` that gate exists
//! because a majority of *voters* is what makes a write durable, so a new host
//! must prove it has read everything its predecessor committed. Here nothing in
//! the fabric ever agreed to anything — the anchor allocated the epoch and the
//! **store** holds the truth — so the recovery question has no one to ask and no
//! answer to wait for. A successor is serving the instant the anchor says it
//! holds the epoch, and correctness comes from the fence the store evaluates,
//! not from replicated state. That is exactly the trade docres wants: fencing
//! amortized onto storage I/O it already pays for.
//!
//! Three `Node`s over the in-memory transport stand in for three machines, and
//! [`MemAnchor`] stands in for one S3 key — swap either and nothing else here
//! changes.
//!
//! ```text
//! cargo run -p groupnet-consistency --example anchored_ownership --features hosted
//! ```
//!
//! [`fenced_ownership`]: https://docs.rs/groupnet-consistency
//! [`Activation::External`]: groupnet_core::Activation::External

use std::collections::HashMap;
use std::io;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::{Fence, HostedRead, HostedReads, HostedWrites};
use groupnet_core::anchor::AnchorRecord;
use groupnet_core::{Activation, HostedConfig, NodeId};
use groupnet_runtime::{
    Anchor, AnchorCas, AnchorFuture, AnchorToken, AnchorWriteIf, Group, GroupProfile, Node, Role,
};
use groupnet_transport_mem::{MemTransport, Network};
use tokio::task::JoinHandle;

/// The hosted group: one shard's worth of documents.
const GROUP: &str = "docs";

/// The three nodes. Which of them hosts is not a coin toss — the rendezvous
/// ranking over these ids and this group name decides who is a *candidate*, and
/// the anchor decides whether the candidate becomes host.
const IDS: [&str; 3] = ["own-a", "own-b", "own-c"];

/// The documents the first host claims, before it dies.
const CLAIMS: [&str; 2] = ["doc:alpha", "doc:beta"];

/// Brisk gossip, so a demo does not spend its life waiting for a prompt. It also
/// sets the anti-entropy cadence, which is the cadence the anchor prompt rides.
const GOSSIP_MS: u64 = 15;

/// One number wearing two hats, as `HostedConfig::lease_ms` documents under this
/// activation: the anchor record's TTL (what a *successor's* clock judges) and
/// the engine lease this host steps down on (what its *own* clock judges). It is
/// also the boot guard, so nothing is elected in the first `LEASE_MS` of the
/// run.
const LEASE_MS: u64 = 600;

/// How far past a record's expiry a claimant must wait before it may supersede
/// it — the margin that absorbs the **pairwise** wall-clock disagreement between
/// the holder that stamped the expiry and the claimant reading it. Every node
/// here shares one process clock, so the margin is pure latency in this run; in
/// a deployment it is the one assumption the tier makes.
const STEAL_MARGIN_MS: u64 = 200;

/// The hosted feed's ring: far larger than this run's write count, so no `Gap`
/// here is ever a ring overflow rather than a migration.
const RING: usize = 32;

/// Polling budget for [`settle`]: 2000 × 5 ms. Generous, because a succession
/// here waits out a whole record TTL plus the steal margin *and* the detector.
const POLL: Duration = Duration::from_millis(5);
const POLLS: usize = 2000;

// ---------------------------------------------------------------------------
// The anchor — one object, and this is all of it.
// ---------------------------------------------------------------------------

/// The external CAS anchor, in memory. **This is all S3 needs to be**, and that
/// is the reason this type is in the example rather than hidden in a helper.
///
/// A `GET` that returns the record together with the version marker the store
/// put on it, and a `PUT` conditional on that marker. Swap the [`Mutex`] for an
/// S3 client, the [`String`] etag for the response's `ETag`,
/// [`AnchorWriteIf::Absent`] for `If-None-Match: *` and
/// [`AnchorWriteIf::Matches`] for `If-Match: <etag>`, and **nothing else in this
/// file changes** — not the profile, not the write path, not the fence. There is
/// no lock service here, no session, no lease API and no callback to register:
/// the tier is written against the smallest thing every object store already
/// does, so a deployment on etcd or `ZooKeeper` adapts *down* to it.
///
/// One object, one group. A process hosting several `External` groups gives each
/// its own `Anchor` over its own key; two groups sharing a key would share one
/// epoch sequence and fight, and no rule in the tier can detect that.
#[derive(Debug, Default)]
struct MemAnchor {
    /// The object: the record it holds and the version it stands at, or `None`
    /// for an object that does not exist yet.
    object: Mutex<Option<(AnchorRecord, String)>>,
    /// The next version marker. A counter rather than a hash of the record, so
    /// two writes of identical bytes still get distinct etags — which is what a
    /// renewal's precondition actually depends on.
    next_etag: AtomicU64,
    /// Every write that **allocated** an epoch, and how. Renewals are not here:
    /// a renewal decides nothing, so it allocates nothing. This is the whole
    /// coordination history of the run.
    allocations: Mutex<Vec<(u64, NodeId, &'static str)>>,
}

impl MemAnchor {
    /// What the object holds right now — ground truth, readable without asking
    /// any node.
    fn record(&self) -> Option<AnchorRecord> {
        self.object
            .lock()
            .expect("lock")
            .as_ref()
            .map(|(record, _)| record.clone())
    }

    /// The epochs this object has handed out, in order.
    fn allocations(&self) -> Vec<(u64, NodeId, &'static str)> {
        self.allocations.lock().expect("lock").clone()
    }
}

impl Anchor for MemAnchor {
    fn load(&self) -> AnchorFuture<'_, io::Result<Option<(AnchorRecord, AnchorToken)>>> {
        Box::pin(async move {
            // A 404 is `Ok(None)`: an absent anchor is a state, not an error.
            Ok(self
                .object
                .lock()
                .expect("lock")
                .as_ref()
                .map(|(record, etag)| (record.clone(), AnchorToken::new(etag.clone()))))
        })
    }

    fn store(
        &self,
        pre: AnchorWriteIf,
        record: AnchorRecord,
    ) -> AnchorFuture<'_, io::Result<AnchorCas>> {
        Box::pin(async move {
            let mut object = self.object.lock().expect("lock");
            let allowed = match (&pre, object.as_ref()) {
                (AnchorWriteIf::Absent, None) => true,
                (AnchorWriteIf::Matches(token), Some((_, etag))) => token.as_str() == etag,
                (AnchorWriteIf::Absent, Some(_)) | (AnchorWriteIf::Matches(_), None) => false,
            };
            if !allowed {
                return Ok(AnchorCas::Mismatch); // a 412: definite, and always "not you"
            }
            if object
                .as_ref()
                .is_none_or(|(current, _)| current.epoch != record.epoch)
            {
                let how = if object.is_none() {
                    "created, If-None-Match: *"
                } else {
                    "superseded, If-Match: <etag>"
                };
                self.allocations.lock().expect("lock").push((
                    record.epoch,
                    record.host.clone(),
                    how,
                ));
            }
            let etag = self.next_etag.fetch_add(1, Ordering::Relaxed).to_string();
            *object = Some((record, etag.clone()));
            Ok(AnchorCas::Stored(AnchorToken::new(etag)))
        })
    }
}

// ---------------------------------------------------------------------------
// The external store — the only thing in this run that can stop a zombie.
// ---------------------------------------------------------------------------

/// A mock object store with fenced writes: `key -> (fence epoch, value)`.
///
/// The same device as `fenced_ownership`'s, unchanged on purpose. Groupnet does
/// not provide this and never will — gossip carries liveness, stores own truth —
/// and the fence token is where the two meet. Note what it never learns: it has
/// no idea an anchor exists, and it would evaluate a `Quorum`-allocated epoch
/// with the identical rule.
#[derive(Debug, Default)]
struct CasStore {
    objects: Mutex<HashMap<String, (u64, String)>>,
}

impl CasStore {
    /// Writes `value` at `key` if `fence_epoch` is at or above the epoch that
    /// last wrote it; otherwise refuses, naming the epoch that owns the key.
    fn put_if_fence_ge(&self, key: &str, fence_epoch: u64, value: &str) -> Result<(), u64> {
        let mut objects = self.objects.lock().expect("lock");
        match objects.get(key) {
            Some((stored, _)) if *stored > fence_epoch => Err(*stored),
            _ => {
                objects.insert(key.to_owned(), (fence_epoch, value.to_owned()));
                Ok(())
            }
        }
    }

    /// Every object, in key order — what an operator would see in the bucket.
    fn dump(&self) -> Vec<(String, u64, String)> {
        let objects = self.objects.lock().expect("lock");
        let mut rows: Vec<(String, u64, String)> = objects
            .iter()
            .map(|(key, (epoch, value))| (key.clone(), *epoch, value.clone()))
            .collect();
        rows.sort();
        rows
    }
}

// ---------------------------------------------------------------------------
// One node's whole participation.
// ---------------------------------------------------------------------------

/// One participant. Every field is an `Option` because [`Member::kill`] takes
/// them away — a process death, not a graceful leave, so the anchor record is
/// left to age out rather than being released.
struct Member {
    id: NodeId,
    net: Network,
    node: Option<Node<MemTransport>>,
    group: Option<Group>,
    writes: Option<HostedWrites<String>>,
    apply: Option<JoinHandle<()>>,
    /// This node's *applied* replica of the ownership index: doc -> owner.
    index: Arc<Mutex<HashMap<String, String>>>,
}

impl Member {
    /// Brings one node up, joined to [`GROUP`] under the `External` profile and
    /// carrying its handle on the shared anchor object.
    fn spawn(net: &Network, id: &str, anchor: &Arc<MemAnchor>) -> Self {
        let me = NodeId::new(id);
        let mut builder =
            Node::builder(me.clone(), net.endpoint(me.clone())).gossip_interval_ms(GOSSIP_MS);
        for seed in IDS.iter().filter(|other| **other != id) {
            builder = builder.seed(NodeId::new(*seed));
        }
        let node = builder.spawn();
        let anchor: Arc<dyn Anchor> = anchor.clone();
        let group = node.join_group_with(
            GROUP,
            GroupProfile::hosted(HostedConfig {
                activation: Activation::External {
                    steal_margin_ms: STEAL_MARGIN_MS,
                },
                lease_ms: LEASE_MS,
            })
            .with_anchor(anchor),
        );
        // The **Local** regime: no `CommitLedger`, no roster, no recovery gate.
        // See this file's header for why that is the right shape here and the
        // wrong one under `Quorum` — the anchor is the authority, and the store
        // is the truth.
        let writes = HostedWrites::new(
            group.clone(),
            me.clone(),
            NonZeroUsize::new(RING).expect("nonzero"),
            |doc: &String| doc.clone().into_bytes(),
        );
        Self {
            id: me,
            net: net.clone(),
            node: Some(node),
            group: Some(group),
            writes: Some(writes),
            apply: None,
            index: Arc::default(),
        }
    }

    /// The follower loop: apply the host's ownership records into this node's
    /// own replica. Nothing here votes and nothing here is waited on — with
    /// `Commit::Local` a write is acknowledged the instant it is in the feed —
    /// so this is a *replica*, not a quorum.
    fn follow(&mut self) {
        let mut reads =
            HostedReads::new(self.group().clone(), self.id.clone(), |bytes: &[u8]| {
                String::from_utf8(bytes.to_vec()).ok()
            });
        // Ties this subscriber to this node's own serving epoch, so a dead
        // host's late tail cannot land behind writes this node has authored.
        self.writes().bind(&mut reads);
        let index = Arc::clone(&self.index);
        self.apply = Some(tokio::spawn(async move {
            while let Some(event) = reads.next().await {
                match event {
                    HostedRead::Wrote { host, key, .. } => {
                        index
                            .lock()
                            .expect("lock")
                            .insert(key, host.as_str().to_owned());
                    }
                    // A `Gap` opens on every migration; the store owns truth, so
                    // a real consumer rebuilds its index from the bucket here.
                    // `Migrated` is the handover reaching a follower.
                    HostedRead::Gap { .. } | HostedRead::Migrated { .. } => {}
                }
            }
        }));
    }

    fn group(&self) -> &Group {
        self.group.as_ref().expect("this member is alive")
    }

    fn writes(&self) -> &HostedWrites<String> {
        self.writes.as_ref().expect("this member is alive")
    }

    fn is_host(&self) -> bool {
        self.group
            .as_ref()
            .is_some_and(|group| group.leadership().role == Role::Host)
    }

    /// How many claims this node's own apply loop has landed.
    fn applied_claims(&self) -> usize {
        self.index.lock().expect("lock").len()
    }

    /// Kills the node outright. Dropping the handles is not enough on its own:
    /// the receive loop holds an `Arc` of the same inner state, so the actors
    /// keep ticking until the endpoint is evicted from the `Network`
    /// (registering the id again replaces the sender and closes the old inbox).
    /// The anchor task dies with them, which is precisely why the record it was
    /// renewing now goes stale instead of being kept alive by a zombie.
    async fn kill(&mut self) {
        if let Some(apply) = self.apply.take() {
            apply.abort();
            let _ = apply.await;
        }
        self.writes = None;
        self.group = None;
        self.node = None;
        drop(self.net.endpoint(self.id.clone()));
    }
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let net = Network::new();
    let anchor = Arc::new(MemAnchor::default());
    let store = Arc::new(CasStore::default());
    let mut members: Vec<Member> = IDS
        .iter()
        .map(|id| Member::spawn(&net, id, &anchor))
        .collect();
    for member in &mut members {
        member.follow();
    }

    let host = act1_claim(&members, &anchor).await;
    act2_ownership(&members[host], &store).await;
    act3_steal(&mut members, host, &anchor, &store).await;
    act4_moral(&anchor, &store);
}

/// One conditional PUT closes an epoch. That is the whole election.
async fn act1_claim(members: &[Member], anchor: &MemAnchor) -> usize {
    println!("== three nodes, one hosted group \"{GROUP}\" ==");
    println!("  activation: External — the epoch comes from one CAS object");
    println!("  no voter roster, no grants, no persisted ledger: the object is the ledger");
    let host = settle("the anchor to award an epoch", || {
        members.iter().position(Member::is_host)
    })
    .await;
    let lead = members[host].group().leadership();
    let record = anchor
        .record()
        .expect("a host activated, so a record exists");
    println!(
        "  {} is host at epoch {} — and it took one conditional PUT to decide",
        members[host].id, lead.epoch
    );
    println!(
        "  the object now reads: epoch {}, host {}, expiring {LEASE_MS}ms out on the \
         holder's own wall clock",
        record.epoch, record.host
    );
    println!(
        "  (that expiry is the only wall-clock number in the tier — every *other* node \
         judges it against its own clock, which is what the steal margin absorbs)"
    );
    println!(
        "  nothing was broadcast to agree this; the candidate was the rendezvous \
         top-ranked node and the store did the rest"
    );
    // The Local regime has no recovery gate: elected *is* permission to serve,
    // because there is no replicated commit for a successor to be complete for.
    let fence = members[host]
        .writes()
        .fence()
        .expect("the Local regime serves the instant the anchor says so");
    println!("  serving immediately under fence {fence} — no recovery gate in this regime");
    host
}

/// Ownership claims: through the host's serialized feed, then into the store
/// under the fence the anchor bought.
async fn act2_ownership(host: &Member, store: &CasStore) {
    println!("\n== act 2: ownership records, carried by the fence ==");
    for doc in CLAIMS {
        claim(host, store, doc).await;
    }
    println!("  every object in the bucket now names the epoch that wrote it");
}

/// The whole docres shape in one function: publish the claim into the host's
/// serialized feed, then write it to the store under the fence that authorized
/// it. Identical to `fenced_ownership`'s, and that is the point — the fence does
/// not remember which activation allocated it.
async fn claim(host: &Member, store: &CasStore, doc: &str) {
    let token = host
        .writes()
        .publish(&doc.to_owned())
        .await
        .expect("the host serves");
    // Taken *after* the write and stamped onto the store operation: the fence is
    // a snapshot, never a lock, and the store is what evaluates it.
    let fence = host.writes().fence().expect("still hosting");
    println!("  [{}] {doc}: token {token:?}", host.id);
    match store.put_if_fence_ge(doc, fence.epoch, host.id.as_str()) {
        Ok(()) => println!("         store accepted the claim under fence {fence}"),
        Err(owner) => println!("         store refused: {doc} is owned at epoch {owner}"),
    }
}

/// The host dies. Its record is not released — it simply stops being renewed,
/// and a survivor supersedes it once it is stale enough to be entitled to.
async fn act3_steal(
    members: &mut [Member],
    host: usize,
    anchor: &MemAnchor,
    store: &Arc<CasStore>,
) {
    println!("\n== act 3: the host dies, and the record ages out ==");
    settle("both followers to apply every claim", || {
        members
            .iter()
            .enumerate()
            .all(|(index, member)| index == host || member.applied_claims() == CLAIMS.len())
            .then_some(())
    })
    .await;

    let zombie: Fence = members[host].writes().fence().expect("still hosting");
    let zombie_id = members[host].id.clone();
    println!("  a worker on {zombie_id} captured fence {zombie} for doc:gamma,");
    println!("  then stalled mid-operation — its store write has not landed yet");

    let held = anchor.record().expect("the host holds the record");
    members[host].kill().await;
    println!("  {zombie_id} dies: handles dropped, endpoint evicted, anchor task gone");
    println!(
        "  its record survives it, at epoch {} — a crash releases nothing, so a \
         successor waits out the TTL plus {STEAL_MARGIN_MS}ms of steal margin",
        held.epoch
    );

    let next = settle("a survivor to supersede the stale record", || {
        members.iter().position(Member::is_host)
    })
    .await;
    let lead = members[next].group().leadership();
    let record = anchor.record().expect("the successor wrote one");
    println!(
        "  {} superseded it at epoch {} — exactly one above the record it took, \
         because the anchor allocates and never reissues",
        members[next].id, lead.epoch
    );
    assert_eq!(record.epoch, held.epoch + 1, "a steal allocates once");
    assert_eq!(record.host, members[next].id);

    // The successor claims the very key the zombie is about to write.
    claim(&members[next], store, "doc:gamma").await;

    println!("  ...and now the zombie's in-flight write finally reaches the store:");
    match store.put_if_fence_ge("doc:gamma", zombie.epoch, zombie_id.as_str()) {
        Ok(()) => println!("  !! accepted — the fence was not enforced, and truth just forked"),
        Err(owner) => println!(
            "  REFUSED: {zombie_id} wrote under fence {zombie}, but doc:gamma is \
             owned at epoch {owner}"
        ),
    }
    println!("  nothing in the fabric stopped that write; the epoch the store held did.");
}

/// What the run was actually about.
fn act4_moral(anchor: &MemAnchor, store: &CasStore) {
    println!("\n== the anchor, at the end ==");
    for (epoch, host, how) in anchor.allocations() {
        println!("  epoch {epoch} -> {host} ({how})");
    }
    println!(
        "  every other write this run made against the object was a renewal at the \
         same epoch: a renewal decides nothing, so it allocates nothing"
    );

    println!("\n== the bucket, at the end ==");
    for (key, epoch, owner) in store.dump() {
        println!("  {key} owned by {owner}, written under fence epoch {epoch}");
    }

    println!("\n== the moral: the anchor is the availability axis ==");
    println!(
        "  A host cut off from every peer but still able to reach that object keeps\n  \
         renewing and keeps hosting — correctly, because nothing else can take the\n  \
         epoch from it. A host that keeps every peer and loses the object cannot\n  \
         renew, so its lease lapses and it demotes. Partitions of the fabric stop\n  \
         being leadership events at all; connectivity to the store is the axis."
    );
    println!(
        "  That is the guaranteed-and-fast quadrant of the dial, and why it is cheap:\n  \
         a fence costs a `PUT` on a store the application was already writing to."
    );
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
