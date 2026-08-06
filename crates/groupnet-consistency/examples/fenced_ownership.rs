//! Fenced ownership: an elected host claims document-ownership records, commits
//! them through a voter majority, and stamps every store write with its fence —
//! so the **store** refuses the one writer gossip cannot stop.
//!
//! Three nodes form a `Hosted` group with `Activation::Quorum` over all three as
//! voters. One is elected host; it publishes ownership claims through
//! [`HostedWrites`] at [`Commit::QuorumApplied`], and every voter runs the
//! follower loop the tier asks for — [`HostedReads`] into the apply, then
//! [`CommitLedger::record`], and [`CommitLedger::refresh`] on a migration.
//!
//! The `CasStore` here stands in for S3/R2 (or docres's own store): every object
//! remembers the fence epoch that wrote it, and refuses a write stamped below
//! it. That one rule is the point of the run:
//!
//! * **Elected is not permission to serve.** A host that has just activated
//!   answers [`HostedError::Recovering`] until the leader-completeness rule says
//!   its applied state contains everything the predecessor committed.
//! * **A doomed writer is stopped by the store, not by the fabric.** A worker
//!   holding a fence from the dead host still issues its write — and the store
//!   rejects it, because the successor claimed the same key at a higher epoch.
//! * **A follower is redirected, never left guessing:**
//!   [`HostedError::NotHost`] names where to go.
//! * **Serving cuts the predecessor's lineage.** `HostedWrites::bind` ties the
//!   successor's subscriber to its own serving epoch, so the dead host's
//!   un-replicated tail — which no subscriber of its own writes can ever fence
//!   for it — is dropped rather than applied behind the writes it authored.
//!
//! Three `Node`s over the in-memory transport stand in for three machines; swap
//! it for `groupnet-transport-udp` and nothing here changes.
//!
//! ```text
//! cargo run -p groupnet-consistency --example fenced_ownership --features hosted
//! ```

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::{
    Commit, CommitLedger, Completeness, Fence, HostedError, HostedRead, HostedReads, HostedWrites,
};
use groupnet_core::{Activation, HostedConfig, NodeId, VoterRoster};
use groupnet_runtime::{Group, GroupProfile, Node, Role};
use groupnet_transport_mem::{MemTransport, Network};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The hosted group: one shard's worth of documents.
const GROUP: &str = "docs";

/// The roster. All three are voters, so a majority is two — which is what
/// survives the host dying halfway through the run.
const IDS: [&str; 3] = ["doc-a", "doc-b", "doc-c"];

/// The documents the first host claims, before it dies.
const CLAIMS: [&str; 2] = ["doc:alpha", "doc:beta"];

/// Brisk gossip, so a demo does not spend its life waiting for a grant round.
const GOSSIP_MS: u64 = 15;

/// A host's authority after its last confirmed renewal — and, in this
/// storage-free posture (no `GrantStore`), also the blackout a rebooted voter
/// sits out before it will grant a new claimant.
const LEASE_MS: u64 = 600;

/// The hosted feed's ring: far larger than this run's write count, so no `Gap`
/// here is ever a ring overflow rather than a migration.
const RING: usize = 32;

/// The deadline a committed write is given. A bound, not an expectation: the
/// healthy path costs one ack round.
const PATIENT: Duration = Duration::from_secs(2);

/// Polling budget for [`settle`]: 2000 × 5 ms, generous enough for a whole
/// migration *plus* the successor's recovery. The cadence is well inside one
/// gossip round on purpose — it is what lets act 3 actually *observe* the
/// elected-but-not-yet-serving window rather than blink past it.
const POLL: Duration = Duration::from_millis(5);
const POLLS: usize = 2000;

// ---------------------------------------------------------------------------
// The external store — the only thing in this run that can stop a zombie.
// ---------------------------------------------------------------------------

/// A mock object store with conditional writes: `key -> (fence epoch, value)`.
///
/// Groupnet does not provide this and never will — gossip carries liveness and
/// coherence signals, stores own truth. The fence token is how the two meet.
#[derive(Debug, Default)]
struct CasStore {
    objects: Mutex<HashMap<String, (u64, String)>>,
}

impl CasStore {
    /// Writes `value` at `key` if `fence_epoch` is at or above the epoch that
    /// last wrote it; otherwise refuses, naming the epoch that owns the key.
    ///
    /// The equivalent of an S3 `If-Match` on a CAS-claimed key: the *store*
    /// evaluates the fence, which is what makes the guarantee end-to-end.
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

    /// The ownership index rebuilt from the store: what a follower does when a
    /// `Gap` tells it its own replica may be incomplete.
    fn claims(&self) -> HashMap<String, String> {
        self.objects
            .lock()
            .expect("lock")
            .iter()
            .map(|(key, (_, value))| (key.clone(), value.clone()))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// One node's whole participation: group, ledger, follower loop, write path.
// ---------------------------------------------------------------------------

/// One participant. Every field is an `Option` because [`Member::kill`] takes
/// them away — a process death, not a graceful leave.
struct Member {
    id: NodeId,
    net: Network,
    node: Option<Node<MemTransport>>,
    group: Option<Group>,
    ledger: Option<Arc<CommitLedger>>,
    writes: Option<HostedWrites<String>>,
    apply: Option<JoinHandle<()>>,
    /// Holds the apply loop without losing its subscription — a voter that has
    /// stopped applying, which is the failure the tier's contract is about.
    gate: watch::Sender<bool>,
    /// This node's *applied* replica of the ownership index: doc -> owner.
    index: Arc<Mutex<HashMap<String, String>>>,
}

impl Member {
    /// Brings one node up, joined to [`GROUP`] under the Quorum profile.
    fn spawn(net: &Network, id: &str) -> Self {
        let me = NodeId::new(id);
        let mut builder =
            Node::builder(me.clone(), net.endpoint(me.clone())).gossip_interval_ms(GOSSIP_MS);
        for seed in IDS.iter().filter(|other| **other != id) {
            builder = builder.seed(NodeId::new(*seed));
        }
        let node = builder.spawn();
        let voters = VoterRoster::new(IDS.iter().map(|id| NodeId::new(*id)));
        let group = node.join_group_with(
            GROUP,
            GroupProfile::hosted(HostedConfig {
                activation: Activation::Quorum { voters },
                lease_ms: LEASE_MS,
            }),
        );
        let ledger = Arc::new(CommitLedger::new(group.clone()));
        // The committed regime: it refuses to build at all unless the group is
        // Hosted with Quorum activation, because `QuorumApplied`'s denominator
        // is that activation's static voter roster. (`CAP_HOSTED` is advisory
        // and matters only for `Commit::AllApplied`, whose set is rumour-derived
        // — a static roster cannot be moved by an advertisement.)
        let writes = HostedWrites::committed(
            group.clone(),
            me.clone(),
            NonZeroUsize::new(RING).expect("nonzero"),
            |doc: &String| doc.clone().into_bytes(),
            Arc::clone(&ledger),
        )
        .expect("a Quorum group supports the committed regime");
        Self {
            id: me,
            net: net.clone(),
            node: Some(node),
            group: Some(group),
            ledger: Some(ledger),
            writes: Some(writes),
            apply: None,
            gate: watch::channel(true).0,
            index: Arc::default(),
        }
    }

    /// The follower loop the tier asks of **every** voter. A voter that votes
    /// without applying is invisible to both rules, and the tier fails closed
    /// around it: commits time out naming it, and a new host stalls recovering.
    fn follow(&mut self, store: &Arc<CasStore>) {
        let mut reads =
            HostedReads::new(self.group().clone(), self.id.clone(), |bytes: &[u8]| {
                String::from_utf8(bytes.to_vec()).ok()
            });
        // The one builder step the loop below cannot do for itself: this
        // subscriber never sees this node's own writes, so nothing delivered
        // through it can ever close the *predecessor's* lineage. Bound to the
        // write path, it closes at the instant this node is admitted to serve —
        // and a dead host's late tail, arriving afterwards, dies instead of
        // landing behind the writes this host has already authored.
        self.writes().bind(&mut reads);
        let ledger = Arc::clone(self.ledger());
        let index = Arc::clone(&self.index);
        let store = Arc::clone(store);
        let mut gate = self.gate.subscribe();
        self.apply = Some(tokio::spawn(async move {
            // The lineage's host, as the last `Migrated` named it: a `Gap`
            // belongs to the lineage, not to a peer.
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
                        // The apply happens *before* the record — that ordering
                        // is the whole meaning of a watermark.
                        index
                            .lock()
                            .expect("lock")
                            .insert(key, writer.as_str().to_owned());
                        ledger.record(&writer, token).await;
                    }
                    HostedRead::Gap { missed_through } => {
                        // Coarse remediation: the store owns truth, so rebuild
                        // the replica from it rather than replaying writes.
                        *index.lock().expect("lock") = store.claims();
                        if let Some(host) = &host {
                            ledger.record(host, missed_through).await;
                        }
                    }
                    HostedRead::Migrated { host: adopted, .. } => {
                        host = adopted;
                        // Re-stamp, so a recovering host can see this voter's
                        // view is *fresh* even while no writes are arriving.
                        ledger.refresh().await;
                    }
                }
            }
        }));
    }

    fn group(&self) -> &Group {
        self.group.as_ref().expect("this member is alive")
    }

    fn ledger(&self) -> &Arc<CommitLedger> {
        self.ledger.as_ref().expect("this member is alive")
    }

    fn writes(&self) -> &HostedWrites<String> {
        self.writes.as_ref().expect("this member is alive")
    }

    fn is_host(&self) -> bool {
        self.group
            .as_ref()
            .is_some_and(|group| group.leadership().role == Role::Host)
    }

    /// Stops applying without losing the subscription, and resumes.
    fn hold(&self, applying: bool) {
        self.gate.send_replace(applying);
    }

    /// How many claims this node's own apply loop has landed.
    fn applied_claims(&self) -> usize {
        self.index.lock().expect("lock").len()
    }

    /// Kills the node outright. Dropping the handles is not enough on its own:
    /// the receive loop holds an `Arc` of the same inner state, so the actors
    /// keep ticking until the endpoint is evicted from the `Network`
    /// (registering the id again replaces the sender and closes the old inbox).
    async fn kill(&mut self) {
        if let Some(apply) = self.apply.take() {
            apply.abort();
            let _ = apply.await;
        }
        self.writes = None;
        self.ledger = None;
        self.group = None;
        self.node = None;
        drop(self.net.endpoint(self.id.clone()));
    }
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let net = Network::new();
    let store = Arc::new(CasStore::default());
    let mut members: Vec<Member> = IDS.iter().map(|id| Member::spawn(&net, id)).collect();
    for member in &mut members {
        member.follow(&store);
    }

    let host = act1_election(&members).await;
    act2_claims(&members[host], &store).await;
    act3_zombie(&mut members, host, &store).await;
    act4_redirect(&members).await;

    println!("\n== the bucket, at the end ==");
    for (key, epoch, owner) in store.dump() {
        println!("  {key} owned by {owner}, written under fence epoch {epoch}");
    }
}

/// An election, and the recovery that gates *service* rather than election.
async fn act1_election(members: &[Member]) -> usize {
    println!("== three voters, one hosted group \"{GROUP}\" ==");
    println!("  activation: Quorum over all three; commits at QuorumApplied");
    let host = settle("an epoch to close", || {
        members.iter().position(Member::is_host)
    })
    .await;
    let lead = members[host].group().leadership();
    println!("  {} activated at epoch {}", members[host].id, lead.epoch);

    // Elected is not permission to serve: the write path refuses until the
    // leader-completeness rule is satisfied for this epoch.
    let fence = settle("the host to finish recovering", || {
        members[host].writes().fence()
    })
    .await;
    println!("  recovery complete — serving under fence {fence}");
    host
}

/// Ownership claims: committed through a voter majority, then stamped into the
/// store under the host's fence.
async fn act2_claims(host: &Member, store: &CasStore) {
    println!("\n== act 2: ownership records, carried by the fence ==");
    for doc in CLAIMS {
        claim(host, store, doc).await;
    }
    println!("  every object in the bucket now names the epoch that wrote it");
}

/// The whole docres shape in one function: commit the claim through the host's
/// serialized feed, then write it to the store under the fence that authorized
/// it.
async fn claim(host: &Member, store: &CasStore, doc: &str) {
    let receipt = host
        .writes()
        .publish_committed(&doc.to_owned(), Commit::QuorumApplied, PATIENT)
        .await
        .expect("the host serves");
    // Taken *after* the commit and stamped onto the store write: the fence is a
    // snapshot, never a lock, and the store is what evaluates it.
    let fence = host.writes().fence().expect("still hosting");
    println!(
        "  [{}] {doc}: token {:?}, {:?}",
        host.id, receipt.token, receipt.outcome
    );
    match store.put_if_fence_ge(doc, fence.epoch, host.id.as_str()) {
        Ok(()) => println!("         store accepted the claim under fence {fence}"),
        Err(owner) => println!("         store refused: {doc} is owned at epoch {owner}"),
    }
}

/// The zombie: a worker holding a fence from a host that is about to die — and
/// the successor's `Recovering` → serving transition it outlives.
async fn act3_zombie(members: &mut [Member], host: usize, store: &CasStore) {
    println!("\n== act 3: the zombie ==");
    // Both followers caught up before the kill, so the successor's recovery
    // target is one it already meets. A majority commit only guarantees *one*
    // of them; this barrier is the demo making the migration reproducible, not
    // the tier requiring it.
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

    // The survivors' apply loops are held across the hand-over: a migration
    // really does catch voters mid-apply, and holding them is what lets this
    // run *show* the recovery gate rather than blink past it.
    for member in &*members {
        member.hold(false);
    }
    members[host].kill().await;
    println!("  {zombie_id} dies: handles dropped, endpoint evicted");

    let next = settle("a successor to activate", || {
        members.iter().position(Member::is_host)
    })
    .await;
    let epoch = members[next].group().leadership().epoch;
    match members[next].writes().recovery() {
        Some(Completeness::Recovering { needed }) if needed.is_empty() => println!(
            "  {} is host at epoch {epoch} — and will not serve: no *fresh* \
             majority has been read at all, so it waits on gossip, not on its \
             own apply loop",
            members[next].id
        ),
        Some(Completeness::Recovering { needed }) => println!(
            "  {} is host at epoch {epoch} — and will not serve: {} writer \
             feed(s) still to drain",
            members[next].id,
            needed.len()
        ),
        _ => println!("  {} is host at epoch {epoch}", members[next].id),
    }
    if members[next].writes().fence().is_none() {
        let refused = members[next]
            .writes()
            .publish(&"doc:gamma".to_owned())
            .await
            .expect_err("an unrecovered host refuses every write, Local included");
        println!("  a write attempted right now: {refused}");
    }

    // Let the voters catch up. They re-stamp their ledgers at the new epoch,
    // the successor reads the fresh majority the rule needs, and the gate opens.
    for member in &*members {
        member.hold(true);
    }
    let fence = settle("the successor to finish recovering", || {
        members[next].writes().fence()
    })
    .await;
    println!(
        "  Recovering -> serving under fence {fence}; its replica holds {} \
         claim(s) — that is what recovery proved",
        members[next].applied_claims()
    );

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
    println!("  gossip cannot stop a doomed writer's disk I/O; the fence can.");
}

/// A non-host is told where to go, not left to guess.
async fn act4_redirect(members: &[Member]) {
    println!("\n== act 4: a follower asks to write ==");
    let follower = members
        .iter()
        .position(|member| member.group.is_some() && !member.is_host())
        .expect("two survivors, one of them a follower");
    let refused = members[follower]
        .writes()
        .publish(&"doc:delta".to_owned())
        .await
        .expect_err("a follower may not write");
    println!(
        "  [{}] publish(doc:delta) -> {refused}",
        members[follower].id
    );
    match refused {
        HostedError::NotHost { host: Some(to), .. } => println!(
            "  the refusal carries the redirect: send the caller to {to}. A \
             `host: None` there would be the promised NoLeader — fail fast on \
             it rather than waiting for a host a minority cannot elect."
        ),
        other => println!("  {other} — nothing to redirect to"),
    }
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
