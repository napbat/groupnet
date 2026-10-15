//! Fenced ownership: an elected host commits document-ownership claims through
//! a voter majority and stamps every store write with its fence — so the
//! **store** refuses the one writer gossip cannot stop.
//!
//! Three nodes form a `Hosted` group with `Activation::Quorum` over all three as
//! voters. The host publishes claims through [`HostedWrites`] at
//! [`Commit::QuorumApplied`]; every voter runs the follower loop the tier asks
//! for — [`HostedReads`] into the apply, *then* [`CommitLedger::record`], and
//! [`CommitLedger::refresh`] on a migration. The `CasStore` stands in for S3 or
//! a database with conditional writes: every object remembers the fence epoch
//! that wrote it and refuses a lower one. What the run shows:
//!
//! * **Elected is not permission to serve.** A freshly activated host answers
//!   [`HostedError::Recovering`] until leader completeness holds; the caller
//!   retries (and this run says so whenever it had to).
//! * **A committed claim is stamped with the fence that authorized it.**
//! * **A deposed host's in-flight write is stopped by the store**, because the
//!   successor claimed the same key at a higher epoch.
//! * **A follower is redirected**, never left guessing:
//!   [`HostedError::NotHost`] names the host.
//!
//! Three `Node`s over the in-memory transport stand in for three machines.
//!
//! ```text
//! cargo run -p groupnet-consistency --example fenced-ownership --features hosted
//! ```

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::Group;
use groupnet_consistency::hosted::{
    Commit, CommitLedger, HostedError, HostedRead, HostedReads, HostedWrites,
};
use groupnet_core::{NodeId, Role, VoterRoster};
use groupnet_runtime::{GroupProfile, Node};
use groupnet_transport_mem::{MemLink, Network};
use tokio::task::JoinHandle;

/// The roster: three voters, so a majority (two) survives the host dying.
const IDS: [&str; 3] = ["doc-a", "doc-b", "doc-c"];

/// A host's authority after its last confirmed renewal.
const LEASE_MS: u64 = 600;

/// The deadline a committed write is given: a bound, not an expectation.
const PATIENT: Duration = Duration::from_secs(2);

const POLL: Duration = Duration::from_millis(5);

/// An ownership index: document -> owning node.
type Index = Arc<Mutex<BTreeMap<String, String>>>;

/// A store with conditional writes: `key -> (fence epoch, owner)`. Groupnet
/// never provides this — stores own truth, and the fence is where the two meet.
#[derive(Debug, Default)]
struct CasStore(Mutex<BTreeMap<String, (u64, String)>>);

impl CasStore {
    /// Writes `owner` at `key` unless a higher fence epoch already wrote it — the
    /// equivalent of an S3 `If-Match` on a CAS-claimed key. The *store*
    /// evaluates the fence, which is what makes the guarantee end-to-end.
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

    /// The ownership index rebuilt from the store.
    fn claims(&self) -> BTreeMap<String, String> {
        let objects = self.0.lock().expect("lock");
        objects
            .iter()
            .map(|(key, (_, owner))| (key.clone(), owner.clone()))
            .collect()
    }
}

/// One voter: its node, its write path, and its follower loop.
struct Member {
    id: NodeId,
    node: Node,
    group: Group,
    writes: HostedWrites<String>,
    index: Index,
    apply: JoinHandle<()>,
}

impl Member {
    async fn spawn(net: &Network, id: &str, store: &Arc<CasStore>) -> Self {
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
        let voters = VoterRoster::new(IDS.iter().map(|id| NodeId::new(*id)));
        let group = node.join_group_with(
            "docs",
            GroupProfile::Quorum {
                lease_ms: LEASE_MS,
                voters,
                storage: None,
            },
        );
        // The committed regime builds only on a Quorum group: `QuorumApplied`
        // counts that activation's static voter roster.
        let ledger = Arc::new(CommitLedger::new(group.clone()));
        let writes = HostedWrites::builder(
            group.clone(),
            me.clone(),
            NonZeroUsize::new(32).expect("nonzero"),
            |doc: &String| doc.clone().into_bytes(),
        )
        .committed(Arc::clone(&ledger))
        .build()
        .expect("a Quorum group supports the committed regime");
        let mut reads = HostedReads::new(group.clone(), me.clone(), |bytes: &[u8]| {
            String::from_utf8(bytes.to_vec()).ok()
        });
        // Serving cuts the predecessor's lineage: a dead host's late tail is
        // dropped rather than applied behind this node's own writes.
        writes.bind(&mut reads);
        let index = Index::default();
        let apply = tokio::spawn(follow(reads, ledger, Arc::clone(&index), Arc::clone(store)));
        Self {
            id: me,
            node,
            group,
            writes,
            index,
            apply,
        }
    }

    fn is_host(&self) -> bool {
        self.group.leadership().role == Role::Host
    }

    /// A process death, not a graceful leave.
    async fn kill(self) {
        self.apply.abort();
        self.node.close().await;
    }
}

/// The follower loop every voter must run: apply, *then* record. A voter that
/// votes without applying is invisible to both commit rules, and the tier fails
/// closed around it — commits time out naming it, and a new host stalls
/// recovering.
async fn follow(
    mut reads: HostedReads<String>,
    ledger: Arc<CommitLedger>,
    index: Index,
    store: Arc<CasStore>,
) {
    let mut host = None;
    while let Some(event) = reads.next().await {
        let applied = match event {
            HostedRead::Wrote {
                host: writer,
                token,
                key,
            } => {
                index.lock().expect("lock").insert(key, writer.to_string());
                Some((writer, token))
            }
            HostedRead::Gap { missed_through } => {
                // Coarse remediation: the store owns truth, so rebuild from it.
                *index.lock().expect("lock") = store.claims();
                host.clone().map(|host| (host, missed_through))
            }
            HostedRead::Migrated { host: adopted, .. } => {
                host = adopted;
                // Re-stamp, so a recovering host sees this voter's view is fresh
                // even while no writes arrive.
                ledger
                    .refresh()
                    .await
                    .expect("the group actor accepts the ledger");
                None
            }
        };
        if let Some((writer, token)) = applied {
            ledger
                .record(&writer, token)
                .await
                .expect("the group actor accepts the ledger");
        }
    }
}

#[tokio::main]
async fn main() {
    let net = Network::new();
    let store = Arc::new(CasStore::default());
    let mut members = Vec::new();
    for id in IDS {
        members.push(Member::spawn(&net, id, &store).await);
    }

    let host = settle("a host to activate", || {
        members.iter().position(Member::is_host)
    })
    .await;
    println!(
        "== {} activated; claims commit at QuorumApplied ==",
        members[host].id
    );
    for doc in ["doc:alpha", "doc:beta"] {
        claim(&members[host], &store, doc).await;
    }

    println!("\n== the zombie ==");
    // A worker on the host captures its fence, then stalls before its store
    // write lands.
    let zombie = members[host].writes.fence().expect("still hosting");
    // Demo determinism, not a tier requirement: both followers caught up, so
    // the successor's recovery target is one it already meets.
    settle("both followers to apply every claim", || {
        members
            .iter()
            .all(|member| member.is_host() || member.index.lock().expect("lock").len() == 2)
            .then_some(())
    })
    .await;
    let dead = members.swap_remove(host);
    println!("  {} captured fence {zombie}, stalled, and died", dead.id);
    dead.kill().await;

    let successor = settle("a successor to activate", || {
        members.iter().find(|member| member.is_host())
    })
    .await;
    claim(successor, &store, "doc:gamma").await;
    match store.put_if_fence_ge("doc:gamma", zombie.epoch, zombie.host.as_str()) {
        Ok(()) => println!("  !! the zombie's write landed: the fence was not enforced"),
        Err(owner) => println!(
            "  the zombie's write under fence {zombie} is REFUSED: doc:gamma is owned at epoch {owner}"
        ),
    }

    println!("\n== a follower asks to write ==");
    let follower = members
        .iter()
        .find(|member| !member.is_host())
        .expect("two survivors, one of them a follower");
    let refused = follower
        .writes
        .publish(&"doc:delta".to_owned())
        .await
        .expect_err("a follower may not write");
    println!("  {}: {refused}", follower.id);

    println!("\n== the bucket ==");
    for (doc, (epoch, owner)) in store.0.lock().expect("lock").iter() {
        println!("  {doc} owned by {owner} under fence epoch {epoch}");
    }
}

/// The whole docres shape: commit the claim through the host's serialized feed,
/// then write it to the store under the fence that authorized it.
async fn claim(host: &Member, store: &CasStore, doc: &str) {
    let mut announced = false;
    let receipt = loop {
        match host
            .writes
            .publish_committed(&doc.to_owned(), Commit::QuorumApplied, PATIENT)
            .await
        {
            // Elected is not permission to serve; the state is transient, so
            // retry.
            Err(HostedError::Recovering) => {
                if !announced {
                    println!("  [{}] elected, not serving yet: recovering", host.id);
                    announced = true;
                }
                tokio::time::sleep(POLL).await;
            }
            published => break published.expect("the host serves"),
        }
    };
    assert!(receipt.is_committed(), "{doc}: {:?}", receipt.outcome);
    // Taken after the commit: the fence is a snapshot, never a lock, and the
    // store is what evaluates it.
    let fence = host.writes.fence().expect("still hosting");
    match store.put_if_fence_ge(doc, fence.epoch, host.id.as_str()) {
        Ok(()) => println!(
            "  [{}] {doc} committed, stored under fence {fence}",
            host.id
        ),
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
        tokio::time::sleep(POLL).await;
    }
    panic!("timed out waiting for {what}");
}
