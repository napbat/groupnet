//! Read-your-writes across two nodes: one writes, the other serves reads out of
//! a local cache, and a session token makes the read honest.
//!
//! `node-a` owns the records. `node-b` answers reads from a cache that can go
//! stale the moment `node-a` writes. The fix is not a lock and not a quorum: it
//! is a [`WriteFeed`] on the writer, a `PeerWrites` apply loop on the reader, and
//! a [`Frontier`] the reader barriers on before serving.
//!
//! The point the run makes twice over:
//!
//! * The frontier means **applied**, not merely delivered. The apply loop here
//!   takes visible time (a real one invalidates a tier, refetches a row); a read
//!   issued straight after the write sees the stale value, and the same read
//!   behind [`FrontierView::reached`] cannot.
//! * Missed writes are **detected, never silent**. Overflow the writer's small
//!   ring while the reader is busy and the reader is told exactly how much it
//!   lost, so it can remediate coarsely — and its barriers stay truthful across
//!   the hole.
//!
//! Two `Node`s over the in-memory transport stand in for two machines; swap it
//! for `groupnet-transport-udp` and nothing here changes.
//!
//! ```text
//! cargo run -p groupnet-consistency --example read_your_writes
//! ```

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::{Frontier, PeerWrite, PeerWrites, WriteFeed};
use groupnet_core::NodeId;
use groupnet_runtime::{Group, Node};
use groupnet_transport_mem::{MemTransport, Network};

const GROUP: &str = "stores";

/// The writer's ring: deliberately tiny, so a short burst can overflow it and
/// show what an unrecoverable lag looks like. Size it for your write rate.
const RING: usize = 2;

/// Stands in for the cost of actually applying an invalidation — evicting a
/// tier, refetching a row. It is what makes "applied" different from
/// "delivered", and the whole reason the frontier is advanced by the apply loop
/// rather than by delivery.
const APPLY_COST: Duration = Duration::from_millis(150);

/// A key the reader has cached.
type Key = String;

#[tokio::main]
async fn main() {
    let net = Network::new();
    let (writer_id, _writer_node, writer_group) = spawn(&net, "node-a", "node-b");
    let (reader_id, _reader_node, reader_group) = spawn(&net, "node-b", "node-a");
    wait_until(|| writer_group.members().len() == 2 && reader_group.members().len() == 2).await;
    println!("== two nodes, one group \"{GROUP}\" ==");
    println!("  {writer_id} owns the records; {reader_id} serves reads from a cache");

    // ---- the reader: a cache, an apply loop, and a frontier ----------------
    let cache: Arc<Mutex<HashMap<Key, String>>> = Arc::default();
    cache
        .lock()
        .expect("lock")
        .insert("user:1".to_owned(), "Ada Lovelace (cached)".to_owned());

    let mut peers = PeerWrites::new(reader_group, reader_id, |bytes: &[u8]| {
        String::from_utf8(bytes.to_vec()).ok()
    });
    let (frontier, view) = Frontier::new();

    // The apply loop is the application's, not the library's — which is exactly
    // why the frontier advances only once the local state really is coherent.
    let applied = Arc::clone(&cache);
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            match event {
                PeerWrite::Wrote { peer, token, key } => {
                    tokio::time::sleep(APPLY_COST).await;
                    applied.lock().expect("lock").remove(&key);
                    println!("  [apply] {peer} wrote {key} at {token:?} -> evicted");
                    frontier.advance(&peer, token);
                }
                PeerWrite::Gap {
                    peer,
                    missed_through,
                } => {
                    // Coarse remediation: we cannot know *which* keys were
                    // missed, so nothing cached may be trusted.
                    tokio::time::sleep(APPLY_COST).await;
                    applied.lock().expect("lock").clear();
                    println!(
                        "  [apply] gap: {peer}'s writes through {missed_through:?} were missed \
                         -> flushed the whole cache"
                    );
                    frontier.advance(&peer, missed_through);
                }
            }
        }
    });

    // ---- act 1: a write, an unbarriered read, and a barriered one ----------
    let feed = WriteFeed::new(
        writer_group,
        NonZeroUsize::new(RING).expect("nonzero"),
        |key: &Key| key.clone().into_bytes(),
    )
    // Every feed life carries an epoch so a restart cannot be mistaken for a
    // rewind. The default is the wall clock; a durable counter (a WAL
    // generation, a boot counter) is better where you have one — and it keeps
    // this demo's tokens readable.
    .with_epoch(1);

    println!("\n== act 1: the barrier is the difference ==");
    let token = feed.publish(&"user:1".to_owned()).await;
    println!("  [write] {writer_id} wrote user:1, session token {token:?}");
    println!("  [read ] without a barrier: {}", read(&cache, "user:1"));

    let reached = view.reached(&writer_id, token).await;
    println!(
        "  [read ] after reached(..) = {reached}: {}",
        read(&cache, "user:1")
    );
    println!("  the second read could not have been stale: the barrier waited for the apply");

    // ---- act 2: outrun the ring, and be told so ----------------------------
    println!("\n== act 2: a lag the ring cannot cover ==");
    println!(
        "  the reader is mid-apply while {writer_id} publishes 4 more writes into {RING} slots"
    );
    cache.lock().expect("lock").extend([
        ("user:2".to_owned(), "Grace Hopper (cached)".to_owned()),
        (
            "user:9".to_owned(),
            "Karen Spärck Jones (cached)".to_owned(),
        ),
    ]);
    let mut last = token;
    for key in ["user:2", "user:3", "user:4", "user:9"] {
        last = feed.publish(&key.to_owned()).await;
    }
    println!("  [write] latest session token {last:?}");

    let reached = view.reached(&writer_id, last).await;
    println!(
        "  [read ] after reached(..) = {reached}: user:2 -> {}",
        read(&cache, "user:2")
    );
    println!(
        "  [read ] after reached(..) = {reached}: user:9 -> {}",
        read(&cache, "user:9")
    );
    println!(
        "  two of those writes fell off the ring — the reader was told, flushed, and its\n  \
         barrier still means what it says. Silence was never an option."
    );
}

/// Brings up one node on `net` seeded with `seed`, joined to [`GROUP`]. The
/// returned [`Node`] must be kept alive: dropping it stops the node.
fn spawn(net: &Network, id: &str, seed: &str) -> (NodeId, Node<MemTransport>, Group) {
    let me = NodeId::new(id);
    let node = Node::builder(me.clone(), net.endpoint(me.clone()))
        .seed(NodeId::new(seed))
        // Brisk cadences so a demo does not spend its life waiting for gossip.
        .gossip_interval_ms(10)
        .spawn();
    let group = node.join_group(GROUP);
    (me, node, group)
}

/// Polls `cond` until it holds, panicking after a generous deadline — a demo
/// that cannot converge should fail loudly, not hang on a later await.
/// Membership is gossiped, so a demo waits for it rather than assuming it.
async fn wait_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for the cluster to settle");
}

/// What the reader would serve for `key` right now.
fn read(cache: &Mutex<HashMap<Key, String>>, key: &str) -> String {
    cache
        .lock()
        .expect("lock")
        .get(key)
        .cloned()
        .unwrap_or_else(|| "<miss — read through to the store>".to_owned())
}
