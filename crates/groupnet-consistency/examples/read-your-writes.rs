//! Read-your-writes across two nodes: one writes, the other serves reads from a
//! local cache, and a session token makes the read honest.
//!
//! `node-a` publishes every write into a [`WriteFeed`] and hands the returned
//! token to its client. `node-b` runs the application's apply loop over
//! [`PeerWrites`] and advances a [`Frontier`] only once a write is *applied* to
//! its cache. The run makes two points:
//!
//! * **Applied, not delivered.** The apply loop takes visible time; a read issued
//!   straight after the write sees the stale value, and the same read behind
//!   [`FrontierView::reached`](groupnet_consistency::FrontierView::reached)
//!   cannot.
//! * **Missed writes are detected, never silent.** A burst that overflows the
//!   writer's two-slot ring reaches the reader as a [`PeerWrite::Gap`]; it
//!   flushes its cache, and its barrier stays truthful across the hole.
//!
//! Two `Node`s over the in-memory transport stand in for two machines.
//!
//! ```text
//! cargo run -p groupnet-consistency --example read-your-writes
//! ```

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::{Frontier, Group, PeerWrite, PeerWrites, WriteFeed, WriteToken};
use groupnet_core::NodeId;
use groupnet_runtime::Node;
use groupnet_transport_mem::{MemLink, Network};

/// The writer's ring: tiny, so a short burst overflows it. Size it for your
/// write rate.
const RING: usize = 2;

/// Stands in for really applying an invalidation (evicting a tier, refetching a
/// row) — the cost that makes "applied" differ from "delivered".
const APPLY_COST: Duration = Duration::from_millis(150);

/// The reader's cache: key -> cached value.
type Cache = Arc<Mutex<HashMap<String, String>>>;

#[tokio::main]
async fn main() {
    let net = Network::new();
    let (writer, _writer_node, writer_group) = spawn(&net, "node-a", "node-b").await;
    let (reader, _reader_node, reader_group) = spawn(&net, "node-b", "node-a").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while writer_group.members().len() < 2 || reader_group.members().len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("membership converges");

    // The reader: a cache, an apply loop, and a frontier the loop advances only
    // once the cache really is coherent.
    let cache: Cache = Arc::default();
    put(&cache, "user:1", "Ada Lovelace (stale)");
    let (frontier, view) = Frontier::new();
    let gap_seen = Arc::new(AtomicBool::new(false));
    let mut peers = PeerWrites::new(reader_group, reader, |bytes: &[u8]| {
        String::from_utf8(bytes.to_vec()).ok()
    });
    let (applied, saw_gap) = (Arc::clone(&cache), Arc::clone(&gap_seen));
    tokio::spawn(async move {
        while let Some(event) = peers.next().await {
            match event {
                PeerWrite::Wrote { peer, token, key } => {
                    tokio::time::sleep(APPLY_COST).await;
                    applied.lock().expect("lock").remove(&key);
                    frontier.advance(&peer, token);
                }
                PeerWrite::Gap {
                    peer,
                    missed_through,
                } => {
                    // Which keys were missed is unknowable, so nothing cached
                    // may be trusted: coarse remediation.
                    saw_gap.store(true, Ordering::Relaxed);
                    applied.lock().expect("lock").clear();
                    println!("  [apply] gap through {missed_through:?} -> flushed the cache");
                    frontier.advance(&peer, missed_through);
                }
                // A sealed life delivered in full: nothing was missed.
                PeerWrite::Sealed { peer, token } => frontier.advance(&peer, token),
                PeerWrite::Renewed { peer, epoch, .. } => {
                    frontier.advance(&peer, WriteToken { epoch, seq: 0 });
                }
            }
        }
    });

    // The writer. A durable epoch (a WAL generation, a boot counter) beats the
    // wall-clock default, and keeps these tokens readable.
    let feed = WriteFeed::new(
        writer_group,
        NonZeroUsize::new(RING).expect("nonzero"),
        |key: &String| key.clone().into_bytes(),
    )
    .with_epoch(1);

    println!("== the barrier is the difference ==");
    let token = feed
        .publish(&"user:1".to_owned())
        .await
        .expect("an idle group accepts the advertisement");
    println!("  {writer} wrote user:1, session token {token:?}");
    println!("  read without a barrier: {}", get(&cache, "user:1"));
    let reached = view.reached(&writer, token).await;
    println!(
        "  read after reached = {reached}: {}",
        get(&cache, "user:1")
    );

    println!("\n== a lag the ring cannot cover ==");
    put(&cache, "user:2", "Grace Hopper (stale)");
    let mut last = token;
    for key in ["user:2", "user:3", "user:4", "user:5"] {
        last = feed
            .publish(&key.to_owned())
            .await
            .expect("an idle group accepts the advertisement");
    }
    println!("  4 writes into {RING} slots, latest token {last:?}");
    let reached = view.reached(&writer, last).await;
    println!(
        "  read after reached = {reached}: {}",
        get(&cache, "user:2")
    );
    if gap_seen.load(Ordering::Relaxed) {
        println!("  writes fell off the ring: the reader was told, flushed, and its barrier held");
    } else {
        println!("  the reader kept pace this run; had it not, the loss would arrive as a Gap");
    }
}

/// One node on `net`, seeded with its peer and joined to the `stores` group.
/// Keep the [`Node`]: dropping it stops the node.
async fn spawn(net: &Network, id: &str, peer: &str) -> (NodeId, Node, Group) {
    let me = NodeId::new(id);
    let node = Node::builder(me.clone())
        .link(MemLink::new(
            net.endpoint(me.clone()),
            vec![NodeId::new(peer)],
        ))
        .seed(NodeId::new(peer))
        .gossip_interval_ms(10)
        .start()
        .await
        .expect("the memory link binds");
    let group = node.join_group("stores");
    (me, node, group)
}

fn put(cache: &Cache, key: &str, value: &str) {
    cache
        .lock()
        .expect("lock")
        .insert(key.to_owned(), value.to_owned());
}

/// What the reader would serve for `key` right now.
fn get(cache: &Cache, key: &str) -> String {
    cache
        .lock()
        .expect("lock")
        .get(key)
        .cloned()
        .unwrap_or_else(|| "<miss: read through to the store>".to_owned())
}
