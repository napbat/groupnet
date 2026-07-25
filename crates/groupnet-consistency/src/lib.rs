//! # groupnet-consistency
//!
//! Session-consistency primitives over the Groupnet fabric — the reusable
//! layer for cross-node write propagation with honest guarantees.
//!
//! Each node publishes its recent writes as a compact ring inside one
//! versioned, gossiped group entry ([`WriteFeed`]); every peer turns entry
//! changes into typed events ([`PeerWrites`]): [`PeerWrite::Wrote`] for each
//! new write, or [`PeerWrite::Gap`] when writes were provably missed (the
//! peer's ring advanced past this subscriber's cursor, or the peer
//! restarted). The application applies each event — a tiered cache calls
//! `invalidate(&key)` per `Wrote` and flushes its tiers on a `Gap`, an
//! index refreshes the id, a replica schedules a refetch — and
//! advances a [`Frontier`], so readers can barrier on *applied* state with
//! [`FrontierView::reached`].
//!
//! # What you get — and what you deliberately don't
//!
//! Provided, and safe to rely on:
//!
//! - **Per-writer total order.** Each node's feed is totally ordered by
//!   [`WriteToken`] (epoch-major, then sequence), and subscribers observe
//!   its writes in publication order.
//! - **Loss is detected, never silent.** Ring overflow past a slow
//!   subscriber — and a writer restart — degrade to an explicit
//!   [`PeerWrite::Gap`], not a skip.
//! - **Read-your-writes and monotonic reads, per writer.**
//!   [`WriteFeed::publish`] resolves to the write's [`WriteToken`]; hand
//!   `(writer, token)` to the client as a session token, and any node
//!   serving that client barriers with [`FrontierView::reached`] before
//!   reading locally. Because the frontier is advanced by *your* apply
//!   loop, "reached" means applied — not merely delivered.
//!
//! Not provided, by design:
//!
//! - **Cross-writer ordering, consensus, fencing.** Two nodes' feeds have no
//!   mutual order, and groupnet's coordinator is derived, not fenced. For
//!   "exactly one writer may proceed", fence at an external authority (a
//!   store with conditional writes) or run a consensus log — this crate will
//!   not pretend to do it with gossip.
//!
//! # Writer restarts (why tokens carry an epoch)
//!
//! A restarted writer starts a fresh ring at sequence 1. Bare sequence
//! numbers would then lie twice: a subscriber's cursor from the old life
//! would sit past the new ring (deaf forever), and an old high watermark
//! would satisfy new-life barriers instantly (stale reads passed off as
//! fresh). So every feed life carries an **epoch**, tokens are
//! `(epoch, seq)`, and ordering is epoch-major:
//!
//! - By default the epoch is the wall-clock time at [`WriteFeed::new`] —
//!   strictly increasing across restarts unless the clock steps backwards.
//!   Applications with a durable counter (a WAL generation, a boot counter)
//!   should prefer [`WriteFeed::with_epoch`].
//! - Subscribers surface an epoch change as a [`PeerWrite::Gap`] covering
//!   the entire previous life (epoch-major ordering makes the new-life
//!   token compare above every old-life token), then resume normal
//!   delivery.
//! - The [`Frontier`] stores one [`WriteToken`] watermark per writer; after
//!   the gap remediation advances it into the new epoch, old-life barriers
//!   remain satisfied (the remediation covered them) and new-life barriers
//!   behave normally.
//!
//! # Semantics (read this once, rely on it forever)
//!
//! - **State-based, not a log.** The feed entry always carries the last N
//!   writes; gossip loss, event lag, and duplication are all safe because
//!   subscribers reconcile against the current entry, and applying a write
//!   notification must be idempotent. Missing writes are *detected*, never
//!   silently dropped.
//! - **Eventual, bounded by propagation latency.** A peer observes a write
//!   after roughly one gossip round (or one round trip, when the engine's
//!   eager delta push is enabled). The barrier for "has it landed?" is the
//!   [`Frontier`], never wall-clock time.
//! - **Keys travel by your codec — and a "key" is any datum.** Provide
//!   encode/decode closures (no forced serde). Encoding `(key, version)`
//!   pairs, or a key plus a small value, turns the feed from invalidation
//!   into write-through propagation with zero API changes; keep the datum
//!   small (it rides the gossiped ring) and keep codecs in lockstep across
//!   nodes. A datum that fails to decode is skipped.
//! - **History is not replayed.** A subscriber starts at each existing peer
//!   feed's current end (a fresh node has nothing stale to fix up). Feeds
//!   appearing later replay their visible window — those writes are
//!   genuinely new to this subscriber.
//! - **One feed per name per group.** The default feed occupies one
//!   reserved entry; independent subsystems sharing a group must use
//!   [`WriteFeed::named`] / [`PeerWrites::named`] to keep their feeds
//!   apart.
//! - **The frontier remembers every writer it has seen.** One token per
//!   peer, bounded by group membership — departed peers' watermarks linger
//!   harmlessly.
//!
//! # Example
//!
//! ```no_run
//! use std::collections::HashSet;
//! use std::num::NonZeroUsize;
//! use std::sync::{Arc, Mutex};
//!
//! use groupnet_consistency::{Frontier, PeerWrite, PeerWrites, WriteFeed};
//! use groupnet_core::NodeId;
//! use groupnet_runtime::Node;
//! use groupnet_transport_mem::Network;
//!
//! # async fn demo() {
//! let net = Network::new();
//! let me = NodeId::new("node-a");
//! let node = Node::builder(me.clone(), net.endpoint(me.clone())).spawn();
//! let group = node.join_group("stores");
//!
//! let feed = WriteFeed::new(
//!     group.clone(),
//!     NonZeroUsize::new(128).unwrap(),
//!     |key: &String| key.clone().into_bytes(),
//! );
//! let mut peers = PeerWrites::new(group, me, |bytes| {
//!     String::from_utf8(bytes.to_vec()).ok()
//! });
//! let (frontier, view) = Frontier::new();
//!
//! // The node-local state kept coherent — a cache, an index, a replica.
//! let fresh: Arc<Mutex<HashSet<String>>> = Arc::default();
//!
//! // After every local durable write (the token is the client's RYW token):
//! let token = feed.publish(&"user:1".to_owned()).await;
//!
//! // Apply peer writes, advancing the frontier only once applied:
//! let local = Arc::clone(&fresh);
//! tokio::spawn(async move {
//!     while let Some(event) = peers.next().await {
//!         match event {
//!             PeerWrite::Wrote { peer, token, key } => {
//!                 local.lock().unwrap().remove(&key); // drop the stale copy
//!                 frontier.advance(&peer, token);
//!             }
//!             PeerWrite::Gap {
//!                 peer,
//!                 missed_through,
//!             } => {
//!                 local.lock().unwrap().clear(); // coarse remediation
//!                 frontier.advance(&peer, missed_through);
//!             }
//!         }
//!     }
//! });
//!
//! // Serving a client that carries a token (writer, token): barrier first.
//! # let writer = NodeId::new("node-b");
//! if view.reached(&writer, token).await {
//!     // local state now reflects that write — serve the read
//! }
//! # }
//! ```

mod feed;
mod frontier;
mod peers;
mod token;
mod wire;

pub use feed::WriteFeed;
pub use frontier::{Frontier, FrontierView};
pub use peers::{PeerWrite, PeerWrites};
pub use token::WriteToken;
