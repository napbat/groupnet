//! Externally-typed sequence floors: per-node, per-key watermarks carried in
//! the *consumer's* sequence space.
//!
//! A router wants one thing from a peer before it sends a read there: how far
//! has that node applied? [`SeqFloors`] carries exactly that — one `u64` per
//! (node, key), gossiped as a TTL'd group entry and readable by every member.
//! The number's meaning is entirely the consumer's (a shard LSN, a WAL
//! offset, a snapshot generation); this crate disseminates it, expires it,
//! and refuses to guess when it is missing.
//!
//! It generalizes the hot-set shape consumers hand-roll over
//! [`Group::set_entry`] / [`Group::node_entry`] today: per-shard applied-LSN
//! hints, TTL'd so an idle or departed publisher fades out on its own, where
//! absence means *route conservatively*, never *route as if zero*.
//!
//! # Absence is the whole design
//!
//! [`SeqFloors::floor_of`] returns `None` for "this node makes no claim about
//! this key" — never published, published and expired, not converged here
//! yet, unknown node, or bytes that did not decode. Every one of those is the
//! same instruction to the caller: **fall back**. Go to the authority, pick
//! another replica, serve uncached. A floor is a fast path, never a proof.
//!
//! Per-key entries make that fallback fine-grained: each key expires on its
//! own schedule, so a publisher that goes quiet about one shard stops
//! advertising *that* shard rather than dragging its whole set down — and the
//! per-entry TTL expiry **is** the idle signal. There is nothing to
//! unpublish and no death notice to wait for.
//!
//! # The monotonicity you get, and where it stops
//!
//! Within one publisher process, [`SeqFloors::publish`] max-folds: a lower
//! (or equal) floor re-advertises the running max, refreshing the TTL without
//! ever walking the number backwards. Readers therefore never observe a
//! regression from a live publisher, however sloppily the call sites are
//! ordered — and that holds across threads, because the fold and the write it
//! produces are one critical section, so the group actor receives them in
//! fold order rather than in whatever order two publishers happened to win
//! the lock.
//!
//! Across a **restart** the fold starts empty, so a publisher that comes back
//! having genuinely lost ground will advertise the lower number. That is
//! honest, and it is why the values worth publishing are durably monotone at
//! the source (an LSN, a WAL generation): with such a producer the case
//! cannot arise, and with a volatile counter the regression is real and you
//! want to see it.
//!
//! # Entry layout
//!
//! One entry per key: `~floor:<key>` for the default set, `~floor:<name>:<key>`
//! for a named one — `~`-prefixed like every other reserved entry, so delta
//! digests disseminate each key incrementally instead of re-shipping a map on
//! every advance. Set names must not contain `:`; the *keys* may, with one
//! sharp edge that follows from the layout: a default-set key of `"lsn:s7"`
//! occupies the same entry as key `"s7"` of the set named `"lsn"`. Keep set
//! names out of the default set's key space (or name every set) and the
//! ambiguity cannot arise.
//!
//! # Not provided, by design
//!
//! - **No generic ordered types.** The value is a `u64`. Consumer sequence
//!   spaces are counters; a trait-generic ordering would buy nothing and cost
//!   a codec contract on the wire.
//! - **No cross-writer aggregation.** There is no group-wide min/max/quorum
//!   floor. [`SeqFloors::floors_for`] hands back the per-node claims and the
//!   caller combines them — which peers count, and how, is a routing policy
//!   this layer must not invent.
//! - **No watch API.** Reads are point-in-time snapshots; drive off
//!   [`Group::events`] or poll. A floor is a hint, and hints do not need
//!   edge-triggered delivery.
//! - **No replay.** The entry is state, not a log: only the current floor
//!   exists, and intermediate values a reader missed are simply gone.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_runtime::{CommandRejected, Group};

/// The reserved group entry prefix every floor occupies (`~`-prefixed like
/// the runtime's other reserved entries).
const FLOOR_KEY: &str = "~floor";

/// The entry-key prefix of a set: `~floor:` for the default set,
/// `~floor:<name>:` for a named one.
fn prefix_for(name: &str) -> String {
    if name.is_empty() {
        format!("{FLOOR_KEY}:")
    } else {
        format!("{FLOOR_KEY}:{name}:")
    }
}

/// The group entry key one floor key occupies within a set.
fn entry_key(prefix: &str, key: &str) -> String {
    let mut out = String::with_capacity(prefix.len() + key.len());
    out.push_str(prefix);
    out.push_str(key);
    out
}

/// `u64` little-endian, 8 bytes — dep-free, and the smallest thing that can
/// ride a gossiped entry.
fn encode(floor: u64) -> Vec<u8> {
    floor.to_le_bytes().to_vec()
}

/// The floor in `bytes`, or `None` unless they are exactly one `u64` LE.
/// Short, long, and garbled all decode to "no claim" — the conservative
/// answer — rather than to a number nobody wrote.
fn decode(bytes: &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

/// Folds `floor` into `published` under `key` and returns the value to
/// advertise: the running max for this process life.
fn fold_max(published: &mut HashMap<String, u64>, key: &str, floor: u64) -> u64 {
    if let Some(current) = published.get_mut(key) {
        *current = (*current).max(floor);
        *current
    } else {
        published.insert(key.to_owned(), floor);
        floor
    }
}

/// Per-node sequence floors in a consumer-defined space: "node *N* has
/// applied *key* through *floor*".
///
/// One handle is both halves. [`publish`](Self::publish) advertises this
/// node's floors (TTL'd, monotone per process life);
/// [`floor_of`](Self::floor_of) and [`floors_for`](Self::floors_for) read
/// what peers advertise. A node that only reads still constructs one — the
/// TTL is used solely by `publish`.
///
/// Reads are **observer-local**: they report what gossip has delivered *to
/// this node* right now, so two members can honestly disagree for about one
/// propagation hop. Treat every read as a hint whose absence means fall back
/// (see the module docs).
///
/// ```no_run
/// use std::time::Duration;
///
/// use groupnet_consistency::SeqFloors;
/// # use groupnet_core::NodeId;
/// # use groupnet_runtime::Group;
/// # fn demo(group: Group, replica: &NodeId) -> Result<(), Box<dyn std::error::Error>> {
/// // Refresh well inside the TTL: publish on every apply, or on a ticker.
/// let floors = SeqFloors::new(group, Duration::from_secs(5));
/// floors.publish("shard-7", 4_210)?; // applied through LSN 4210
///
/// // Read side: use the replica only if it claims to be caught up. `None`
/// // (silent, expired, or never published) falls back, never routes.
/// let usable = floors
///     .floor_of(replica, "shard-7")
///     .is_some_and(|floor| floor >= 4_210);
/// # let _ = usable;
/// # Ok(())
/// # }
/// ```
pub struct SeqFloors {
    group: Group,
    /// The set's entry-key prefix; a floor key is appended verbatim.
    prefix: String,
    ttl_ms: u64,
    /// The running max advertised per key this process life.
    published: Mutex<HashMap<String, u64>>,
}

impl fmt::Debug for SeqFloors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeqFloors")
            .field("group", &self.group.id())
            .field("prefix", &self.prefix)
            .field("ttl_ms", &self.ttl_ms)
            .finish_non_exhaustive()
    }
}

impl SeqFloors {
    /// The default floor set over `group`, publishing entries that expire
    /// `ttl` after each receiver adopts them.
    ///
    /// Size `ttl` at several refresh intervals: it is how long a *stale*
    /// claim survives after a publisher goes quiet, and how quickly a live
    /// one is dropped if a refresh is lost. Sub-millisecond TTLs are raised
    /// to 1 ms — the engine reads a zero TTL as "never expires", and a floor
    /// that never expires is precisely the stale claim this type exists to
    /// prevent.
    #[must_use]
    pub fn new(group: Group, ttl: Duration) -> Self {
        Self::build(prefix_for(""), group, ttl)
    }

    /// A named floor set — independent subsystems sharing one group name
    /// their sets so their keys occupy distinct entries. An empty name is
    /// the default set ([`new`](Self::new)).
    ///
    /// # Panics
    /// If `name` contains `:`, which is the layout's own separator: the name
    /// would silently merge with a neighbouring set's key space.
    #[must_use]
    pub fn named(name: &str, group: Group, ttl: Duration) -> Self {
        assert!(
            !name.contains(':'),
            "floor set names must not contain ':' (got {name:?})"
        );
        Self::build(prefix_for(name), group, ttl)
    }

    fn build(prefix: String, group: Group, ttl: Duration) -> Self {
        Self {
            group,
            prefix,
            ttl_ms: u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1),
            published: Mutex::new(HashMap::new()),
        }
    }

    /// Advertises that this node has reached `floor` for `key`, and refreshes
    /// the entry's TTL.
    ///
    /// Monotone per process life: `floor` is folded into the running max
    /// *before* the write is attempted, so a lower or equal value still
    /// refreshes the TTL without regressing what peers read, and a write that
    /// is rejected is re-carried by the next `publish` of that key (the floor
    /// is state, not a log). Call it on every advance, and on a ticker no
    /// slower than a fraction of the TTL if advances can go quiet while the
    /// claim is still true.
    ///
    /// Concurrent publishers of the same key **serialize**: the fold and the
    /// enqueue happen under one lock, so writes reach the group actor in fold
    /// order and no peer can transiently read a lower floor overtaking a
    /// higher one. Without that, two threads could fold to 10 then 11 and
    /// still enqueue 11 then 10.
    ///
    /// # Errors
    /// [`CommandRejected`] if the group actor's bounded inbox is full or the
    /// actor has shut down; the fold happened, the write did not. Retry, or
    /// let the next advance carry it — but a claim that is never written does
    /// expire, which is the safe direction.
    pub fn publish(&self, key: &str, floor: u64) -> Result<(), CommandRejected> {
        let mut published = self
            .published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = fold_max(&mut published, key, floor);
        // The lock is deliberately held across the write: `set_entry` is a
        // synchronous `try_send` with no await, so there is no suspension
        // point for a std mutex to be held across (unlike the ack ledger,
        // whose writes do await) — and folding without enqueuing under the
        // same lock is exactly what would let a peer observe a regression.
        self.group.set_entry(
            entry_key(&self.prefix, key),
            encode(value),
            Some(self.ttl_ms),
        )
    }

    /// The floor `node` currently advertises for `key` in this set, as gossip
    /// shows it here.
    ///
    /// `None` covers every flavour of "no claim": never published, expired,
    /// not converged here yet, unknown node, undecodable bytes. All of them
    /// mean fall back — never treat it as zero or as "behind".
    #[must_use]
    pub fn floor_of(&self, node: &NodeId, key: &str) -> Option<u64> {
        decode(&self.group.node_entry(node, &entry_key(&self.prefix, key))?)
    }

    /// Every live member currently advertising a floor for `key` in this set,
    /// with that floor, in node-id order.
    ///
    /// Members making no claim are absent from the result rather than present
    /// with a placeholder, so the caller cannot accidentally route to one.
    /// This node appears alongside its peers when it publishes. Combining the
    /// claims — a min across replicas, a quorum, a pick-the-highest — is the
    /// caller's policy; see the module docs on cross-writer aggregation.
    #[must_use]
    pub fn floors_for(&self, key: &str) -> Vec<(NodeId, u64)> {
        let entry = entry_key(&self.prefix, key);
        self.group
            .members()
            .into_iter()
            .filter_map(|node| {
                let floor = decode(&self.group.node_entry(&node, &entry)?)?;
                Some((node, floor))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{decode, encode, entry_key, fold_max, prefix_for};

    #[test]
    fn set_names_and_keys_map_to_distinct_entries() {
        assert_eq!(entry_key(&prefix_for(""), "shard-7"), "~floor:shard-7");
        assert_eq!(
            entry_key(&prefix_for("lsn"), "shard-7"),
            "~floor:lsn:shard-7"
        );
        // Distinct names never share an entry, and neither do distinct keys:
        // per-key entries are what makes dissemination and expiry per-key.
        assert_ne!(
            entry_key(&prefix_for("lsn"), "s"),
            entry_key(&prefix_for("idx"), "s")
        );
        assert_ne!(
            entry_key(&prefix_for("lsn"), "a"),
            entry_key(&prefix_for("lsn"), "b")
        );
        // An empty name is the default set.
        assert_eq!(prefix_for(""), prefix_for(""));
        assert_ne!(prefix_for(""), prefix_for("lsn"));
    }

    #[test]
    fn floors_round_trip_and_malformed_bytes_decode_to_no_claim() {
        for floor in [0, 1, 42, u64::MAX] {
            assert_eq!(decode(&encode(floor)), Some(floor));
        }
        assert_eq!(encode(1).len(), 8, "one u64 LE, nothing else");
        // Anything that is not exactly eight bytes is "no claim", not a
        // number nobody wrote.
        for cut in 0..8 {
            assert_eq!(decode(&encode(9)[..cut]), None, "short at {cut}");
        }
        assert_eq!(decode(&[0u8; 9]), None, "long");
        assert_eq!(decode(b"garbled!!"), None);
        assert_eq!(decode(b"garbled"), None);
    }

    #[test]
    fn publishing_folds_to_the_running_max() {
        let mut published = HashMap::new();
        assert_eq!(fold_max(&mut published, "s7", 10), 10);
        // A lower publish re-advertises the max (refreshing the TTL) rather
        // than walking the floor backwards…
        assert_eq!(fold_max(&mut published, "s7", 7), 10);
        assert_eq!(fold_max(&mut published, "s7", 10), 10, "equal is not lower");
        // …and an advance still advances.
        assert_eq!(fold_max(&mut published, "s7", 11), 11);
        // Keys fold independently.
        assert_eq!(fold_max(&mut published, "s8", 3), 3);
        assert_eq!(published.get("s7"), Some(&11));
    }

    #[test]
    fn a_rejected_write_is_re_carried_by_the_next_publish() {
        let mut published = HashMap::new();
        // The fold happens before the enqueue, so a write that never reached
        // the actor still leaves the max recorded…
        assert_eq!(fold_max(&mut published, "s7", 10), 10);
        // …and the next publish — even a lower one — carries it.
        assert_eq!(fold_max(&mut published, "s7", 4), 10);
    }
}
