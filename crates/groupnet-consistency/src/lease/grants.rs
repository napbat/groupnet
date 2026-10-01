//! The granter's sans-IO half of the coherence-lease tier: [`GrantLedger`],
//! what this node has granted to each reader, and therefore when a reader
//! that never acknowledges a write provably cannot be serving.
//!
//! Like [`LeaseCore`](super::LeaseCore) and
//! [`CoherenceCore`](super::CoherenceCore) it reads no clock and touches no
//! group: instants arrive as [`ClockMs`] on this node's own monotonic clock,
//! renewals and grant maps arrive as decoded values, and the answers come back
//! as values — so the deterministic simulator drives the same rules the tokio
//! shell does.
//!
//! # The three rules it enforces
//!
//! * **A grant is only as old as the renewal it confirms.** Every renewal is
//!   stamped with the instant this node first saw it, which is after the
//!   reader recorded its publish instant `s_i`. The reader's window from that
//!   renewal closes by `s_i + D - rate_margin` on its clock, so by the
//!   first-seen instant plus `D` on this node's clock the reader cannot be
//!   serving on anything this node granted — the same inequality the TTL
//!   argument rests on, measured on the writer's own clock and immune to what
//!   membership does to the entry.
//! * **A grant does not grow past a write the reader has not applied.** While
//!   a coherent write of this node is in flight and a reader has not applied
//!   it, that reader is granted no renewal first seen after the write began
//!   ([`fold`](GrantLedger::fold)) — the newest older one stands, so its
//!   window keeps closing. A reader that keeps renewing but stops applying (a
//!   stalled apply loop, a partition that carries its renewals but not this
//!   node's writes) therefore runs out of lease one `D` after the write began,
//!   and the write can end on that lapse instead of waiting on an
//!   acknowledgement that may never come. Once the write has *ended* —
//!   excused, timed out, or completed while the reader was out of view — the
//!   reader is granted nothing new at all until it advertises having applied
//!   it. A grant is therefore also a certificate: whoever holds a grant from
//!   this node issued after a write of its ended has applied that write. A
//!   reader returning from a lapse cannot serve on a fresh grant that
//!   overtook the write itself on the wire, nor on a resync that predates it.
//! * **A lapse only excuses a reader that counts this node.** A reader whose
//!   roster does not include this node serves without this node's grants, so
//!   their expiry proves nothing about it. The reader's own map is the proof:
//!   it is stamped with the reader's lease life
//!   ([`GranterLife`]), and once a reader's life has granted this
//!   node's current renewal it counts this node for the rest of that life
//!   ([`LeaseCore::pin`](super::LeaseCore::pin) — a reader's roster only
//!   grows). Without that proof a reader is excused by acknowledgement alone.
//!   A reader whose map declares its life departed is excused at once: a
//!   departed life never serves again
//!   ([`LeaseCore::depart`](super::LeaseCore::depart)).
//!
//! A restarted node has no record of what its previous life granted, so a new
//! ledger presumes every reader was granted at its birth: nothing is excused
//! by lapse in the first `D` of a life.

use std::collections::BTreeMap;

use groupnet_core::NodeId;

use super::LeaseConfig;
use super::core::ClockMs;
use super::wire::{GrantMap, GranterLife, RenewalId};
use crate::token::WriteToken;

/// What this node knows about one reader's lease.
#[derive(Debug, Default)]
struct Reader {
    /// The newest renewal of the reader this node has seen, and when it first
    /// saw it.
    seen: Option<(RenewalId, ClockMs)>,
    /// The newest renewal this life has granted the reader, and when this node
    /// first saw it.
    granted: Option<(RenewalId, ClockMs)>,
    /// The reader life whose map proved it counts this node as a granter.
    counts_me: Option<u64>,
    /// A reader life whose map declared it departed.
    departed: Option<u64>,
}

/// The granter's half: what this node has granted each reader, the cap an
/// outstanding write puts on those grants, and when a silent reader can be
/// excused.
///
/// One instance per lease set, shared by the granter (which
/// [`fold`](Self::fold)s every map this node publishes) and every coherent
/// write (which [`begin`](Self::begin)s, asks
/// [`excusable_at`](Self::excusable_at), and [`end`](Self::end)s). See the
/// module docs for the rules.
#[derive(Debug)]
pub struct GrantLedger {
    me: NodeId,
    /// `duration` in milliseconds, never zero.
    duration_ms: u64,
    /// This node's lease life — the epoch its own row carries.
    life: u64,
    /// When this life started: what a previous life granted is presumed open
    /// until one lease duration past it.
    born: ClockMs,
    /// Whether this life has departed: it excuses no reader by lapse.
    departed: bool,
    readers: BTreeMap<NodeId, Reader>,
    /// Every coherent write in flight, by writer feed and token, and when it
    /// began.
    inflight: BTreeMap<(NodeId, WriteToken), ClockMs>,
    /// The newest token of each writer feed whose coherent write has ended,
    /// however it ended. A reader that has not applied it is granted nothing
    /// new.
    ended: BTreeMap<NodeId, WriteToken>,
}

impl GrantLedger {
    /// A ledger for `me` in lease life `life` (the epoch its renewals carry),
    /// leasing under `cfg`, born at `now`.
    #[must_use]
    pub fn new(me: NodeId, cfg: &LeaseConfig, life: u64, now: ClockMs) -> Self {
        Self {
            me,
            duration_ms: cfg.duration_ms().max(1),
            life,
            born: now,
            departed: false,
            readers: BTreeMap::new(),
            inflight: BTreeMap::new(),
            ended: BTreeMap::new(),
        }
    }

    /// The life this node's maps are stamped with.
    #[must_use]
    pub fn life(&self) -> GranterLife {
        GranterLife {
            epoch: self.life,
            departed: self.departed,
        }
    }

    /// Departs this life: every map folded from now on says so, and no reader
    /// is excused by lapse again — a departed writer's remaining writes end on
    /// acknowledgements or not at all, because readers stop counting it once
    /// membership forgets it.
    pub fn depart(&mut self) {
        self.departed = true;
    }

    /// Registers a coherent write of `writer`'s feed through `token`, begun at
    /// `now`, so the grants of every reader that has not applied it stop
    /// growing. Pair with [`end`](Self::end).
    pub fn begin(&mut self, writer: &NodeId, token: WriteToken, now: ClockMs) {
        self.inflight.entry((writer.clone(), token)).or_insert(now);
    }

    /// Unregisters a coherent write, however it ended. From the next
    /// [`fold`](Self::fold) on, a reader that has applied it is no longer
    /// capped by it, and one that has not is granted nothing new until it has.
    pub fn end(&mut self, writer: &NodeId, token: WriteToken) {
        self.inflight.remove(&(writer.clone(), token));
        let newest = self.ended.entry(writer.clone()).or_insert(token);
        *newest = (*newest).max(token);
    }

    /// How many coherent writes are registered.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.inflight.len()
    }

    /// The map this node publishes at `now`: one grant per reader in
    /// `visible` (every live renewal in this node's view), held back where a
    /// write of this node is ahead of the reader, plus this node's own
    /// [`GranterLife`] row.
    ///
    /// `applied(reader, writer)` is how far `reader` advertises having applied
    /// `writer`'s feed. A reader behind an ended write is granted no new
    /// renewal, and one behind a write in flight none first seen after that
    /// write began: the newest renewal it was already granted in its current
    /// life stands, or nothing if it has none.
    ///
    /// Every grant returned is recorded as made, before the caller publishes
    /// it: a publish that then fails only makes this node believe it granted
    /// more recently than it did, which delays an excuse and never hastens one.
    pub fn fold(
        &mut self,
        now: ClockMs,
        visible: impl IntoIterator<Item = (NodeId, RenewalId)>,
        applied: impl Fn(&NodeId, &NodeId) -> Option<WriteToken>,
    ) -> GrantMap {
        let mut grants = GrantMap::new();
        for (reader, id) in visible {
            if reader == self.me {
                continue;
            }
            let behind = |writer: &NodeId, token: WriteToken| {
                applied(&reader, writer).is_none_or(|done| done < token)
            };
            let current = !self
                .ended
                .iter()
                .any(|(writer, token)| behind(writer, *token));
            let cap = self
                .inflight
                .iter()
                .filter(|((writer, token), _)| behind(writer, *token))
                .map(|(_, began)| *began)
                .min();
            let record = self.readers.entry(reader.clone()).or_default();
            let first_seen = match record.seen {
                Some((seen, at)) if seen == id => at,
                Some((seen, _)) if seen > id => now,
                _ => {
                    record.seen = Some((id, now));
                    now
                }
            };
            if current && cap.is_none_or(|began| first_seen <= began) {
                if record.granted.is_none_or(|(granted, _)| granted < id) {
                    record.granted = Some((id, first_seen));
                }
                grants.insert(reader, id);
            } else if let Some((granted, _)) = record
                .granted
                .filter(|(granted, _)| granted.epoch == id.epoch && *granted <= id)
            {
                grants.insert(reader, granted);
            }
        }
        self.life().stamp(&self.me, &mut grants);
        grants
    }

    /// Folds in `reader`'s own published map: whether its current life counts
    /// this node (it granted this node's current renewal), and whether that
    /// life has departed. A map without a [`GranterLife`] row proves nothing.
    pub fn observe_reader_map(&mut self, reader: &NodeId, map: &GrantMap) {
        let Some(life) = GranterLife::of(reader, map) else {
            return;
        };
        let record = self.readers.entry(reader.clone()).or_default();
        if life.departed {
            record.departed = Some(life.epoch);
        }
        if map.get(&self.me).is_some_and(|id| id.epoch == self.life) {
            record.counts_me = Some(life.epoch);
        } else if record.counts_me.is_some_and(|counted| counted < life.epoch) {
            record.counts_me = None;
        }
    }

    /// The instant from which `reader` may be excused from a write it has not
    /// acknowledged, or `None` if no lapse can excuse it.
    ///
    /// * `None` once this life has departed, while this node has never seen a
    ///   renewal of `reader`, and while `reader`'s current life (the newest
    ///   renewal seen) has not proved it counts this node.
    /// * At once for a reader whose current life has departed.
    /// * Otherwise one lease duration past the first-seen instant of the newest
    ///   renewal this life granted that reader life — or past this life's
    ///   birth, when it granted none.
    #[must_use]
    pub fn excusable_at(&self, reader: &NodeId) -> Option<ClockMs> {
        if self.departed {
            return None;
        }
        let record = self.readers.get(reader)?;
        let (seen, _) = record.seen?;
        if record.departed == Some(seen.epoch) {
            return Some(ClockMs::ZERO);
        }
        if record.counts_me != Some(seen.epoch) {
            return None;
        }
        let granted_at = match record.granted {
            Some((granted, at)) if granted.epoch == seen.epoch => at.max(self.born),
            _ => self.born,
        };
        Some(granted_at.saturating_add_ms(self.duration_ms))
    }

    /// Every reader whose newest grant from this life may still be open at
    /// `now` — a write's wait set must include them even once their renewal
    /// has left this node's view, because a reap or a deletion removes the
    /// entry without closing the window this node granted on it.
    pub fn open_grants(&self, now: ClockMs) -> impl Iterator<Item = &NodeId> {
        self.readers
            .iter()
            .filter(move |(_, record)| {
                record
                    .granted
                    .is_some_and(|(_, at)| now < at.saturating_add_ms(self.duration_ms))
            })
            .map(|(reader, _)| reader)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use groupnet_core::NodeId;

    use super::GrantLedger;
    use crate::lease::LeaseConfig;
    use crate::lease::core::ClockMs;
    use crate::lease::wire::{GrantMap, GranterLife, RenewalId};
    use crate::token::WriteToken;

    /// D = 1000 ms.
    fn cfg() -> LeaseConfig {
        LeaseConfig {
            duration: Duration::from_secs(1),
            renew_every: Duration::from_millis(333),
            rate_margin: Duration::from_millis(10),
        }
    }

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    fn renewal(epoch: u64, seq: u64) -> RenewalId {
        RenewalId { epoch, seq }
    }

    fn token(seq: u64) -> WriteToken {
        WriteToken { epoch: 1, seq }
    }

    /// A writer `w` of lease life 5, born at 0.
    fn ledger() -> GrantLedger {
        GrantLedger::new(node("w"), &cfg(), 5, ClockMs::ZERO)
    }

    /// The map reader `r` of life `life` publishes, granting `w`'s life-5
    /// renewal when `counts` holds.
    fn reader_map(life: u64, counts: bool, departed: bool) -> GrantMap {
        let mut map = GrantMap::new();
        if counts {
            map.insert(node("w"), renewal(5, 3));
        }
        GranterLife {
            epoch: life,
            departed,
        }
        .stamp(&node("r"), &mut map);
        map
    }

    fn nothing_applied(_: &NodeId, _: &NodeId) -> Option<WriteToken> {
        None
    }

    #[test]
    fn a_fold_grants_every_visible_renewal_and_stamps_its_own_life() {
        let mut ledger = ledger();
        let map = ledger.fold(
            ClockMs(10),
            [(node("r"), renewal(2, 1)), (node("w"), renewal(5, 9))],
            nothing_applied,
        );
        assert_eq!(map.get(&node("r")), Some(&renewal(2, 1)));
        assert_eq!(
            GranterLife::of(&node("w"), &map),
            Some(GranterLife {
                epoch: 5,
                departed: false
            }),
            "its own row is its life, never a grant to itself"
        );
    }

    #[test]
    fn an_outstanding_write_stops_a_lagging_reader_s_grant_from_growing() {
        let mut ledger = ledger();
        let _ = ledger.fold(ClockMs(100), [(node("r"), renewal(2, 1))], nothing_applied);
        ledger.begin(&node("w"), token(7), ClockMs(150));
        // Renewal 2 arrives after the write began, and `r` has not applied it.
        let map = ledger.fold(ClockMs(400), [(node("r"), renewal(2, 2))], nothing_applied);
        assert_eq!(map.get(&node("r")), Some(&renewal(2, 1)), "capped at 1");
        // `r` applies the write: the cap lifts and renewal 2 is granted.
        let map = ledger.fold(ClockMs(410), [(node("r"), renewal(2, 2))], |_, _| {
            Some(token(7))
        });
        assert_eq!(map.get(&node("r")), Some(&renewal(2, 2)));
    }

    #[test]
    fn a_renewal_seen_before_the_write_began_is_still_granted() {
        let mut ledger = ledger();
        let _ = ledger.fold(ClockMs(100), [(node("r"), renewal(2, 4))], nothing_applied);
        ledger.begin(&node("w"), token(1), ClockMs(150));
        let map = ledger.fold(ClockMs(160), [(node("r"), renewal(2, 4))], nothing_applied);
        assert_eq!(map.get(&node("r")), Some(&renewal(2, 4)));
        ledger.end(&node("w"), token(1));
        assert_eq!(ledger.in_flight(), 0);
        let map = ledger.fold(ClockMs(500), [(node("r"), renewal(2, 5))], |_, _| {
            Some(token(1))
        });
        assert_eq!(map.get(&node("r")), Some(&renewal(2, 5)));
    }

    #[test]
    fn a_reader_behind_an_ended_write_is_granted_nothing_new_until_it_applies_it() {
        let mut ledger = ledger();
        let _ = ledger.fold(ClockMs(100), [(node("r"), renewal(2, 4))], nothing_applied);
        // The write ended without `r` — excused, timed out, or `r` was out of
        // view — and `r` has applied only the write before it.
        ledger.begin(&node("w"), token(2), ClockMs(150));
        ledger.end(&node("w"), token(2));
        let behind = |_: &NodeId, _: &NodeId| Some(token(1));
        for at in [500, 5_000, 50_000] {
            let map = ledger.fold(ClockMs(at), [(node("r"), renewal(2, 9))], behind);
            assert_eq!(
                map.get(&node("r")),
                Some(&renewal(2, 4)),
                "however long it waits, the grant does not grow"
            );
        }
        // A reader this node never granted, and that has applied nothing of
        // `w`, is behind too: it gets no grant at all.
        let map = ledger.fold(
            ClockMs(600),
            [(node("new"), renewal(1, 1))],
            nothing_applied,
        );
        assert_eq!(map.get(&node("new")), None);
        // `r` applies it: the next fold grants its newest renewal.
        let map = ledger.fold(ClockMs(600), [(node("r"), renewal(2, 9))], |_, _| {
            Some(token(2))
        });
        assert_eq!(map.get(&node("r")), Some(&renewal(2, 9)));
    }

    #[test]
    fn a_capped_reader_with_no_grant_in_its_current_life_is_granted_nothing() {
        let mut ledger = ledger();
        let _ = ledger.fold(ClockMs(100), [(node("r"), renewal(2, 4))], nothing_applied);
        ledger.begin(&node("w"), token(1), ClockMs(150));
        // `r` restarted: its new life was first seen after the write began.
        let map = ledger.fold(ClockMs(300), [(node("r"), renewal(3, 1))], nothing_applied);
        assert_eq!(
            map.get(&node("r")),
            None,
            "an old life's grant confirms nothing"
        );
    }

    #[test]
    fn a_lapse_excuses_only_a_reader_whose_current_life_counts_this_node() {
        let mut ledger = ledger();
        let _ = ledger.fold(
            ClockMs(1_200),
            [(node("r"), renewal(2, 1))],
            nothing_applied,
        );
        assert_eq!(ledger.excusable_at(&node("r")), None, "no proof yet");
        ledger.observe_reader_map(&node("r"), &reader_map(2, false, false));
        assert_eq!(ledger.excusable_at(&node("r")), None, "r does not count w");
        ledger.observe_reader_map(&node("r"), &reader_map(2, true, false));
        assert_eq!(
            ledger.excusable_at(&node("r")),
            Some(ClockMs(2_200)),
            "one D past the first sight of the newest grant"
        );
        // Counting is permanent within the life: a map that no longer grants
        // `w` (its renewal expired there) does not withdraw the proof.
        ledger.observe_reader_map(&node("r"), &reader_map(2, false, false));
        assert_eq!(ledger.excusable_at(&node("r")), Some(ClockMs(2_200)));
        // A new life of `r` must prove it all over again.
        let _ = ledger.fold(
            ClockMs(1_300),
            [(node("r"), renewal(3, 1))],
            nothing_applied,
        );
        ledger.observe_reader_map(&node("r"), &reader_map(3, false, false));
        assert_eq!(ledger.excusable_at(&node("r")), None);
    }

    #[test]
    fn a_map_without_a_life_row_proves_nothing() {
        let mut ledger = ledger();
        let _ = ledger.fold(ClockMs(10), [(node("r"), renewal(2, 1))], nothing_applied);
        let mut old = GrantMap::new();
        old.insert(node("w"), renewal(5, 3));
        ledger.observe_reader_map(&node("r"), &old);
        assert_eq!(ledger.excusable_at(&node("r")), None);
    }

    #[test]
    fn a_new_life_presumes_its_previous_life_granted_at_birth() {
        let mut ledger = GrantLedger::new(node("w"), &cfg(), 5, ClockMs(400));
        // Seen, counted, never granted by this life (a capped first fold).
        ledger.begin(&node("w"), token(1), ClockMs(400));
        let map = ledger.fold(ClockMs(450), [(node("r"), renewal(2, 1))], nothing_applied);
        assert_eq!(map.get(&node("r")), None);
        ledger.observe_reader_map(&node("r"), &reader_map(2, true, false));
        assert_eq!(ledger.excusable_at(&node("r")), Some(ClockMs(1_400)));
    }

    #[test]
    fn a_departed_reader_is_excused_at_once_and_a_departed_writer_excuses_nobody() {
        let mut ledger = ledger();
        let _ = ledger.fold(ClockMs(10), [(node("r"), renewal(2, 1))], nothing_applied);
        ledger.observe_reader_map(&node("r"), &reader_map(2, false, true));
        assert_eq!(ledger.excusable_at(&node("r")), Some(ClockMs::ZERO));
        ledger.depart();
        assert_eq!(ledger.excusable_at(&node("r")), None);
        let map = ledger.fold(ClockMs(20), Vec::new(), nothing_applied);
        assert_eq!(
            GranterLife::of(&node("w"), &map),
            Some(GranterLife {
                epoch: 5,
                departed: true
            })
        );
    }

    #[test]
    fn a_grant_stays_open_for_one_duration_after_its_renewal_was_first_seen() {
        let mut ledger = ledger();
        let _ = ledger.fold(ClockMs(100), [(node("r"), renewal(2, 1))], nothing_applied);
        let open = |ledger: &GrantLedger, now| {
            ledger
                .open_grants(ClockMs(now))
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(open(&ledger, 1_099), vec![node("r")]);
        assert_eq!(open(&ledger, 1_100), Vec::<NodeId>::new());
    }
}
