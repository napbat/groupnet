//! The **external CAS anchor**: the record a claimant writes into a
//! linearizable store to become host, and the rules that decide what to write.
//!
//! This is the sans-IO half of [`Activation::External`]. Nothing here performs
//! I/O, opens a connection, or reads a clock — `now_wall_ms` arrives as an
//! argument, exactly as it does in the pattern this lifts (shardstore's
//! `caslog/epoch.rs`). The driver supplies bytes and etags; every *verdict* is
//! a pure function in this module, which is what lets the deterministic
//! simulator and a real object-store driver run the identical decisions.
//!
//! # What the anchor is, and what it buys
//!
//! A linearizable compare-and-set register — an S3/R2/GCS object touched with
//! `If-None-Match: *` and `If-Match: <etag>`, an etcd key, a CAS row. The
//! anchor *allocates* the epoch: an epoch number exists only because one
//! conditional write created it, so **no two nodes ever hold the same epoch**,
//! at any instant, at disjoint times, across any partition, and with **no
//! node-local storage of any kind**. That is the tier's headline property
//! (`X-S1`), and it is strictly stronger than [`Activation::Quorum`]'s, whose
//! epoch uniqueness is conditional on voters remembering what they granted.
//!
//! There is deliberately no persisted-ledger analogue here, and none is
//! coming: the anchor *is* the ledger.
//!
//! # The one place a clock is consulted
//!
//! [`AnchorRecord::stealable`] — and only it. A claimant may supersede an
//! expired record once
//!
//! ```text
//! now_wall_ms >= expires_at_wall_ms + steal_margin_ms
//! ```
//!
//! which is shardstore's TTL + skew-margin rule verbatim. The honest
//! assumption is *claimant wall-clock skew ≤ the configured margin*; when it
//! is violated the deposed holder may still believe itself live for the
//! excess. What that costs is bounded, **always cross-epoch** (a successor's
//! epoch is strictly higher by construction) and **always fenced** at the
//! store, so it is succession timing and never epoch uniqueness. Stated as the
//! rule the tier rests on: *the record is succession, the fence at the store
//! is safety.*
//!
//! # Wall-clock milliseconds are not [`Time`](crate::Time)
//!
//! Every number in this module is an absolute **wall-clock** millisecond,
//! because it is judged by a *different node's* clock after a round trip
//! through a store. The engine's logical [`Time`](crate::Time) never appears
//! here and this module's milliseconds never reach the engine: a host's
//! step-down instant arrives separately, as the `lease_until` the driver puts
//! on [`Command::AnchorActivated`](crate::Command::AnchorActivated).
//!
//! [`Activation::External`]: crate::Activation::External
//! [`Activation::Quorum`]: crate::Activation::Quorum

use crate::NodeId;

/// The anchor's whole contents: who holds the group, under which epoch, and
/// until when by the holder's own wall clock.
///
/// Three fields, on purpose. There is no successor hint (cooperative handoff
/// is a later milestone, and an unexercised branch in the steal rule is worse
/// than a missing feature) and no etag — the etag is the store's, carried by
/// the driver alongside the record rather than inside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnchorRecord {
    /// The epoch this record awards. Allocated by the anchor: strictly
    /// increasing across every write, so it names exactly one hostship for
    /// ever.
    pub epoch: u64,
    /// The node that holds `epoch`.
    pub host: NodeId,
    /// When the holder's claim lapses, in absolute **wall-clock**
    /// milliseconds on the holder's clock. Advisory: it bounds how long a dead
    /// holder blocks succession, and a stale stamp can never compromise
    /// fencing — see [`stealable`](Self::stealable).
    pub expires_at_wall_ms: u64,
}

impl AnchorRecord {
    /// Whether a claimant other than the holder may supersede this record at
    /// `now_wall_ms`.
    ///
    /// `steal_margin_ms` absorbs the disagreement between the holder's clock
    /// (which wrote `expires_at_wall_ms`) and the claimant's (which is reading
    /// it). Saturating, so a record parked at [`u64::MAX`] is never stealable
    /// rather than wrapping into immediately stealable.
    ///
    /// Note that this asks nothing about *who* is asking: entitlement is
    /// [`plan_claim`]'s business, and the holder itself is entitled whether or
    /// not the record has expired.
    #[must_use]
    pub const fn stealable(&self, now_wall_ms: u64, steal_margin_ms: u64) -> bool {
        now_wall_ms >= self.expires_at_wall_ms.saturating_add(steal_margin_ms)
    }
}

/// What [`plan_claim`] decided a claimant should do with the anchor.
///
/// The two write arms map onto the two conditional writes every object store
/// offers — `Create` is an if-none-match `PUT`, `Supersede` is an if-match
/// `PUT` against the etag the record was loaded with — so a driver never has
/// to infer which precondition to use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimPlan {
    /// The anchor is empty: write this record with an **absent**
    /// precondition. Losing that race means someone else got there first, and
    /// the claimant re-loads and re-plans.
    Create(AnchorRecord),
    /// A record exists and this claimant is entitled to replace it: write this
    /// one with an **etag** precondition against the record that was loaded.
    /// The epoch is always strictly above the loaded one.
    Supersede(AnchorRecord),
    /// A live record names somebody else. Write nothing.
    Yield {
        /// The earliest wall-clock instant a steal could become entitled —
        /// the loaded record's expiry plus the steal margin. A driver sleeps
        /// to it rather than spinning; it is a hint, and re-planning at that
        /// instant may yield again if the holder renewed meanwhile.
        retry_at_wall_ms: u64,
    },
}

/// The claim rule: given the record that was just loaded, decide what to
/// write.
///
/// * **Nothing loaded** ⇒ [`ClaimPlan::Create`] at `max(epoch_hint, 1)`. Epoch
///   0 is the engine's "nothing adopted" sentinel and must never name a
///   hostship, so genesis floors at 1.
/// * **A record this node is entitled to take** ⇒ [`ClaimPlan::Supersede`] at
///   `max(loaded.epoch + 1, epoch_hint)`. Entitlement is *names this node* or
///   *stealable* — nothing else. The `+ 1` is what makes the anchor an
///   allocator; the hint is a **floor**, not an override, so a claimant that
///   has gossiped a higher epoch than the anchor shows (a record rolled back,
///   a stale read) still bids strictly above everything it has seen.
/// * **Anything else** ⇒ [`ClaimPlan::Yield`].
///
/// A node that already holds the record is entitled, so a *restart* plans a
/// `Supersede` at a strictly higher epoch rather than resuming the old one.
/// That is the tier's rule, not an accident: hostship is never resumed, only
/// re-won, and the record naming this node is evidence of an epoch rather than
/// a grant of authority (row X5 / row 12b consume it that way).
///
/// A holder that still has its etag renews with [`renewal_record`] instead;
/// this path is for winning an epoch, and it always costs one.
#[must_use]
pub fn plan_claim(
    local: &NodeId,
    epoch_hint: u64,
    loaded: Option<&AnchorRecord>,
    now_wall_ms: u64,
    ttl_ms: u64,
    steal_margin_ms: u64,
) -> ClaimPlan {
    let expires_at_wall_ms = now_wall_ms.saturating_add(ttl_ms);
    let Some(current) = loaded else {
        return ClaimPlan::Create(AnchorRecord {
            epoch: epoch_hint.max(1),
            host: local.clone(),
            expires_at_wall_ms,
        });
    };
    let entitled = current.host == *local || current.stealable(now_wall_ms, steal_margin_ms);
    if !entitled {
        return ClaimPlan::Yield {
            retry_at_wall_ms: current.expires_at_wall_ms.saturating_add(steal_margin_ms),
        };
    }
    ClaimPlan::Supersede(AnchorRecord {
        epoch: current.epoch.saturating_add(1).max(epoch_hint),
        host: local.clone(),
        expires_at_wall_ms,
    })
}

/// The record a holder writes to extend the epoch it already has.
///
/// Same epoch, same host, a fresh expiry — a renewal decides nothing, so it
/// allocates nothing. A driver writes this against the etag it holds; a
/// mismatch means it has been superseded and must abdicate, which is a hard
/// signal rather than a retry.
#[must_use]
pub fn renewal_record(local: &NodeId, epoch: u64, now_wall_ms: u64, ttl_ms: u64) -> AnchorRecord {
    AnchorRecord {
        epoch,
        host: local.clone(),
        expires_at_wall_ms: now_wall_ms.saturating_add(ttl_ms),
    }
}

/// Resolve an **ambiguous** conditional write by reading the record back.
///
/// A conditional `PUT` that times out, or whose connection drops, genuinely
/// has no answer: it may have applied. The only honest resolution is a
/// read-back, and this is the rule for judging it — the write applied **iff**
/// the record now standing is **byte-identical to the one that was attempted**,
/// and names this node.
///
/// # Why the whole record, and not the `(epoch, host)` pair
///
/// For a *claim* the pair would do: an attempted claim's epoch is strictly
/// above anything that was standing, so finding it means our own write put it
/// there. A **renewal** is the case that breaks it — same epoch, same host,
/// only the expiry moves — so an attempted renewal's pair is byte-identical to
/// the record it means to replace, and "my renewal applied" and "my *old*
/// record is still standing" are the same observation. A store whose writes
/// fail while its reads succeed (a write throttle, a read-only window, expired
/// write credentials) would then resolve **every** failed renewal as won: the
/// engine lease would extend for ever off a record quietly ageing out beneath
/// it, until a rival steals at `expires + margin` and two nodes host with
/// perfect clocks.
///
/// `expires_at_wall_ms` is exactly the discriminator, because a renewal always
/// moves it strictly forward — the driver's pacing floor puts renewal rounds at
/// least half a lease apart, so the expiry a renewal writes is strictly later
/// than the one it replaces. One rule can therefore serve both: for a claim the
/// epoch has already decided the positive case, so the extra equality never
/// costs a win that should have stood — it can only ever *refuse* (a record at
/// our epoch stamped with an expiry we did not write is not this attempt),
/// which is the direction this predicate is allowed to be wrong in.
///
/// The other two halves stay load-bearing. Matching the expiry and epoch alone
/// would let a *different* node's write look like ours (impossible while the
/// anchor is the sole allocator, and precisely the thing not worth assuming);
/// matching the host alone would mistake an older record of our own for the new
/// one. An absent record means the write did not apply — a create that
/// succeeded leaves something behind.
///
/// Fail-closed by construction: anything but an exact match reads as *not
/// applied*, so an ambiguous round costs a re-plan, never a hostship this node
/// did not win — and never a lease extension it did not earn.
#[must_use]
pub fn ambiguous_applied(
    local: &NodeId,
    attempted: &AnchorRecord,
    reread: Option<&AnchorRecord>,
) -> bool {
    reread.is_some_and(|rec| rec.host == *local && rec == attempted)
}

#[cfg(test)]
mod tests {
    use super::{AnchorRecord, ClaimPlan, ambiguous_applied, plan_claim, renewal_record};
    use crate::NodeId;

    const TTL: u64 = 1_000;
    const MARGIN: u64 = 200;

    fn n(id: &str) -> NodeId {
        NodeId::new(id)
    }

    /// A record naming `host` at `epoch`, expiring at `expires`.
    fn rec(epoch: u64, host: &str, expires: u64) -> AnchorRecord {
        AnchorRecord {
            epoch,
            host: n(host),
            expires_at_wall_ms: expires,
        }
    }

    /// `plan_claim` for `me` against `loaded` at `now`, at the fixture TTL and
    /// margin.
    fn plan(me: &str, hint: u64, loaded: Option<&AnchorRecord>, now: u64) -> ClaimPlan {
        plan_claim(&n(me), hint, loaded, now, TTL, MARGIN)
    }

    /// An empty anchor is genesis: create, and never at epoch 0 — that number
    /// is the engine's "nothing adopted" sentinel and must not name a host.
    #[test]
    fn an_absent_record_is_created_at_the_hint_floored_at_one() {
        for (hint, want) in [(0, 1), (1, 1), (2, 2), (99, 99)] {
            assert_eq!(
                plan("a", hint, None, 5_000),
                ClaimPlan::Create(rec(want, "a", 6_000)),
                "hint {hint}"
            );
        }
    }

    /// The holder is entitled whatever the clock says — and takes a *new*
    /// epoch, because hostship is re-won and never resumed. This is the
    /// restart path.
    #[test]
    fn a_record_naming_us_is_superseded_at_a_higher_epoch_expired_or_not() {
        let held = rec(7, "a", 10_000);
        // Long before expiry, at expiry, and long after: same verdict.
        for now in [0, 5_000, 10_000, 99_000] {
            assert_eq!(
                plan("a", 0, Some(&held), now),
                ClaimPlan::Supersede(AnchorRecord {
                    epoch: 8,
                    host: n("a"),
                    expires_at_wall_ms: now + TTL,
                }),
                "at {now}"
            );
        }
    }

    /// A live record naming somebody else is refused, and the retry hint is
    /// the instant a steal could first become entitled.
    #[test]
    fn a_live_record_held_by_another_yields_until_expiry_plus_margin() {
        let held = rec(7, "b", 10_000);
        for now in [0, 5_000, 10_000, 10_199] {
            assert_eq!(
                plan("a", 99, Some(&held), now),
                ClaimPlan::Yield {
                    retry_at_wall_ms: 10_200
                },
                "at {now}"
            );
        }
    }

    /// The steal boundary, to the millisecond: `expiry + margin` is entitled,
    /// one millisecond earlier is not. The margin is the whole clock-skew
    /// assumption, so it is asserted exactly rather than approximately.
    #[test]
    fn the_steal_boundary_is_expiry_plus_margin_exactly() {
        let held = rec(7, "b", 10_000);
        assert!(!held.stealable(10_199, MARGIN));
        assert!(held.stealable(10_200, MARGIN));
        assert!(held.stealable(10_201, MARGIN));

        assert_eq!(
            plan("a", 0, Some(&held), 10_199),
            ClaimPlan::Yield {
                retry_at_wall_ms: 10_200
            }
        );
        assert_eq!(
            plan("a", 0, Some(&held), 10_200),
            ClaimPlan::Supersede(rec(8, "a", 11_200)),
            "the first instant a steal is entitled"
        );
    }

    /// A zero margin is a legal (if unwise) configuration: the boundary
    /// collapses onto the expiry itself and nothing else changes.
    #[test]
    fn a_zero_margin_steals_at_the_expiry_instant() {
        let held = rec(7, "b", 10_000);
        assert!(!held.stealable(9_999, 0));
        assert!(held.stealable(10_000, 0));
        assert_eq!(
            plan_claim(&n("a"), 0, Some(&held), 10_000, TTL, 0),
            ClaimPlan::Supersede(rec(8, "a", 11_000))
        );
    }

    /// The hint is a floor, not an override: a claimant bids at least one
    /// above what the anchor shows, and at least the hint, whichever is
    /// higher. Never below either.
    #[test]
    fn the_hint_dominates_when_it_is_higher_and_never_lowers_the_epoch() {
        let stale = rec(3, "b", 0); // long expired: stealable at any `now`
        for (hint, want) in [(0, 4), (3, 4), (4, 4), (5, 5), (100, 100)] {
            let ClaimPlan::Supersede(record) = plan("a", hint, Some(&stale), 50_000) else {
                panic!("an expired record is stealable");
            };
            assert_eq!(record.epoch, want, "hint {hint}");
            assert!(
                record.epoch > stale.epoch,
                "an anchor write always allocates"
            );
            assert!(record.epoch >= hint, "the hint is a floor");
        }
    }

    /// Absurd inputs saturate rather than wrapping into an immediately
    /// stealable record or a retry hint in the past.
    #[test]
    fn saturating_arithmetic_never_wraps_into_a_steal() {
        let forever = rec(1, "b", u64::MAX);
        assert!(
            !forever.stealable(u64::MAX - 1, MARGIN),
            "expiry + margin must saturate, not wrap to zero"
        );
        assert_eq!(
            plan("a", 0, Some(&forever), u64::MAX - 1),
            ClaimPlan::Yield {
                retry_at_wall_ms: u64::MAX
            }
        );
        // A TTL that would overflow the expiry parks it at "never" instead of
        // at zero.
        let ClaimPlan::Create(record) = plan_claim(&n("a"), 1, None, u64::MAX, TTL, MARGIN) else {
            panic!("an absent record creates");
        };
        assert_eq!(record.expires_at_wall_ms, u64::MAX);
        // And an epoch at the ceiling cannot wrap back to a spent number.
        let ceiling = rec(u64::MAX, "a", 0);
        assert_eq!(
            plan("a", 0, Some(&ceiling), 50_000),
            ClaimPlan::Supersede(rec(u64::MAX, "a", 51_000))
        );
    }

    /// A renewal decides nothing, so it allocates nothing: same epoch, same
    /// host, a fresh expiry.
    #[test]
    fn a_renewal_keeps_the_epoch_and_only_moves_the_expiry() {
        let renewed = renewal_record(&n("a"), 7, 10_000, TTL);
        assert_eq!(renewed, rec(7, "a", 11_000));
        assert_eq!(
            renewal_record(&n("a"), 7, 20_000, TTL).expires_at_wall_ms,
            21_000
        );
        assert_eq!(
            renewal_record(&n("a"), 7, u64::MAX, TTL).expires_at_wall_ms,
            u64::MAX,
            "saturating"
        );
    }

    /// The ambiguous-write truth table: applied **iff** the read-back holds
    /// exactly the record that was attempted. Every other reading is
    /// "not applied", which is the fail-closed direction.
    #[test]
    fn an_ambiguous_write_applied_only_on_an_exact_record_match() {
        let me = n("a");
        let attempted = rec(8, "a", 11_000);
        assert!(
            ambiguous_applied(&me, &attempted, Some(&rec(8, "a", 11_000))),
            "exact"
        );

        assert!(!ambiguous_applied(&me, &attempted, None), "absent");
        assert!(
            !ambiguous_applied(&me, &attempted, Some(&rec(8, "b", 11_000))),
            "our epoch, another host"
        );
        assert!(
            !ambiguous_applied(&me, &attempted, Some(&rec(7, "a", 11_000))),
            "our own older record"
        );
        assert!(
            !ambiguous_applied(&me, &attempted, Some(&rec(9, "a", 11_000))),
            "a later record of ours: this attempt is not what is standing"
        );
        assert!(
            !ambiguous_applied(&me, &attempted, Some(&rec(9, "b", 11_000))),
            "superseded outright"
        );
    }

    /// The renewal-ambiguity case the pair alone cannot see: a renewal's
    /// attempted `(epoch, host)` is *identical* to the record it replaces, so
    /// only the expiry can tell "my renewal applied" from "my old record is
    /// still standing". A failed renewal must read as **lost**.
    #[test]
    fn a_renewal_that_did_not_apply_is_not_mistaken_for_the_record_it_replaces() {
        let me = n("a");
        let standing = rec(8, "a", 11_000);
        let renewal = renewal_record(&me, 8, 10_500, TTL); // expires at 11_500

        assert!(
            !ambiguous_applied(&me, &renewal, Some(&standing)),
            "the write failed and the old record is still there: not applied"
        );
        assert!(
            ambiguous_applied(&me, &renewal, Some(&rec(8, "a", 11_500))),
            "the write landed: the expiry it stamped is standing"
        );
        // A renewal always moves the expiry strictly forward, which is what
        // makes the field a discriminator rather than a coincidence.
        assert!(renewal.expires_at_wall_ms > standing.expires_at_wall_ms);

        // For a *claim* the epoch alone already decided the positive case —
        // an attempted claim bids strictly above everything standing — so the
        // extra equality never turns a win into a loss it should have kept. It
        // can only ever refuse, which is the fail-closed direction: a record at
        // our epoch carrying an expiry we did not write is not this attempt.
        let claim = rec(9, "a", 12_000);
        assert!(ambiguous_applied(&me, &claim, Some(&rec(9, "a", 12_000))));
        for expires in [0, 11_999, u64::MAX] {
            assert!(
                !ambiguous_applied(&me, &claim, Some(&rec(9, "a", expires))),
                "expiry {expires}"
            );
        }
    }
}
