//! [`Activation::External`]: closing an epoch at a **linearizable external CAS
//! anchor** instead of in the fabric at all.
//!
//! The skeleton in the parent module is untouched — same rendezvous-ranked
//! candidate, same `(epoch, host)` fencing order, same adoption, same row 12b,
//! same repair beacon, same lease and step-down. What changes is where an
//! epoch comes from: under [`Settle`](Activation::Settle) a standing claim
//! activates when its window shuts, under [`Quorum`](Activation::Quorum) when
//! a majority of a roster grants it, and under `External` when a conditional
//! write to a store outside the cluster succeeds.
//!
//! That difference removes something rather than adding it. There is no claim
//! to stand, so **[`Role::Claimant`] is never entered**; no bid to broadcast,
//! so no `LeadClaim` frame is ever built; no endorsement to count, so no
//! `LeadGrant` frame and no `PersistGrant` effect either. An `External`
//! group's only election frame is `LeadState`, carrying the adopted pair for
//! repair exactly as it always did. That is the tier's **X-purity** property,
//! and it is pinned by a test asserting on a whole run's effect stream.
//!
//! # The engine prompts; the driver performs
//!
//! The engine holds no connection, no etag and no wall clock, so it cannot
//! touch the anchor and does not pretend to. It emits
//! [`Effect::AnchorClaimDue`] and consumes the outcome as
//! [`Command::AnchorActivated`] or [`Command::AnchorObserved`]. The *decisions*
//! that surround the store call — what epoch to bid, whether a record may be
//! stolen, whether an ambiguous write applied — are pure functions in
//! [`crate::anchor`], so the deterministic simulator and a real object-store
//! driver run one copy of them.
//!
//! # The rows
//!
//! * **X1 (the claim prompt).** [`tick_follower`](super) applies row 1's
//!   ordinary [`ClaimGuard`](super) — not leaving, past the boot guard,
//!   top-ranked, not already the adopted host — and where a `Settle` node
//!   would open a claim, an `External` one emits
//!   `AnchorClaimDue { epoch_hint: highest_seen + 1 }` on the anti-entropy
//!   cadence. Nothing is written down: no epoch is spent, no role changes, and
//!   nothing is announced, because prompting a driver decides nothing.
//! * **X2 (activation).** `AnchorActivated { epoch, lease_until }` with
//!   `epoch >= highest_seen` activates through row 4's
//!   [`activate`](super) **verbatim** — one
//!   [`Effect::LeadershipChanged`] and the `LeadState` broadcast to every live
//!   member. The bar is `>=` rather than `>` on purpose: row X5 raises
//!   `highest_seen` to the epoch of a record naming this node, so a strict
//!   bar would reject the very round that re-wins it and wedge the node at
//!   `(epoch, None)` while the anchor named it. The equality it admits is
//!   exactly the *hostless-or-ours* one — an epoch adopted for **another**
//!   host dies in row X6 rather than being activated over.
//! * **X3 (extension).** The same epoch again while already `Host` is a
//!   *renewal*, not an activation: `lease_until` moves to the later of the two
//!   and nothing is announced, because nothing changed. A round can only ever
//!   push the lease out, never pull it in — the same rule row Q8 applies.
//! * **X4 (observation-adopt).** `AnchorObserved { epoch, host }` naming
//!   somebody else is adopted when it outranks the adopted pair, which deposes
//!   this node if it was hosting. A pair that does not outrank ours is inert.
//! * **X5 (self-shadow).** A record naming *this node* at a strictly higher
//!   epoch is row 12b verbatim: the epoch is learned, the hostship is not, and
//!   the pair becomes `(epoch, None)` for this node to re-win above. This is
//!   what makes a restart a **re-win** rather than a resume — the record
//!   naming us is evidence of an epoch, never a grant of authority.
//! * **X6 (the gates).** Both commands are dropped silently outside
//!   `External`, and below their monotone bars; `AnchorActivated` is dropped
//!   additionally while this node is **leaving** (row 15 has already given the
//!   hostship up, and a round in flight must not hand it back) and at an epoch
//!   already adopted for another host. A command is *driver input*, so a stale,
//!   duplicated or misrouted report has to die here; a misconfigured driver
//!   must not be able to make an `Eventual`, `Settle` or `Quorum` group host on
//!   nobody's authority.
//! * **X7 (the renewal prompt).** A host that is **still the top-ranked live
//!   candidate** emits `AnchorClaimDue` on the same anti-entropy cadence,
//!   hinting the epoch it already holds. The driver renews at that epoch while
//!   it still has its etag, and re-plans through
//!   [`plan_claim`](crate::anchor::plan_claim) — which bids strictly higher —
//!   if it does not. Reached only after row 6's lapse check, so a host that
//!   has already run out of lease steps down instead of asking for more.
//!
//!   The rank gate is row Q7's, verbatim and for the same reason: *a host that
//!   no longer ranks should be letting its lease lapse, not asking for an
//!   extension.* Rows 5, Q7 and X7 are then one design — every activation's
//!   renewal is rank-gated, and the three differ only in what evidence extends
//!   the lease (own rank, a fresh majority, a fresh anchor round). Without it
//!   an outranked incumbent renews for ever while the new top-ranked node burns
//!   a store round trip per anti-entropy interval yielding to it, for ever;
//!   with it the incumbent's record simply ages out and the rendezvous top
//!   steals, so "the host in the common case lands where the coordinator
//!   ranking points" survives churn. Nothing about safety turns on it either
//!   way — the anchor allocates the epoch, and a lapse is fenced like any other
//!   succession — so this is a *liveness and cost* rule, which is exactly the
//!   kind row 6 already is.
//!
//! # Why row 5 is gated to `Settle`
//!
//! A `Settle` host renews by re-reading its own rank; that is evidence there,
//! and it is evidence of nothing here. An `External` host's authority comes
//! from the anchor and is extended only by row X3, so a node that has lost the
//! anchor lapses on row 6 and demotes however top-ranked it still looks to
//! itself. That is the fail-closed posture the whole tier depends on, and row
//! 5's one-line condition is where it is enforced.
//!
//! The other half of the same coin: a host cut off from the entire *fabric*
//! that can still reach the anchor keeps renewing and keeps hosting, correctly
//! — nobody else can take the epoch. Partitions stop being the availability
//! axis; anchor connectivity becomes it.
//!
//! [`Activation::External`]: crate::Activation::External
//! [`Activation::Quorum`]: crate::Activation::Quorum
//! [`Command::AnchorActivated`]: crate::Command::AnchorActivated
//! [`Command::AnchorObserved`]: crate::Command::AnchorObserved
//! [`Effect::AnchorClaimDue`]: crate::Effect::AnchorClaimDue
//! [`Effect::LeadershipChanged`]: crate::Effect::LeadershipChanged

use std::cmp::Ordering;

use crate::config::Activation;
use crate::{Effect, GroupEngine, NodeId, Time};

use super::{Election, Role, cmp_pair};

impl Election {
    /// Whether this group's epochs are allocated by an external CAS anchor.
    pub(super) const fn is_external(&self) -> bool {
        matches!(self.cfg.activation, Activation::External { .. })
    }
}

impl GroupEngine {
    /// Whether this group closes its epochs at an external anchor.
    pub(super) fn is_external(&self) -> bool {
        self.election.as_ref().is_some_and(Election::is_external)
    }

    /// Row X1's emission: prompt the driver to run an anchor round, bidding at
    /// least `epoch_hint`.
    ///
    /// Rides the anti-entropy cadence for the same reason row 3's claim
    /// re-offer does — a prompt lost to a busy or crashed driver must
    /// self-heal, and the engine cannot observe a round it does not perform,
    /// so the only sound shape is a repeated level signal. The driver
    /// debounces it against its own in-flight round; see
    /// [`Effect::AnchorClaimDue`](crate::Effect::AnchorClaimDue).
    ///
    /// Called only where row 1's guard has already opened, and returns before
    /// anything is written down: no epoch is spent, no role changes, and no
    /// claim exists to abandon. That is what "`Role::Claimant` is never
    /// entered" means structurally rather than conventionally.
    ///
    /// Carries its own mode gate, as [`external_renewal_prompt`] does, so
    /// neither row can be called into the wrong activation by a future edit at
    /// the call site.
    ///
    /// [`external_renewal_prompt`]: GroupEngine::external_renewal_prompt
    pub(super) fn external_claim_prompt(
        &self,
        epoch_hint: u64,
        anti_entropy_due: bool,
    ) -> Vec<Effect> {
        if !self.is_external() || !anti_entropy_due {
            return Vec::new();
        }
        vec![Effect::AnchorClaimDue { epoch_hint }]
    }

    /// Row X7: a host's renewal prompt, hinting the epoch it already holds.
    ///
    /// The hint is deliberately *not* `highest_seen + 1`: a renewal keeps the
    /// epoch, because it decides nothing and so allocates nothing. It is still
    /// only a floor — a driver that has lost its etag re-plans through
    /// [`plan_claim`](crate::anchor::plan_claim), which bids strictly above
    /// whatever the anchor actually shows, so a hint equal to a spent epoch
    /// can never re-litigate it.
    ///
    /// A no-op outside `External` (a `Settle` host renewed on rank in row 5, a
    /// `Quorum` host asked its roster in row Q7), and reached only after row 6
    /// has confirmed the lease has not already lapsed.
    ///
    /// **Rank-gated, exactly as row Q7's renewal round is.** A host that is no
    /// longer the top-ranked live candidate stops being prompted; its record
    /// ages out, its engine lease lapses on row 6, and the rendezvous top
    /// supersedes it at a strictly higher epoch. The alternative — prompting
    /// regardless of rank — is safe but livelocked in cost: the incumbent
    /// renews for ever while the top-ranked node spends a store round trip per
    /// anti-entropy interval reading a live record and yielding to it.
    pub(super) fn external_renewal_prompt(&self) -> Vec<Effect> {
        if !self.is_external() || !self.is_coordinator() {
            return Vec::new();
        }
        let epoch_hint = self.election.as_ref().map_or(0, |el| el.epoch);
        vec![Effect::AnchorClaimDue { epoch_hint }]
    }

    /// Rows X2, X3 and X6: the driver reports an anchor round it **won**.
    ///
    /// Four verdicts, in the order their guards must be read:
    ///
    /// * not an `External` group, or this node is **leaving** — dropped
    ///   silently (X6);
    /// * an epoch below what we have already observed, or at an epoch we have
    ///   adopted for *another* host — dropped silently (X6);
    /// * already `Host` at exactly this epoch — a renewal, so the lease moves
    ///   to the later of the two and nothing is announced (X3);
    /// * anything else — row 4's activation, verbatim (X2).
    ///
    /// The last arm covers one case worth naming: a `Host` handed a
    /// **strictly higher** epoch re-activates at it. That is a node whose
    /// driver re-won the anchor rather than renewing — after an ambiguous
    /// write, or a steal-back following a lost etag — and re-activating is
    /// exactly right, because the fence really did move and every observer
    /// must be told.
    ///
    /// **The leaving gate is row 15's, mirrored.** A leave demotes *before* it
    /// disseminates, precisely so this node never serves an epoch it has
    /// announced it is gone from; an anchor round already in flight when
    /// [`Command::Leave`](crate::Command::Leave) landed would otherwise come
    /// back and re-activate it at the very epoch row 15 just gave up. Row X1's
    /// [`ClaimGuard`](super) already refuses to *prompt* while leaving, and this
    /// is the same rule applied to the report a prompt issued earlier.
    ///
    /// Visible to the whole `engine` module rather than just to `election`,
    /// because the caller is [`GroupEngine::apply`](crate::GroupEngine::apply)
    /// — this row's trigger is a local command, not a frame.
    pub(in crate::engine) fn on_anchor_activated(
        &mut self,
        epoch: u64,
        lease_until: Time,
    ) -> Vec<Effect> {
        if self.leaving {
            return Vec::new(); // X6: row 15 has already given this node up
        }
        let Some(el) = self.election.as_ref().filter(|el| el.is_external()) else {
            return Vec::new(); // X6: wrong mode, or no election at all
        };
        if epoch < el.highest_seen {
            // X6: a stale or duplicated report. The epoch is spent — either we
            // moved past it ourselves, or the anchor awarded above it — and
            // activating on it would regress the fence.
            return Vec::new();
        }
        // X6, the equality carve-out. The `>=` bar exists for exactly one
        // shape: the adopted pair at this epoch is *hostless* (row X5's shadow,
        // or row 6's lapse) or is already ours (row X3's renewal). An equal
        // epoch whose adopted host is somebody **else** came through row X4,
        // and activating over it would serve an epoch the anchor awarded
        // elsewhere. One anchor never produces that report; a misrouted or
        // misconfigured driver is exactly what this row is here to kill.
        if el.epoch == epoch && el.host.as_ref().is_some_and(|host| *host != self.local) {
            return Vec::new();
        }
        if el.role == Role::Host && el.epoch == epoch {
            // X3: the same epoch again is a renewal. A round only ever pushes
            // the lease out; an out-of-order report can never pull it in.
            if let Some(el) = self.election.as_mut() {
                el.lease_until = el.lease_until.max(lease_until);
            }
            return Vec::new();
        }
        // X2: row 4's activation, to the byte — the lease is passed in because
        // only the driver knows the instant its anchor round began.
        self.activate(epoch, lease_until)
    }

    /// Rows X4, X5 and X6: the driver reports an anchor record it **read** and
    /// did not win.
    ///
    /// The same three-way decision [`on_lead_state`](super) makes about a
    /// gossiped pair, minus its row 13 repair — there is no sender to teach
    /// back, because the report came from this node's own driver.
    pub(in crate::engine) fn on_anchor_observed(
        &mut self,
        epoch: u64,
        host: &NodeId,
    ) -> Vec<Effect> {
        let names_self = *host == self.local;
        let Some(el) = self.election.as_ref().filter(|el| el.is_external()) else {
            return Vec::new(); // X6
        };
        let order = cmp_pair(
            self.group.as_str(),
            (epoch, Some(host)),
            (el.epoch, el.host.as_ref()),
        );
        match order {
            // X5: a better pair naming us. Row 12b verbatim — the epoch is
            // learned, the hostship is not, and this node re-wins above it.
            Ordering::Greater if names_self => self.learn_self_named(epoch),
            // X4: a better pair naming somebody else is adopted whole, which
            // deposes this node if it was hosting.
            Ordering::Greater => self.adopt(epoch, Some(host)),
            // X6: an equal or lower pair teaches nothing and announces
            // nothing. The anchor is the allocator, so a record we already
            // outrank is simply a read that lost a race.
            Ordering::Less | Ordering::Equal => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{Activation, GroupMode, HostedConfig, VoterRoster};
    use crate::{Command, Config, Effect, GroupEngine, GroupId, NodeId, Role, Time};

    const GROUP: &str = "g";
    const LEASE_MS: u64 = 4_000;

    fn n(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn hosted(activation: Activation) -> Config {
        Config {
            mode: GroupMode::Hosted(HostedConfig {
                activation,
                lease_ms: LEASE_MS,
            }),
            ..Config::default()
        }
    }

    fn external() -> Config {
        hosted(Activation::External {
            steal_margin_ms: 500,
        })
    }

    /// A started engine for `id` in `config`, alone in its group.
    fn started(id: &str, config: Config) -> GroupEngine {
        let mut engine = GroupEngine::new(GroupId::new(GROUP), n(id), [], config);
        engine.start(Time::ZERO);
        engine
    }

    /// Every configuration that is not `External`, for the row X6 mode gate.
    fn not_external() -> Vec<(&'static str, Config)> {
        vec![
            ("eventual", Config::default()),
            (
                "settle",
                hosted(Activation::Settle {
                    claim_settle_ms: 500,
                }),
            ),
            (
                "quorum",
                hosted(Activation::Quorum {
                    voters: VoterRoster::new([n("a")]),
                }),
            ),
        ]
    }

    /// The activation trichotomy, as the two predicates the rows branch on.
    /// Row 5's renewal is `is_settle` and nothing else, which is what stops an
    /// `External` host renewing its lease off its own rank.
    #[test]
    fn the_activation_predicates_partition_the_three_policies() {
        let external = started("a", external());
        assert!(external.is_external());
        assert!(!external.is_settle());
        assert!(!external.is_quorum());

        for (name, config) in not_external() {
            let engine = started("a", config);
            assert!(!engine.is_external(), "{name}");
        }
        assert!(
            started(
                "a",
                hosted(Activation::Settle {
                    claim_settle_ms: 500
                })
            )
            .is_settle()
        );
        assert!(
            !started(
                "a",
                hosted(Activation::Quorum {
                    voters: VoterRoster::new([n("a")])
                })
            )
            .is_settle()
        );
        // An `Eventual` group has no activation at all, so it is neither.
        let eventual = started("a", Config::default());
        assert!(!eventual.is_settle() && !eventual.is_external());
    }

    /// Row X6, the mode gate: both commands are inert everywhere but
    /// `External`. A driver pointed at the wrong group must not be able to
    /// hand it a hostship.
    #[test]
    fn both_anchor_commands_are_inert_outside_external() {
        for (name, config) in not_external() {
            for cmd in [
                Command::AnchorActivated {
                    epoch: 9,
                    lease_until: Time(9_000),
                },
                Command::AnchorObserved {
                    epoch: 9,
                    host: n("b"),
                },
            ] {
                let mut engine = started("a", config.clone());
                let effects = engine.apply(cmd);
                assert!(effects.is_empty(), "{name} answered an anchor command");
                assert_eq!(engine.leadership(), (0, None), "{name}");
                assert_eq!(engine.role(), Role::Follower, "{name}");
                assert_eq!(engine.observed_epoch(), 0, "{name}");
            }
        }
    }

    /// Row X6, the monotone gate on X2: an activation below what this node has
    /// already observed is a stale or duplicated report, and dies here rather
    /// than regressing the fence.
    ///
    /// The fixture is row **X5**'s shape, which is the one the `>=` bar exists
    /// for: a record naming *this* node raised `highest_seen` to 7 and left the
    /// adopted pair hostless at `(7, None)`, so the round that re-wins epoch 7
    /// must land.
    #[test]
    fn an_activation_below_the_observed_epoch_is_dropped() {
        let mut engine = started("a", external());
        engine.apply(Command::AnchorObserved {
            epoch: 7,
            host: n("a"), // row X5: the epoch is learned, the hostship is not
        });
        assert_eq!(engine.observed_epoch(), 7);
        assert_eq!(engine.leadership(), (7, None));

        for epoch in [0, 1, 6] {
            let effects = engine.apply(Command::AnchorActivated {
                epoch,
                lease_until: Time(99_000),
            });
            assert!(effects.is_empty(), "epoch {epoch} activated");
            assert_eq!(engine.role(), Role::Follower, "epoch {epoch}");
        }
        // The bar is `>=`, not `>`: an activation *at* the observed epoch is
        // the shape row X5 leaves behind, and rejecting it would wedge a
        // restarted node at `(epoch, None)` while the anchor named it.
        let effects = engine.apply(Command::AnchorActivated {
            epoch: 7,
            lease_until: Time(99_000),
        });
        assert!(!effects.is_empty());
        assert_eq!(engine.role(), Role::Host);
    }

    /// Row X6's equality carve-out: the `>=` bar admits an equal epoch only
    /// where the adopted pair at it is hostless or already ours. A pair adopted
    /// through row X4 for **another** node is an epoch the anchor awarded
    /// elsewhere, and an activation at it — which one anchor cannot produce,
    /// and a misrouted driver can — is inert.
    #[test]
    fn an_activation_at_an_epoch_adopted_for_another_host_is_dropped() {
        let mut engine = started("a", external());
        engine.apply(Command::AnchorObserved {
            epoch: 7,
            host: n("b"), // row X4: adopted whole
        });
        assert_eq!(engine.leadership(), (7, Some(&n("b"))));

        let effects = engine.apply(Command::AnchorActivated {
            epoch: 7,
            lease_until: Time(99_000),
        });
        assert!(
            effects.is_empty(),
            "activated over an equal-epoch pair naming somebody else"
        );
        assert_eq!(engine.leadership(), (7, Some(&n("b"))));
        assert_eq!(engine.role(), Role::Follower);
        assert_eq!(engine.host_lease_until(), None);

        // Strictly above it still activates: that is a fence that really moved.
        let won = engine.apply(Command::AnchorActivated {
            epoch: 8,
            lease_until: Time(99_000),
        });
        assert!(!won.is_empty());
        assert_eq!(engine.leadership(), (8, Some(&n("a"))));
    }

    /// The benign equality the carve-out must keep: a delayed renewal landing
    /// after row 6's lapse. The adopted pair is `(epoch, None)` — hostless at
    /// our own epoch — so the report re-activates rather than dying, and the
    /// node picks the group back up instead of waiting a whole epoch out.
    #[test]
    fn a_delayed_renewal_after_a_lapse_re_activates_at_the_same_epoch() {
        let mut engine = started("a", external());
        engine.apply(Command::AnchorActivated {
            epoch: 3,
            lease_until: Time(10_000),
        });
        engine.on_tick(Time(10_000)); // row 6: the lease lapses
        assert_eq!(engine.role(), Role::Follower);
        assert_eq!(engine.leadership(), (3, None));

        let late = engine.apply(Command::AnchorActivated {
            epoch: 3,
            lease_until: Time(20_000),
        });
        assert!(
            late.iter()
                .any(|e| matches!(e, Effect::LeadershipChanged { .. })),
            "a hostless pair at our own epoch is ours to re-take"
        );
        assert_eq!(engine.role(), Role::Host);
        assert_eq!(engine.leadership(), (3, Some(&n("a"))));
        assert_eq!(engine.host_lease_until(), Some(Time(20_000)));
    }

    /// Row X6's leaving gate, mirroring row X1's: a round already in flight
    /// when [`Command::Leave`] landed comes back to a node that row 15 has
    /// already stepped down, and must not re-activate it at the epoch it just
    /// announced it was gone from.
    #[test]
    fn an_activation_that_lands_after_a_leave_is_inert() {
        for epoch in [5, 6] {
            let mut engine = started("a", external());
            engine.apply(Command::AnchorActivated {
                epoch: 5,
                lease_until: Time(99_000),
            });
            assert_eq!(engine.role(), Role::Host);

            engine.apply(Command::Leave);
            assert_eq!(engine.leadership(), (5, None), "row 15 demoted first");

            let late = engine.apply(Command::AnchorActivated {
                epoch,
                lease_until: Time(99_000),
            });
            assert!(late.is_empty(), "a leaving node activated at epoch {epoch}");
            assert_eq!(engine.role(), Role::Follower, "epoch {epoch}");
            assert_eq!(engine.leadership(), (5, None), "epoch {epoch}");
            assert_eq!(engine.host_lease_until(), None, "epoch {epoch}");
        }
    }

    /// Row X3: the same epoch again while hosting is a renewal. The lease
    /// moves to the later of the two, never to the earlier, so a report that
    /// overtook another cannot shorten an authority already granted.
    #[test]
    fn a_repeat_activation_extends_the_lease_and_never_shortens_it() {
        let mut engine = started("a", external());
        engine.apply(Command::AnchorActivated {
            epoch: 3,
            lease_until: Time(10_000),
        });
        assert_eq!(engine.host_lease_until(), Some(Time(10_000)));

        let extended = engine.apply(Command::AnchorActivated {
            epoch: 3,
            lease_until: Time(14_000),
        });
        assert!(extended.is_empty(), "a renewal announces nothing");
        assert_eq!(engine.host_lease_until(), Some(Time(14_000)));

        let stale = engine.apply(Command::AnchorActivated {
            epoch: 3,
            lease_until: Time(11_000),
        });
        assert!(stale.is_empty());
        assert_eq!(
            engine.host_lease_until(),
            Some(Time(14_000)),
            "a late report must not pull the lease in"
        );
    }

    /// Row X2's third arm: a host handed a *strictly higher* epoch re-activates
    /// at it. The fence moved, so every observer must be told.
    #[test]
    fn a_host_re_activates_at_a_strictly_higher_epoch() {
        let mut engine = started("a", external());
        engine.apply(Command::AnchorActivated {
            epoch: 3,
            lease_until: Time(10_000),
        });
        let effects = engine.apply(Command::AnchorActivated {
            epoch: 4,
            lease_until: Time(20_000),
        });
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(e, Effect::LeadershipChanged { .. }))
                .count(),
            1,
            "a moved fence is announced"
        );
        assert_eq!(engine.leadership(), (4, Some(&n("a"))));
        assert_eq!(engine.host_lease_until(), Some(Time(20_000)));
    }

    /// Row X6 on X4: an observation that does not outrank the adopted pair
    /// teaches nothing and announces nothing.
    #[test]
    fn an_observation_that_does_not_outrank_is_inert() {
        let mut engine = started("a", external());
        engine.apply(Command::AnchorActivated {
            epoch: 5,
            lease_until: Time(10_000),
        });
        for (epoch, host) in [(4, "b"), (5, "a"), (1, "c")] {
            let effects = engine.apply(Command::AnchorObserved {
                epoch,
                host: n(host),
            });
            assert!(effects.is_empty(), "({epoch}, {host}) was not inert");
            assert_eq!(engine.leadership(), (5, Some(&n("a"))));
            assert_eq!(engine.role(), Role::Host);
        }
    }
}
