//! Hosted-mode election under [`Activation::External`]: the claim prompt and
//! its guard, activation and lease extension from the anchor, observation
//! adopt and self-shadow, the rank-gated renewal prompt, the fail-closed
//! step-down, and the tier's purity — an `External` group never bids, never
//! grants, and never persists.
//!
//! Every test drives a real engine. The anchor itself is *not* modelled here:
//! the driver half is a later slice, so what a real driver would report after
//! a store round-trip arrives as the [`Command`] it will send. That is the
//! whole point of the command surface — the engine's rows are falsifiable
//! without a store, a runtime, or a clock.

use groupnet_core::{
    Activation, Command, Config, Effect, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId,
    Role, Status, Time,
};
use groupnet_testkit::frames::*;

/// The default anti-entropy cadence, which the prompt rides.
const AE_MS: u64 = 200;

/// The lease every fixture runs with. It is also the anchor TTL and — the part
/// that matters here — the boot guard, so a fixture engine started at
/// [`Time::ZERO`] may not prompt before this instant.
///
/// A whole number of anti-entropy rounds, deliberately: the prompt rides the
/// gossip cadence, so a boot guard that lapsed *between* two rounds would make
/// every "prompts at the guard" assertion off by one round and read as if the
/// guard were longer than it is.
const LEASE_MS: u64 = 3 * AE_MS;

fn external_config(lease_ms: u64) -> Config {
    Config {
        mode: GroupMode::Hosted(HostedConfig {
            activation: Activation::External {
                steal_margin_ms: 500,
            },
            lease_ms,
        }),
        ..Config::default()
    }
}

/// The anchor prompts `effects` asks for, as their epoch hints.
fn prompts(effects: &[Effect]) -> Vec<u64> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::AnchorClaimDue { epoch_hint } => Some(*epoch_hint),
            _ => None,
        })
        .collect()
}

/// One engine plus **every effect it has ever emitted**, so a purity claim can
/// be made over a whole run rather than over a sampled tick.
struct Run {
    engine: GroupEngine,
    all: Vec<Effect>,
    saw_claimant: bool,
}

impl Run {
    /// A started engine for `id` that already knows `peers`, in `config`.
    /// The `start` batch and the digest that taught it its peers are both
    /// captured, so nothing this engine has ever emitted escapes the purity
    /// assertion.
    fn new(id: &NodeId, peers: &[NodeId], config: Config) -> Self {
        let mut engine = GroupEngine::new(GroupId::new(TEST_GROUP), id.clone(), [], config);
        let mut all = Vec::new();
        if let Some(from) = peers.first() {
            let digest = peers
                .iter()
                .map(|p| ndigest(p.as_str(), 0, Status::Alive, 0))
                .collect();
            all.extend(engine.on_message(from.clone(), &digest_frame(digest, vec![]), Time::ZERO));
        }
        all.extend(engine.start(Time::ZERO));
        Self {
            engine,
            all,
            saw_claimant: false,
        }
    }

    /// An [`Activation::External`] run at the fixture lease.
    fn external(id: &NodeId, peers: &[NodeId]) -> Self {
        Self::new(id, peers, external_config(LEASE_MS))
    }

    fn record(&mut self, effects: Vec<Effect>) -> Vec<Effect> {
        self.saw_claimant |= self.engine.role() == Role::Claimant;
        self.all.extend(effects.iter().cloned());
        effects
    }

    fn tick(&mut self, at: u64) -> Vec<Effect> {
        let effects = self.engine.on_tick(Time(at));
        self.record(effects)
    }

    fn apply(&mut self, cmd: Command) -> Vec<Effect> {
        let effects = self.engine.apply(cmd);
        self.record(effects)
    }

    fn deliver(&mut self, from: &NodeId, wire: &[u8], at: u64) -> Vec<Effect> {
        let effects = self.engine.on_message(from.clone(), wire, Time(at));
        self.record(effects)
    }

    /// The **X-purity** pin: over everything this engine has emitted, not one
    /// `LeadClaim`, not one `LeadGrant`, not one `PersistGrant` — and
    /// [`Role::Claimant`] was never observed at any step. An `External` group
    /// has no bid to stand and no endorsement to collect, and this is that
    /// claim in its falsifiable form.
    fn assert_pure(&self) {
        assert!(
            claim_frames(&self.all).is_empty(),
            "an External group put a claim on the wire: {:?}",
            claim_frames(&self.all)
        );
        assert!(
            grant_frames(&self.all).is_empty(),
            "an External group put a grant on the wire"
        );
        assert!(
            persisted_grants(&self.all).is_empty(),
            "an External group asked for a grant persist"
        );
        assert!(
            !self.saw_claimant,
            "an External group entered Role::Claimant"
        );
        assert_ne!(self.engine.role(), Role::Claimant);
    }
}

#[test]
fn the_top_ranked_node_prompts_once_past_its_boot_guard() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);

    // The boot guard is one lease long: a node that has just joined hears an
    // incumbent's LeadState out before deciding the group is vacant, and under
    // External that means before it spends an anchor round.
    for at in [AE_MS, 2 * AE_MS] {
        let effects = run.tick(at);
        assert!(prompts(&effects).is_empty(), "prompted early, at {at}");
        assert!(election_frames(&effects).is_empty(), "wire traffic at {at}");
    }
    assert_eq!(run.engine.role(), Role::Follower);
    assert_eq!(run.engine.observed_epoch(), 0);

    let opened = run.tick(LEASE_MS);
    assert_eq!(
        prompts(&opened),
        vec![1],
        "the first bid is one above the epoch-0 sentinel"
    );
    // A prompt decides nothing: no epoch is spent, no role changes, nothing is
    // announced, and nothing reaches the wire.
    assert_eq!(run.engine.role(), Role::Follower);
    assert_eq!(run.engine.observed_epoch(), 0);
    assert_eq!(run.engine.leadership(), (0, None));
    assert!(leadership_changes(&opened).is_empty());
    assert!(election_frames(&opened).is_empty());
    run.assert_pure();
}

#[test]
fn a_node_that_is_not_top_ranked_never_prompts() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[1], &[rank[0].clone(), rank[2].clone()]);

    for at in [LEASE_MS, LEASE_MS + AE_MS, LEASE_MS + 2 * AE_MS] {
        let effects = run.tick(at);
        assert!(
            prompts(&effects).is_empty(),
            "a second-ranked node prompted at {at}"
        );
    }
    // The premise, asserted rather than assumed: this really is a node the
    // rendezvous ranking put second, for the whole window above. (Left long
    // enough for the detector to bury its peers it would become the top-ranked
    // live candidate of a group of one — a different row, and a legitimate
    // one.)
    assert!(
        !run.engine.is_coordinator(),
        "the top-ranked peer is still live"
    );
    assert_eq!(run.engine.role(), Role::Follower);
    assert_eq!(run.engine.leadership(), (0, None));
    run.assert_pure();
}

#[test]
fn the_prompt_rides_the_anti_entropy_cadence() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);

    assert_eq!(prompts(&run.tick(LEASE_MS)), vec![1]);
    // Between rounds the guard is just as open, and the engine still says
    // nothing: a prompt is a repeated level signal on the gossip cadence, not
    // a per-tick poll a driver would have to rate-limit itself.
    for at in [LEASE_MS + 1, LEASE_MS + AE_MS - 1] {
        assert!(
            prompts(&run.tick(at)).is_empty(),
            "prompted off-cadence at {at}"
        );
    }
    assert_eq!(
        prompts(&run.tick(LEASE_MS + AE_MS)),
        vec![1],
        "and it repeats, so a prompt lost to a busy driver self-heals"
    );
    run.assert_pure();
}

#[test]
fn activation_announces_the_pair_and_broadcasts_nothing_but_lead_state() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);
    run.tick(LEASE_MS);

    // What the driver reports after winning its conditional write. The lease
    // is the driver's, anchored at the instant its round *began*.
    let activated = run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(LEASE_MS + LEASE_MS),
    });

    assert_eq!(
        leadership_changes(&activated),
        vec![(1, Some(rank[0].clone()))],
        "activation is the first thing anyone hears about"
    );
    let states = state_frames(&activated);
    assert_eq!(states.len(), 2, "the new pair goes to every live member");
    for (to, epoch, host) in &states {
        assert!(rank[1..].contains(to));
        assert_eq!((*epoch, host.clone()), (1, Some(rank[0].clone())));
    }
    // One announcement plus two sends, and nothing else in the batch — no
    // claim, no grant, no persist, no timer churn beyond what row 4 already
    // emitted under Settle.
    assert_eq!(activated.len(), 3, "{activated:?}");

    assert_eq!(run.engine.role(), Role::Host);
    assert_eq!(run.engine.leadership(), (1, Some(&rank[0])));
    assert_eq!(run.engine.observed_epoch(), 1);
    assert_eq!(run.engine.host_lease_until(), Some(Time(2 * LEASE_MS)));

    // Row X7: a host keeps prompting, hinting the epoch it already holds —
    // a renewal decides nothing, so it allocates nothing.
    assert_eq!(prompts(&run.tick(LEASE_MS + AE_MS)), vec![1]);
    run.assert_pure();
}

#[test]
fn a_repeat_activation_at_the_same_epoch_only_extends_the_lease() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);
    run.tick(LEASE_MS);
    run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(8_000),
    });

    let renewed = run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(12_000),
    });
    assert!(
        renewed.is_empty(),
        "a renewal changed nobody's belief, so it announces nothing"
    );
    assert_eq!(run.engine.host_lease_until(), Some(Time(12_000)));

    // A report that overtook another cannot shorten an authority already
    // granted: a round only ever pushes the lease out.
    let late = run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(9_000),
    });
    assert!(late.is_empty());
    assert_eq!(run.engine.host_lease_until(), Some(Time(12_000)));
    assert_eq!(run.engine.leadership(), (1, Some(&rank[0])));
    run.assert_pure();
}

#[test]
fn an_observed_record_naming_another_node_deposes_this_one() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);
    run.tick(LEASE_MS);
    run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(20_000),
    });
    assert_eq!(run.engine.role(), Role::Host);

    // The driver re-read the anchor and found somebody else there — a steal
    // that beat this host to the record.
    let deposed = run.apply(Command::AnchorObserved {
        epoch: 2,
        host: rank[1].clone(),
    });
    assert_eq!(
        leadership_changes(&deposed),
        vec![(2, Some(rank[1].clone()))]
    );
    assert_eq!(run.engine.role(), Role::Follower);
    assert_eq!(run.engine.leadership(), (2, Some(&rank[1])));
    assert_eq!(run.engine.observed_epoch(), 2);
    assert_eq!(run.engine.host_lease_until(), None);

    // And the guard reopens against it: the adopted host is somebody else, so
    // the top-ranked node bids above the pair it was fenced by.
    assert_eq!(prompts(&run.tick(LEASE_MS + AE_MS)), vec![3]);
    run.assert_pure();
}

#[test]
fn a_record_naming_this_node_is_learned_as_an_epoch_never_as_a_hostship() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    // The restart shape: this process has no memory of the hostship the anchor
    // still records for it.
    let mut run = Run::external(&rank[0], &rank[1..]);
    assert_eq!(run.engine.leadership(), (0, None));

    let shadow = run.apply(Command::AnchorObserved {
        epoch: 5,
        host: rank[0].clone(),
    });
    assert_eq!(
        leadership_changes(&shadow),
        vec![(5, None)],
        "row 12b: the epoch is taken with the hostship stripped off"
    );
    assert_eq!(run.engine.leadership(), (5, None));
    assert_eq!(run.engine.observed_epoch(), 5);
    assert_eq!(run.engine.role(), Role::Follower);

    // The same record read again is the fixed point this rule leaves behind:
    // inert, so a driver polling the anchor cannot churn the effect stream.
    assert!(
        run.apply(Command::AnchorObserved {
            epoch: 5,
            host: rank[0].clone(),
        })
        .is_empty()
    );
    assert_eq!(run.engine.leadership(), (5, None));

    // Hostship is re-won, never resumed: the prompt bids strictly above the
    // epoch the anchor already recorded for this node.
    assert_eq!(prompts(&run.tick(LEASE_MS)), vec![6]);
    // And the round that wins it activates — the `>=` bar on row X2 exists for
    // exactly this shape, since row X5 already raised `highest_seen` to 5.
    let rewon = run.apply(Command::AnchorActivated {
        epoch: 6,
        lease_until: Time(20_000),
    });
    assert_eq!(leadership_changes(&rewon), vec![(6, Some(rank[0].clone()))]);
    assert_eq!(run.engine.role(), Role::Host);
    run.assert_pure();
}

#[test]
fn a_host_that_stops_winning_anchor_rounds_demotes_at_its_exact_lease() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);
    run.tick(LEASE_MS);
    run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(10_000),
    });

    // The row-5 gate, which is the whole fail-closed posture: this node is and
    // stays the group's top-ranked live candidate, and under `Settle` that
    // alone would renew its lease for ever. Under `External` rank is evidence
    // of nothing — only a fresh anchor round (row X3) extends the lease.
    let held = run.tick(9_999);
    assert!(run.engine.is_coordinator(), "still top-ranked");
    assert!(leadership_changes(&held).is_empty());
    assert_eq!(run.engine.role(), Role::Host);
    assert_eq!(
        run.engine.host_lease_until(),
        Some(Time(10_000)),
        "rank must not have renewed anything"
    );

    let lapsed = run.tick(10_000);
    assert_eq!(
        leadership_changes(&lapsed),
        vec![(1, None)],
        "the lease lapses at exactly its instant, not a tick later"
    );
    assert_eq!(run.engine.role(), Role::Follower);
    assert_eq!(run.engine.leadership(), (1, None));
    assert_eq!(run.engine.host_lease_until(), None);
    assert!(run.engine.is_coordinator(), "and it is still top-ranked");

    // Hostless, but not hopeless: the guard reopens and the node asks the
    // anchor for a strictly higher epoch.
    assert_eq!(prompts(&run.tick(10_200)), vec![2]);
    run.assert_pure();
}

/// Row X7 is **rank-gated**, exactly as row Q7's renewal round is: an
/// `External` host that is no longer the group's top-ranked live candidate is
/// never prompted again, so nothing extends its lease and it demotes at the
/// instant the anchor's last winning round bought it.
///
/// The premise is the tier's own: the anchor awards an epoch, the ranking does
/// not — row X2 activates whoever the driver says won a conditional write,
/// which is why a second-ranked node hosts here at all. What rank decides is
/// whether it is still *asked* to renew, and the answer is the same one rows 5
/// and Q7 give: a host that no longer ranks should be letting its lease lapse,
/// not asking for an extension.
///
/// The silence is made falsifiable rather than asserted into. Every tick below
/// is a genuine anti-entropy round — proved by row 7's repair beacon reaching
/// the wire in the same batch, which rides exactly the cadence the prompt would
/// have — and not one of them carries a prompt.
#[test]
fn an_outranked_host_is_never_prompted_to_renew_and_lapses() {
    /// The lease the anchor's last round bought. Short on purpose: the only
    /// thing standing between this node and row 6 is the clock, and the whole
    /// window has to stay inside the detector's, or the top-ranked peer would be
    /// buried and this node would legitimately become a candidate again.
    const LEASE_UNTIL: u64 = 1_000;

    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[1], &[rank[0].clone(), rank[2].clone()]);
    let activated = run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(LEASE_UNTIL),
    });
    assert_eq!(
        leadership_changes(&activated),
        vec![(1, Some(rank[1].clone()))],
        "the anchor awards the epoch; the ranking does not"
    );
    assert_eq!(run.engine.role(), Role::Host);

    let mut at = AE_MS;
    while at < LEASE_UNTIL {
        let effects = run.tick(at);
        assert!(
            !run.engine.is_coordinator(),
            "premise: the top-ranked peer is still live at {at}"
        );
        assert!(
            !state_frames(&effects).is_empty(),
            "the tick at {at} ran no anti-entropy round, so its silence proves nothing"
        );
        assert!(
            prompts(&effects).is_empty(),
            "an outranked host was prompted to renew at {at}"
        );
        assert_eq!(run.engine.role(), Role::Host, "at {at}");
        assert_eq!(
            run.engine.host_lease_until(),
            Some(Time(LEASE_UNTIL)),
            "nothing may have moved the lease at {at}"
        );
        at += AE_MS;
    }

    let lapsed = run.tick(LEASE_UNTIL);
    assert_eq!(
        leadership_changes(&lapsed),
        vec![(1, None)],
        "with nothing renewing it, the lease lapses at exactly its instant"
    );
    assert!(
        prompts(&lapsed).is_empty(),
        "row X7 sits past row 6's lapse check: a host out of lease steps down \
         rather than asking the anchor for more"
    );
    assert_eq!(run.engine.role(), Role::Follower);
    assert_eq!(run.engine.leadership(), (1, None));
    assert_eq!(run.engine.host_lease_until(), None);
    run.assert_pure();
}

#[test]
fn leaving_gives_up_the_hostship_and_stops_the_prompt() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);
    run.tick(LEASE_MS);
    run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(50_000),
    });

    let left = run.apply(Command::Leave);
    assert_eq!(
        leadership_changes(&left),
        vec![(1, None)],
        "a leave gives up hostship before it disseminates"
    );
    assert_eq!(run.engine.role(), Role::Follower);
    assert_eq!(run.engine.leadership(), (1, None));

    for at in [LEASE_MS + AE_MS, 10_000, 60_000] {
        assert!(
            prompts(&run.tick(at)).is_empty(),
            "a leaving node prompted at {at}"
        );
    }
    run.assert_pure();
}

/// Row X6's leaving gate, which is row 15's rule applied to *driver input*: a
/// leave demotes before it disseminates so this node never serves an epoch it
/// has announced it is gone from, and an anchor round already in flight when
/// [`Command::Leave`] landed must not undo that.
///
/// The report tested is the dangerous one — an activation at **the very epoch
/// the leaver was still holding**, which is what a renewal round in flight
/// comes back as. Without the gate it walks straight into row X2 (the pair is
/// `(1, None)`, so nothing else refuses it) and re-hosts a node the whole
/// cluster is being told has gone.
#[test]
fn an_anchor_round_that_lands_after_a_leave_never_re_hosts_the_leaver() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);
    run.tick(LEASE_MS);
    run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(50_000),
    });
    assert_eq!(run.engine.role(), Role::Host);

    run.apply(Command::Leave);
    assert_eq!(run.engine.leadership(), (1, None), "row 15 demoted first");

    // The round that was in flight when the leave landed. Both readings of it:
    // the renewal it was (epoch 1) and the re-win it could have become (2).
    for epoch in [1, 2] {
        let late = run.apply(Command::AnchorActivated {
            epoch,
            lease_until: Time(90_000),
        });
        assert!(
            late.is_empty(),
            "a leaving node re-activated at epoch {epoch}"
        );
        assert_eq!(run.engine.role(), Role::Follower, "epoch {epoch}");
        assert_eq!(run.engine.leadership(), (1, None), "epoch {epoch}");
        assert_eq!(run.engine.host_lease_until(), None, "epoch {epoch}");
    }

    // And it stays inert: nothing prompts it again, and no tick resurrects it.
    for at in [LEASE_MS + AE_MS, 60_000] {
        let effects = run.tick(at);
        assert!(prompts(&effects).is_empty(), "prompted at {at}");
        assert!(leadership_changes(&effects).is_empty(), "at {at}");
    }
    assert_eq!(run.engine.role(), Role::Follower);
    run.assert_pure();
}

#[test]
fn an_activation_below_what_gossip_taught_is_dropped() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);
    // Gossip and the anchor feed one bar: a LeadState repair raises
    // `highest_seen`, and an anchor report under it is stale by construction.
    run.deliver(&rank[1], &lead_state_frame(9, Some(rank[1].as_str())), 10);
    assert_eq!(run.engine.observed_epoch(), 9);
    assert_eq!(run.engine.leadership(), (9, Some(&rank[1])));

    for epoch in [0, 1, 8] {
        let effects = run.apply(Command::AnchorActivated {
            epoch,
            lease_until: Time(50_000),
        });
        assert!(
            effects.is_empty(),
            "epoch {epoch} activated over a higher pair"
        );
        assert_eq!(run.engine.role(), Role::Follower, "epoch {epoch}");
        assert_eq!(
            run.engine.leadership(),
            (9, Some(&rank[1])),
            "epoch {epoch}"
        );
    }
    // The bid the guard actually asks for is above the bar, and it lands.
    assert_eq!(prompts(&run.tick(LEASE_MS)), vec![10]);
    let won = run.apply(Command::AnchorActivated {
        epoch: 10,
        lease_until: Time(50_000),
    });
    assert_eq!(leadership_changes(&won), vec![(10, Some(rank[0].clone()))]);
    run.assert_pure();
}

#[test]
fn both_anchor_commands_are_inert_under_every_other_activation() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let configs: Vec<(&str, Config)> = vec![
        ("eventual", Config::default()),
        ("settle", settle_config(500, LEASE_MS)),
        ("quorum", quorum_config(&["a", "b", "c"], LEASE_MS)),
    ];

    for (name, config) in configs {
        for cmd in [
            Command::AnchorActivated {
                epoch: 7,
                lease_until: Time(50_000),
            },
            Command::AnchorObserved {
                epoch: 7,
                host: rank[1].clone(),
            },
        ] {
            let mut run = Run::new(&rank[0], &rank[1..], config.clone());
            let effects = run.apply(cmd);
            assert!(effects.is_empty(), "{name} answered an anchor command");
            assert!(election_frames(&effects).is_empty(), "{name}");
            assert_eq!(run.engine.leadership(), (0, None), "{name}");
            assert_eq!(run.engine.observed_epoch(), 0, "{name}");
            assert_eq!(run.engine.role(), Role::Follower, "{name}");
        }
    }
}

#[test]
fn a_long_external_run_never_enters_claimant_and_never_bids() {
    let rank = rank_by_placement(&["a", "b", "c"]);
    let mut run = Run::external(&rank[0], &rank[1..]);

    // A schedule with every transition the tier has: prompt, win, renew, get
    // deposed by a steal, get taught back by gossip, lapse, re-win.
    run.tick(LEASE_MS);
    run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(9_000),
    });
    run.tick(LEASE_MS + AE_MS);
    run.apply(Command::AnchorActivated {
        epoch: 1,
        lease_until: Time(14_000),
    });
    run.apply(Command::AnchorObserved {
        epoch: 2,
        host: rank[1].clone(),
    });
    run.deliver(
        &rank[2],
        &lead_state_frame(2, Some(rank[1].as_str())),
        6_000,
    );
    run.tick(6_200);
    run.apply(Command::AnchorObserved {
        epoch: 3,
        host: rank[0].clone(),
    });
    run.tick(6_400);
    run.apply(Command::AnchorActivated {
        epoch: 4,
        lease_until: Time(10_400),
    });
    run.tick(10_400); // the lease lapses
    run.tick(10_600);

    assert_eq!(run.engine.leadership(), (4, None));
    assert_eq!(run.engine.observed_epoch(), 4);
    // The only election frames an External group ever built are LeadState.
    assert_eq!(
        election_frames(&run.all).len(),
        state_frames(&run.all).len(),
        "an External group put a non-LeadState election frame on the wire"
    );
    assert!(!state_frames(&run.all).is_empty(), "and it did build those");
    run.assert_pure();
}
