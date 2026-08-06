//! Hosted-mode election: claim guards, the settle window, claim conflicts,
//! adoption/fencing, lease hysteresis, and the mode's inertness in `Eventual`.
//!
//! Every test drives a real engine through the fixture frames, so what is
//! asserted is the wire behaviour a peer would actually observe.

use groupnet_core::{Command, Effect, GroupEngine, NodeId, Role, Status, Time, placement, wire};
use groupnet_testkit::frames::*;

/// The fixture group's placement ranking of `ids`, best-ranked first — the
/// same rendezvous order the coordinator, the claim guard, and the equal-epoch
/// fencing tiebreak all read.
fn ranked(ids: &[&str]) -> Vec<NodeId> {
    let members: Vec<(NodeId, u32)> = ids.iter().map(|id| (NodeId::new(*id), 1)).collect();
    placement::owners(TEST_GROUP, &members, ids.len())
}

/// Teaches `engine` that `peers` exist and are alive, via one digest frame.
fn learn(engine: &mut GroupEngine, peers: &[NodeId], now: Time) {
    let digest = peers
        .iter()
        .map(|p| ndigest(p.as_str(), 0, Status::Alive, 0))
        .collect();
    let from = peers.first().cloned().expect("at least one peer");
    engine.on_message(from, &digest_frame(digest, vec![]), now);
}

/// The election frames `effects` sends, as `(recipient, body)` pairs. Digest
/// and probe traffic is filtered out — an election test asserts on the
/// election wire only.
fn lead_bodies(effects: &[Effect]) -> Vec<(NodeId, wire::LeadBody)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Send { to, wire } => {
                let frame = wire::decode(wire)?;
                frame.lead.map(|body| (to.clone(), body))
            }
            _ => None,
        })
        .collect()
}

/// The leadership transitions `effects` announces, in order.
fn leadership_changes(effects: &[Effect]) -> Vec<(u64, Option<NodeId>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::LeadershipChanged { epoch, host } => Some((*epoch, host.clone())),
            _ => None,
        })
        .collect()
}

fn claim_of(epoch: u64, claimant: &NodeId) -> wire::LeadBody {
    wire::LeadBody::Claim {
        epoch,
        claimant: claimant.clone(),
    }
}

fn state_of(epoch: u64, host: &NodeId) -> wire::LeadBody {
    wire::LeadBody::State {
        epoch,
        host: Some(host.clone()),
    }
}

/// The hostless pair at `epoch` — what a lapsed lease, a step-down, or row 12b
/// leaves behind, and still a fence the cluster is ordered by.
fn hostless_state_of(epoch: u64) -> wire::LeadBody {
    wire::LeadBody::State { epoch, host: None }
}

/// An engine for `id` parked on the hostless pair `(epoch, None)`, reached the
/// way a restarted or deposed host reaches it: row 12b takes a higher pair that
/// names us with the hostship stripped off.
fn parked_hostless(id: &NodeId, peers: &[NodeId], epoch: u64) -> GroupEngine {
    let mut e = started(id, peers, 4_000);
    let taught = peers.first().expect("at least one peer").clone();
    e.on_message(
        taught,
        &lead_state_frame(epoch, Some(id.as_str())),
        Time(10),
    );
    assert_eq!(e.leadership(), (epoch, None), "row 12b sets up the fence");
    e
}

/// A started engine for `id` that already knows `peers`, hosted with a 500ms
/// settle window and a `lease_ms` lease.
fn started(id: &NodeId, peers: &[NodeId], lease_ms: u64) -> GroupEngine {
    let mut engine = hosted_engine(id.as_str(), &[], 500, lease_ms);
    learn(&mut engine, peers, Time::ZERO);
    engine.start(Time::ZERO);
    engine
}

#[test]
fn the_top_ranked_node_claims_once_past_its_boot_guard() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[0], &rank[1..], 4_000);

    // The boot guard runs one settle window from `start`: a node that has just
    // joined hears an incumbent out before deciding the group is vacant.
    let early = e.on_tick(Time(300));
    assert!(
        lead_bodies(&early).is_empty(),
        "the boot guard must suppress an early claim"
    );
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.observed_epoch(), 0);

    let opened = e.on_tick(Time(500));
    let claims = lead_bodies(&opened);
    assert_eq!(claims.len(), 2, "a claim goes to every live member");
    for (to, body) in &claims {
        assert!(
            rank[1..].contains(to),
            "claimed at an unexpected peer {to:?}"
        );
        assert_eq!(*body, claim_of(1, &rank[0]));
    }
    assert_eq!(e.role(), Role::Claimant);
    assert_eq!(e.observed_epoch(), 1);
    assert_eq!(
        e.leadership(),
        (0, None),
        "a claim is a bid, not an adoption"
    );
    assert!(
        leadership_changes(&opened).is_empty(),
        "a claim notifies nobody"
    );
}

#[test]
fn a_node_that_is_not_top_ranked_never_claims() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[1], &[rank[0].clone(), rank[2].clone()], 4_000);

    for at in [300, 500, 700, 900] {
        let effects = e.on_tick(Time(at));
        assert!(
            lead_bodies(&effects).is_empty(),
            "a non-top-ranked node claimed at {at}"
        );
    }
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.leadership(), (0, None));
}

#[test]
fn claims_over_a_host_that_is_absent_or_outranked() {
    let rank = ranked(&["a", "b", "c"]);
    // An unknown host (crashed, or never known) and a live-but-outranked one
    // are the same guard: the top-ranked live candidate claims either way.
    for host in [NodeId::new("ghost"), rank[1].clone()] {
        let mut e = started(&rank[0], &rank[1..], 4_000);
        let adopted = e.on_message(
            rank[1].clone(),
            &lead_state_frame(3, Some(host.as_str())),
            Time(10),
        );
        assert_eq!(leadership_changes(&adopted), vec![(3, Some(host.clone()))]);
        assert_eq!(e.leadership(), (3, Some(&host)));

        let claims = lead_bodies(&e.on_tick(Time(500)));
        assert_eq!(claims.len(), 2, "claiming over host {host:?}");
        assert_eq!(
            claims[0].1,
            claim_of(4, &rank[0]),
            "a claim is always one above the highest epoch seen"
        );
    }
}

#[test]
fn a_standing_claim_activates_when_its_window_closes() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[0], &rank[1..], 4_000);
    e.on_tick(Time(500)); // claims epoch 1, settling at 1000

    let activated = e.on_tick(Time(1_000));
    assert_eq!(
        leadership_changes(&activated),
        vec![(1, Some(rank[0].clone()))],
        "activation is the first thing anyone hears about"
    );
    let states = lead_bodies(&activated);
    assert_eq!(states.len(), 2, "the new pair goes to every live member");
    for (to, body) in &states {
        assert!(rank[1..].contains(to));
        assert_eq!(*body, state_of(1, &rank[0]));
    }
    assert_eq!(e.role(), Role::Host);
    assert_eq!(e.leadership(), (1, Some(&rank[0])));
    assert_eq!(e.host_lease_until(), Some(Time(5_000)));
}

#[test]
fn the_settle_deadline_and_the_lease_arm_the_driver_timer() {
    // Both election deadlines are shorter than the gossip cadence here, so the
    // armed timer is exactly the election's — the sim (and any driver with a
    // real timer) would otherwise activate, or step down, a round late.
    let rank = ranked(&["a", "b", "c"]);
    let mut e = hosted_engine(rank[0].as_str(), &[], 20, 30);
    learn(&mut e, &rank[1..], Time::ZERO);
    e.start(Time::ZERO);

    let claimed = e.on_tick(Time(30));
    assert_eq!(e.role(), Role::Claimant);
    assert_eq!(
        armed(&claimed),
        Some(Time(50)),
        "the settle deadline must arm the timer"
    );

    let activated = e.on_tick(Time(50));
    assert_eq!(e.role(), Role::Host);
    assert_eq!(
        armed(&activated),
        Some(Time(80)),
        "the lease expiry must arm the timer"
    );
}

/// The instant the effects ask to be ticked at.
fn armed(effects: &[Effect]) -> Option<Time> {
    effects.iter().find_map(|e| match e {
        Effect::ArmTimer { at } => Some(*at),
        _ => None,
    })
}

#[test]
fn a_claim_yields_to_a_better_pair_and_stands_against_a_worse_one() {
    let rank = ranked(&["a", "b", "c", "d"]);
    // `rank[1]` is the best of the three members it knows, so it claims; the
    // absent `rank[0]` outranks it when it turns up.
    let claimant = || {
        let mut e = started(&rank[1], &[rank[2].clone(), rank[3].clone()], 4_000);
        e.on_tick(Time(500));
        assert_eq!(e.role(), Role::Claimant);
        e
    };

    let mut yields = claimant();
    yields.on_message(
        rank[0].clone(),
        &lead_claim_frame(1, rank[0].as_str()),
        Time(600),
    );
    assert_eq!(
        yields.role(),
        Role::Follower,
        "a better-ranked claim at the same epoch ends ours"
    );
    assert!(
        yields.members().any(|n| *n == rank[0]),
        "a claim proves its claimant is live, so rank must count it"
    );

    let mut stands = claimant();
    stands.on_message(
        rank[2].clone(),
        &lead_claim_frame(1, rank[2].as_str()),
        Time(600),
    );
    assert_eq!(
        stands.role(),
        Role::Claimant,
        "a worse-ranked claim at the same epoch changes nothing"
    );

    let mut outbid = claimant();
    outbid.on_message(
        rank[2].clone(),
        &lead_claim_frame(2, rank[2].as_str()),
        Time(600),
    );
    assert_eq!(
        outbid.role(),
        Role::Follower,
        "the order is epoch-major: a higher epoch wins whatever its rank"
    );
    assert_eq!(outbid.observed_epoch(), 2);
}

#[test]
fn adopts_a_better_pair_and_teaches_back_a_worse_one() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[2], &[rank[0].clone(), rank[1].clone()], 4_000);

    let adopted = e.on_message(
        rank[0].clone(),
        &lead_state_frame(3, Some(rank[0].as_str())),
        Time(10),
    );
    assert_eq!(
        leadership_changes(&adopted),
        vec![(3, Some(rank[0].clone()))]
    );
    assert!(
        lead_bodies(&adopted).is_empty(),
        "adopting answers the sender nothing"
    );
    assert_eq!(e.leadership(), (3, Some(&rank[0])));
    assert_eq!(e.observed_epoch(), 3);

    let taught = e.on_message(
        rank[1].clone(),
        &lead_state_frame(2, Some(rank[1].as_str())),
        Time(20),
    );
    assert_eq!(
        lead_bodies(&taught),
        vec![(rank[1].clone(), state_of(3, &rank[0]))],
        "a worse pair is repaired, not adopted"
    );
    assert!(leadership_changes(&taught).is_empty());
    assert_eq!(e.leadership(), (3, Some(&rank[0])));

    let quiet = e.on_message(
        rank[0].clone(),
        &lead_state_frame(3, Some(rank[0].as_str())),
        Time(30),
    );
    assert!(quiet.is_empty(), "an equal pair exchanges nothing");
}

#[test]
fn a_demoted_host_learns_the_epoch_of_its_own_hostship_but_never_the_hostship() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[0], &rank[1..], 4_000);
    e.on_tick(Time(500));
    e.on_tick(Time(1_000));
    assert_eq!(e.role(), Role::Host);

    // Fenced by a higher pair: we step down and adopt it.
    let fenced = e.on_message(
        rank[1].clone(),
        &lead_state_frame(2, Some(rank[1].as_str())),
        Time(1_100),
    );
    assert_eq!(
        leadership_changes(&fenced),
        vec![(2, Some(rank[1].clone()))]
    );
    assert_eq!(e.role(), Role::Follower);

    // An echo of our own past hostship — at a *higher* epoch than we hold —
    // must not resurrect an authority we have already given up. Its epoch,
    // though, is the fence the cluster is ordered by, and it is learned: the
    // pair is taken with the hostship stripped off (row 12b).
    let echo = e.on_message(
        rank[2].clone(),
        &lead_state_frame(5, Some(rank[0].as_str())),
        Time(1_200),
    );
    assert_eq!(
        leadership_changes(&echo),
        vec![(5, None)],
        "the epoch is learned and announced, hostless"
    );
    assert!(
        lead_bodies(&echo).is_empty(),
        "a pair naming us is never answered"
    );
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(
        e.leadership(),
        (5, None),
        "hostship is entered only by our own activation"
    );
    assert_eq!(e.observed_epoch(), 5);
}

/// The restart wedge, at engine level. A host that came back with its election
/// state wiped settles a *stale* epoch and self-hosts; the survivors hold a
/// higher pair that names it. Refusing that pair whole — the pre-row-12b
/// behaviour — left the cluster agreeing on the host and disagreeing on the
/// epoch for ever, with the stale host unable to claim its way out because it
/// believed itself to be the adopted host.
#[test]
fn a_stale_host_steps_down_to_the_epoch_of_a_higher_pair_naming_itself() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[0], &rank[1..], 4_000);
    e.on_tick(Time(500)); // claims the stale epoch 1
    e.on_tick(Time(1_000)); // and activates it
    assert_eq!(e.role(), Role::Host);
    assert_eq!(e.leadership(), (1, Some(&rank[0])));

    // First contact with a survivor: the pair the cluster remembers for us.
    let fenced = e.on_message(
        rank[1].clone(),
        &lead_state_frame(3, Some(rank[0].as_str())),
        Time(1_100),
    );
    assert_eq!(
        leadership_changes(&fenced),
        vec![(3, None)],
        "the stale host steps down, hostless, at the epoch it was taught"
    );
    assert!(
        lead_bodies(&fenced).is_empty(),
        "a pair naming us is never answered"
    );
    assert_eq!(e.role(), Role::Follower, "the demotion is real");
    assert_eq!(
        e.leadership(),
        (3, None),
        "the epoch is learned; the hostship in it is not"
    );
    assert_eq!(e.observed_epoch(), 3);
    assert_eq!(e.host_lease_until(), None);

    // Every peer still holds that pair and teaches it back on every round, so
    // the state row 12b leaves behind has to be a fixed point of it.
    let again = e.on_message(
        rank[2].clone(),
        &lead_state_frame(3, Some(rank[0].as_str())),
        Time(1_200),
    );
    assert!(
        again.is_empty(),
        "re-delivery must neither re-announce the step-down nor answer the sender"
    );
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.leadership(), (3, None));
    assert_eq!(e.observed_epoch(), 3);
}

/// The same fixed point reached the other way: a host whose lease lapsed holds
/// `(e, None)` while every peer still holds `(e, Some(us))` — a pair that
/// outranks ours (`Some` beats `None` at equal epochs) and names us, so row 12b
/// fires on every repair round. It must be inert, or a step-down would announce
/// itself for ever and the observable pair would flap.
#[test]
fn an_equal_epoch_echo_of_our_own_hostship_is_inert() {
    let rank = ranked(&["a", "b", "c", "d"]);
    let mut e = started(&rank[1], &[rank[2].clone(), rank[3].clone()], 2_000);
    e.on_tick(Time(500)); // claim epoch 1
    e.on_tick(Time(1_000)); // activate; lease runs to 3000
    assert_eq!(e.role(), Role::Host);

    // A better-ranked member turns up, so the lease is never renewed again.
    e.on_message(
        rank[0].clone(),
        &digest_frame(vec![ndigest(rank[0].as_str(), 0, Status::Alive, 0)], vec![]),
        Time(1_100),
    );
    let lapsed = e.on_tick(Time(3_000));
    assert_eq!(leadership_changes(&lapsed), vec![(1, None)]);
    assert_eq!(e.leadership(), (1, None));

    for at in [3_100, 3_200] {
        let echo = e.on_message(
            rank[2].clone(),
            &lead_state_frame(1, Some(rank[1].as_str())),
            Time(at),
        );
        assert!(
            echo.is_empty(),
            "an equal-epoch echo of our own hostship changed something at {at}"
        );
        assert_eq!(e.role(), Role::Follower);
        assert_eq!(e.leadership(), (1, None));
        assert_eq!(e.observed_epoch(), 1);
    }
}

/// The other half of row 12b: the step-down is what *lets* the stale host fix
/// the cluster. Hostless at the taught epoch, the claim guard no longer bars it
/// — so a top-ranked node claims one above the pair that fenced it, activates,
/// and its `LeadState` re-fences every survivor onto a pair they all agree on.
#[test]
fn a_stepped_down_stale_host_re_earns_the_group_above_the_pair_that_fenced_it() {
    let rank = ranked(&["a", "b", "c"]);
    // A 20ms settle window, so the whole restart-and-recover sequence runs
    // inside one probe interval and the peer set is not thinned by the detector
    // while the election is being asserted.
    let mut e = hosted_engine(rank[0].as_str(), &[], 20, 4_000);
    learn(&mut e, &rank[1..], Time::ZERO);
    e.start(Time::ZERO);
    e.on_tick(Time(30)); // claims the stale epoch 1
    e.on_tick(Time(50)); // and activates it
    assert_eq!(e.role(), Role::Host);
    e.on_message(
        rank[1].clone(),
        &lead_state_frame(3, Some(rank[0].as_str())),
        Time(60),
    );
    assert_eq!(e.leadership(), (3, None));

    let claimed = e.on_tick(Time(70));
    let claims = lead_bodies(&claimed);
    assert_eq!(claims.len(), 2, "a claim goes to every live member");
    for (to, body) in &claims {
        assert!(rank[1..].contains(to));
        assert_eq!(
            *body,
            claim_of(4, &rank[0]),
            "the claim must outbid the epoch we were fenced by"
        );
    }
    assert_eq!(e.role(), Role::Claimant);
    assert_eq!(e.observed_epoch(), 4);

    let activated = e.on_tick(Time(90));
    assert_eq!(
        leadership_changes(&activated),
        vec![(4, Some(rank[0].clone()))],
        "hostship is re-entered only through this node's own activation"
    );
    assert_eq!(e.role(), Role::Host);
    assert_eq!(e.leadership(), (4, Some(&rank[0])));
    let states = lead_bodies(&activated);
    assert_eq!(states.len(), 2);
    for (to, body) in &states {
        assert!(rank[1..].contains(to));
        assert_eq!(
            *body,
            state_of(4, &rank[0]),
            "the fresh pair is what fences the survivors' stale one"
        );
    }
}

#[test]
fn a_host_serves_out_its_lease_after_losing_rank_then_steps_down() {
    let rank = ranked(&["a", "b", "c", "d"]);
    let host = || {
        let mut e = started(&rank[1], &[rank[2].clone(), rank[3].clone()], 2_000);
        e.on_tick(Time(500)); // claim epoch 1
        e.on_tick(Time(1_000)); // activate; lease runs to 3000
        assert_eq!(e.role(), Role::Host);
        assert_eq!(e.host_lease_until(), Some(Time(3_000)));
        e
    };
    let better = |e: &mut GroupEngine, status, at| {
        e.on_message(
            rank[0].clone(),
            &digest_frame(vec![ndigest(rank[0].as_str(), 0, status, 0)], vec![]),
            at,
        );
    };

    let mut lapses = host();
    better(&mut lapses, Status::Alive, Time(1_100));
    assert_eq!(
        lapses.coordinator(),
        Some(&rank[0]),
        "a better-ranked member takes the rank"
    );

    let inside = lapses.on_tick(Time(2_999));
    assert!(
        leadership_changes(&inside).is_empty(),
        "losing rank does not depose a host inside its lease"
    );
    assert_eq!(lapses.role(), Role::Host);
    assert_eq!(
        lapses.host_lease_until(),
        Some(Time(3_000)),
        "a host that is not top-ranked does not renew"
    );

    let expired = lapses.on_tick(Time(3_000));
    assert_eq!(
        leadership_changes(&expired),
        vec![(1, None)],
        "the lease lapsed: step down before anyone can step up"
    );
    assert_eq!(lapses.role(), Role::Follower);
    assert_eq!(
        lapses.leadership(),
        (1, None),
        "the epoch is kept, so a later pair is still fenced against it"
    );
    assert_eq!(lapses.host_lease_until(), None);

    // The hysteresis half: rank recovered inside the window keeps the host.
    let mut recovers = host();
    better(&mut recovers, Status::Alive, Time(1_100));
    better(&mut recovers, Status::Dead, Time(2_000));
    let renewed = recovers.on_tick(Time(2_500));
    assert!(leadership_changes(&renewed).is_empty());
    assert_eq!(recovers.role(), Role::Host);
    assert_eq!(
        recovers.host_lease_until(),
        Some(Time(4_500)),
        "top-ranked again: the tick renews the lease"
    );
    recovers.on_tick(Time(3_100));
    assert_eq!(
        recovers.role(),
        Role::Host,
        "a renewed lease survives the original expiry"
    );
}

#[test]
fn a_stale_claim_is_answered_with_the_pair_we_hold() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[2], &[rank[0].clone(), rank[1].clone()], 4_000);
    e.on_message(
        rank[0].clone(),
        &lead_state_frame(3, Some(rank[0].as_str())),
        Time(10),
    );

    for epoch in [2, 3] {
        let taught = e.on_message(
            rank[1].clone(),
            &lead_claim_frame(epoch, rank[1].as_str()),
            Time(20),
        );
        assert_eq!(
            lead_bodies(&taught),
            vec![(rank[1].clone(), state_of(3, &rank[0]))],
            "a claim at epoch {epoch} is behind the pair we hold"
        );
        assert!(leadership_changes(&taught).is_empty());
    }
    assert_eq!(e.observed_epoch(), 3);

    // A claim above our epoch is not repaired — only recorded — and its
    // claimant is learned as a live member.
    let newcomer = NodeId::new("newcomer");
    let seen = e.on_message(
        newcomer.clone(),
        &lead_claim_frame(9, newcomer.as_str()),
        Time(30),
    );
    assert!(
        lead_bodies(&seen).is_empty(),
        "a claim above our epoch has nothing to learn from us"
    );
    assert!(seen.contains(&Effect::MembershipChanged));
    assert!(e.members().any(|n| *n == newcomer));
    assert_eq!(e.observed_epoch(), 9);
    assert_eq!(
        e.leadership(),
        (3, Some(&rank[0])),
        "a claim adopts nothing"
    );
}

/// Row 9 against a **hostless** fence, both ends of it.
///
/// A node holding `(5, None)` — a lapsed lease, a step-down, row 12b's shadow —
/// still holds the fence: epochs up to 5 are spent. Answering only when a host
/// is adopted would let a claim for 3 stand unopposed, settle, and briefly serve
/// an epoch the cluster has already closed. So the pair is taught back hostless
/// too, and the second half asserts what that does to the claimant: `(5, None)`
/// outranks anything a node claiming 3 can hold, so it adopts the fence and the
/// adoption takes its claim with it (row 12's `outclaimed`).
#[test]
fn a_hostless_fence_answers_a_claim_from_below_and_kills_it() {
    let rank = ranked(&["a", "b", "c", "d"]);

    // --- The fence-holder's half. ---
    let mut fence = parked_hostless(&rank[0], &rank[1..], 5);
    let taught = fence.on_message(
        rank[2].clone(),
        &lead_claim_frame(3, rank[2].as_str()),
        Time(20),
    );
    assert_eq!(
        lead_bodies(&taught),
        vec![(rank[2].clone(), hostless_state_of(5))],
        "a claim below the fence must be answered, host or no host"
    );
    assert!(leadership_changes(&taught).is_empty());
    assert_eq!(
        fence.leadership(),
        (5, None),
        "answering a claim adopts nothing"
    );

    // --- The claimant's half, on the very frame the fence just sent. ---
    // `rank[1]` is the best of the three members it knows, so it claims; the
    // epoch-2 claim it hears first puts its own bid at 3.
    let mut claimant = started(&rank[1], &[rank[2].clone(), rank[3].clone()], 4_000);
    claimant.on_message(
        rank[2].clone(),
        &lead_claim_frame(2, rank[2].as_str()),
        Time(100),
    );
    let opened = claimant.on_tick(Time(500));
    assert_eq!(claimant.role(), Role::Claimant);
    assert_eq!(
        lead_bodies(&opened).first().map(|(_, body)| body.clone()),
        Some(claim_of(3, &rank[1]))
    );

    let fenced = claimant.on_message(rank[0].clone(), &lead_state_frame(5, None), Time(600));
    assert_eq!(
        leadership_changes(&fenced),
        vec![(5, None)],
        "the fence is a better pair, so it is adopted"
    );
    assert!(
        lead_bodies(&fenced).is_empty(),
        "adopting answers the sender nothing"
    );
    assert_eq!(
        claimant.role(),
        Role::Follower,
        "the claim was outranked by the pair it just adopted"
    );
    assert_eq!(claimant.leadership(), (5, None));
    assert_eq!(claimant.observed_epoch(), 5);

    // The settle window it was in would have closed here. It activates nothing:
    // the claim is gone, and the only thing left to do is re-bid *above* the
    // fence it was taught.
    let after = claimant.on_tick(Time(1_000));
    assert!(
        leadership_changes(&after).is_empty(),
        "a fenced claim must never activate the epoch it was fenced out of"
    );
    let rebid = lead_bodies(&after);
    assert_eq!(rebid.len(), 2, "the re-bid goes to every live member");
    for (to, body) in rebid {
        assert!(rank[2..].contains(&to));
        assert_eq!(
            body,
            claim_of(6, &rank[1]),
            "the re-bid must outrank the fence, not re-litigate it"
        );
    }
}

/// The one claim a hostless fence does **not** answer: one at exactly its own
/// epoch. `(5, None)` means the group has no host for epoch 5, and a bid for 5
/// is the legitimate hostless-recovery bid — the way a group with no host gets
/// one again. Answering it would teach the claimant a pair it already holds and
/// achieve nothing but noise.
#[test]
fn a_hostless_fence_stays_silent_at_its_own_epoch() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = parked_hostless(&rank[0], &rank[1..], 5);

    let quiet = e.on_message(
        rank[2].clone(),
        &lead_claim_frame(5, rank[2].as_str()),
        Time(20),
    );
    assert!(
        lead_bodies(&quiet).is_empty(),
        "the recovery bid for our own hostless epoch must go unanswered"
    );
    assert!(
        leadership_changes(&quiet).is_empty(),
        "a claim adopts nothing"
    );
    assert_eq!(e.leadership(), (5, None));
    assert_eq!(e.observed_epoch(), 5);
}

#[test]
fn leaving_gives_up_hostship_before_the_leave_disseminates() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[0], &rank[1..], 4_000);
    e.on_tick(Time(500));
    e.on_tick(Time(1_000));
    assert_eq!(e.role(), Role::Host);

    let left = e.apply(Command::Leave);
    assert_eq!(leadership_changes(&left), vec![(1, None)]);
    assert!(
        matches!(left.first(), Some(Effect::LeadershipChanged { .. })),
        "the demotion precedes the leave itself"
    );
    assert!(left.contains(&Effect::MembershipChanged));
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.leadership(), (1, None));

    // A claim is given up just as promptly, and silently — it changed no one's
    // belief, so there is nothing to announce.
    let mut claiming = started(&rank[0], &rank[1..], 4_000);
    claiming.on_tick(Time(500));
    assert_eq!(claiming.role(), Role::Claimant);
    let left = claiming.apply(Command::Leave);
    assert!(leadership_changes(&left).is_empty());
    assert_eq!(claiming.role(), Role::Follower);
    assert!(
        lead_bodies(&claiming.on_tick(Time(1_000))).is_empty(),
        "a departed node neither settles its claim nor re-broadcasts it"
    );
}

#[test]
fn a_grant_is_dropped_under_settle_activation() {
    let rank = ranked(&["a", "b", "c"]);
    let mut e = started(&rank[0], &rank[1..], 4_000);

    let effects = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(7, rank[0].as_str(), rank[1].as_str()),
        Time(10),
    );
    assert!(
        effects.is_empty(),
        "Settle closes an epoch by time, not votes"
    );
    assert_eq!(e.observed_epoch(), 0, "a grant teaches us no epoch");
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.leadership(), (0, None));
}

#[test]
fn an_eventual_group_runs_no_election() {
    let mut e = engine("a", &["b"]);
    learn(&mut e, &[NodeId::new("b")], Time::ZERO);
    e.start(Time::ZERO);

    for frame in [
        lead_claim_frame(4, "b"),
        lead_grant_frame(4, "b", "c"),
        lead_state_frame(4, Some("b")),
    ] {
        assert!(
            e.on_message(NodeId::new("b"), &frame, Time(10)).is_empty(),
            "an Eventual group drops every election frame"
        );
    }
    assert_eq!(e.role(), Role::Follower);
    assert_eq!(e.leadership(), (0, None));
    assert_eq!(e.observed_epoch(), 0);
    assert_eq!(e.host_lease_until(), None);

    // Nor does it ever emit one, however long it runs unattended — including
    // as the sole (and so top-ranked) member of its own view.
    for at in (100..=3_000).step_by(100) {
        let effects = e.on_tick(Time(at));
        assert!(
            lead_bodies(&effects).is_empty(),
            "an Eventual group emitted an election frame at {at}"
        );
        assert!(leadership_changes(&effects).is_empty());
    }
}

/// The minority side, from the inside. A `Quorum` group whose voters never
/// answer is deliberately hostless: the claim guard is unchanged, so a
/// top-ranked node still opens and broadcasts claims, but only a
/// majority-of-the-roster tally closes one of its epochs — so every claim
/// expires silently when its window shuts.
///
/// Ticked across twenty windows against a three-voter roster it cannot reach,
/// this node therefore never reaches [`Role::Host`], never announces
/// leadership, never arms a lease, and never puts a grant on the wire (its own
/// self-grant is counted, not sent). Unavailable, never unsafe — which is the
/// whole point of the CP posture. The grant rows themselves live in
/// `tests/election_quorum.rs`.
#[test]
fn a_quorum_group_without_a_voter_majority_never_activates() {
    // Under Quorum the lease is also the claim window and the boot guard.
    const LEASE_MS: u64 = 500;
    const WINDOWS: u64 = 20;

    let rank = ranked(&["a", "b", "c"]);
    let voters: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    let mut e = hosted_quorum_engine(rank[0].as_str(), &[], &voters, LEASE_MS);
    learn(&mut e, &rank[1..], Time::ZERO);
    e.start(Time::ZERO);

    let mut claims = 0usize;
    for at in (100..=WINDOWS * LEASE_MS).step_by(100) {
        let effects = e.on_tick(Time(at));
        for (_, body) in lead_bodies(&effects) {
            match body {
                wire::LeadBody::Claim { .. } => claims += 1,
                other => panic!("a Quorum group put {other:?} on the wire at {at}"),
            }
        }
        assert!(
            leadership_changes(&effects).is_empty(),
            "a Quorum group announced leadership at {at}"
        );
        assert_ne!(e.role(), Role::Host, "a Quorum group activated at {at}");
        assert_eq!(e.leadership(), (0, None), "still hostless at {at}");
        assert_eq!(e.host_lease_until(), None, "no lease armed at {at}");
    }
    assert!(
        claims > 0,
        "the claim guard is untouched — claims are still bid, they just never close"
    );

    // And a grant that matches no round this node is running is dropped, and
    // teaches it nothing — an epoch is learned from a claim or an adopted pair,
    // never from somebody's endorsement of one.
    let before = e.observed_epoch();
    let effects = e.on_message(
        rank[1].clone(),
        &lead_grant_frame(before + 5, rank[0].as_str(), rank[1].as_str()),
        Time(WINDOWS * LEASE_MS),
    );
    assert!(
        effects.is_empty(),
        "a grant for no standing round is dropped"
    );
    assert_eq!(e.observed_epoch(), before, "a grant teaches us no epoch");
    assert_ne!(e.role(), Role::Host);
    assert_eq!(e.leadership(), (0, None));
}
