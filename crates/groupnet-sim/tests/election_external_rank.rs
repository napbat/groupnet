//! Deterministic Simulation Testing for **the rank gate under
//! `Activation::External`** — what it costs a group that candidacy and renewal
//! are both gated on the rendezvous ranking, and how the group comes back.
//!
//! Split out of `election_external_failover.rs` the same way that file is split
//! out of `election_external.rs`: the partition, the store outage and the
//! failover budget live there, the *rank gate's own* scenarios live here. Each
//! is a claim the tier makes in its own words, and each is a price the design
//! doc states beside the rule that charges it.
//!
//! * **X-rank — [`a_group_whose_top_ranked_node_lost_the_anchor_stays_hostless`].**
//!   The documented cost of a rank-gated candidate set: one node's store
//!   connectivity can pin the whole group hostless.
//! * **X-rank-compound —
//!   [`a_returning_top_ranked_node_without_the_anchor_unseats_a_working_host`].**
//!   The same cost compounded with the rank-gated *renewal*: an incumbent that
//!   is anchor-connected and serving perfectly well is outranked by a returning
//!   node that **cannot reach the store at all**, and the two rules together
//!   take the group away from the host that works and give it to nobody. The
//!   price of the CP posture, asserted rather than described.
//! * **X-handback —
//!   [`an_incumbent_that_loses_rank_lapses_and_the_rendezvous_top_takes_it_back`].**
//!   Renewal is rank-gated under *every* activation, `External` included: row X7
//!   reads `is_coordinator()` exactly as rows 5 and Q7 do. So an outranked
//!   incumbent stops being prompted, lets its record age out, and the returning
//!   rendezvous top supersedes it at a strictly higher epoch — the group lands
//!   back where the coordinator ranking points, and it costs a handful of
//!   yielded rounds rather than one per anti-entropy interval for ever.

use std::collections::BTreeSet;

use groupnet_core::{
    Activation, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Role, Time,
    placement,
};
use groupnet_sim::{AnchorEvent, Simulation};

/// The engine lease, the anchor record's TTL and the boot guard — one number,
/// as `HostedConfig::lease_ms` documents under this activation.
const LEASE_MS: u64 = 400;
/// The anti-entropy cadence, which the anchor prompt rides.
const GOSSIP_INTERVAL_MS: u64 = 60;
/// How far past a record's expiry a claimant must wait before it may steal.
const STEAL_MARGIN_MS: u64 = 150;
/// One store round trip.
const ANCHOR_LATENCY_MS: u64 = 15;

fn hosted(activation: Activation) -> Config {
    Config {
        gossip_interval_ms: GOSSIP_INTERVAL_MS,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 1_000,
        indirect_probes: 2,
        fanout: 4,
        anti_entropy_interval_ms: GOSSIP_INTERVAL_MS,
        anti_entropy_fanout: 2,
        eager_push: true,
        full_digest_every: 4,
        max_delta_frame_bytes: 4_096,
        mode: GroupMode::Hosted(HostedConfig {
            activation,
            lease_ms: LEASE_MS,
        }),
    }
}

fn cfg() -> Config {
    hosted(Activation::External {
        steal_margin_ms: STEAL_MARGIN_MS,
    })
}

fn nodes(ids: &[&str]) -> BTreeSet<NodeId> {
    ids.iter().map(|id| NodeId::new(*id)).collect()
}

/// A cluster of `members` bootstrapped all-to-all in `config`, with the anchor
/// armed at the External configuration. (A `Quorum` contrast run arms it too
/// and simply never prompts it — nothing in that tier emits `AnchorClaimDue`,
/// which is itself worth leaving observable.)
fn cluster(group: &str, members: &BTreeSet<NodeId>, config: &Config) -> Simulation {
    let mut sim = Simulation::new(10);
    sim.enable_anchor(LEASE_MS, STEAL_MARGIN_MS);
    sim.set_anchor_latency(ANCHOR_LATENCY_MS);
    for id in members {
        sim.add(engine(group, id, members, config));
    }
    sim
}

fn engine(group: &str, id: &NodeId, peers: &BTreeSet<NodeId>, config: &Config) -> GroupEngine {
    let seeds = peers.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(GroupId::new(group), id.clone(), seeds, config.clone())
}

/// An `(epoch, host)` pair: the unit that names a serializer.
type Pair = (u64, Option<NodeId>);

fn pair_of(sim: &Simulation, node: &NodeId) -> Pair {
    sim.leadership_of(node)
        .expect("a live node is in the simulation")
}

fn sole_host(sim: &Simulation, label: &str) -> NodeId {
    let hosts = sim.hosts();
    assert_eq!(
        hosts.len(),
        1,
        "{label}: expected exactly one host: {hosts:?}"
    );
    hosts.into_iter().next().expect("length asserted above")
}

/// The pair the register itself holds.
fn anchor_pair(sim: &Simulation, label: &str) -> (u64, NodeId) {
    let record = sim
        .anchor_record()
        .unwrap_or_else(|| panic!("{label}: the anchor is still empty"));
    (record.epoch, record.host)
}

/// **X-purity**, asserted at the end of every External run in this file. See
/// `election_external.rs` for why it is counted at issuance.
fn assert_pure(sim: &Simulation, all: &BTreeSet<NodeId>, label: &str) {
    assert_eq!(
        sim.claim_frames_seen(),
        0,
        "{label}: an External group put a claim on the wire"
    );
    assert_eq!(
        sim.grant_frames_seen(),
        0,
        "{label}: an External group put a grant on the wire"
    );
    for node in all {
        assert_eq!(
            sim.persisted_grant_of(node),
            None,
            "{label}: {node} persisted a grant under External"
        );
    }
}

/// Steps the simulation **one scheduled event at a time** up to `until`,
/// running `check` after each one.
///
/// No sampling cadence, and so no sampling gap: an engine only moves when it
/// takes a frame, a tick or an anchor round's command, so a property checked
/// after every event is checked at every instant it could have changed.
fn step_through(sim: &mut Simulation, until: u64, mut check: impl FnMut(&Simulation, u64)) {
    while let Some(at) = sim.step_until(Time(until)) {
        check(sim, at.0);
    }
}

/// How many rounds of each kind `node` has run — the floors the shaped
/// scenarios earn.
fn rounds_by(sim: &Simulation, node: &NodeId, what: AnchorEvent, since: u64) -> usize {
    sim.anchor_log
        .iter()
        .filter(|(at, who, event)| at.0 >= since && who == node && *event == what)
        .count()
}

// ---------------------------------------------------------------------------
// X-rank — the documented cost of a rank-gated candidate set.
// ---------------------------------------------------------------------------

/// **X-rank.** Only the *top-ranked* node loses the anchor, and the whole group
/// stays hostless — even though two perfectly healthy peers could reach the
/// store and win a round in a millisecond.
///
/// This is deliberate and it is documented as such: candidacy is rank-gated
/// (row X1 is row 1's guard verbatim), which is what keeps the election free of
/// duelling timeouts, and the price is that one node's store connectivity can
/// pin the group. Asserting it here means a future change that quietly widens
/// the candidate set has to come and edit this test — which is exactly the
/// conversation such a change should start.
///
/// The operator's signal for this state is the anchor error at the driver, not
/// anything in the fabric: to every peer, and to the node itself, the cluster
/// looks perfectly healthy. That is what the two assertions on the anchor log
/// say — the second-ranked node never even *asked*.
#[test]
fn a_group_whose_top_ranked_node_lost_the_anchor_stays_hostless() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-rank";
    let label = "X-rank";
    let top = placement::owner(group, &members).expect("a non-empty cluster");
    let mut sim = cluster(group, &members, &cfg());
    sim.block_anchor(&top);

    step_through(&mut sim, 6_000, |sim, at| {
        assert!(
            sim.hosts().is_empty(),
            "{:?} hosted at {at} — only the top-ranked node is a candidate",
            sim.hosts()
        );
    });
    assert!(
        sim.anchor_record().is_none(),
        "the register was written by a node that never prompts"
    );
    assert!(
        sim.anchor_log.is_empty(),
        "a non-candidate ran an anchor round: {:?}",
        sim.anchor_log
    );
    // The premise, asserted rather than assumed: the group is *healthy*. Every
    // node sees every other, and the pinned node is the rendezvous owner.
    for node in &members {
        assert_eq!(sim.members_of(node), members, "{node} lost sight of a peer");
    }

    // Heal it, and the group is elected on the next prompt.
    sim.heal_anchor(&top);
    sim.run_until(Time(6_000 + GOSSIP_INTERVAL_MS + 2 * ANCHOR_LATENCY_MS));
    assert_eq!(sole_host(&sim, label), top);
    assert_eq!(anchor_pair(&sim, label), (1, top));
    assert_pure(&sim, &members, label);
}

/// **X-rank-compound.** The rank gate on *candidacy* (row X1) and the rank gate
/// on *renewal* (row X7) are each defensible alone. Composed, they have a price
/// this test names: a group with a **working, willing, anchor-connected host**
/// can be left hostless by the return of a node that ranks above it and cannot
/// reach the store at all.
///
/// The schedule is the hand-back's, with one change — the returning node's
/// anchor is blocked:
///
/// * the top-ranked node crashes and the second takes the group, serving it
///   perfectly well on an anchor it can reach;
/// * the top-ranked node comes back **without store access**. It outranks the
///   incumbent the moment gossip re-admits it, so row X7 stops prompting the
///   incumbent, its record ages out and its engine lease lapses on row 6;
/// * and row X1 will not let anybody else bid, because the top-ranked node is
///   the only candidate — and its rounds die at the store.
///
/// So the group is hostless *because* it has a healthy host and a higher-ranked
/// node that cannot host, which is the compound cost the design doc now states
/// beside both rules. Both halves are asserted: the incumbent **lapses**, and
/// the group stays hostless — event by event, right past the instant its record
/// became stealable — until the top-ranked node's anchor heals, at which point
/// it wins immediately.
///
/// Nothing here is a safety failure: the anchor still allocates every epoch and
/// the eventual hand-back is cross-epoch and store-fenced like any other. It is
/// availability, spent deliberately, and a future change that widens the
/// candidate set to buy it back has to come and edit this test.
#[test]
fn a_returning_top_ranked_node_without_the_anchor_unseats_a_working_host() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-rank-compound";
    let label = "X-rank-compound";
    let top = placement::owner(group, &members).expect("a non-empty cluster");
    let mut sim = cluster(group, &members, &cfg());
    sim.run_until(Time(2_000));
    assert_eq!(sole_host(&sim, label), top, "the premise: rank elected it");

    sim.crash(&top);
    sim.run_until(Time(5_000));
    let incumbent = sole_host(&sim, &format!("{label} (after the crash)"));
    assert_ne!(incumbent, top);
    let (epoch, holder) = anchor_pair(&sim, label);
    assert_eq!(holder, incumbent, "premise: the incumbent holds the record");
    let stealable_at = sim
        .anchor_record()
        .expect("a record")
        .expires_at_wall_ms
        .saturating_add(STEAL_MARGIN_MS);

    // The top-ranked node returns — with no way to reach the store. It outranks
    // the incumbent regardless, which is all row X7 reads.
    let back_at = 5_000;
    sim.block_anchor(&top);
    sim.add(engine(group, &top, &members, &cfg()));

    let mut lapsed_at = None;
    step_through(&mut sim, back_at + 8_000, |sim, at| {
        if lapsed_at.is_none() {
            if sim.hosts().is_empty() {
                lapsed_at = Some(at);
            }
            return;
        }
        assert!(
            sim.hosts().is_empty(),
            "{:?} hosted at {at}: the only candidate is the returning node, and it \
             cannot reach the anchor",
            sim.hosts()
        );
    });
    let lapsed_at = lapsed_at.unwrap_or_else(|| {
        panic!("{label}: the outranked incumbent kept the group it was no longer prompted for")
    });
    assert!(
        back_at + 8_000 > stealable_at,
        "{label}: the window closed at {} before the record was even stealable at \
         {stealable_at} — the hostless stretch would prove nothing",
        back_at + 8_000
    );
    assert_eq!(
        anchor_pair(&sim, label),
        (epoch, incumbent.clone()),
        "{label}: the record changed hands while nobody could write to the store"
    );
    // The premise, asserted rather than assumed: the deposed incumbent is
    // healthy, connected, and could have reached the anchor the whole time.
    for node in &members {
        assert_eq!(sim.members_of(node), members, "{node} lost sight of a peer");
    }
    assert!(
        rounds_by(&sim, &incumbent, AnchorEvent::Renew, back_at - 1_000) > 0,
        "{label}: the host that lapsed was never reaching the store anyway — the \
         compound cost needs a *working* host to be a cost at all"
    );
    assert_eq!(
        rounds_by(&sim, &incumbent, AnchorEvent::Renew, lapsed_at),
        0,
        "{label}: an outranked host was still renewing"
    );

    // Heal the top-ranked node's anchor and the group is elected on the next
    // prompt — the availability was spent on the gate, not on anything broken.
    let healed_at = back_at + 8_000;
    sim.heal_anchor(&top);
    sim.run_until(Time(healed_at + GOSSIP_INTERVAL_MS + 2 * ANCHOR_LATENCY_MS));
    assert_eq!(sole_host(&sim, label), top);
    let (regained, holder) = anchor_pair(&sim, label);
    assert_eq!(holder, top);
    assert!(
        regained > epoch,
        "{label}: {regained} does not fence the incumbent's {epoch}"
    );
    println!(
        "X-rank-compound: hostless from {lapsed_at} to {healed_at} \
         (stealable from {stealable_at}), epoch {epoch} -> {regained}"
    );
    assert_pure(&sim, &members, label);
}

// ---------------------------------------------------------------------------
// X-handback — renewal is rank-gated here too, so the group comes back.
// ---------------------------------------------------------------------------

/// How many yielded rounds the returning candidate is allowed to spend before
/// it is entitled to steal. The arithmetic it bounds: the incumbent stops being
/// prompted the moment gossip re-admits the returning node, its record then has
/// at most one `LEASE_MS` of TTL left, and a steal is entitled `STEAL_MARGIN_MS`
/// after that — so roughly `(LEASE_MS + STEAL_MARGIN_MS) / GOSSIP_INTERVAL_MS`
/// prompts, which is ~9, with the rest of this number as slack.
///
/// It is a *bound on cost*, and it is the whole point of the rank gate: without
/// one, this same window produces a yielded round every anti-entropy interval
/// for as long as the mismatch lasts, which is for ever.
const YIELD_BUDGET: usize = 20;

/// **X-handback.** An incumbent that is no longer the top-ranked live candidate
/// **lets its record lapse**, and the returning rendezvous top takes the group
/// back by superseding the expired record at a strictly higher epoch.
///
/// The shape is the ordinary one: the top-ranked node crashes, the second takes
/// the group, and then the first comes back. All three activations answer it the
/// same way, for one reason — **renewal is rank-gated under every activation**.
/// Row 5's tick-re-rank reads `is_coordinator()`; row Q7's renewal round opens
/// only for a coordinator ("a host that no longer ranks should be letting its
/// lease lapse, not asking the roster to extend it"); row X7's renewal prompt is
/// gated the same way. What differs between the three is only the *evidence*
/// that extends a lease — this node's own view, a fresh majority, a fresh anchor
/// round — never who is entitled to go looking for it.
///
/// The hand-back is walked event by event, and every step of it is asserted:
///
/// * the outranked incumbent **stops asking**: not one anchor round of its own
///   after it demotes, which is the rank gate in its falsifiable form;
/// * it demotes into a **hostless pair at its own epoch**, so the fence it held
///   still orders whatever comes next;
/// * the returning node takes the record by **stealing** it, never by being
///   handed it — there is no cooperative handoff in this milestone, and a record
///   is superseded on its expiry or not at all;
/// * and it costs a bounded handful of yielded rounds ([`YIELD_BUDGET`]) rather
///   than one per anti-entropy interval indefinitely.
///
/// Safety is not what is at stake either way: the anchor allocated both epochs,
/// so this handover is cross-epoch and store-fenced exactly like a crash-driven
/// one. What the gate buys is that the common case lands where the coordinator
/// ranking points, without pinning a candidate into a permanent store round trip
/// per gossip round.
#[test]
fn an_incumbent_that_loses_rank_lapses_and_the_rendezvous_top_takes_it_back() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-handback";
    let label = "X-handback";
    let top = placement::owner(group, &members).expect("a non-empty cluster");
    let mut sim = cluster(group, &members, &cfg());
    sim.run_until(Time(2_000));
    assert_eq!(sole_host(&sim, label), top, "the premise: rank elected it");

    sim.crash(&top);
    sim.run_until(Time(5_000));
    let successor = sole_host(&sim, &format!("{label} (after the crash)"));
    assert_ne!(successor, top);
    let (epoch, _) = anchor_pair(&sim, label);

    // The top-ranked node returns. It outranks the incumbent again the moment
    // gossip re-admits it — and from that instant nothing prompts the incumbent.
    let back_at = 5_000;
    sim.add(engine(group, &top, &members, &cfg()));
    let (mut lapsed_at, mut retaken_at) = (None, None);
    step_through(&mut sim, back_at + 10_000, |sim, at| {
        if lapsed_at.is_none() && sim.role_of(&successor) != Some(Role::Host) {
            lapsed_at = Some(at);
        }
        if retaken_at.is_none() && sim.role_of(&top) == Some(Role::Host) {
            retaken_at = Some(at);
        }
    });
    let lapsed_at =
        lapsed_at.unwrap_or_else(|| panic!("{label}: the outranked incumbent never gave it up"));
    let retaken_at =
        retaken_at.unwrap_or_else(|| panic!("{label}: the rendezvous top never took it back"));
    assert!(
        lapsed_at <= retaken_at,
        "{label}: the successor was elected at {retaken_at} while the incumbent still \
         held the group until {lapsed_at} — a same-instant overlap, not a hand-back"
    );

    assert!(
        sim.is_member(&successor, &top) && sim.is_member(&top, &successor),
        "premise: the returning node is a live member again"
    );
    assert_eq!(
        placement::owner(group, &members),
        Some(top.clone()),
        "premise: it is the rendezvous owner of the live set once more"
    );

    assert_eq!(
        sole_host(&sim, label),
        top,
        "the group did not land back on the node the ranking points at"
    );
    let (regained, holder) = anchor_pair(&sim, label);
    assert_eq!(holder, top);
    assert!(
        regained > epoch,
        "a hand-back allocates like every other succession: {regained} does not fence {epoch}"
    );
    for node in &members {
        assert_eq!(
            pair_of(&sim, node),
            (regained, Some(top.clone())),
            "{node} did not converge on the register's pair after the hand-back"
        );
    }

    // The rank gate, in its falsifiable form: the demoted incumbent runs no
    // anchor round of any kind after it steps down — it is neither a host to
    // renew (row X7) nor a candidate to claim (row X1).
    let asked_after = sim
        .anchor_log
        .iter()
        .filter(|(at, who, _)| at.0 >= lapsed_at && who == &successor)
        .count();
    assert_eq!(
        asked_after, 0,
        "{label}: the demoted incumbent ran {asked_after} more anchor rounds: {:?}",
        sim.anchor_log
    );
    // And the returning node *stole* the record rather than being handed it.
    assert!(
        rounds_by(&sim, &top, AnchorEvent::Steal, back_at) > 0,
        "{label}: the record changed hands without being superseded on its expiry"
    );
    let yields = rounds_by(&sim, &top, AnchorEvent::Yield, back_at);
    assert!(
        yields <= YIELD_BUDGET,
        "{label}: the returning candidate spent {yields} rounds yielding, past the \
         {YIELD_BUDGET} the hand-back arithmetic allows — the pinned shape is back"
    );
    println!(
        "X-handback: lapsed at {lapsed_at}, retaken at {retaken_at} \
         (epoch {epoch} -> {regained}), {yields} yielded rounds"
    );
    assert_pure(&sim, &members, label);
}
