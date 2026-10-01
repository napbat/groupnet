//! Seeded donor/joiner schedules around a completed local build's Ready
//! recapture, with participation required as in production. The builder's
//! claim stays visible and renewed from its local build until the recapture's
//! Ready claim: a joiner that samples in the window between them, during a
//! recapture its own arrival interrupts, or during a recovery lapse that
//! retires the donor's capture, waits for that image instead of scanning the
//! origin. A recapture that never comes ends the claim at the builder's own
//! stall bound, and the joiner then takes over within one observation.

use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapMemberIdentity, BootstrapOperation, BootstrapParticipant, BootstrapPresence,
    BootstrapScope, BootstrapStage, ClaimEngine, ClaimIdentity, ClaimPhase,
};
use groupnet_core::{NodeId, Status, Time};
use groupnet_sim::SplitMix64;

const CONFIG: BootstrapConfig = BootstrapConfig {
    max_members: 8,
    max_member_bytes: 32,
    max_scope_bytes: 64,
    settle_ms: 3,
    renew_ms: 2,
    claim_ttl_ms: 6,
    observe_ms: 3,
    donor_wait_ms: 10,
    total_ms: 30,
};
const DONOR: usize = 0;
const JOINER: usize = 1;
const HORIZON: u64 = 400;

fn name(node: usize) -> NodeId {
    NodeId::from(if node == DONOR { "node-a" } else { "node-b" })
}

/// Native TTL source. Each node's claim and presence publications, in order,
/// reach the other node `lag` ms later (its presence and claims `extra` ms
/// after its membership) and the publisher at once. Remaining TTL counts from
/// publication, so transit never extends it.
struct Source {
    claims: [Vec<(u64, Option<BootstrapClaim>)>; 2],
    presence: [Vec<(u64, BootstrapPresence)>; 2],
    joined: Option<u64>,
    lag: u64,
    extra: u64,
}

struct Cut {
    members: Vec<BootstrapMember>,
    roster: Vec<BootstrapMemberIdentity>,
    participants: Vec<BootstrapParticipant>,
    claims: Vec<BootstrapClaim>,
}

impl Source {
    fn new(lag: u64, extra: u64) -> Self {
        Self {
            claims: [Vec::new(), Vec::new()],
            presence: [Vec::new(), Vec::new()],
            joined: None,
            lag,
            extra,
        }
    }

    fn delay(&self, from: usize, to: usize) -> u64 {
        if from == to { 0 } else { self.lag + self.extra }
    }

    fn withdraw(&mut self, node: usize, now: u64, identity: &ClaimIdentity) {
        let current = self.claims[node]
            .last()
            .and_then(|(_, claim)| claim.as_ref());
        if current.is_some_and(|claim| claim.identity == *identity) {
            self.claims[node].push((now, None));
        }
    }

    fn member(&self, node: usize, reader: usize, now: u64) -> bool {
        node == DONOR
            || node == reader
            || self.joined.is_some_and(|joined| now >= joined + self.lag)
    }

    /// One complete native cut as `reader` sees it at `now`.
    fn cut(&self, reader: usize, now: u64) -> Cut {
        let mut cut = Cut {
            members: Vec::new(),
            roster: Vec::new(),
            participants: Vec::new(),
            claims: Vec::new(),
        };
        for node in [DONOR, JOINER] {
            if !self.member(node, reader, now) {
                continue;
            }
            let seen = now.checked_sub(self.delay(node, reader));
            let fresh = |at: u64| {
                seen.is_some_and(|seen| at <= seen)
                    && CONFIG.claim_ttl_ms.saturating_sub(now - at) > 0
            };
            let presence = self.presence[node]
                .iter()
                .rev()
                .find(|(at, _)| seen.is_some_and(|seen| *at <= seen))
                .filter(|(at, _)| fresh(*at));
            let member = BootstrapMemberIdentity {
                node: name(node),
                presence: presence.map(|(_, presence)| presence.identity.clone()),
                member_incarnation: 1,
                status: Status::Alive,
            };
            if let Some((at, presence)) = presence {
                cut.participants.push(BootstrapParticipant {
                    member: member.clone(),
                    renewal: presence.renewal,
                    remaining_ms: CONFIG.claim_ttl_ms - (now - at),
                });
            }
            if let Some((at, Some(claim))) = self.claims[node]
                .iter()
                .rev()
                .find(|(at, _)| seen.is_some_and(|seen| *at <= seen))
                && fresh(*at)
            {
                cut.claims.push(BootstrapClaim {
                    remaining_ms: CONFIG.claim_ttl_ms - (now - at),
                    ..claim.clone()
                });
            }
            cut.members.push(BootstrapMember {
                node: name(node),
                eligible: true,
            });
            cut.roster.push(member);
        }
        cut
    }
}

/// The donor's worker around its Ready recapture, as the session drives it.
#[derive(Default)]
struct Donor {
    build: Option<(BootstrapOperation, ClaimIdentity, u64)>,
    builds: u32,
    ready_at: Option<u64>,
    capture: Option<(BootstrapOperation, ClaimIdentity, u64)>,
    recaptures: u32,
    failed: u32,
    built: Option<u64>,
    /// Recovery lapsed: no recapture starts before this time.
    suspended_until: Option<u64>,
    /// The lapse revoked the in-flight recapture's guard.
    doomed: bool,
    retired: u32,
}

#[derive(Default)]
struct Joiner {
    in_window: u32,
    in_lapse: u32,
    build: Option<u64>,
    declined: Option<u64>,
    donor: Option<u64>,
}

struct Schedule {
    build_len: u64,
    affirm: u64,
    encode: u64,
    join: u64,
    /// A recovery lapse `(start, length)`.
    lapse: Option<(u64, u64)>,
}

/// Both workers over one source. Each worker publishes the renewals a tick
/// schedules before it samples the source, as the session does.
struct World {
    seed: u64,
    build_len: u64,
    affirm: u64,
    encode: u64,
    lapse: Option<(u64, u64)>,
    source: Source,
    engines: [Option<ClaimEngine>; 2],
    donor: Donor,
    joiner: Joiner,
}

impl World {
    fn engine(&self, node: usize) -> ClaimEngine {
        let mut engine = ClaimEngine::new(
            CONFIG,
            BootstrapScope {
                domain: "origin".to_owned(),
                partition: "bucket".to_owned(),
            },
            name(node),
            BootId(u128::from(self.seed) * 4 + u128::from(node == JOINER) + 1),
            1,
        )
        .unwrap();
        engine.require_participation().unwrap();
        engine
    }

    fn apply(&mut self, node: usize, now: u64, effects: Vec<BootstrapEffect>) {
        let seed = self.seed;
        let mut queue = std::collections::VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            match effect {
                BootstrapEffect::PublishClaim(claim) => {
                    self.source.claims[node].push((now, Some(claim)));
                }
                BootstrapEffect::WithdrawClaim(identity) => {
                    self.source.withdraw(node, now, &identity);
                }
                BootstrapEffect::PublishPresence(presence) => {
                    self.source.presence[node].push((now, presence));
                }
                BootstrapEffect::ObserveClaims { op, .. } => {
                    let donor = self.engines[DONOR].as_ref().unwrap();
                    if node == JOINER
                        && (donor.ready_recapture_pending() || self.donor.capture.is_some())
                    {
                        self.joiner.in_window += 1;
                    }
                    if node == JOINER && self.donor.suspended_until.is_some_and(|until| now < until)
                    {
                        self.joiner.in_lapse += 1;
                    }
                    let cut = self.source.cut(node, now);
                    let step = self.engines[node].as_mut().unwrap().step(
                        BootstrapEvent::ParticipantsObserved {
                            op,
                            members: cut.members,
                            roster: cut.roster,
                            participants: cut.participants,
                            claims: cut.claims,
                        },
                    );
                    if step.rejection.is_some() {
                        // The session declines the acquisition: origin.
                        assert_eq!(node, JOINER, "seed {seed}: donor cut refused");
                        self.joiner.declined.get_or_insert(now);
                    }
                    queue.extend(step.effects);
                }
                BootstrapEffect::BuildOrigin { op, selected } => {
                    if node == DONOR {
                        self.donor.builds += 1;
                        self.donor.build = Some((op, selected, now + self.build_len));
                    } else {
                        self.joiner.build.get_or_insert(now);
                    }
                }
                BootstrapEffect::RecaptureCurrent { op, selected } => {
                    self.donor.recaptures += 1;
                    if self.verify(DONOR, now) {
                        self.donor.capture = Some((op, selected, now + self.encode));
                    } else {
                        self.donor.failed += 1;
                        let engine = self.engines[DONOR].as_mut().unwrap();
                        let step = engine.step(BootstrapEvent::BuildFailed { op, selected });
                        queue.extend(step.effects);
                    }
                }
                BootstrapEffect::DonorAvailable { selected, .. } => {
                    assert_eq!(node, JOINER, "seed {seed}");
                    assert_eq!(selected.node, name(DONOR), "seed {seed}");
                    self.joiner.donor.get_or_insert(now);
                }
                BootstrapEffect::FallbackOrigin => {
                    assert_eq!(node, JOINER, "seed {seed}: donor stopped donating");
                    self.joiner.declined.get_or_insert(now);
                }
                _ => {}
            }
        }
    }

    fn verify(&mut self, node: usize, now: u64) -> bool {
        let engine = self.engines[node].as_mut().unwrap();
        let Ok(op) = engine.begin_roster_observation() else {
            return false;
        };
        let cut = self.source.cut(node, now);
        engine
            .verify_participant_roster(
                op,
                &cut.members,
                &cut.roster,
                &cut.participants,
                &cut.claims,
            )
            .is_ok()
    }

    /// A recovery lapse suspends the Ready generation, and the worker retires
    /// the donor capture, as `suspend_local` does.
    fn retire(&mut self, now: u64) {
        let engine = self.engines[DONOR].as_mut().unwrap();
        if engine.stage() != BootstrapStage::DonorAvailable {
            return;
        }
        let Some(selected) = engine.selected().cloned() else {
            return;
        };
        let step = engine.step(BootstrapEvent::CaptureRetired { selected });
        self.donor.retired += 1;
        self.apply(DONOR, now, step.effects);
    }

    /// The donor's build completes, recovery affirms `affirm` later, and the
    /// worker's maintenance turns start and finish the recapture. A lapse
    /// suspends those turns; the serial worker handles it only once a running
    /// recapture, whose guard the lapse revoked, has failed.
    fn donor_turn(&mut self, now: u64) {
        let seed = self.seed;
        if let Some((op, selected, end)) = self.donor.build.clone()
            && end == now
        {
            self.donor.build = None;
            let engine = self.engines[DONOR].as_mut().unwrap();
            let step = engine.step(BootstrapEvent::LocalOnlyBuilt { op, selected });
            assert_eq!(step.rejection, None, "seed {seed}");
            self.apply(DONOR, now, step.effects);
            self.donor.ready_at = Some(now + self.affirm);
        }
        if let Some((start, len)) = self.lapse
            && now == start
        {
            self.donor.suspended_until = Some(start + len);
            if self.donor.capture.is_some() {
                self.donor.doomed = true;
            } else {
                self.retire(now);
            }
        }
        if let Some((op, selected, end)) = self.donor.capture.clone()
            && end == now
        {
            self.donor.capture = None;
            if self.engines[DONOR].as_ref().unwrap().current_operation() == Some(op) {
                let event = if !self.donor.doomed && self.verify(DONOR, now) {
                    self.donor.built = Some(now);
                    BootstrapEvent::Built { op, selected }
                } else {
                    self.donor.failed += 1;
                    BootstrapEvent::BuildFailed { op, selected }
                };
                let step = self.engines[DONOR].as_mut().unwrap().step(event);
                self.apply(DONOR, now, step.effects);
            }
            if std::mem::take(&mut self.donor.doomed) {
                self.retire(now);
            }
        }
        if self
            .donor
            .ready_at
            .is_some_and(|at| now >= at && (now - at).is_multiple_of(CONFIG.renew_ms))
            && self.donor.suspended_until.is_none_or(|until| now >= until)
            && self.donor.capture.is_none()
            && self.engines[DONOR].as_ref().unwrap().ready_recapture_due()
            && self.verify(DONOR, now)
        {
            let engine = self.engines[DONOR].as_mut().unwrap();
            let step = engine.step(BootstrapEvent::StartReadyRecapture);
            self.apply(DONOR, now, step.effects);
        }
    }
}

fn run(seed: u64, schedule: &Schedule, source: Source) -> World {
    let mut world = World {
        seed,
        build_len: schedule.build_len,
        affirm: schedule.affirm,
        encode: schedule.encode,
        lapse: schedule.lapse,
        source,
        engines: [None, None],
        donor: Donor::default(),
        joiner: Joiner::default(),
    };
    let mut donor = world.engine(DONOR);
    let started = donor.step(BootstrapEvent::Start).effects;
    world.engines[DONOR] = Some(donor);
    world.apply(DONOR, 0, started);
    for now in 0..HORIZON {
        if now == schedule.join {
            world.source.joined = Some(now);
            let mut joiner = world.engine(JOINER);
            let _ = joiner.step(BootstrapEvent::Tick(Time(now)));
            let started = joiner.step(BootstrapEvent::Start).effects;
            world.engines[JOINER] = Some(joiner);
            world.apply(JOINER, now, started);
        }
        for node in [DONOR, JOINER] {
            let Some(engine) = world.engines[node].as_mut() else {
                continue;
            };
            let effects = engine.step(BootstrapEvent::Tick(Time(now))).effects;
            world.apply(node, now, effects);
            if node == DONOR {
                world.donor_turn(now);
            }
        }
        let joiner = &world.joiner;
        if joiner.donor.is_some() || joiner.build.is_some() || joiner.declined.is_some() {
            break;
        }
    }
    let donor = world.engines[DONOR].as_ref().unwrap();
    assert_ne!(donor.stage(), BootstrapStage::Fallback, "seed {seed}");
    assert_eq!(world.donor.builds, 1, "seed {seed}: the donor scanned once");
    world
}

/// The joiner's first observation lands anywhere from the donor's last build
/// renewals, through the window between its local build and its recapture,
/// to the recapture itself, which the joiner's arrival may interrupt. The
/// joiner never scans the origin: it waits and then finds the Ready donor.
#[test]
fn joiner_sampling_between_local_build_and_ready_claim_waits_for_the_donor() {
    let (mut in_window, mut interrupted) = (0, 0);
    for seed in 0..96_u64 {
        let mut rng = SplitMix64::new(seed);
        // At least 6 ms, so the joiner arrives after the donor's selection.
        let build_len = 6 + u64::from(rng.below(4));
        let (affirm, encode) = (rng.below(3), 1 + rng.below(4));
        // Selection settles at 3 and starts the build at once.
        let build_end = CONFIG.settle_ms + build_len;
        let first_sample = build_end - 2 + u64::from(rng.below(affirm + encode + 5));
        let (affirm, encode) = (u64::from(affirm), u64::from(encode));
        let schedule = Schedule {
            build_len,
            affirm,
            encode,
            join: first_sample - CONFIG.settle_ms,
            lapse: None,
        };
        let source = Source::new(u64::from(rng.below(3)), u64::from(rng.below(2)));
        let World { donor, joiner, .. } = run(seed, &schedule, source);
        assert_eq!(
            joiner.build, None,
            "seed {seed}: the joiner scanned the origin"
        );
        assert_eq!(joiner.declined, None, "seed {seed}: the joiner fell back");
        let found = joiner.donor.expect("the joiner found the donor");
        assert!(
            donor.built.is_some_and(|built| built <= found),
            "seed {seed}"
        );
        // A recapture that failed was interrupted by the join; the next one
        // ran under the new roster, and nothing retried the old one.
        assert!(donor.failed <= 1, "seed {seed}: {} failures", donor.failed);
        assert_eq!(donor.recaptures, donor.failed + 1, "seed {seed}");
        if joiner.in_window > 0 {
            in_window += 1;
        }
        if donor.failed > 0 {
            interrupted += 1;
        }
    }
    assert!(
        in_window > 24,
        "the window was sampled in {in_window} seeds"
    );
    assert!(
        interrupted > 8,
        "a join interrupted {interrupted} recaptures"
    );
}

/// A donor whose recapture never starts renews its Building claim up to its
/// own stall bound and then withdraws it. The joiner follows it until then and
/// scans the origin itself within one observation once it is gone.
#[test]
fn unrecaptured_local_build_ends_its_claim_at_the_stall_bound_and_the_joiner_takes_over() {
    for seed in 0..48_u64 {
        let mut rng = SplitMix64::new(seed);
        let build_len = 6 + u64::from(rng.below(4));
        let build_end = CONFIG.settle_ms + build_len;
        let first_sample = build_end - 2 + u64::from(rng.below(4));
        let schedule = Schedule {
            build_len,
            affirm: HORIZON,
            encode: 1,
            join: first_sample - CONFIG.settle_ms,
            lapse: None,
        };
        let World {
            donor,
            joiner,
            source,
            ..
        } = run(seed, &schedule, Source::new(u64::from(rng.below(3)), 0));
        assert_eq!(donor.recaptures, 0, "seed {seed}");
        let claims = &source.claims[DONOR];
        let released = claims
            .iter()
            .rev()
            .find(|(_, claim)| claim.is_none())
            .map(|(at, _)| *at)
            .expect("the claim was withdrawn");
        assert_eq!(
            released,
            build_end + CONFIG.donor_wait_ms,
            "seed {seed}: the claim ends at the builder's stall bound"
        );
        assert!(
            claims.iter().any(|(at, claim)| *at > build_end
                && *at < released
                && claim
                    .as_ref()
                    .is_some_and(|claim| claim.phase == ClaimPhase::Building)),
            "seed {seed}: the pending claim was renewed"
        );
        let took_over = joiner
            .build
            .expect("the joiner scanned once the claim was gone");
        assert_eq!(joiner.declined, None, "seed {seed}");
        assert!(
            took_over >= released,
            "seed {seed}: scanned at {took_over} while the claim lived to {released}"
        );
        assert!(
            took_over <= released + source.lag + CONFIG.observe_ms,
            "seed {seed}: took over at {took_over}, claim gone at {released}"
        );
    }
}

/// A recovery lapse, as a join itself can trigger, retires the donor's
/// capture: a pending one, a running recapture whose guard it revokes, or an
/// already Ready one. The joiner's first observation lands inside the lapse,
/// which may outlast a claim TTL. The donor keeps a renewed Building claim
/// through it and recaptures once recovery is Ready again, so the joiner
/// never scans the origin: it waits and then finds the Ready donor.
#[test]
fn recovery_lapse_retiring_the_capture_keeps_the_joiner_waiting() {
    let (mut in_lapse, mut waited, mut beyond_ttl, mut ready_retired) = (0, 0, 0, 0);
    for seed in 0..96_u64 {
        let mut rng = SplitMix64::new(seed);
        let build_len = 6 + u64::from(rng.below(4));
        let (affirm, encode) = (rng.below(3), 1 + rng.below(4));
        let build_end = CONFIG.settle_ms + build_len;
        let start = build_end + u64::from(rng.below(affirm + encode + 3));
        // Shorter than the stall bound, so the pending claim outlives it.
        let len = 1 + rng.below(9);
        let first_sample = start + u64::from(rng.below(len));
        let len = u64::from(len);
        let schedule = Schedule {
            build_len,
            affirm: u64::from(affirm),
            encode: u64::from(encode),
            join: first_sample - CONFIG.settle_ms,
            lapse: Some((start, len)),
        };
        let source = Source::new(u64::from(rng.below(3)), u64::from(rng.below(2)));
        let World { donor, joiner, .. } = run(seed, &schedule, source);
        assert_eq!(
            joiner.build, None,
            "seed {seed}: the joiner scanned the origin"
        );
        assert_eq!(joiner.declined, None, "seed {seed}: the joiner fell back");
        let found = joiner.donor.expect("the joiner found the donor");
        assert!(
            donor.built.is_some_and(|built| built <= found),
            "seed {seed}"
        );
        assert!(donor.retired >= 1, "seed {seed}: the lapse retired nothing");
        // At most the join and the lapse each fail one recapture.
        assert!(donor.failed <= 2, "seed {seed}: {} failures", donor.failed);
        if joiner.in_lapse > 0 {
            in_lapse += 1;
        }
        // A Ready claim published just before the lapse can still reach the
        // lagging joiner first; otherwise it waited out the whole lapse.
        if donor.built.is_some_and(|built| built >= start + len) {
            waited += 1;
            if len > CONFIG.claim_ttl_ms {
                beyond_ttl += 1;
            }
        }
        if donor.recaptures > donor.failed + 1 {
            ready_retired += 1;
        }
    }
    assert_eq!(in_lapse, 96, "every joiner first sampled inside the lapse");
    assert!(waited > 64, "{waited} joiners waited out the lapse");
    assert!(
        beyond_ttl > 16,
        "{beyond_ttl} of them outlasted a claim TTL"
    );
    assert!(
        ready_retired > 4,
        "{ready_retired} lapses retired a Ready capture"
    );
}
