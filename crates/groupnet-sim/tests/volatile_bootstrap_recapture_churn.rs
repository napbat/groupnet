//! Seeded SWIM churn and failures around a completed local build's Ready
//! recapture, with participation required as in production.
//!
//! A joiner follows the donor's origin build. From then on, as on a loaded
//! pair of one-CPU nodes whose image capture itself stalls the donor, each
//! node's view of the other passes through Suspect and Dead windows, and both
//! refute with higher incarnations, while every presence keeps renewing
//! unchanged. The recapture's cuts at C and after encoding differ in exactly
//! that churn: the first attempt goes Ready, no maintenance recheck retires
//! it, and the joiner finds it without scanning the origin. A second schedule
//! also fails attempts outright: each retry waits out a doubling backoff, and
//! all of them start inside the claim window that opened with the build.

use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapMemberIdentity, BootstrapOperation, BootstrapParticipant, BootstrapPresence,
    BootstrapScope, BootstrapStage, ClaimEngine, ClaimIdentity,
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
    donor_wait_ms: 24,
    total_ms: 60,
};
const DONOR: usize = 0;
const JOINER: usize = 1;
const HORIZON: u64 = 300;
/// How long the donor keeps its Ready capture under churn once the joiner
/// has found it.
const SETTLE: u64 = 60;

fn name(node: usize) -> NodeId {
    NodeId::from(if node == DONOR { "node-a" } else { "node-b" })
}

/// The retry pause after `failures` consecutive failed recaptures: one
/// observation, doubling, capped at a quarter of the stall bound.
fn backoff(failures: u32) -> u64 {
    (CONFIG.observe_ms << (failures - 1)).min((CONFIG.donor_wait_ms / 4).max(CONFIG.observe_ms))
}

/// One node's SWIM opinion of a member.
#[derive(Clone, Copy)]
struct View {
    status: Status,
    incarnation: u64,
    /// A suspicion `(since, until)`: it hardens into a Dead verdict two ms
    /// after `since` and is refuted at `until`.
    suspect: Option<(u64, u64)>,
}

/// Native TTL source. Publications reach the other node `lag` ms later and
/// the publisher at once; remaining TTL counts from publication. Membership
/// status and incarnation are each reader's own view, which the churn moves.
struct Source {
    claims: [Vec<(u64, Option<BootstrapClaim>)>; 2],
    presence: [Vec<(u64, BootstrapPresence)>; 2],
    joined: Option<u64>,
    lag: u64,
    /// `views[reader][node]`.
    views: [[View; 2]; 2],
}

struct Cut {
    members: Vec<BootstrapMember>,
    roster: Vec<BootstrapMemberIdentity>,
    participants: Vec<BootstrapParticipant>,
    claims: Vec<BootstrapClaim>,
}

impl Source {
    fn new(lag: u64) -> Self {
        let alive = View {
            status: Status::Alive,
            incarnation: 1,
            suspect: None,
        };
        Self {
            claims: [Vec::new(), Vec::new()],
            presence: [Vec::new(), Vec::new()],
            joined: None,
            lag,
            views: [[alive; 2]; 2],
        }
    }

    fn delay(&self, from: usize, to: usize) -> u64 {
        if from == to { 0 } else { self.lag }
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

    /// `reader` suspects `node` from `now` until `until`.
    fn suspect(&mut self, reader: usize, node: usize, now: u64, until: u64) {
        let view = &mut self.views[reader][node];
        if view.suspect.is_none() {
            view.status = Status::Suspect;
            view.suspect = Some((now, until.max(now + 1)));
        }
    }

    /// `node` refutes a suspicion of itself: every view of it, its own
    /// included, sees the higher incarnation.
    fn refute(&mut self, node: usize) {
        for reader in [DONOR, JOINER] {
            self.views[reader][node].incarnation += 1;
        }
    }

    /// Harden or refute suspicions at `now`.
    fn advance(&mut self, now: u64) {
        let mut refuted = Vec::new();
        for reader in [DONOR, JOINER] {
            for node in [DONOR, JOINER] {
                let view = &mut self.views[reader][node];
                match view.suspect {
                    Some((_, until)) if now >= until => {
                        view.status = Status::Alive;
                        view.suspect = None;
                        refuted.push(node);
                    }
                    Some((since, _)) if now >= since + 2 => view.status = Status::Dead,
                    _ => {}
                }
            }
        }
        for node in refuted {
            self.refute(node);
        }
    }

    /// One complete native cut as `reader` sees it at `now`. A member it does
    /// not see Alive keeps its presence, but its claim is not reported, as
    /// the native source does.
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
            let view = self.views[reader][node];
            let member = BootstrapMemberIdentity {
                node: name(node),
                presence: presence.map(|(_, presence)| presence.identity.clone()),
                member_incarnation: view.incarnation,
                status: view.status,
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
                && member.eligible()
            {
                cut.claims.push(BootstrapClaim {
                    remaining_ms: CONFIG.claim_ttl_ms - (now - at),
                    ..claim.clone()
                });
            }
            cut.members.push(BootstrapMember {
                node: name(node),
                eligible: member.eligible(),
            });
            cut.roster.push(member);
        }
        cut
    }
}

#[derive(Default)]
struct Donor {
    build: Option<(BootstrapOperation, ClaimIdentity, u64)>,
    builds: u32,
    /// The local build completed: the claim window opened here.
    pending_at: Option<u64>,
    ready_at: Option<u64>,
    capture: Option<(
        BootstrapOperation,
        ClaimIdentity,
        u64,
        Vec<BootstrapMemberIdentity>,
    )>,
    /// When each recapture started.
    starts: Vec<u64>,
    /// When each failed.
    failures: Vec<u64>,
    built: Option<u64>,
    retired: u32,
    /// A cut after encoding differed from C in some member's status.
    status_churn: bool,
    /// ... or in some member's incarnation.
    incarnation_churn: bool,
}

#[derive(Default)]
struct Joiner {
    following: bool,
    build: Option<u64>,
    declined: Option<u64>,
    donor: Option<u64>,
}

struct Schedule {
    build_len: u64,
    affirm: u64,
    encode: u64,
    join: u64,
    /// Suspicions and refutations per 32 ms, for each kind of churn.
    churn: u32,
    /// Fail a finished recapture with this chance in 4, whatever its cuts.
    fail: u32,
}

struct World {
    seed: u64,
    rng: SplitMix64,
    schedule: Schedule,
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
                        assert_eq!(node, JOINER, "seed {seed}: donor cut refused");
                        // The session reports a refused cut; a follower inside
                        // its grace samples again.
                        let step = self.engines[node]
                            .as_mut()
                            .unwrap()
                            .step(BootstrapEvent::ObservationFailed { op });
                        queue.extend(step.effects);
                    } else {
                        queue.extend(step.effects);
                    }
                }
                BootstrapEffect::FollowBuilder { selected, .. } => {
                    assert_eq!(node, JOINER, "seed {seed}");
                    assert_eq!(selected.node, name(DONOR), "seed {seed}");
                    self.joiner.following = true;
                }
                BootstrapEffect::BuildOrigin { op, selected } => {
                    if node == DONOR {
                        self.donor.builds += 1;
                        let end = now + self.schedule.build_len;
                        self.donor.build = Some((op, selected, end));
                    } else {
                        self.joiner.build.get_or_insert(now);
                    }
                }
                BootstrapEffect::RecaptureCurrent { op, selected } => {
                    self.donor.starts.push(now);
                    if let Some(at_c) = self.verify(DONOR, now) {
                        let end = now + self.schedule.encode;
                        self.donor.capture = Some((op, selected, end, at_c));
                    } else {
                        self.fail(now, op, selected);
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

    /// One verified complete cut, as the worker's `current_participation`,
    /// returning the exact roster sampled.
    fn verify(&mut self, node: usize, now: u64) -> Option<Vec<BootstrapMemberIdentity>> {
        let engine = self.engines[node].as_mut().unwrap();
        let op = engine.begin_roster_observation().ok()?;
        let cut = self.source.cut(node, now);
        engine
            .verify_participant_roster(
                op,
                &cut.members,
                &cut.roster,
                &cut.participants,
                &cut.claims,
            )
            .ok()?;
        Some(cut.roster)
    }

    fn fail(&mut self, now: u64, op: BootstrapOperation, selected: ClaimIdentity) {
        self.donor.failures.push(now);
        let step = self.engines[DONOR]
            .as_mut()
            .unwrap()
            .step(BootstrapEvent::BuildFailed { op, selected });
        self.apply(DONOR, now, step.effects);
    }

    /// Seeded suspicions and refutations for this ms. The joiner's view of
    /// the donor is churned only once it follows the donor, as a follower
    /// that first samples a suspected builder rightly builds itself.
    fn churn(&mut self, now: u64) {
        self.source.advance(now);
        if self.source.joined.is_none() {
            return;
        }
        let rate = self.schedule.churn;
        if self.rng.below(32) < rate {
            // The donor's capture stalls it: the joiner suspects it and it
            // refutes, raising its own incarnation in its own view too.
            self.source.refute(DONOR);
            if self.joiner.following {
                let until = now + 1 + u64::from(self.rng.below(4));
                self.source.suspect(JOINER, DONOR, now, until);
            }
        }
        if self.rng.below(32) < rate {
            // The loaded joiner is suspected, and maybe declared Dead.
            let until = now + 1 + u64::from(self.rng.below(6));
            self.source.suspect(DONOR, JOINER, now, until);
        }
    }

    /// The donor's worker: the build completes, recovery affirms, and each
    /// maintenance turn starts a due recapture or rechecks a Ready capture.
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
            self.donor.pending_at = Some(now);
            self.donor.ready_at = Some(now + self.schedule.affirm);
        }
        if let Some((op, selected, end, at_c)) = self.donor.capture.clone()
            && end == now
        {
            self.donor.capture = None;
            let fails = self.rng.below(4) < self.schedule.fail;
            match self.verify(DONOR, now) {
                Some(after) if !fails => {
                    self.donor.status_churn |= at_c
                        .iter()
                        .zip(&after)
                        .any(|(c, after)| c.status != after.status);
                    self.donor.incarnation_churn |= at_c
                        .iter()
                        .zip(&after)
                        .any(|(c, after)| c.member_incarnation != after.member_incarnation);
                    self.donor.built = Some(now);
                    let step = self.engines[DONOR]
                        .as_mut()
                        .unwrap()
                        .step(BootstrapEvent::Built { op, selected });
                    assert_eq!(step.rejection, None, "seed {seed}");
                    self.apply(DONOR, now, step.effects);
                }
                _ => self.fail(now, op, selected),
            }
        }
        let turn = self
            .donor
            .ready_at
            .is_some_and(|at| now >= at && (now - at) % CONFIG.renew_ms == 0);
        if !turn || self.donor.capture.is_some() {
            return;
        }
        let engine = self.engines[DONOR].as_ref().unwrap();
        if engine.ready_recapture_due() {
            if self.verify(DONOR, now).is_some() {
                let engine = self.engines[DONOR].as_mut().unwrap();
                let step = engine.step(BootstrapEvent::StartReadyRecapture);
                assert_eq!(step.rejection, None, "seed {seed}");
                self.apply(DONOR, now, step.effects);
            }
        } else if self.donor.built.is_some()
            && engine.stage() == BootstrapStage::DonorAvailable
            && self.verify(DONOR, now).is_none()
        {
            // The worker's maintenance recheck retires a Ready capture whose
            // cut no longer verifies.
            let engine = self.engines[DONOR].as_mut().unwrap();
            let selected = engine.selected().cloned().unwrap();
            let step = engine.step(BootstrapEvent::CaptureRetired { selected });
            self.donor.retired += 1;
            self.apply(DONOR, now, step.effects);
        }
    }
}

fn run(seed: u64, schedule: Schedule, lag: u64) -> World {
    let mut world = World {
        seed,
        rng: SplitMix64::new(seed ^ 0x5eed),
        schedule,
        source: Source::new(lag),
        engines: [None, None],
        donor: Donor::default(),
        joiner: Joiner::default(),
    };
    let mut donor = world.engine(DONOR);
    let started = donor.step(BootstrapEvent::Start).effects;
    world.engines[DONOR] = Some(donor);
    world.apply(DONOR, 0, started);
    let mut found = None;
    for now in 0..HORIZON {
        world.churn(now);
        if now == world.schedule.join {
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
        if joiner.build.is_some() || joiner.declined.is_some() {
            break;
        }
        if joiner.donor.is_some() && found.is_none() {
            // The transfer completes: the joiner's candidate retires while its
            // presence keeps renewing, and the donor keeps its capture.
            found = Some(now);
            let engine = world.engines[JOINER].as_mut().unwrap();
            let step = engine.step(BootstrapEvent::RetireCandidate);
            world.apply(JOINER, now, step.effects);
        }
        if found.is_some_and(|found| now >= found + SETTLE) {
            break;
        }
    }
    let donor = world.engines[DONOR].as_ref().unwrap();
    assert_ne!(donor.stage(), BootstrapStage::Fallback, "seed {seed}");
    assert_eq!(world.donor.builds, 1, "seed {seed}: the donor scanned once");
    world
}

/// Churn through the build, the recapture and a long Ready tenure never
/// fails or retires the donor's image: exactly one recapture, taken while
/// C and the post-encode cut differ in status or incarnation in most seeds,
/// and the joiner finds it without an origin scan.
#[test]
fn swim_churn_during_encode_keeps_one_ready_recapture() {
    let (mut status_churn, mut incarnation_churn) = (0, 0);
    for seed in 0..96_u64 {
        let mut rng = SplitMix64::new(seed);
        let build_len = 10 + u64::from(rng.below(6));
        let join = CONFIG.settle_ms + 1 + u64::from(rng.below(3));
        let schedule = Schedule {
            build_len,
            affirm: u64::from(rng.below(3)),
            encode: 2 + u64::from(rng.below(5)),
            join,
            churn: 6 + rng.below(6),
            fail: 0,
        };
        let World { donor, joiner, .. } = run(seed, schedule, u64::from(rng.below(2)));
        assert_eq!(
            donor.failures,
            [],
            "seed {seed}: churn failed recaptures started at {:?}",
            donor.starts
        );
        assert_eq!(donor.starts.len(), 1, "seed {seed}: {:?}", donor.starts);
        assert_eq!(donor.retired, 0, "seed {seed}: churn retired the capture");
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
        status_churn += u32::from(donor.status_churn);
        incarnation_churn += u32::from(donor.incarnation_churn);
    }
    assert!(
        status_churn > 24,
        "{status_churn} captures saw a status change during encode"
    );
    assert!(
        incarnation_churn > 48,
        "{incarnation_churn} captures saw an incarnation change during encode"
    );
}

/// With attempts failing outright as well, each retry waits at least its
/// doubling backoff after the failure before it, every attempt starts
/// inside the claim window that opened with the local build, and the claim
/// is withdrawn when that window closes without a Ready image.
#[test]
fn failed_recaptures_are_paced_inside_the_claim_window() {
    let (mut retried, mut exhausted) = (0, 0);
    for seed in 0..96_u64 {
        let mut rng = SplitMix64::new(seed);
        let schedule = Schedule {
            build_len: 10 + u64::from(rng.below(6)),
            affirm: u64::from(rng.below(3)),
            encode: 1 + u64::from(rng.below(3)),
            join: CONFIG.settle_ms + 1 + u64::from(rng.below(3)),
            churn: 6 + rng.below(6),
            fail: 2 + rng.below(2),
        };
        let encode = schedule.encode;
        let World {
            donor,
            joiner,
            source,
            ..
        } = run(seed, schedule, u64::from(rng.below(2)));
        let opened = donor.pending_at.expect("the local build completed");
        let window = opened + CONFIG.donor_wait_ms;
        // Every attempt after the first follows a failure by its backoff.
        for (retry, (start, failed)) in (1_u32..).zip(donor.starts[1..].iter().zip(&donor.failures))
        {
            assert!(
                *start >= failed + backoff(retry),
                "seed {seed}: retry {retry} too soon: starts {:?} failures {:?}",
                donor.starts,
                donor.failures
            );
        }
        assert!(
            donor.starts.iter().all(|start| *start < window),
            "seed {seed}: an attempt outside the window: {:?} after {window}",
            donor.starts
        );
        if donor.starts.len() > 2 {
            retried += 1;
        }
        if donor.built.is_none() {
            exhausted += 1;
            let released = source.claims[DONOR]
                .iter()
                .rev()
                .find(|(_, claim)| claim.is_none())
                .map(|(at, _)| *at)
                .expect("the claim was withdrawn");
            assert!(
                (window..=window + CONFIG.renew_ms + encode).contains(&released),
                "seed {seed}: claim withdrawn at {released}, window closed at {window}"
            );
        } else {
            assert_eq!(joiner.build, None, "seed {seed}: the joiner scanned");
            assert!(joiner.donor.is_some(), "seed {seed}");
        }
    }
    assert!(retried > 24, "{retried} donors retried twice or more");
    assert!(exhausted > 4, "{exhausted} donors never went Ready");
}
