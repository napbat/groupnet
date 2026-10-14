//! Seeded `StatefulSet` rolling-update schedules around a donor's Ready
//! capture, with participation required as in production and its timing
//! scaled from it: one millisecond here is 250 there, so a 500 ms renewal, a
//! 3 s claim TTL and settle, a 1 s observation, a 30 s stall bound and a 30 s
//! pod start. The peer restarts. Its leave lapses the donor's lease, which
//! fails a running capture or retires a Ready one; the donor reaps it, and its
//! seed resolver re-registers the name without presence, each a roster change
//! that retires the capture taken in between; and its replacement joins with
//! a new boot, which lapses the donor's lease again. Before the leave, the
//! peer's first write may retire the donor's capture as a new writer. Each of
//! those fails an attempt or retires a capture younger than a claim window, so
//! the failures' backoff doubles towards its cap of a quarter of the stall
//! bound. None of them was an attempt to serve the replacement: the donor
//! starts the replacement's capture on its first maintenance turn after the
//! join's lapse, through any backoff, and the replacement finds the Ready
//! donor within one observation of that capture, never scanning the origin.
//!
//! If the peer crashed instead, its unsealed tail fails the donor's lapse
//! proof: the donor retires its candidate and rebuilds from the origin, and
//! nothing it captured before the crash is ever offered to the replacement.

use std::collections::BTreeSet;

use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapMemberIdentity, BootstrapOperation, BootstrapParticipant, BootstrapPresence,
    BootstrapScope, ClaimEngine, ClaimIdentity, PresenceIdentity,
};
use groupnet_core::{NodeId, Status, Time};
use groupnet_sim::SplitMix64;

const CONFIG: BootstrapConfig = BootstrapConfig {
    max_members: 8,
    max_member_bytes: 32,
    max_scope_bytes: 64,
    settle_ms: 12,
    renew_ms: 2,
    claim_ttl_ms: 12,
    observe_ms: 4,
    donor_wait_ms: 120,
    total_ms: 240,
};
/// The replacement's start, from its predecessor's stop to its gossip.
const POD_START: u64 = 120;
const HORIZON: u64 = 1_000;
const DONOR: usize = 0;
const JOINER: usize = 1;

fn name(node: usize) -> NodeId {
    NodeId::from(if node == DONOR { "node-a" } else { "node-b" })
}

/// One seeded restart of the peer.
#[derive(Clone, Copy, Debug)]
struct Schedule {
    build_len: u64,
    affirm: u64,
    encode: u64,
    lag: u64,
    /// The peer's first write retires the donor's Ready capture.
    write: Option<u64>,
    leave: u64,
    lapse_leave: u64,
    reap: u64,
    readd: u64,
    /// The re-registered name is Alive without presence, so cuts are
    /// incomplete until the replacement's presence arrives; otherwise it is
    /// suspected, and the cut lists it without presence.
    readd_alive: bool,
    lapse_join: u64,
    /// The peer crashed: no seal, so the donor's lapse proof fails.
    crash: bool,
}

impl Schedule {
    fn join(&self) -> u64 {
        self.leave + POD_START
    }

    fn seeded(rng: &mut SplitMix64, crash: bool) -> Self {
        let leave = 50 + rng.below(40);
        let reap = leave + 16 + rng.below(24);
        let write = (rng.below(2) == 0).then(|| 40 + rng.below(leave - 40));
        let (leave, reap) = (u64::from(leave), u64::from(reap));
        Self {
            build_len: 20 + u64::from(rng.below(10)),
            affirm: u64::from(rng.below(3)),
            encode: 1 + u64::from(rng.below(3)),
            lag: u64::from(rng.below(3)),
            write: write.map(u64::from),
            leave,
            lapse_leave: 6 + u64::from(rng.below(16)),
            reap,
            readd: reap + u64::from(rng.below(4)),
            // A crashed peer's re-registered name stays suspected: the
            // donor's own rebuild needs a complete cut.
            readd_alive: !crash && rng.below(2) == 0,
            lapse_join: 6 + u64::from(rng.below(8)),
            crash,
        }
    }
}

struct Cut {
    members: Vec<BootstrapMember>,
    roster: Vec<BootstrapMemberIdentity>,
    participants: Vec<BootstrapParticipant>,
    claims: Vec<BootstrapClaim>,
}

impl Cut {
    fn push(&mut self, member: BootstrapMemberIdentity, renewal: u64, remaining_ms: u64) {
        if member.presence.is_some() {
            self.participants.push(BootstrapParticipant {
                member: member.clone(),
                renewal,
                remaining_ms,
            });
        }
        self.members.push(BootstrapMember {
            node: member.node.clone(),
            eligible: member.eligible(),
        });
        self.roster.push(member);
    }
}

/// Which image the donor's captures come from, across a crash.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Image {
    /// The image the donor started with.
    #[default]
    Original,
    /// The lapse proof failed and the candidate retired its image.
    Retired,
    /// The donor completed a local build after the crash.
    Rebuilt,
}

/// The donor's worker around its captures, as the session drives it.
#[derive(Default)]
struct Worker {
    build: Option<(BootstrapOperation, ClaimIdentity, u64)>,
    builds: u32,
    ready_at: Option<u64>,
    /// A running capture and when its encoding ends.
    capture: Option<(BootstrapOperation, ClaimIdentity, u64)>,
    /// The donor holds a Ready capture.
    ready: bool,
    suspended_until: Option<u64>,
    /// The lapse revoked the running capture's guard.
    doomed: bool,
    /// A lapse just ended: the worker runs a maintenance turn at once.
    resumed: bool,
    image: Image,
    published_before_crash: BTreeSet<ClaimIdentity>,
    /// The first attempt whose cut named the replacement, and whether a
    /// failure's backoff held when that turn offered its cut.
    served: Option<(u64, bool)>,
    failed: u32,
    retired: u32,
}

#[derive(Default)]
struct Joiner {
    found: Option<(u64, ClaimIdentity)>,
    built: Option<u64>,
    declined: Option<u64>,
}

struct World {
    seed: u64,
    schedule: Schedule,
    donor: ClaimEngine,
    joiner: Option<ClaimEngine>,
    claims: [Vec<(u64, Option<BootstrapClaim>)>; 2],
    presence: [Vec<(u64, BootstrapPresence)>; 2],
    worker: Worker,
    replacement: Joiner,
}

fn engine(seed: u64, node: usize) -> ClaimEngine {
    let mut engine = ClaimEngine::new(
        CONFIG,
        BootstrapScope {
            domain: "origin".to_owned(),
            partition: "bucket".to_owned(),
        },
        name(node),
        BootId(u128::from(seed) * 4 + u128::from(node == JOINER) * 2 + 1),
        1,
    )
    .unwrap();
    engine.require_participation().unwrap();
    engine
}

impl World {
    fn fresh(at: u64, now: u64) -> Option<u64> {
        CONFIG
            .claim_ttl_ms
            .checked_sub(now - at)
            .filter(|remaining| *remaining > 0)
    }

    /// `node`'s latest claim as published by `seen`, with its remaining TTL
    /// at `now`.
    fn claim(&self, node: usize, seen: u64, now: u64) -> Option<BootstrapClaim> {
        let (at, claim) = self.claims[node].iter().rev().find(|(at, _)| *at <= seen)?;
        let remaining_ms = Self::fresh(*at, now)?;
        claim.as_ref().map(|claim| BootstrapClaim {
            remaining_ms,
            ..claim.clone()
        })
    }

    /// `node`'s engine presence as published by `seen`.
    fn engine_presence(
        &self,
        node: usize,
        seen: u64,
        now: u64,
    ) -> Option<(BootstrapPresence, u64)> {
        let (at, presence) = self.presence[node]
            .iter()
            .rev()
            .find(|(at, _)| *at <= seen)?;
        Some((presence.clone(), Self::fresh(*at, now)?))
    }

    /// The peer's previous life, whose presence renewed every interval until
    /// it stopped.
    fn old_life(&self) -> PresenceIdentity {
        PresenceIdentity {
            node: name(JOINER),
            boot: BootId(u128::from(self.seed) * 4 + 4),
            session: 1,
        }
    }

    fn me(&self, node: usize, now: u64) -> Option<(BootstrapMemberIdentity, u64, u64)> {
        let (presence, remaining) = self.engine_presence(node, now, now)?;
        Some((
            BootstrapMemberIdentity {
                node: name(node),
                presence: Some(presence.identity),
                member_incarnation: 1,
                status: Status::Alive,
            },
            presence.renewal,
            remaining,
        ))
    }

    /// The donor's complete cut at `now`: itself, and the peer as it sees
    /// it `lag` behind.
    fn donor_cut(&self, now: u64) -> Cut {
        let schedule = &self.schedule;
        let seen = now.saturating_sub(schedule.lag);
        let mut cut = Cut {
            members: Vec::new(),
            roster: Vec::new(),
            participants: Vec::new(),
            claims: Vec::new(),
        };
        if let Some((me, renewal, remaining)) = self.me(DONOR, now) {
            cut.push(me, renewal, remaining);
        }
        if let Some(claim) = self.claim(DONOR, now, now) {
            cut.claims.push(claim);
        }
        let peer = |presence: Option<PresenceIdentity>, status| BootstrapMemberIdentity {
            node: name(JOINER),
            presence,
            member_incarnation: 1,
            status,
        };
        let replaced = self
            .joiner
            .is_some()
            .then(|| self.engine_presence(JOINER, seen, now))
            .flatten();
        if let Some((presence, remaining)) = replaced {
            cut.push(
                peer(Some(presence.identity), Status::Alive),
                presence.renewal,
                remaining,
            );
            if let Some(claim) = self.claim(JOINER, seen, now) {
                cut.claims.push(claim);
            }
        } else if seen < schedule.leave {
            let renewed = seen - seen % CONFIG.renew_ms;
            cut.push(
                peer(Some(self.old_life()), Status::Alive),
                renewed / CONFIG.renew_ms + 1,
                CONFIG.claim_ttl_ms - (now - renewed),
            );
        } else if seen < schedule.reap {
            // A planned stop withdrew its presence; a crashed process's last
            // one lives out its TTL, and then only its suspicion is left.
            let renewed = schedule.leave - schedule.leave % CONFIG.renew_ms;
            match Self::fresh(renewed, now).filter(|_| schedule.crash) {
                Some(remaining) => cut.push(
                    peer(Some(self.old_life()), Status::Alive),
                    renewed / CONFIG.renew_ms + 1,
                    remaining,
                ),
                None => cut.push(peer(None, Status::Dead), 0, 0),
            }
        } else if seen >= schedule.readd {
            let status = if schedule.readd_alive {
                Status::Alive
            } else {
                Status::Suspect
            };
            cut.push(peer(None, status), 0, 0);
        }
        cut
    }

    /// The replacement's complete cut at `now`.
    fn joiner_cut(&self, now: u64) -> Cut {
        let seen = now.saturating_sub(self.schedule.lag);
        let mut cut = Cut {
            members: Vec::new(),
            roster: Vec::new(),
            participants: Vec::new(),
            claims: Vec::new(),
        };
        if let Some((presence, remaining)) = self.engine_presence(DONOR, seen, now) {
            cut.push(
                BootstrapMemberIdentity {
                    node: name(DONOR),
                    presence: Some(presence.identity),
                    member_incarnation: 1,
                    status: Status::Alive,
                },
                presence.renewal,
                remaining,
            );
        }
        if let Some(claim) = self.claim(DONOR, seen, now) {
            cut.claims.push(claim);
        }
        if let Some((me, renewal, remaining)) = self.me(JOINER, now) {
            cut.push(me, renewal, remaining);
        }
        if let Some(claim) = self.claim(JOINER, now, now) {
            cut.claims.push(claim);
        }
        cut
    }

    fn withdraw(&mut self, node: usize, now: u64, identity: &ClaimIdentity) {
        let current = self.claims[node]
            .last()
            .and_then(|(_, claim)| claim.as_ref());
        if current.is_some_and(|claim| claim.identity == *identity) {
            self.claims[node].push((now, None));
        }
    }

    fn step(&mut self, node: usize, event: BootstrapEvent) -> Vec<BootstrapEffect> {
        let engine = if node == DONOR {
            &mut self.donor
        } else {
            self.joiner.as_mut().unwrap()
        };
        let step = engine.step(event);
        assert_eq!(step.rejection, None, "seed {}: node {node}", self.seed);
        step.effects
    }

    fn apply(&mut self, node: usize, now: u64, effects: Vec<BootstrapEffect>) {
        let seed = self.seed;
        let mut queue = std::collections::VecDeque::from(effects);
        while let Some(effect) = queue.pop_front() {
            match effect {
                BootstrapEffect::PublishClaim(claim) => {
                    if node == DONOR && self.worker.image == Image::Original {
                        self.worker
                            .published_before_crash
                            .insert(claim.identity.clone());
                    }
                    self.claims[node].push((now, Some(claim)));
                }
                BootstrapEffect::WithdrawClaim(identity) => self.withdraw(node, now, &identity),
                BootstrapEffect::PublishPresence(presence) => {
                    self.presence[node].push((now, presence));
                }
                BootstrapEffect::ObserveClaims { op, .. } => {
                    let cut = if node == DONOR {
                        self.donor_cut(now)
                    } else {
                        self.joiner_cut(now)
                    };
                    let event = BootstrapEvent::ParticipantsObserved {
                        op,
                        members: cut.members,
                        roster: cut.roster,
                        participants: cut.participants,
                        claims: cut.claims,
                    };
                    let engine = if node == DONOR {
                        &mut self.donor
                    } else {
                        self.joiner.as_mut().unwrap()
                    };
                    let step = engine.step(event);
                    if step.rejection.is_some() {
                        assert_eq!(node, JOINER, "seed {seed}: donor cut refused");
                        self.replacement.declined.get_or_insert(now);
                    }
                    queue.extend(step.effects);
                }
                BootstrapEffect::BuildOrigin { op, selected } => {
                    if node == DONOR {
                        self.worker.builds += 1;
                        self.worker.build = Some((op, selected, now + self.schedule.build_len));
                    } else {
                        self.replacement.built.get_or_insert(now);
                    }
                }
                BootstrapEffect::RecaptureCurrent { op, selected } => {
                    assert_eq!(node, DONOR, "seed {seed}");
                    assert!(
                        self.worker.image != Image::Retired,
                        "seed {seed}: a capture started from the image the crash retired"
                    );
                    if self.verify(now) {
                        self.worker.capture = Some((op, selected, now + self.schedule.encode));
                    } else {
                        self.worker.failed += 1;
                        let effects =
                            self.step(DONOR, BootstrapEvent::BuildFailed { op, selected });
                        queue.extend(effects);
                    }
                }
                BootstrapEffect::DonorAvailable { selected, .. } => {
                    assert_eq!(node, JOINER, "seed {seed}: the donor followed");
                    self.replacement.found.get_or_insert((now, selected));
                }
                BootstrapEffect::FallbackOrigin => {
                    assert_eq!(node, JOINER, "seed {seed}: the donor fell back");
                    self.replacement.declined.get_or_insert(now);
                }
                _ => {}
            }
        }
    }

    fn verify(&mut self, now: u64) -> bool {
        let Ok(op) = self.donor.begin_roster_observation() else {
            return false;
        };
        let cut = self.donor_cut(now);
        self.donor
            .verify_participant_roster(
                op,
                &cut.members,
                &cut.roster,
                &cut.participants,
                &cut.claims,
            )
            .is_ok()
    }

    /// The worker retires the Ready capture: a lapse suspended it, its
    /// journal saw a new writer, or the maintenance recheck saw its roster
    /// change.
    fn retire(&mut self, now: u64) {
        self.worker.ready = false;
        self.worker.retired += 1;
        let selected = self.donor.selected().cloned().unwrap();
        let effects = self.step(DONOR, BootstrapEvent::CaptureRetired { selected });
        self.apply(DONOR, now, effects);
    }

    fn lapse(&mut self, now: u64, len: u64) {
        self.worker.suspended_until = Some(now + len);
        if self.worker.capture.is_some() {
            self.worker.doomed = true;
        } else if self.donor.ready_recapture_pending() || self.worker.ready {
            // As `suspend_local`: a Ready capture is retired, and a pending
            // one renews its claim for a fresh window.
            self.retire(now);
        }
    }

    /// The crashed peer's unsealed tail failed the lapse proof: the
    /// candidate retires and the recovery rebuilds under a fresh one.
    fn crashed(&mut self, now: u64) {
        self.worker.image = Image::Retired;
        self.worker.ready = false;
        let effects = self.step(DONOR, BootstrapEvent::RetireCandidate);
        self.apply(DONOR, now, effects);
        let effects = self.step(DONOR, BootstrapEvent::Start);
        self.apply(DONOR, now, effects);
    }

    fn donor_turn(&mut self, now: u64) {
        let schedule = self.schedule;
        let seed = self.seed;
        if let Some((op, selected, end)) = self.worker.build.clone()
            && end == now
        {
            self.worker.build = None;
            if self.worker.image != Image::Original {
                self.worker.image = Image::Rebuilt;
            }
            let effects = self.step(DONOR, BootstrapEvent::LocalOnlyBuilt { op, selected });
            self.apply(DONOR, now, effects);
            self.worker.ready_at = Some(now + schedule.affirm);
        }
        if schedule.write == Some(now) && self.worker.ready {
            self.retire(now);
        }
        if now == schedule.leave + schedule.lag {
            self.lapse(now, schedule.lapse_leave);
        }
        if now == schedule.join() + schedule.lag {
            self.lapse(now, schedule.lapse_join);
        }
        if let Some((op, selected, end)) = self.worker.capture.clone()
            && end == now
        {
            self.worker.capture = None;
            if self.donor.current_operation() == Some(op) {
                let event = if !std::mem::take(&mut self.worker.doomed) && self.verify(now) {
                    self.worker.ready = true;
                    BootstrapEvent::Built { op, selected }
                } else {
                    self.worker.failed += 1;
                    BootstrapEvent::BuildFailed { op, selected }
                };
                let effects = self.step(DONOR, event);
                self.apply(DONOR, now, effects);
            }
        }
        if self.worker.suspended_until == Some(now) {
            self.worker.suspended_until = None;
            if schedule.crash && now < schedule.join() {
                self.crashed(now);
            } else {
                self.worker.resumed = true;
            }
        }
        let turn = self.worker.ready_at.is_some_and(|at| now >= at)
            && self.worker.suspended_until.is_none()
            && self.worker.capture.is_none()
            && (now.is_multiple_of(CONFIG.renew_ms) | std::mem::take(&mut self.worker.resumed));
        if !turn {
            return;
        }
        if self.worker.ready && !self.verify(now) {
            self.retire(now);
        }
        if self.donor.ready_recapture_pending() && self.verify(now) {
            let names_replacement = self.donor.participant_roster().is_some_and(|roster| {
                roster.iter().any(|member| {
                    member.node == name(JOINER)
                        && member
                            .presence
                            .as_ref()
                            .is_some_and(|presence| presence.boot != self.old_life().boot)
                })
            });
            let backing_off = self.donor.ready_recapture_backing_off();
            let effects = self.step(DONOR, BootstrapEvent::StartReadyRecapture);
            self.apply(DONOR, now, effects);
            if names_replacement && self.worker.served.is_none() {
                assert!(
                    self.worker.capture.is_some(),
                    "seed {seed}: the first cut naming the replacement started nothing at {now}"
                );
                self.worker.served = Some((now, backing_off));
            }
        }
    }
}

fn run(seed: u64, schedule: Schedule) -> World {
    let mut world = World {
        seed,
        schedule,
        donor: engine(seed, DONOR),
        joiner: None,
        claims: [Vec::new(), Vec::new()],
        presence: [Vec::new(), Vec::new()],
        worker: Worker::default(),
        replacement: Joiner::default(),
    };
    let started = world.step(DONOR, BootstrapEvent::Start);
    world.apply(DONOR, 0, started);
    for now in 1..HORIZON {
        if now == schedule.join() {
            let mut joiner = engine(seed, JOINER);
            let _ = joiner.step(BootstrapEvent::Tick(Time(now)));
            world.joiner = Some(joiner);
            let started = world.step(JOINER, BootstrapEvent::Start);
            world.apply(JOINER, now, started);
        }
        let effects = world.step(DONOR, BootstrapEvent::Tick(Time(now)));
        world.apply(DONOR, now, effects);
        world.donor_turn(now);
        if world.joiner.is_some() {
            let effects = world.step(JOINER, BootstrapEvent::Tick(Time(now)));
            world.apply(JOINER, now, effects);
        }
        let replacement = &world.replacement;
        if replacement.found.is_some()
            || replacement.built.is_some()
            || replacement.declined.is_some()
        {
            break;
        }
    }
    world
}

/// A planned restart of the peer, sealed: whatever its leave, reap,
/// re-registration, first write and join failed or retired before, the
/// donor's capture for the replacement starts on its first turn after the
/// join's lapse, and the replacement finds it within one observation of the
/// capture, or of its own settle if the capture was quicker.
#[test]
fn a_replacement_is_served_on_the_first_turn_after_its_join() {
    let (mut backed_off, mut doomed_at_leave, mut readd_alive, mut written) = (0, 0, 0, 0);
    for seed in 0..160_u64 {
        let mut rng = SplitMix64::new(seed);
        let schedule = Schedule::seeded(&mut rng, false);
        let world = run(seed, schedule);
        let World {
            worker,
            replacement,
            ..
        } = &world;
        assert_eq!(
            replacement.built, None,
            "seed {seed}: the replacement scanned the origin: {schedule:?}"
        );
        assert_eq!(
            replacement.declined, None,
            "seed {seed}: the replacement fell back: {schedule:?}"
        );
        assert_eq!(worker.builds, 1, "seed {seed}: the donor scanned once");
        let (served, was_backing_off) = worker
            .served
            .unwrap_or_else(|| panic!("seed {seed}: no capture named the replacement"));
        let resumed = schedule.join() + schedule.lag + schedule.lapse_join;
        assert_eq!(
            served, resumed,
            "seed {seed}: served at {served}, Ready again at {resumed}: {schedule:?}"
        );
        let (found, _) = replacement
            .found
            .clone()
            .unwrap_or_else(|| panic!("seed {seed}: the replacement found no donor"));
        let offered =
            (served + schedule.encode + schedule.lag).max(schedule.join() + CONFIG.settle_ms);
        assert!(
            found <= offered + CONFIG.observe_ms,
            "seed {seed}: found the donor at {found}, offered at {offered}: {schedule:?}"
        );
        backed_off += u32::from(was_backing_off);
        doomed_at_leave += u32::from(worker.failed > 0);
        readd_alive += u32::from(schedule.readd_alive);
        written += u32::from(schedule.write.is_some());
    }
    assert!(
        backed_off > 60,
        "a failure's backoff held at the replacement's first turn in {backed_off} seeds"
    );
    assert!(
        doomed_at_leave > 8,
        "{doomed_at_leave} seeds failed an attempt"
    );
    assert!(readd_alive > 40 && written > 40);
}

/// The peer crashed: no seal, so the donor's lapse proof fails and its
/// candidate retires, Ready capture and claim window included. Nothing
/// starts a capture of the retired image; the donor rebuilds once, and the
/// replacement installs only what that rebuild captured.
#[test]
fn a_crashed_peer_retires_every_capture_taken_before_it() {
    for seed in 0..96_u64 {
        let mut rng = SplitMix64::new(seed);
        let schedule = Schedule::seeded(&mut rng, true);
        let world = run(seed, schedule);
        let World {
            worker,
            replacement,
            ..
        } = &world;
        assert_ne!(worker.image, Image::Original, "seed {seed}");
        assert_eq!(worker.builds, 2, "seed {seed}: the donor rebuilt once");
        assert_eq!(
            replacement.built, None,
            "seed {seed}: the replacement scanned: {schedule:?}"
        );
        let (_, selected) = replacement
            .found
            .clone()
            .unwrap_or_else(|| panic!("seed {seed}: the replacement found no donor"));
        assert!(
            !worker.published_before_crash.contains(&selected),
            "seed {seed}: the replacement installed a capture taken before the crash"
        );
    }
}
