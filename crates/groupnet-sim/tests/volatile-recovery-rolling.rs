//! A rolling update's second stop, seeded across the first rejoiner's
//! recovery after its peer install. `me` installed donor `a`'s image; `a`
//! then stops at one boundary of `me`'s peer proof or Ready life — sealed, as
//! the binary's planned stop is, or crashed — and its departure plays out at
//! production timing, seeded: its grant freezes, `me`'s lease lapses, `a` dies
//! and is reaped, the seed resolver may relearn it with no state, and its next
//! life gossips after a pod start of seeded length, which may land anywhere in
//! `me`'s lapse proof. A sealed stop never costs `me` a fallback or an origin
//! build; a crash always takes the full fallback.

use std::collections::BTreeMap;

use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, CaptureId, JournalCursor, NativeCut, ReservationId,
};
use groupnet_core::volatile_bootstrap::transfer::{NativeCoverageReceipt, NativeHandoffReceipt};
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapMemberIdentity, BootstrapOperation, BootstrapScope, ClaimIdentity,
    PresenceIdentity,
};
use groupnet_core::volatile_recovery::{
    Mark, Peer, RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryEvent, RecoveryMode,
    RecoveryOperation, RecoveryStage, Renewal,
};
use groupnet_core::{NodeId, Status, Time};
use groupnet_sim::SplitMix64;

/// s3cache's serve-lease duration.
const LEASE_MS: u64 = 2_000;
/// Its renewal cadence, a third of the lease.
const RENEW_MS: u64 = LEASE_MS / 3;
/// Groupnet's suspicion window before a silent member is declared `Dead`.
const SUSPECT_MS: u64 = 500;
/// s3cache's `dead_timeout_ms` (the lease): a `Dead` member is reaped after
/// twice that.
const REAP_MS: u64 = 2 * LEASE_MS;
/// s3cache's policy: a member non-live this long before the lapse is exempt.
const OLD_NONLIVE_MS: u64 = LEASE_MS / 4;
/// The seed resolver's refresh period: a moved seed is relearned once, at
/// some phase of it after the replacement's address appears.
const SEED_REFRESH_MS: u64 = 15_000;
/// s3cache's participation presence TTL (`claim_ttl_ms`): a stopped member's
/// presence outlives its exit by this long.
const PRESENCE_TTL_MS: u64 = 3_000;
/// A replacement pod's start, from the old life's exit to its gossip:
/// production took 30 s on 2026-10-01; seeded down to a fast start.
const POD_START_MS: (u64, u64) = (1_000, 35_000);
/// The peer image transfer.
const TRANSFER_MS: u64 = 2_000;
/// `a`'s life `1` wrote through this sequence; its seal is the next one.
const LAST_WRITE: u64 = 4;
/// Seeds per boundary and stop.
const SEEDS: u64 = 160;
/// Room, after `a`'s next life has written, for `me` to finish any recovery:
/// a lapse proof's renewal wait and settle, or a full origin rebuild.
const SETTLE_MS: u64 = 20_000;

/// Where in `me`'s recovery `a` stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Boundary {
    /// As `me` first samples the peer roster and heads after its install.
    Sampling,
    /// As `me` waits for its frontier to reach the sampled heads.
    Frontiers,
    /// As `me` rechecks the roster and heads after that barrier.
    Recheck,
    /// As `me` asks its lease to affirm the peer image.
    Affirm,
    /// As `me` is Ready and adopts the image to donate it.
    Ready,
    /// During `me`'s Ready recapture, a seeded moment after it is Ready.
    Recapture,
}

const BOUNDARIES: [Boundary; 6] = [
    Boundary::Sampling,
    Boundary::Frontiers,
    Boundary::Recheck,
    Boundary::Affirm,
    Boundary::Ready,
    Boundary::Recapture,
];

fn within(rng: &mut SplitMix64, (low, high): (u64, u64)) -> u64 {
    low + u64::from(rng.below(u32::try_from(high - low + 1).unwrap()))
}

/// `a`'s departure, every instant drawn from its stop.
#[derive(Debug)]
struct Departure {
    sealed: bool,
    stop: u64,
    /// `me` applied `a`'s seal (a planned stop only).
    seal_delivered: u64,
    exit: u64,
    /// `a`'s old record turns non-live, is declared `Dead`, and is reaped.
    nonlive: u64,
    reaped: u64,
    /// The seed resolver relearns `a`, with no state, after the reap.
    relearned: Option<(u64, u64, u64)>,
    /// `a`'s next life gossips, `me` crosses into it (renewal or gap), it
    /// grants `me`'s lease, and it writes `(2, 1)`.
    next_life: u64,
    crossed: u64,
    grants: u64,
    next_write: u64,
    /// `me`'s serve-lease lapses: `a` stopped granting it.
    lapse: u64,
}

impl Departure {
    fn draw(rng: &mut SplitMix64, stop: u64, sealed: bool) -> Self {
        let seal_delivered = stop + within(rng, (1, 50));
        let exit = seal_delivered + within(rng, (1, 100));
        let nonlive = exit + within(rng, (100, 400));
        let reaped = nonlive + SUSPECT_MS + REAP_MS + within(rng, (0, 200));
        let next_life = exit + within(rng, POD_START_MS);
        let moved = exit + within(rng, (1_000, 2_000)) + within(rng, (0, SEED_REFRESH_MS));
        let relearned = (rng.below(4) != 0 && moved > reaped && moved < next_life).then(|| {
            let nonlive = moved + within(rng, (100, 400));
            (moved, nonlive, nonlive + SUSPECT_MS + REAP_MS)
        });
        let crossed = next_life + within(rng, (1, 50));
        Self {
            sealed,
            stop,
            seal_delivered,
            exit,
            nonlive,
            reaped,
            relearned,
            next_life,
            crossed,
            grants: next_life + within(rng, (100, 700)),
            next_write: next_life + within(rng, (0, 3_000)),
            lapse: stop + within(rng, (LEASE_MS - RENEW_MS, LEASE_MS)),
        }
    }

    /// The renewal of `me`'s lease published by `at`.
    fn renewals(at: u64) -> u64 {
        1 + at / RENEW_MS
    }

    /// `me`'s roster-wide confirmation: frozen while a granter that confirms
    /// nothing new is in the roster — `a`'s old record after its stop, or its
    /// next life before it grants.
    fn confirmed(&self, at: u64) -> u64 {
        let old_until = self.reaped.min(self.next_life);
        let frozen_from = if at >= self.stop && at < old_until {
            Some(self.stop)
        } else if at >= self.next_life && at < self.grants {
            Some(if old_until >= self.next_life {
                self.stop
            } else {
                self.next_life
            })
        } else {
            None
        };
        Self::renewals(frozen_from.unwrap_or(at))
    }

    fn sealed_mark(&self, at: u64) -> Option<Mark> {
        (self.sealed && at >= self.seal_delivered && at < self.crossed).then_some(Mark {
            epoch: 1,
            sequence: LAST_WRITE + 1,
        })
    }

    fn renewal(&self, at: u64) -> Option<Renewal> {
        (self.sealed && at >= self.crossed).then_some(Renewal {
            sealed: Mark {
                epoch: 1,
                sequence: LAST_WRITE + 1,
            },
            epoch: 2,
        })
    }

    /// `a` as `me` observes it at `at`, if its roster still holds it.
    fn peer(&self, at: u64) -> Option<Peer> {
        let crossing = |peer: Peer| Peer {
            renewal: self.renewal(at),
            sealed: self.sealed_mark(at),
            ..peer
        };
        if at >= self.next_life {
            return Some(crossing(Peer {
                grant: (at >= self.grants).then(|| mark(Self::renewals(at))),
                head: (at >= self.next_write).then_some(Mark {
                    epoch: 2,
                    sequence: 1,
                }),
                ..live()
            }));
        }
        if at < self.reaped {
            let alive = at < self.nonlive;
            return Some(crossing(Peer {
                alive,
                old_nonlive: !alive && at - self.nonlive >= OLD_NONLIVE_MS,
                grant: Some(mark(Self::renewals(self.stop))),
                ..live()
            }));
        }
        let (since, nonlive, reaped) = self.relearned?;
        (at >= since && at < reaped).then(|| {
            let alive = at < nonlive;
            crossing(Peer {
                alive,
                grants_lease: false,
                old_nonlive: !alive && at - nonlive >= OLD_NONLIVE_MS,
                grant: None,
                head: None,
                ..live()
            })
        })
    }

    /// When the run has nothing left to show: `a`'s next life has written and
    /// `me` has had [`SETTLE_MS`] to recover around it.
    fn settled(&self) -> u64 {
        self.next_write.max(self.crossed) + SETTLE_MS
    }

    /// When `me` has applied `head` of `a`, if it ever will.
    fn delivered(&self, head: Mark) -> Option<u64> {
        if head.epoch == 1 {
            Some(0)
        } else {
            self.sealed.then(|| self.next_write.max(self.crossed) + 1)
        }
    }
}

fn mark(sequence: u64) -> Mark {
    Mark { epoch: 1, sequence }
}

/// `a` live and granting, before its stop, at its last write.
fn live() -> Peer {
    Peer {
        node: NodeId::from("a"),
        alive: true,
        grants_lease: true,
        old_nonlive: false,
        grant: None,
        head: Some(mark(LAST_WRITE)),
        renewal: None,
        sealed: None,
    }
}

fn claim(node: &str, boot: u128, session: u64) -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from(node),
        incarnation: BootId(boot),
        session,
        attempt: 1,
    }
}

fn member(claim: &ClaimIdentity) -> BootstrapMemberIdentity {
    BootstrapMemberIdentity {
        node: claim.node.clone(),
        presence: Some(PresenceIdentity {
            node: claim.node.clone(),
            boot: claim.incarnation,
            session: claim.session,
        }),
        member_incarnation: 1,
        status: Status::Alive,
    }
}

fn donor() -> ClaimIdentity {
    claim("a", 8, 5)
}

fn follower() -> ClaimIdentity {
    claim("me", 7, 9)
}

fn members() -> Vec<BootstrapMemberIdentity> {
    vec![member(&donor()), member(&follower())]
}

fn handoff(recovery: RecoveryOperation) -> NativeHandoffReceipt {
    let capture = CaptureId {
        scope: BootstrapScope {
            domain: "o".to_owned(),
            partition: "b".to_owned(),
        },
        donor: donor(),
        recovery_generation: 1,
        serial: 1,
    };
    let reservation = ReservationId {
        capture: capture.clone(),
        follower: follower(),
        serial: 1,
    };
    let cut = NativeCut {
        writer: b"a".to_vec(),
        epoch: 1,
        sequence: LAST_WRITE,
    };
    let barrier = BarrierReceipt {
        reservation: reservation.clone(),
        attach_operation: 1,
        barrier_operation: 2,
        cursor: JournalCursor {
            capture,
            position: 0,
        },
        covered_cuts: vec![cut.clone()],
        members: members(),
    };
    let parent = BootstrapOperation {
        session: 9,
        incarnation: BootId(7),
        generation: 1,
        token: 1,
    };
    NativeHandoffReceipt {
        recovery,
        install: BootstrapOperation { token: 2, ..parent },
        coverage: NativeCoverageReceipt {
            parent,
            staged_through: barrier.cursor.clone(),
            proven_cuts: vec![cut.clone()],
            members: members(),
            buffered_bytes: 0,
            barrier,
        },
        attachment: AttachToken {
            reservation,
            operation: 1,
        },
        schema: 1,
        applier_generation: 1,
        continued_cuts: vec![cut],
        buffered_bytes: 0,
    }
}

/// s3cache's production recovery configuration.
fn config() -> RecoveryConfig {
    RecoveryConfig {
        max_members: 256,
        max_member_bytes: 256,
        max_barrier_rounds: 4,
        total_ms: 600_000,
        attempt_ms: 60_000,
        settle_ms: LEASE_MS,
        poll_ms: 100,
    }
}

/// Something that happens to `me` at an instant.
#[derive(Debug)]
enum Happening {
    /// An adapter answer to the engine.
    Answer(RecoveryEvent),
    /// A roster observation, answered with the roster as it stands then.
    Observe {
        op: RecoveryOperation,
        peer_heads: bool,
    },
    /// The engine's own timer.
    Timer,
    /// `a` stops: the seeded moment in `me`'s Ready recapture.
    Stop,
    /// `me`'s serve-lease lapsed.
    Lapse,
    /// `me`'s feed gapped from `a`'s next life: the crash's unknown tail.
    Gap,
}

/// What one run cost `me`, and what its lapse proof saw of `a`.
#[derive(Debug, Default)]
struct Run {
    fallbacks: usize,
    origin_builds: usize,
    acquisitions: usize,
    distrusted_after_stop: bool,
    seen: Seen,
}

/// The departure shapes a lapse proof observed.
#[derive(Debug, Default)]
struct Seen {
    /// `a` relearned with no state, behind a delivered seal.
    relearned: bool,
    /// `a` gone from the roster.
    vanished: bool,
    /// `a`'s next life.
    next_life: bool,
}

/// `me`'s view of the world and its pending happenings.
struct World {
    rng: SplitMix64,
    engine: RecoveryEngine,
    boundary: Boundary,
    sealed: bool,
    now: u64,
    sequence: u64,
    pending: BTreeMap<(u64, u64), Happening>,
    departure: Option<Departure>,
    peer_samples: usize,
    run: Run,
}

impl World {
    fn new(seed: u64, boundary: Boundary, sealed: bool) -> Self {
        let engine = RecoveryEngine::new(config(), RecoveryMode::Leased, NodeId::from("me"), seed)
            .unwrap()
            .with_bootstrap()
            .unwrap();
        Self {
            rng: SplitMix64::new(seed),
            engine,
            boundary,
            sealed,
            now: 0,
            sequence: 0,
            pending: BTreeMap::new(),
            departure: None,
            peer_samples: 0,
            run: Run::default(),
        }
    }

    fn at(&mut self, at: u64, happening: Happening) {
        self.sequence += 1;
        self.pending.insert((at, self.sequence), happening);
    }

    fn after(&mut self, range: (u64, u64), happening: Happening) {
        let at = self.now + within(&mut self.rng, range);
        self.at(at, happening);
    }

    fn stop(&mut self) {
        if self.departure.is_some() {
            return;
        }
        let departure = Departure::draw(&mut self.rng, self.now, self.sealed);
        self.at(departure.lapse, Happening::Lapse);
        if !departure.sealed {
            self.at(departure.crossed, Happening::Gap);
        }
        self.departure = Some(departure);
    }

    fn peers(&self) -> Vec<Peer> {
        match &self.departure {
            None => vec![Peer {
                grant: Some(mark(Departure::renewals(self.now))),
                ..live()
            }],
            Some(departure) => departure.peer(self.now).into_iter().collect(),
        }
    }

    fn confirmed(&self) -> Mark {
        mark(
            self.departure
                .as_ref()
                .map_or(Departure::renewals(self.now), |departure| {
                    departure.confirmed(self.now)
                }),
        )
    }

    /// The roster's participation identities: `a`'s presence outlives its
    /// exit by its TTL, and a next life is another boot.
    fn identities(&self) -> Vec<BootstrapMemberIdentity> {
        let present = self.departure.as_ref().is_none_or(|departure| {
            self.now < departure.exit + PRESENCE_TTL_MS || self.now >= departure.next_life
        });
        let mut identities = members();
        if !present {
            identities[0].presence = None;
            identities[0].status = Status::Dead;
        } else if self
            .departure
            .as_ref()
            .is_some_and(|departure| self.now >= departure.next_life)
        {
            identities[0] = member(&claim("a", 10, 5));
        }
        identities
    }

    fn lapse_stage(&self) -> bool {
        matches!(
            self.engine.state().stage,
            RecoveryStage::SamplingInitial
                | RecoveryStage::WaitingRenewals
                | RecoveryStage::SamplingHeads
                | RecoveryStage::RecheckingHeads
        )
    }

    fn effect(&mut self, effect: RecoveryEffect) {
        match effect {
            RecoveryEffect::Invalidate {
                op,
                distrust_bodies,
            } => {
                self.run.distrusted_after_stop |= distrust_bodies && self.departure.is_some();
                self.after(
                    (1, 10),
                    Happening::Answer(RecoveryEvent::Invalidated { op }),
                );
            }
            RecoveryEffect::AcquireBaseline { op } => {
                self.run.acquisitions += 1;
                let answer = if self.departure.is_none() {
                    RecoveryEvent::PeerBaselineInstalled {
                        op,
                        handoff: Box::new(handoff(op)),
                    }
                } else {
                    // No donor is left: the follower builds from origin.
                    RecoveryEvent::BootstrapDeclined { op }
                };
                self.after((TRANSFER_MS, TRANSFER_MS + 500), Happening::Answer(answer));
            }
            RecoveryEffect::RebuildOrigin { op } => {
                self.run.origin_builds += 1;
                self.after(
                    (1_000, 2_000),
                    Happening::Answer(RecoveryEvent::Materialized { op }),
                );
            }
            RecoveryEffect::ObservePeerHeads { op } => {
                self.peer_samples += 1;
                let boundary = match self.peer_samples {
                    1 => Boundary::Sampling,
                    _ => Boundary::Recheck,
                };
                if self.boundary == boundary {
                    self.stop();
                }
                self.after(
                    (1, 5),
                    Happening::Observe {
                        op,
                        peer_heads: true,
                    },
                );
            }
            RecoveryEffect::ObservePeers { op } => {
                self.after(
                    (1, 5),
                    Happening::Observe {
                        op,
                        peer_heads: false,
                    },
                );
            }
            RecoveryEffect::WaitFrontiers { op, heads } => {
                if self.boundary == Boundary::Frontiers
                    && self.engine.state().stage == RecoveryStage::PeerWaitingFrontiers
                {
                    self.stop();
                }
                let reached = heads.iter().try_fold(self.now, |latest, (_, head)| {
                    let delivered = self
                        .departure
                        .as_ref()
                        .map_or(Some(0), |departure| departure.delivered(*head))?;
                    Some(latest.max(delivered))
                });
                if let Some(reached) = reached {
                    let at = reached + within(&mut self.rng, (1, 20));
                    self.at(
                        at,
                        Happening::Answer(RecoveryEvent::FrontiersReached { op }),
                    );
                }
            }
            RecoveryEffect::Affirm { op } => {
                if self.boundary == Boundary::Affirm {
                    self.stop();
                }
                self.after(
                    (1, 5),
                    Happening::Answer(RecoveryEvent::Affirmed { op, accepted: true }),
                );
            }
            RecoveryEffect::AdoptLocalBaseline { .. } => match self.boundary {
                Boundary::Ready => self.stop(),
                Boundary::Recapture => self.after((100, 800), Happening::Stop),
                _ => {}
            },
            RecoveryEffect::FellBack { .. } => self.run.fallbacks += 1,
            RecoveryEffect::ArmTimer(due) => self.at(due.0, Happening::Timer),
            RecoveryEffect::CloseGate { .. }
            | RecoveryEffect::CancelBaseline { .. }
            | RecoveryEffect::SuspendLocalBaseline { .. }
            | RecoveryEffect::ResumeLocalBaseline { .. } => {}
        }
    }

    fn step(&mut self, event: RecoveryEvent) {
        let step = self.engine.step(event);
        for effect in step.effects {
            self.effect(effect);
        }
    }

    /// Takes the observation `op` asked for from the world as it stands now.
    fn observe(&mut self, op: RecoveryOperation, peer_heads: bool) {
        let peers = self.peers();
        let event = if peer_heads {
            RecoveryEvent::PeerHeadsObserved {
                op,
                peers,
                identities: self.identities(),
            }
        } else {
            if self.lapse_stage() {
                self.note_lapse_observation(&peers);
            }
            RecoveryEvent::PeersObserved {
                op,
                peers,
                confirmed: Some(self.confirmed()),
            }
        };
        self.answer(event);
    }

    fn answer(&mut self, event: RecoveryEvent) {
        let op = match &event {
            RecoveryEvent::Invalidated { op }
            | RecoveryEvent::Materialized { op }
            | RecoveryEvent::BootstrapDeclined { op }
            | RecoveryEvent::PeerBaselineInstalled { op, .. }
            | RecoveryEvent::PeersObserved { op, .. }
            | RecoveryEvent::PeerHeadsObserved { op, .. }
            | RecoveryEvent::FrontiersReached { op }
            | RecoveryEvent::Affirmed { op, .. } => *op,
            other => panic!("not an adapter answer: {other:?}"),
        };
        // The shell drops an answer its engine has since fenced.
        if self.engine.accepts_operation(op) {
            self.step(event);
        }
    }

    fn note_lapse_observation(&mut self, peers: &[Peer]) {
        match peers.first() {
            None => self.run.seen.vanished = true,
            Some(peer)
                if peer.renewal.is_some() || peer.head.is_some_and(|head| head.epoch == 2) =>
            {
                self.run.seen.next_life = true;
            }
            Some(peer) if peer.head.is_none() && peer.sealed.is_some() => {
                self.run.seen.relearned = true;
            }
            Some(_) => {}
        }
    }

    /// Runs `me` from its cold start until `a`'s next life has written and
    /// settled.
    fn run(mut self) -> (Run, RecoveryEngine) {
        self.step(RecoveryEvent::Start);
        while let Some(((at, _), happening)) = self.pending.pop_first() {
            if self
                .departure
                .as_ref()
                .is_some_and(|departure| at > departure.settled())
            {
                break;
            }
            self.now = at;
            self.step(RecoveryEvent::Tick(Time(at)));
            match happening {
                Happening::Answer(event) => self.answer(event),
                Happening::Observe { op, peer_heads } => self.observe(op, peer_heads),
                Happening::Timer => {}
                Happening::Stop => self.stop(),
                Happening::Lapse => self.step(RecoveryEvent::LeaseLapse { count: 1 }),
                Happening::Gap => self.step(RecoveryEvent::FeedGap { lapses: 1 }),
            }
        }
        assert!(
            self.departure.is_some(),
            "{:?}: `a` never stopped",
            self.boundary
        );
        (self.run, self.engine)
    }
}

#[test]
fn a_sealed_second_stop_anywhere_in_the_rejoiners_recovery_costs_no_fallback() {
    let mut relearned = 0;
    let mut vanished = 0;
    let mut next_life = 0;
    for boundary in BOUNDARIES {
        for seed in 1..=SEEDS {
            let (run, engine) = World::new(seed, boundary, true).run();
            let context = format!("{boundary:?} seed {seed}: {run:?}");
            assert_eq!(run.fallbacks, 0, "{context}");
            assert_eq!(run.origin_builds, 0, "{context}");
            assert_eq!(run.acquisitions, 1, "{context}");
            assert!(!run.distrusted_after_stop, "{context}");
            assert_eq!(engine.state().stage, RecoveryStage::Ready, "{context}");
            assert!(engine.state().recovered, "{context}");
            relearned += usize::from(run.seen.relearned);
            vanished += usize::from(run.seen.vanished);
            next_life += usize::from(run.seen.next_life);
        }
    }
    // The schedule earns every departure shape inside the lapse proof: the
    // reap in most runs, the next life in about one in seven, and the seed
    // relearn in about one in ten.
    let runs = BOUNDARIES.len() * usize::try_from(SEEDS).unwrap();
    assert!(vanished >= runs / 2, "{vanished} of {runs} saw `a` reaped");
    assert!(
        next_life >= runs / 10,
        "{next_life} of {runs} saw `a`'s next life"
    );
    assert!(
        relearned >= runs / 20,
        "{relearned} of {runs} saw `a` relearned empty"
    );
}

#[test]
fn a_crashed_second_stop_anywhere_in_the_rejoiners_recovery_takes_the_full_fallback() {
    for boundary in BOUNDARIES {
        for seed in 1..=SEEDS {
            let (run, engine) = World::new(seed, boundary, false).run();
            let context = format!("{boundary:?} seed {seed}: {run:?}");
            assert!(run.distrusted_after_stop, "{context}");
            assert!(run.origin_builds >= 1, "{context}");
            assert_eq!(engine.state().stage, RecoveryStage::Ready, "{context}");
            assert!(engine.state().recovered, "{context}");
        }
    }
}
