//! Queued virtual-time faults for a durable named event subscriber.

use std::collections::HashMap;
use std::num::NonZeroU64;

use groupnet_core::Time;
use groupnet_core::replication::{
    Batch, BoundComparison, CommitSubscriberAck, Comparison, Config, Coverage,
    DurableDeliveryReceipt, Effect, Event, FencedCheckpoint, Mode, Operation, ProofId,
    RegisterReceipt, RegisterSubscriber, ResumeSubscriber, RetentionPolicy, Scope, SessionEngine,
    SourceHistory, SourceProof, SourceSubscriberState, Stage, Step, Stream, SubscriberAckReceipt,
    SubscriberId, SubscriberKey, SubscriptionEpoch, SubscriptionError, SubscriptionLimits,
};
use groupnet_sim::SplitMix64;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "events".into(),
            kind: "v1".into(),
        },
        partition: "p".into(),
    }
}

fn key() -> SubscriberKey {
    SubscriberKey {
        scope: scope(),
        subscriber: SubscriberId {
            name: "billing".into(),
        },
    }
}

fn cursor(at: u8) -> groupnet_core::replication::Cursor {
    groupnet_core::replication::Cursor {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        position: vec![at],
    }
}

fn at(cursor: &groupnet_core::replication::Cursor) -> u8 {
    cursor.position[0]
}

fn proof(retained: u8) -> SourceProof {
    SourceProof {
        id: ProofId(vec![9]),
        head: cursor(2),
        retained_from: cursor(retained),
        read_authority: false,
    }
}

fn comparison(a: u8, b: u8) -> BoundComparison {
    BoundComparison {
        left: cursor(a),
        right: cursor(b),
        proof: ProofId(vec![9]),
        order: match a.cmp(&b) {
            std::cmp::Ordering::Less => Comparison::Before,
            std::cmp::Ordering::Equal => Comparison::Equal,
            std::cmp::Ordering::Greater => Comparison::After,
        },
    }
}

fn policy() -> RetentionPolicy {
    RetentionPolicy {
        fingerprint: vec![7],
        max_bytes: 1024,
        max_events: 100,
        max_age_ms: 1000,
        max_lag_events: 100,
    }
}

#[derive(Clone, Debug)]
enum Action {
    Effect(Box<Effect>),
    Reply { event: Box<Event>, sequence: u64 },
    Timer(Time),
    Restart,
    LateOldApply,
}

#[derive(Clone, Copy, Debug, Default)]
struct FaultCounts {
    partition_drops: u64,
    random_request_drops: u64,
    lost_replies: u64,
    duplicated_replies: u64,
    reordered_replies: u64,
    crashes: u64,
    fenced_old_applies: u64,
    rejected_old_acks: u64,
}

impl FaultCounts {
    fn add(&mut self, other: Self) {
        self.partition_drops += other.partition_drops;
        self.random_request_drops += other.random_request_drops;
        self.lost_replies += other.lost_replies;
        self.duplicated_replies += other.duplicated_replies;
        self.reordered_replies += other.reordered_replies;
        self.crashes += other.crashes;
        self.fenced_old_applies += other.fenced_old_applies;
        self.rejected_old_acks += other.rejected_old_acks;
    }
}

#[derive(Clone, Debug)]
struct Scheduled {
    when: u64,
    action: Action,
}

#[derive(Debug)]
struct Model {
    rng: SplitMix64,
    now: u64,
    session_id: u64,
    config: Config,
    engine: SessionEngine,
    queued: Vec<Scheduled>,
    registration: Option<RegisterReceipt>,
    registrations: HashMap<Vec<u8>, RegisterReceipt>,
    source_ack: u8,
    retained: u8,
    ordinal: u64,
    sink_epoch: u64,
    sink_cursor: u8,
    durable_effects: u8,
    ack_requests: HashMap<Vec<u8>, SubscriberAckReceipt>,
    restarted: bool,
    old_ack: Option<(Operation, CommitSubscriberAck)>,
    old_ack_sequence: Option<u64>,
    old_reply_rejected: bool,
    old_apply_fenced: bool,
    next_reply_sequence: u64,
    highest_delivered_reply: u64,
    faults: FaultCounts,
}

impl Model {
    fn new(seed: u64) -> Self {
        let config = Config {
            tail_check_ms: 4,
            retry_ms: 2,
            max_retries: 100,
            attempt_timeout_ms: 12,
            ..Config::default()
        };
        let engine = SessionEngine::new(scope(), Mode::EventComplete, config, seed + 1)
            .expect("bounded session");
        let mut model = Self {
            rng: SplitMix64::new(seed),
            now: 0,
            session_id: seed + 1,
            config,
            engine,
            queued: Vec::new(),
            registration: None,
            registrations: HashMap::new(),
            source_ack: 1,
            retained: 0,
            ordinal: 0,
            sink_epoch: 0,
            sink_cursor: 1,
            durable_effects: 0,
            ack_requests: HashMap::new(),
            restarted: false,
            old_ack: None,
            old_ack_sequence: None,
            old_reply_rejected: false,
            old_apply_fenced: false,
            next_reply_sequence: 1,
            highest_delivered_reply: 0,
            faults: FaultCounts::default(),
        };
        let session = NonZeroU64::new(seed + 1).expect("nonzero");
        model.step(Event::StartSubscription {
            request: Box::new(RegisterSubscriber {
                key: key(),
                incarnation: session,
                start: cursor(1),
                policy: policy(),
                request_id: session.get().to_le_bytes().to_vec(),
                expected_prior_ordinal: None,
                reset_from: None,
            }),
            limits: SubscriptionLimits::default(),
        });
        model
    }

    fn later(&mut self, action: Action, delay: u64) {
        self.queued.push(Scheduled {
            when: self.now + delay,
            action,
        });
    }

    fn step(&mut self, event: Event) -> bool {
        let result = self.engine.step(event);
        let rejection = result.rejection;
        self.enqueue(result);
        assert!(
            self.retained <= self.source_ack,
            "retention passed durable source ack"
        );
        assert!(
            self.durable_effects <= 1,
            "sink duplicated a durable effect"
        );
        rejection == Some(groupnet_core::replication::Reject::StaleOperation)
    }

    fn enqueue(&mut self, step: Step) {
        for effect in step.effects {
            match effect {
                Effect::ArmTimer(due) => {
                    self.queued.push(Scheduled {
                        when: due.0.max(self.now),
                        action: Action::Timer(due),
                    });
                }
                Effect::CommitSubscriberAck { op, ref request } if !self.restarted => {
                    // The sink already committed its effect and cursor, but
                    // the source ack has not run. A delayed old reply can
                    // still arrive after the new process binds a higher epoch.
                    self.old_ack = Some((op, (**request).clone()));
                    self.old_ack_sequence = Some(self.next_reply_sequence);
                    self.next_reply_sequence += 1;
                    self.later(Action::Restart, 0);
                    self.restarted = true;
                }
                Effect::IrrecoverableGap => panic!("protected history cannot disappear"),
                effect => {
                    let delay = self.rng.next_u64() % 4;
                    self.later(Action::Effect(Box::new(effect)), delay);
                }
            }
        }
    }

    fn reply(&mut self, event: Event) {
        let faulted = (15..35).contains(&self.now);
        if faulted && self.rng.next_u64() % 7 == 0 {
            self.faults.lost_replies += 1;
            return; // Lost notification/response; core timeout must recover.
        }
        let delay = if faulted {
            self.rng.next_u64() % 19
        } else {
            self.rng.next_u64() % 3
        };
        let sequence = self.next_reply_sequence;
        self.next_reply_sequence += 1;
        if faulted && self.rng.next_u64() % 5 == 0 {
            self.faults.duplicated_replies += 1;
            self.later(
                Action::Reply {
                    event: Box::new(event.clone()),
                    sequence,
                },
                delay + 16,
            );
        }
        self.later(
            Action::Reply {
                event: Box::new(event),
                sequence,
            },
            delay,
        );
    }

    fn register(&mut self, op: groupnet_core::replication::Operation, request: RegisterSubscriber) {
        if let Some(receipt) = self.registrations.get(&request.request_id) {
            self.reply(Event::SubscriberRegistered {
                op,
                receipt: Box::new(receipt.clone()),
            });
            return;
        }
        let accepted = match (&self.registration, request.expected_prior_ordinal) {
            (None, None) if self.ordinal == 0 => true,
            (Some(prior), Some(expected)) => {
                prior.epoch.ordinal == expected && at(&request.start) == self.source_ack
            }
            _ => false,
        };
        if !accepted {
            self.reply(Event::SubscriberRejected {
                op,
                error: SubscriptionError::ConditionalMismatch,
            });
            return;
        }
        self.ordinal += 1;
        let receipt = RegisterReceipt {
            key: request.key,
            incarnation: request.incarnation,
            request_id: request.request_id.clone(),
            epoch: SubscriptionEpoch {
                ordinal: NonZeroU64::new(self.ordinal).expect("monotonic"),
                native: self.ordinal.to_le_bytes().to_vec(),
                history: cursor(1).history,
            },
            protected: request.start,
            proof: proof(self.retained),
            retained_to_protected: comparison(self.retained, self.source_ack),
            protected_to_head: comparison(self.source_ack, 2),
            policy_fingerprint: request.policy.fingerprint,
        };
        self.registration = Some(receipt.clone());
        self.registrations
            .insert(request.request_id, receipt.clone());
        self.reply(Event::SubscriberRegistered {
            op,
            receipt: Box::new(receipt),
        });
    }

    fn sink_apply(&mut self, ordinal: u64, through: u8) -> bool {
        if ordinal != self.sink_epoch {
            return false;
        }
        if self.sink_cursor < through {
            self.sink_cursor = through;
            self.durable_effects += 1;
        }
        true
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one virtual source/sink effect dispatcher keeps each fault schedule tied to the same durable ledgers"
    )]
    fn execute(&mut self, effect: Effect) {
        if self.now < 15 {
            self.faults.partition_drops += 1;
            return; // Every source/sink request is unreachable in this interval.
        }
        if self.now < 35 && self.rng.next_u64() % 8 == 0 {
            self.faults.random_request_drops += 1;
            return; // Loss continues intermittently after the partition heals.
        }
        match effect {
            Effect::ReadCurrentSubscriber { op, .. } => {
                let state = self.registration.as_ref().map(|receipt| {
                    let verified = proof(self.retained);
                    SourceSubscriberState {
                        key: receipt.key.clone(),
                        epoch: receipt.epoch.clone(),
                        acknowledged: cursor(self.source_ack),
                        policy_fingerprint: receipt.policy_fingerprint.clone(),
                        retained_to_ack: comparison(self.retained, self.source_ack),
                        ack_to_head: comparison(self.source_ack, 2),
                        proof: verified,
                    }
                });
                self.reply(Event::CurrentSubscriberRead {
                    op,
                    state: state.map(Box::new),
                });
            }
            Effect::RegisterSubscriber { op, request } => self.register(op, *request),
            Effect::ReadSubscriberRegistration { op, request_id, .. } => {
                self.reply(Event::SubscriberRegistrationRead {
                    op,
                    receipt: self.registrations.get(&request_id).cloned().map(Box::new),
                });
            }
            Effect::BindSinkEpoch { op, registration } => {
                if registration.epoch.ordinal.get() < self.sink_epoch {
                    self.reply(Event::SubscriberRejected {
                        op,
                        error: SubscriptionError::FenceLost,
                    });
                    return;
                }
                self.sink_epoch = registration.epoch.ordinal.get();
                let checkpoint = FencedCheckpoint {
                    key: registration.key.clone(),
                    request_id: registration.request_id.clone(),
                    epoch: registration.epoch.clone(),
                    cursor: cursor(self.sink_cursor),
                    durable: true,
                };
                self.reply(Event::SinkEpochBound {
                    op,
                    checkpoint: Box::new(checkpoint),
                    source_to_sink: Box::new(comparison(
                        at(&registration.protected),
                        self.sink_cursor,
                    )),
                    sink_to_head: Box::new(comparison(self.sink_cursor, 2)),
                });
            }
            Effect::CheckSubscriberTail {
                op,
                registration,
                from,
            } => {
                if self.registration.as_ref().is_none_or(|current| {
                    current.epoch != registration.epoch || self.source_ack != at(&from)
                }) {
                    self.reply(Event::SubscriberRejected {
                        op,
                        error: SubscriptionError::ConditionalMismatch,
                    });
                    return;
                }
                self.reply(Event::SubscriberTail {
                    op,
                    proof: proof(self.retained),
                    comparisons: vec![
                        comparison(self.retained, at(&from)),
                        comparison(at(&from), 2),
                        comparison(self.retained, 2),
                    ],
                });
            }
            Effect::ScanSubscriber { op, from, .. } => {
                if at(&from) != self.source_ack || self.source_ack >= 2 {
                    self.reply(Event::Failed { op });
                    return;
                }
                self.reply(Event::SubscriberScanned {
                    op,
                    batch: Box::new(Batch {
                        coverage: Coverage {
                            from,
                            through: cursor(2),
                            proof: ProofId(vec![9]),
                            certificate: vec![8],
                        },
                        payload_id: op.token,
                        events: 1,
                        bytes: 8,
                        advance: comparison(1, 2),
                        end_to_head: comparison(2, 2),
                    }),
                });
            }
            Effect::ApplySubscriberBatch {
                op,
                registration,
                batch,
                previous_sink,
            } => {
                if !self.sink_apply(
                    registration.epoch.ordinal.get(),
                    at(&batch.coverage.through),
                ) {
                    self.reply(Event::SubscriberRejected {
                        op,
                        error: SubscriptionError::FenceLost,
                    });
                    return;
                }
                let request_id = [op.session.to_le_bytes(), op.token.to_le_bytes()].concat();
                self.reply(Event::SubscriberApplied {
                    op,
                    receipt: Box::new(DurableDeliveryReceipt {
                        operation: op,
                        key: registration.key,
                        epoch: registration.epoch,
                        previous_sink: previous_sink.clone(),
                        through: batch.coverage.through,
                        sink_cursor: cursor(self.sink_cursor),
                        ack_request_id: request_id,
                        durable: true,
                    }),
                    through_to_sink: Box::new(comparison(2, self.sink_cursor)),
                    previous_to_sink: Box::new(comparison(at(&previous_sink), self.sink_cursor)),
                });
            }
            Effect::CommitSubscriberAck { op, request } => {
                if let Some(receipt) = self.ack_requests.get(&request.request_id).cloned() {
                    self.reply(Event::SubscriberAcked {
                        op,
                        receipt: Box::new(receipt),
                    });
                    return;
                }
                if self.registration.as_ref().is_none_or(|registration| {
                    registration.epoch != request.epoch || self.source_ack != at(&request.previous)
                }) {
                    self.reply(Event::SubscriberRejected {
                        op,
                        error: SubscriptionError::ConditionalMismatch,
                    });
                    return;
                }
                self.source_ack = at(&request.through);
                self.retained = self.source_ack;
                let receipt = SubscriberAckReceipt {
                    request: *request,
                    durable: true,
                };
                self.ack_requests
                    .insert(receipt.request.request_id.clone(), receipt.clone());
                self.reply(Event::SubscriberAcked {
                    op,
                    receipt: Box::new(receipt),
                });
            }
            Effect::ReadSubscriberAck { op, request } => {
                self.reply(Event::SubscriberAckRead {
                    op,
                    receipt: self
                        .ack_requests
                        .get(&request.request_id)
                        .cloned()
                        .map(Box::new),
                });
            }
            other => panic!("unexpected named effect {other:?}"),
        }
    }

    fn restart(&mut self) {
        assert_eq!(self.source_ack, 1, "crash must precede source ack");
        assert_eq!(self.sink_cursor, 2, "sink effect persisted before crash");
        assert_eq!(self.durable_effects, 1);
        let (old_op, old) = self.old_ack.clone().expect("old ack was issued");
        self.faults.crashes += 1;
        let next_session = self.session_id + 10_000;
        self.session_id = next_session;
        self.engine = SessionEngine::new(scope(), Mode::EventComplete, self.config, next_session)
            .expect("new process incarnation");
        self.step(Event::Tick(Time(self.now)));
        self.step(Event::ResumeSubscription {
            request: Box::new(ResumeSubscriber {
                key: key(),
                incarnation: NonZeroU64::new(next_session).expect("nonzero"),
                policy: policy(),
                request_id: next_session.to_le_bytes().to_vec(),
            }),
            limits: SubscriptionLimits::default(),
        });
        self.later(
            Action::Reply {
                event: Box::new(Event::SubscriberAcked {
                    op: old_op,
                    receipt: Box::new(SubscriberAckReceipt {
                        request: old,
                        durable: true,
                    }),
                }),
                sequence: self.old_ack_sequence.expect("old response order"),
            },
            20,
        );
        self.later(Action::LateOldApply, 1);
    }

    fn run(mut self) -> FaultCounts {
        for _ in 0..20_000 {
            assert!(self.queued.len() < 2_000, "unbounded virtual queue");
            let next_time = self
                .queued
                .iter()
                .map(|item| item.when)
                .min()
                .expect("work");
            let choices: Vec<_> = self
                .queued
                .iter()
                .enumerate()
                .filter_map(|(index, item)| (item.when == next_time).then_some(index))
                .collect();
            let choice = choices[usize::try_from(
                self.rng.next_u64() % u64::try_from(choices.len()).expect("bounded queue"),
            )
            .expect("index fits")];
            let item = self.queued.swap_remove(choice);
            self.now = item.when.max(self.now);
            self.step(Event::Tick(Time(self.now)));
            match item.action {
                Action::Effect(effect) => self.execute(*effect),
                Action::Reply { event, sequence } => {
                    if sequence < self.highest_delivered_reply {
                        self.faults.reordered_replies += 1;
                    }
                    self.highest_delivered_reply = self.highest_delivered_reply.max(sequence);
                    let is_old_ack = matches!(
                        event.as_ref(),
                        Event::SubscriberAcked { op, .. }
                            if self.old_ack.as_ref().is_some_and(|(old, _)| old == op)
                    );
                    let stale = self.step(*event);
                    if is_old_ack {
                        assert!(stale, "old source ack reply must be rejected");
                        self.old_reply_rejected = true;
                        self.faults.rejected_old_acks += 1;
                    }
                }
                Action::Timer(due) => {
                    self.step(Event::Tick(due));
                }
                Action::Restart => self.restart(),
                Action::LateOldApply => {
                    if self.sink_epoch < 2 {
                        self.later(Action::LateOldApply, 1);
                    } else {
                        let (old_session, old_ordinal) = self
                            .old_ack
                            .as_ref()
                            .map(|(old, ack)| (old.session, ack.epoch.ordinal.get()))
                            .expect("old lineage");
                        assert_ne!(old_session, self.session_id);
                        assert!(!self.sink_apply(old_ordinal, 2));
                        assert_eq!(self.sink_cursor, 2);
                        assert_eq!(self.durable_effects, 1);
                        self.old_apply_fenced = true;
                        self.faults.fenced_old_applies += 1;
                    }
                }
            }
            assert!(self.retained <= self.source_ack);
            assert!(self.durable_effects <= 1);
            if self.restarted
                && self.now >= 35
                && self.source_ack == 2
                && self.engine.state().stage == Stage::Protected
                && self.old_reply_rejected
                && self.old_apply_fenced
            {
                assert_eq!(self.sink_cursor, 2);
                assert_eq!(self.ordinal, 2);
                assert_eq!(self.durable_effects, 1);
                return self.faults;
            }
        }
        panic!("healed schedule did not reach the protected ack");
    }
}

#[test]
fn queued_loss_reorder_partition_and_crash_heal_without_duplicate_effects() {
    let mut covered = FaultCounts::default();
    for seed in 0..64 {
        covered.add(Model::new(seed).run());
    }
    assert!(covered.partition_drops > 0);
    assert!(covered.random_request_drops > 0);
    assert!(covered.lost_replies > 0);
    assert!(covered.duplicated_replies > 0);
    assert!(covered.reordered_replies > 0);
    assert_eq!(covered.crashes, 64);
    assert_eq!(covered.fenced_old_applies, 64);
    assert_eq!(covered.rejected_old_acks, 64);
}
