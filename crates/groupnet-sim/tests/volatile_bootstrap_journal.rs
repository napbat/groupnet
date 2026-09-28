//! Seeded image-cut and donor-local suffix schedules under virtual time.

use groupnet_core::volatile_bootstrap::journal::{
    CaptureId, DeltaIdentity, DonorJournal, Invalidation, JournalConfig, JournalCursor,
    JournalError, JournalState, NativeCut,
};
use groupnet_core::volatile_bootstrap::{BootstrapScope, ClaimIdentity};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

fn identity(name: &str, session: u64) -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from(name),
        incarnation: 7,
        session,
        attempt: 1,
    }
}

fn config() -> JournalConfig {
    JournalConfig {
        max_encoded_bytes: 64,
        max_decoded_bytes: 64,
        max_events: 6,
        max_suffix_bytes: 32,
        max_event_bytes: 8,
        max_identity_bytes: 4,
        max_followers: 2,
        max_follower_id_bytes: 8,
        max_cuts: 1,
        max_cut_bytes: 4,
        max_members: 2,
        max_membership_bytes: 16,
        max_scope_bytes: 8,
        max_batch_events: 2,
        max_batch_bytes: 16,
        max_inflight_bytes: 16,
        max_total_ms: 30,
        max_follower_ms: 8,
    }
}

fn captured(seed: u64) -> (DonorJournal, JournalCursor) {
    captured_with(seed, config())
}

fn captured_with(seed: u64, limits: JournalConfig) -> (DonorJournal, JournalCursor) {
    let donor = identity("donor", seed + 1);
    let id = CaptureId {
        scope: BootstrapScope {
            domain: "o".to_owned(),
            partition: "b".to_owned(),
        },
        donor: donor.clone(),
        recovery_generation: 9,
        serial: seed + 1,
    };
    let mut journal = DonorJournal::new(limits, id).unwrap();
    journal
        .begin_capture(
            Time(0),
            32,
            32,
            vec![donor, identity("peer", seed + 100)],
            vec![NativeCut {
                writer: b"w".to_vec(),
                epoch: 1,
                sequence: 0,
            }],
        )
        .unwrap();
    let cut = journal.finish_capture(Time(0), 8, 8).unwrap();
    (journal, cut)
}

#[derive(Clone)]
enum Delivery {
    Append(u8, Vec<u8>),
    Reserve,
    BeginAttach,
    ConfirmAttach(groupnet_core::volatile_bootstrap::journal::AttachToken),
    Pump,
    Ack(Box<groupnet_core::volatile_bootstrap::journal::JournalBatch>),
}

fn prefix_image(effects: &[Vec<u8>], through: u64) -> Option<u8> {
    let mut image = Some(7);
    for effect in effects.iter().take(usize::try_from(through).unwrap()) {
        apply(&mut image, effect);
    }
    image
}

/// Drive queued source callbacks and follower work independently. The
/// reference image uses only the ordered effects, never journal cursors.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the event queue, independent reference image, and safety floors form one schedule"
)]
fn queued_transfer_reconstructs_every_exact_barrier() {
    let mut stale_replies = 0;
    let mut continued_barriers = 0;
    for seed in 0..48 {
        let mut limits = config();
        limits.max_follower_ms = 28;
        let (mut journal, cut) = captured_with(seed, limits);
        let mut rng = SplitMix64::new(seed + 900);
        let mut queue: Vec<(u64, Delivery)> =
            vec![(1, Delivery::Reserve), (2, Delivery::BeginAttach)];
        let mut at = 1u64;
        for index in 1..=5 {
            at += 1 + u64::from(rng.below(2));
            queue.push((at, Delivery::Append(index, mutation(&mut rng, index))));
        }
        for now in 3..=24 {
            queue.push((now, Delivery::Pump));
        }
        let mut ordered = Vec::new();
        let mut native_sequence = 0;
        let mut reservation = None;
        let mut barrier = None;
        let mut image = Some(7);
        let mut pending = false;
        while !queue.is_empty() {
            let due = queue.iter().map(|(due, _)| *due).min().unwrap();
            let choices = queue
                .iter()
                .enumerate()
                .filter_map(|(index, (time, _))| (*time == due).then_some(index))
                .collect::<Vec<_>>();
            let choice = choices[rng.below(u32::try_from(choices.len()).unwrap()) as usize];
            let (_, event) = queue.swap_remove(choice);
            match event {
                Delivery::Append(index, effect) => {
                    // The native publisher's order is fixed; queue delivery
                    // can still race reservation, attachment, and acks.
                    append(
                        &mut journal,
                        due,
                        index,
                        &mut native_sequence,
                        effect.clone(),
                    );
                    ordered.push(effect);
                }
                Delivery::Reserve => {
                    reservation = Some(
                        journal
                            .reserve(Time(due), identity("peer", seed + 100), &cut)
                            .unwrap(),
                    );
                }
                Delivery::BeginAttach => {
                    let token = journal
                        .begin_attach(Time(due), reservation.as_ref().unwrap())
                        .unwrap();
                    queue.push((
                        due + 1 + u64::from(rng.below(2)),
                        Delivery::ConfirmAttach(token),
                    ));
                }
                Delivery::ConfirmAttach(token) => {
                    journal.confirm_attach(Time(due), &token).unwrap();
                }
                Delivery::Pump => {
                    let Some(id) = &reservation else {
                        continue;
                    };
                    if pending || journal.barrier(Time(due), id) == Err(JournalError::Stage) {
                        continue;
                    }
                    if barrier.is_none() {
                        barrier = Some(journal.barrier(Time(due), id).unwrap());
                    }
                    let receipt = barrier.as_ref().unwrap();
                    if journal.acknowledged(Time(due), id).unwrap() == receipt.cursor {
                        assert_eq!(image, prefix_image(&ordered, receipt.cursor.position));
                        if journal.current_cursor().unwrap().position > receipt.cursor.position {
                            barrier =
                                Some(journal.advance_barrier(Time(due), id, receipt).unwrap());
                            continued_barriers += 1;
                        }
                    }
                    if let Some(batch) = journal
                        .read_batch(Time(due), id, barrier.as_ref().unwrap())
                        .unwrap()
                    {
                        pending = true;
                        queue.push((
                            due + 1 + u64::from(rng.below(2)),
                            Delivery::Ack(Box::new(batch)),
                        ));
                    }
                }
                Delivery::Ack(batch) => {
                    let id = reservation.as_ref().unwrap();
                    let operation = batch.operation;
                    let through = batch.through.clone();
                    for delta in &batch.deltas {
                        apply(&mut image, &delta.effect);
                    }
                    drop(batch);
                    journal
                        .ack_batch(Time(due), id, operation, &through)
                        .unwrap();
                    pending = false;
                    if due < 27 {
                        queue.push((due + 1, Delivery::Pump));
                    }
                    assert_eq!(image, prefix_image(&ordered, through.position));
                    assert_eq!(
                        journal.ack_batch(Time(due), id, operation, &through),
                        Err(JournalError::Stale)
                    );
                    stale_replies += 1;
                }
            }
            assert!(journal.retained_bytes() <= limits.max_suffix_bytes);
            assert!(journal.inflight_bytes() <= limits.max_inflight_bytes);
        }
        assert_eq!(journal.current_cursor().unwrap().position, 5);
        assert_eq!(
            journal
                .acknowledged(Time(28), reservation.as_ref().unwrap())
                .unwrap()
                .position,
            5,
            "seed {seed}"
        );
        assert_eq!(image, prefix_image(&ordered, 5), "seed {seed}");
        journal
            .release(Time(28), reservation.as_ref().unwrap())
            .unwrap();
    }
    assert!(continued_barriers >= 48);
    assert!(stale_replies >= 48);
}

/// Cancellation, expiry, and donor revocation race the exact batch reply;
/// the delayed callback cannot resurrect a released follower or image.
#[test]
fn queued_disposal_fences_delayed_batch_callbacks() {
    let mut late_rejected = 0;
    let mut ack_before_disposal = 0;
    let mut ack_after_disposal = 0;
    for seed in 0..48 {
        let mut rng = SplitMix64::new(seed + 70_000);
        let mut limits = config();
        limits.max_follower_ms = 5;
        let (mut journal, cut) = captured_with(seed, limits);
        let mut native_sequence = 0;
        append(&mut journal, 0, 1, &mut native_sequence, vec![1, 5]);
        append(&mut journal, 0, 2, &mut native_sequence, vec![0]);
        let reservation = journal
            .reserve(Time(1), identity("peer", seed + 100), &cut)
            .unwrap();
        let attach = journal.begin_attach(Time(1), &reservation).unwrap();
        journal.confirm_attach(Time(1), &attach).unwrap();
        let barrier = journal.barrier(Time(2), &reservation).unwrap();
        let batch = journal
            .read_batch(Time(2), &reservation, &barrier)
            .unwrap()
            .unwrap();
        let operation = batch.operation;
        let through = batch.through.clone();
        let effects = batch
            .deltas
            .iter()
            .map(|d| d.effect.clone())
            .collect::<Vec<_>>();
        drop(batch);
        let mut image = Some(7);
        let fault_due = if seed % 3 == 2 { 5u64 } else { 6u64 };
        let ack_due = if rng.below(2) == 0 {
            3 + u64::from(rng.below(2))
        } else {
            7 + u64::from(rng.below(2))
        };
        let mut queue = vec![(ack_due, true), (fault_due, false)];
        let mut disposed = false;
        while !queue.is_empty() {
            let due = queue.iter().map(|(due, _)| *due).min().unwrap();
            let choices = queue
                .iter()
                .enumerate()
                .filter_map(|(index, (time, _))| (*time == due).then_some(index))
                .collect::<Vec<_>>();
            let choice = choices[rng.below(u32::try_from(choices.len()).unwrap()) as usize];
            let (_, ack) = queue.swap_remove(choice);
            if ack {
                let result = journal.ack_batch(Time(due), &reservation, operation, &through);
                if disposed {
                    assert_eq!(result, Err(JournalError::Stale));
                    ack_after_disposal += 1;
                } else {
                    result.unwrap();
                    for effect in &effects {
                        apply(&mut image, effect);
                    }
                    assert_eq!(image, None);
                    ack_before_disposal += 1;
                }
            } else {
                match seed % 3 {
                    0 => journal.invalidate(Invalidation::DonorLost),
                    1 => journal.tick(Time(due)).unwrap(),
                    _ => journal.release(Time(due), &reservation).unwrap(),
                }
                if seed % 3 != 2 {
                    assert_eq!(journal.take_aborted(), vec![reservation.clone()]);
                    let held = (journal.inflight_bytes() > 0).then_some(operation);
                    journal.retire_aborted(&reservation, held).unwrap();
                }
                disposed = true;
                assert_eq!(journal.inflight_bytes(), 0);
            }
        }
        assert_eq!(
            journal.ack_batch(Time(9), &reservation, operation, &through),
            Err(JournalError::Stale)
        );
        late_rejected += 1;
        assert!(journal.take_aborted().len() <= config().max_followers);
    }
    assert_eq!(late_rejected, 48);
    assert!(ack_before_disposal > 0);
    assert!(ack_after_disposal > 0);
}

fn apply(state: &mut Option<u8>, effect: &[u8]) {
    match effect {
        [0] => *state = None,
        [1, value] => *state = Some(*value),
        [2] => {}
        _ => panic!("model effect encoding"),
    }
}

fn mutation(rng: &mut SplitMix64, index: u8) -> Vec<u8> {
    match rng.below(4) {
        0 => vec![0],
        1 => vec![2],
        _ => vec![1, index],
    }
}

fn append(
    journal: &mut DonorJournal,
    now: u64,
    index: u8,
    native_sequence: &mut u64,
    effect: Vec<u8>,
) {
    let identity = if index % 2 == 0 {
        DeltaIdentity::Local(vec![index])
    } else {
        *native_sequence += 1;
        DeltaIdentity::Native(NativeCut {
            writer: b"w".to_vec(),
            epoch: 1,
            sequence: *native_sequence,
        })
    };
    journal.append(Time(now), 9, identity, effect).unwrap();
}

fn consume_barrier(
    journal: &mut DonorJournal,
    reservation: &groupnet_core::volatile_bootstrap::journal::ReservationId,
    barrier: &groupnet_core::volatile_bootstrap::journal::BarrierReceipt,
    now: u64,
    state: &mut Option<u8>,
) {
    while let Some(batch) = journal.read_batch(Time(now), reservation, barrier).unwrap() {
        for delta in &batch.deltas {
            apply(state, &delta.effect);
        }
        assert!(journal.inflight_bytes() <= config().max_inflight_bytes);
        let operation = batch.operation;
        let through = batch.through.clone();
        drop(batch);
        let wrong = journal.ack_batch(Time(now), reservation, operation + 1, &through);
        assert_eq!(wrong, Err(JournalError::Stale));
        journal
            .ack_batch(Time(now), reservation, operation, &through)
            .unwrap();
        assert_eq!(
            journal.ack_batch(Time(now), reservation, operation, &through),
            Err(JournalError::Stale)
        );
    }
    assert_eq!(
        journal.acknowledged(Time(now), reservation).unwrap(),
        barrier.cursor
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one virtual-time image/suffix schedule retains all fault branches and earned floors"
)]
fn image_cut_suffix_and_expiry_schedules_never_skip_or_resurrect() {
    let mut completed = 0;
    let mut expired = 0;
    let mut invalidated = 0;
    let mut overflowed = 0;
    let mut stale_tokens = 0;
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        let (mut journal, cut) = captured(seed);
        let mut live = Some(7u8);
        let mut native_sequence = 0;
        for index in 1..=3 {
            let effect = mutation(&mut rng, index);
            apply(&mut live, &effect);
            append(
                &mut journal,
                u64::from(index),
                index,
                &mut native_sequence,
                effect,
            );
        }
        let follower = identity("peer", seed + 100);
        let reservation = journal.reserve(Time(4), follower, &cut).unwrap();
        let attach = journal.begin_attach(Time(4), &reservation).unwrap();
        assert_eq!(
            journal.barrier(Time(4), &reservation),
            Err(JournalError::Stage)
        );
        journal.confirm_attach(Time(4), &attach).unwrap();
        let first = journal.barrier(Time(4), &reservation).unwrap();
        assert_eq!(first.cursor.position, 3);
        assert_eq!(first.covered_cuts[0].sequence, native_sequence);
        let mut follower_state = Some(7u8);
        consume_barrier(&mut journal, &reservation, &first, 4, &mut follower_state);
        assert_eq!(follower_state, live, "seed {seed} at B");
        assert!(journal.retained_bytes() <= config().max_suffix_bytes);

        match seed % 4 {
            0 => {
                for index in 4..=5 {
                    let effect = mutation(&mut rng, index);
                    apply(&mut live, &effect);
                    append(
                        &mut journal,
                        u64::from(index),
                        index,
                        &mut native_sequence,
                        effect,
                    );
                }
                let second = journal
                    .advance_barrier(Time(6), &reservation, &first)
                    .unwrap();
                assert_eq!(second.cursor.position, 5);
                assert_eq!(second.covered_cuts[0].sequence, native_sequence);
                assert_eq!(
                    journal.advance_barrier(Time(6), &reservation, &first),
                    Err(JournalError::Stale)
                );
                stale_tokens += 1;
                consume_barrier(&mut journal, &reservation, &second, 6, &mut follower_state);
                assert_eq!(follower_state, live, "seed {seed} after B2");
                journal.release(Time(7), &reservation).unwrap();
                assert_eq!(journal.inflight_bytes(), 0);
                completed += 1;
            }
            1 => {
                let held = journal.read_batch(Time(5), &reservation, &first).unwrap();
                assert!(held.is_none());
                journal.tick(Time(12)).unwrap();
                assert_eq!(journal.take_aborted(), vec![reservation.clone()]);
                assert_eq!(
                    journal.barrier(Time(12), &reservation),
                    Err(JournalError::Stale)
                );
                journal.retire_aborted(&reservation, None).unwrap();
                expired += 1;
            }
            2 => {
                journal.invalidate(Invalidation::DonorLost);
                assert_eq!(journal.state(), JournalState::Invalidated);
                assert_eq!(journal.take_aborted(), vec![reservation.clone()]);
                journal.retire_aborted(&reservation, None).unwrap();
                assert_eq!(journal.retained_bytes(), 0);
                invalidated += 1;
            }
            _ => {
                for index in 4..=7 {
                    let effect = mutation(&mut rng, index);
                    apply(&mut live, &effect);
                    if index < 7 {
                        append(
                            &mut journal,
                            u64::from(index),
                            index,
                            &mut native_sequence,
                            effect,
                        );
                    } else {
                        let result =
                            journal.append(Time(7), 9, DeltaIdentity::Local(vec![7]), effect);
                        assert_eq!(result, Err(JournalError::Capacity));
                    }
                }
                assert_eq!(journal.invalidation(), Some(Invalidation::Capacity));
                assert_eq!(journal.take_aborted(), vec![reservation.clone()]);
                journal.retire_aborted(&reservation, None).unwrap();
                overflowed += 1;
            }
        }
        assert!(journal.inflight_bytes() <= config().max_inflight_bytes);
        assert!(journal.retained_bytes() <= config().max_suffix_bytes);
        assert!(journal.take_aborted().len() <= config().max_followers);
    }
    assert_eq!(
        (completed, expired, invalidated, overflowed),
        (16, 16, 16, 16)
    );
    assert_eq!(stale_tokens, 16);
}
