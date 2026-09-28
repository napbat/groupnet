use super::*;
use crate::volatile_bootstrap::{BootstrapScope, ClaimIdentity};
use crate::{NodeId, Time};
use std::sync::{Arc, Barrier, Mutex};

fn config() -> JournalConfig {
    JournalConfig {
        max_encoded_bytes: 128,
        max_decoded_bytes: 128,
        max_events: 4,
        max_suffix_bytes: 32,
        max_event_bytes: 12,
        max_identity_bytes: 8,
        max_followers: 2,
        max_follower_id_bytes: 8,
        max_cuts: 2,
        max_cut_bytes: 16,
        max_members: 3,
        max_membership_bytes: 24,
        max_scope_bytes: 16,
        max_batch_events: 2,
        max_batch_bytes: 16,
        max_inflight_bytes: 32,
        max_total_ms: 30,
        max_follower_ms: 12,
    }
}

fn identity(name: &str, session: u64) -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from(name),
        incarnation: 3,
        session,
        attempt: 1,
    }
}

fn capture_id() -> CaptureId {
    CaptureId {
        scope: BootstrapScope {
            domain: "o".to_owned(),
            partition: "b".to_owned(),
        },
        donor: identity("donor", 1),
        recovery_generation: 2,
        serial: 7,
    }
}

fn members() -> Vec<ClaimIdentity> {
    vec![identity("donor", 1), identity("peer", 2)]
}

fn cuts() -> Vec<NativeCut> {
    vec![NativeCut {
        writer: b"w".to_vec(),
        epoch: 1,
        sequence: 0,
    }]
}

fn captured() -> (DonorJournal, JournalCursor) {
    let mut journal = DonorJournal::new(config(), capture_id()).unwrap();
    let admission = journal
        .begin_capture(Time(1), 64, 64, members(), cuts())
        .unwrap();
    assert_eq!(admission.started, Time(1));
    let cut = journal.finish_capture(Time(2), 20, 24).unwrap();
    (journal, cut)
}

fn native(sequence: u64) -> DeltaIdentity {
    DeltaIdentity::Native(NativeCut {
        writer: b"w".to_vec(),
        epoch: 1,
        sequence,
    })
}

fn local(name: &str) -> DeltaIdentity {
    DeltaIdentity::Local(name.as_bytes().to_vec())
}

#[test]
fn image_admission_precedes_clone_and_retains_exact_cut() {
    let mut journal = DonorJournal::new(config(), capture_id()).unwrap();
    let admission = journal
        .begin_capture(Time(1), 64, 64, members(), cuts())
        .unwrap();
    assert_eq!(journal.state(), JournalState::Capturing);
    assert_eq!(journal.current_cursor(), None);
    assert_eq!(admission.encoded_bytes, 64);
    let cut = journal.finish_capture(Time(2), 20, 24).unwrap();
    assert_eq!(cut.position, 0);
    assert_eq!(cut.capture, capture_id());
    assert_eq!(journal.image_charge().unwrap().started, Time(1));
    assert_eq!(journal.state(), JournalState::Active);

    let (mut too_large, _) = captured();
    assert_eq!(
        too_large.begin_capture(Time(3), 1, 1, members(), cuts()),
        Err(JournalError::Stage)
    );
    let mut bad_clone = DonorJournal::new(config(), capture_id()).unwrap();
    bad_clone
        .begin_capture(Time(1), 8, 8, members(), cuts())
        .unwrap();
    assert_eq!(
        bad_clone.finish_capture(Time(2), 9, 8),
        Err(JournalError::Capacity)
    );
    assert_eq!(bad_clone.state(), JournalState::Invalidated);
    assert_eq!(bad_clone.image_charge(), None);
}

#[test]
fn every_effect_including_local_repair_is_contiguous_and_exactly_deduplicated() {
    let (mut journal, cut) = captured();
    let one = journal
        .append(Time(3), 2, native(1), b"put".to_vec())
        .unwrap();
    let two = journal
        .append(Time(4), 2, local("repair"), b"fix".to_vec())
        .unwrap();
    let three = journal
        .append(Time(5), 2, native(2), b"del".to_vec())
        .unwrap();
    assert_eq!(
        [cut.position, one.position, two.position, three.position],
        [0, 1, 2, 3]
    );
    assert_eq!(journal.covered_cuts()[0].sequence, 2);
    assert_eq!(
        journal.append(Time(6), 2, local("repair"), b"fix".to_vec()),
        Ok(two)
    );
    assert_eq!(journal.current_cursor(), Some(three));
    assert_eq!(
        journal.append(Time(8), 2, local("repair"), b"other".to_vec()),
        Err(JournalError::Conflict)
    );
    assert_eq!(journal.invalidation(), Some(Invalidation::Conflict));

    let (mut wrong_generation, _) = captured();
    assert_eq!(
        wrong_generation.append(Time(3), 3, local("x"), b"x".to_vec()),
        Err(JournalError::Stale)
    );
    assert_eq!(wrong_generation.invalidation(), Some(Invalidation::Rebuild));
}

#[test]
fn native_noop_advances_exact_writer_coverage_without_index_change() {
    let (mut journal, _) = captured();
    let noop = journal
        .append(Time(3), 2, native(1), b"noop".to_vec())
        .unwrap();
    let actual = journal
        .append(Time(4), 2, native(2), b"put".to_vec())
        .unwrap();
    assert_eq!((noop.position, actual.position), (1, 2));
    assert_eq!(journal.covered_cuts()[0].sequence, 2);
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one attachment and exact-barrier lifecycle exercises successive batches and stale callbacks"
)]
fn attach_must_be_confirmed_before_barrier_and_batch_acks_are_exact() {
    let (mut journal, cut) = captured();
    let follower = identity("peer", 2);
    let reservation = journal.reserve(Time(3), follower.clone(), &cut).unwrap();
    assert_eq!(
        journal.barrier(Time(3), &reservation),
        Err(JournalError::Stage)
    );
    let attach = journal.begin_attach(Time(3), &reservation).unwrap();
    assert_eq!(
        journal.barrier(Time(3), &reservation),
        Err(JournalError::Stage)
    );
    journal.confirm_attach(Time(4), &attach).unwrap();
    journal.confirm_attach(Time(4), &attach).unwrap();
    journal
        .append(Time(5), 2, native(1), b"p".to_vec())
        .unwrap();
    journal
        .append(Time(5), 2, local("repair"), b"r".to_vec())
        .unwrap();
    journal
        .append(Time(5), 2, native(2), b"d".to_vec())
        .unwrap();
    let barrier = journal.barrier(Time(6), &reservation).unwrap();
    assert_eq!(barrier.cursor.position, 3);
    assert_eq!(barrier.covered_cuts[0].sequence, 2);
    journal
        .append(Time(6), 2, native(3), b"later".to_vec())
        .unwrap();
    assert_eq!(journal.covered_cuts()[0].sequence, 3);
    assert_eq!(journal.barrier(Time(6), &reservation).unwrap(), barrier);
    let first = journal
        .read_batch(Time(6), &reservation, &barrier)
        .unwrap()
        .unwrap();
    assert_eq!(
        journal.advance_barrier(Time(6), &reservation, &barrier),
        Err(JournalError::Stale)
    );
    assert_eq!(
        first
            .deltas
            .iter()
            .map(|delta| delta.position)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(journal.inflight_bytes() > 0);
    assert_eq!(
        journal.read_batch(Time(6), &reservation, &barrier),
        Err(JournalError::Capacity)
    );
    assert_eq!(
        journal.ack_batch(Time(7), &reservation, first.operation + 1, &first.through),
        Err(JournalError::Stale)
    );
    let first_operation = first.operation;
    let first_through = first.through.clone();
    drop(first);
    assert_eq!(
        journal
            .ack_batch(Time(7), &reservation, first_operation, &first_through)
            .unwrap(),
        first_through
    );
    assert_eq!(journal.inflight_bytes(), 0);
    let second = journal
        .read_batch(Time(8), &reservation, &barrier)
        .unwrap()
        .unwrap();
    assert_eq!(second.deltas.len(), 1);
    let second_operation = second.operation;
    let second_through = second.through.clone();
    drop(second);
    journal
        .ack_batch(Time(8), &reservation, second_operation, &second_through)
        .unwrap();
    assert_eq!(
        journal.acknowledged(Time(8), &reservation).unwrap(),
        barrier.cursor
    );
    let next_barrier = journal
        .advance_barrier(Time(8), &reservation, &barrier)
        .unwrap();
    assert_eq!(next_barrier.cursor.position, 4);
    assert_eq!(next_barrier.covered_cuts[0].sequence, 3);
    assert_eq!(
        journal.barrier(Time(8), &reservation).unwrap(),
        next_barrier
    );
    assert_eq!(
        journal.advance_barrier(Time(8), &reservation, &barrier),
        Err(JournalError::Stale)
    );
    assert_eq!(
        journal.read_batch(Time(8), &reservation, &barrier),
        Err(JournalError::Stale)
    );
    let later = journal
        .read_batch(Time(8), &reservation, &next_barrier)
        .unwrap()
        .unwrap();
    assert_eq!(
        later
            .deltas
            .iter()
            .map(|delta| delta.position)
            .collect::<Vec<_>>(),
        [4]
    );
    let later_operation = later.operation;
    let later_through = later.through.clone();
    drop(later);
    journal
        .ack_batch(Time(8), &reservation, later_operation, &later_through)
        .unwrap();
    journal.release(Time(9), &reservation).unwrap();
    let reopened = journal.reserve(Time(9), follower, &cut).unwrap();
    assert_ne!(reopened.serial, reservation.serial);
    assert_eq!(
        journal.release(Time(9), &reservation),
        Err(JournalError::Stale)
    );
    assert_eq!(
        journal.confirm_attach(Time(9), &attach),
        Err(JournalError::Stale)
    );
}

#[test]
fn unknown_writer_gap_and_membership_change_invalidate_without_authority() {
    let (mut unknown, _) = captured();
    let extra = DeltaIdentity::Native(NativeCut {
        writer: b"new".to_vec(),
        epoch: 1,
        sequence: 1,
    });
    assert_eq!(
        unknown.append(Time(3), 2, extra, b"x".to_vec()),
        Err(JournalError::Conflict)
    );
    assert_eq!(unknown.invalidation(), Some(Invalidation::Membership));

    let (mut gap, _) = captured();
    assert_eq!(
        gap.append(Time(3), 2, native(2), b"x".to_vec()),
        Err(JournalError::Conflict)
    );
    assert_eq!(gap.invalidation(), Some(Invalidation::Gap));

    let (mut changed, _) = captured();
    let mut altered = members();
    altered[1].session = 3;
    assert_eq!(
        changed.observe_membership(Time(3), &altered),
        Err(JournalError::Conflict)
    );
    assert_eq!(changed.invalidation(), Some(Invalidation::Membership));
}

#[test]
fn capacity_and_time_expiry_release_all_followers_without_truncating_shared_suffix() {
    let (mut journal, cut) = captured();
    let a = journal.reserve(Time(3), identity("a", 3), &cut).unwrap();
    let b = journal.reserve(Time(3), identity("b", 4), &cut).unwrap();
    let attach_a = journal.begin_attach(Time(3), &a).unwrap();
    journal.confirm_attach(Time(3), &attach_a).unwrap();
    let attach_b = journal.begin_attach(Time(3), &b).unwrap();
    journal.confirm_attach(Time(3), &attach_b).unwrap();
    journal
        .append(Time(4), 2, native(1), b"p".to_vec())
        .unwrap();
    let barrier = journal.barrier(Time(4), &a).unwrap();
    let batch = journal.read_batch(Time(4), &a, &barrier).unwrap().unwrap();
    let batch_operation = batch.operation;
    let batch_through = batch.through.clone();
    drop(batch);
    journal
        .ack_batch(Time(5), &a, batch_operation, &batch_through)
        .unwrap();
    assert_eq!(journal.retained_bytes(), 2);
    let barrier_b = journal.barrier(Time(5), &b).unwrap();
    let held_b = journal
        .read_batch(Time(5), &b, &barrier_b)
        .unwrap()
        .unwrap();
    assert!(journal.inflight_bytes() > 0);
    let held_bytes = held_b.bytes;
    let held_operation = held_b.operation;
    journal.tick(Time(15)).unwrap();
    assert_eq!(journal.inflight_bytes(), held_bytes);
    assert_eq!(journal.take_aborted(), vec![a.clone(), b.clone()]);
    assert!(journal.take_aborted().is_empty());
    assert_eq!(journal.retire_aborted(&b, None), Err(JournalError::Stale));
    journal.retire_aborted(&a, None).unwrap();
    drop(held_b);
    journal.retire_aborted(&b, Some(held_operation)).unwrap();
    assert_eq!(journal.inflight_bytes(), 0);
    assert_eq!(journal.retained_bytes(), 2);
    assert_eq!(journal.tick(Time(31)), Err(JournalError::Expired));
    assert_eq!(journal.state(), JournalState::Invalidated);
    assert_eq!(journal.retained_bytes(), 0);
    assert_eq!(journal.image_charge(), None);
}

#[test]
fn retained_suffix_overflow_aborts_candidate_and_exact_reservations() {
    let (mut journal, cut) = captured();
    let follower = journal.reserve(Time(3), identity("peer", 2), &cut).unwrap();
    for (time, name) in [(4, "a"), (5, "b"), (6, "c"), (7, "d")] {
        journal
            .append(Time(time), 2, local(name), b"x".to_vec())
            .unwrap();
    }
    assert_eq!(
        journal.append(Time(8), 2, local("e"), b"x".to_vec()),
        Err(JournalError::Capacity)
    );
    assert_eq!(journal.invalidation(), Some(Invalidation::Capacity));
    assert_eq!(journal.take_aborted(), vec![follower.clone()]);
    journal.retire_aborted(&follower, None).unwrap();
    assert_eq!(journal.retained_bytes(), 0);
}

#[test]
fn undrained_abort_notifications_consume_follower_capacity() {
    let (mut journal, cut) = captured();
    let first = journal.reserve(Time(3), identity("a", 3), &cut).unwrap();
    journal.tick(Time(15)).unwrap();
    assert_eq!(journal.take_aborted(), vec![first.clone()]);
    let second = journal.reserve(Time(15), identity("b", 4), &cut).unwrap();
    journal.tick(Time(27)).unwrap();
    assert_eq!(journal.take_aborted(), vec![second.clone()]);
    assert_eq!(
        journal.reserve(Time(27), identity("c", 5), &cut),
        Err(JournalError::Capacity)
    );
    journal.retire_aborted(&first, None).unwrap();
    let third = journal.reserve(Time(27), identity("c", 5), &cut).unwrap();
    assert_ne!(third.serial, second.serial);
    assert_eq!(journal.tick(Time(31)), Err(JournalError::Expired));
    assert_eq!(journal.take_aborted(), vec![third.clone()]);
    journal.retire_aborted(&second, None).unwrap();
    journal.retire_aborted(&third, None).unwrap();
}

#[test]
fn locked_capture_and_mutation_put_effect_in_image_or_contiguous_suffix() {
    struct Model {
        index: Vec<Vec<u8>>,
        image: Option<Vec<Vec<u8>>>,
        journal: DonorJournal,
    }
    for _ in 0..48 {
        let shared = Arc::new(Mutex::new(Model {
            index: Vec::new(),
            image: None,
            journal: DonorJournal::new(config(), capture_id()).unwrap(),
        }));
        let start = Arc::new(Barrier::new(3));
        let capture = {
            let shared = Arc::clone(&shared);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                let mut model = shared.lock().unwrap();
                model
                    .journal
                    .begin_capture(Time(1), 64, 64, members(), cuts())
                    .unwrap();
                model.image = Some(model.index.clone());
                model.journal.finish_capture(Time(1), 16, 16).unwrap();
            })
        };
        let mutation = {
            let shared = Arc::clone(&shared);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                let mut model = shared.lock().unwrap();
                model.index.push(b"put".to_vec());
                if model.journal.state() == JournalState::Active {
                    model
                        .journal
                        .append(Time(2), 2, local("mut"), b"put".to_vec())
                        .unwrap();
                }
            })
        };
        start.wait();
        capture.join().unwrap();
        mutation.join().unwrap();
        let model = shared.lock().unwrap();
        let image_has_mutation = model.image.as_ref().unwrap().contains(&b"put".to_vec());
        let suffix_has_mutation = model.journal.current_cursor().unwrap().position == 1;
        assert_ne!(image_has_mutation, suffix_has_mutation);
        assert_eq!(model.index, [b"put".to_vec()]);
    }
}
