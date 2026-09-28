//! Seeded response-loss schedules for exact donor reservation and batch replay.

use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, CaptureId, DeltaIdentity, DonorJournal, Invalidation, JournalBatch, JournalConfig,
    JournalCursor, JournalError, ReservationId,
};
use groupnet_core::volatile_bootstrap::{BootId, BootstrapScope, ClaimIdentity};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

#[derive(Clone)]
enum Reply {
    Reservation(ReservationId),
    Attachment(AttachToken),
    Batch(Box<JournalBatch>),
    Ack(JournalCursor),
}

fn identity(name: &str, session: u64) -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from(name),
        incarnation: BootId(7),
        session,
        attempt: 1,
    }
}

fn captured(seed: u64) -> (DonorJournal, JournalCursor, ClaimIdentity) {
    let config = JournalConfig {
        max_encoded_bytes: 16,
        max_decoded_bytes: 16,
        max_events: 4,
        max_suffix_bytes: 32,
        max_event_bytes: 8,
        max_identity_bytes: 4,
        max_followers: 1,
        max_follower_id_bytes: 8,
        max_cuts: 1,
        max_cut_bytes: 8,
        max_members: 2,
        max_membership_bytes: 16,
        max_scope_bytes: 8,
        max_batch_events: 2,
        max_batch_bytes: 8,
        max_inflight_bytes: 16,
        max_total_ms: 30,
        max_follower_ms: 8,
    };
    let donor = identity("donor", seed + 1);
    let follower = identity("peer", seed + 100);
    let id = CaptureId {
        scope: BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        donor: donor.clone(),
        recovery_generation: 2,
        serial: seed + 1,
    };
    let mut journal = DonorJournal::new(config, id).unwrap();
    journal
        .begin_capture(Time(0), 8, 8, vec![donor, follower.clone()], vec![])
        .unwrap();
    let cut = journal.finish_capture(Time(0), 8, 8).unwrap();
    (journal, cut, follower)
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded response-loss schedule asserts exact readback and healthy progress floors"
)]
fn seeded_lost_duplicate_reordered_replies_recover_or_expire_without_wrong_ack() {
    let mut lost = 0;
    let mut duplicates = 0;
    let mut expired = 0;
    let mut cancelled = 0;
    let mut progressed = 0;
    for seed in 0..96 {
        let mut rng = SplitMix64::new(seed + 91_000);
        let (mut journal, cut, follower) = captured(seed);
        let reservation = journal.reserve(Time(1), follower.clone(), &cut).unwrap();
        let attachment = journal.begin_attach(Time(2), &reservation).unwrap();
        journal.confirm_attach(Time(2), &attachment).unwrap();
        journal
            .append(Time(3), 2, DeltaIdentity::Local(b"a".to_vec()), vec![1])
            .unwrap();
        let barrier = journal.barrier(Time(4), &reservation).unwrap();
        let batch = journal
            .read_batch(Time(4), &reservation, &barrier)
            .unwrap()
            .unwrap();
        assert_eq!(
            journal.read_batch(Time(4), &reservation, &barrier),
            Ok(Some(batch.clone()))
        );
        assert_eq!(journal.inflight_bytes(), batch.bytes);
        let wrong_operation = batch.operation.checked_add(1).unwrap();
        assert_eq!(
            journal.ack_batch(Time(5), &reservation, wrong_operation, &batch.through),
            Err(JournalError::Stale)
        );
        let through = journal
            .ack_batch(Time(5), &reservation, batch.operation, &batch.through)
            .unwrap();
        assert_eq!(
            journal.ack_batch(Time(5), &reservation, batch.operation, &through),
            Ok(through.clone())
        );
        assert_eq!(
            journal.ack_batch(Time(5), &reservation, wrong_operation, &through),
            Err(JournalError::Stale)
        );

        // Network replies are independent of source mutation order. Some are
        // lost; others are duplicated and delivered in a seeded order.
        let originals = [
            Reply::Reservation(reservation.clone()),
            Reply::Attachment(attachment.clone()),
            Reply::Batch(Box::new(batch.clone())),
            Reply::Ack(through.clone()),
        ];
        let mut queued = Vec::new();
        for reply in originals {
            if rng.below(3) == 0 {
                lost += 1;
            } else {
                queued.push((rng.below(4), reply.clone()));
                if rng.below(3) == 0 {
                    queued.push((rng.below(4), reply));
                    duplicates += 1;
                }
            }
        }
        queued.sort_by_key(|(delay, _)| *delay);
        let fault = seed % 6;
        if fault == 0 {
            journal.invalidate(Invalidation::DonorLost);
            cancelled += 1;
        } else if fault == 1 {
            journal.tick(Time(10)).unwrap();
            expired += 1;
        }
        for (_, reply) in queued {
            match reply {
                Reply::Reservation(id) => assert_eq!(id, reservation),
                Reply::Attachment(token) => assert_eq!(token, attachment),
                Reply::Batch(received) => assert_eq!(*received, batch),
                Reply::Ack(received) => assert_eq!(received, through),
            }
        }
        if fault <= 1 {
            if fault == 0 {
                assert!(journal.reserved_for(Time(10), &follower, &cut).is_err());
            } else {
                assert_eq!(journal.reserved_for(Time(10), &follower, &cut), Ok(None));
            }
            assert!(journal.attachment_for(Time(10), &reservation).is_err());
            assert!(
                journal
                    .read_batch(Time(10), &reservation, &barrier)
                    .is_err()
            );
            continue;
        }
        assert_eq!(
            journal.reserved_for(Time(6), &follower, &cut),
            Ok(Some(reservation.clone()))
        );
        assert_eq!(
            journal.attachment_for(Time(6), &reservation),
            Ok(attachment)
        );
        journal
            .append(Time(6), 2, DeltaIdentity::Local(b"b".to_vec()), vec![2])
            .unwrap();
        let next = journal
            .advance_barrier(Time(7), &reservation, &barrier)
            .unwrap();
        assert_eq!(next.cursor.position, 2);
        let next_batch = journal
            .read_batch(Time(7), &reservation, &next)
            .unwrap()
            .unwrap();
        assert_eq!(next_batch.from.position, 1);
        assert_eq!(next_batch.through.position, 2);
        assert_eq!(
            journal.ack_batch(
                Time(7),
                &reservation,
                next_batch.operation,
                &next_batch.through
            ),
            Ok(next_batch.through)
        );
        progressed += 1;
    }
    assert!(lost > 40 && duplicates > 25);
    assert_eq!(expired, 16);
    assert_eq!(cancelled, 16);
    assert_eq!(progressed, 64);
}
