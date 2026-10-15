//! Request-body codec tests for [`super`].

use super::*;
use groupnet_core::volatile_bootstrap::BootstrapOperation;

fn limits() -> PhaseLimits {
    PhaseLimits {
        max_body_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
        max_scope_bytes: std::num::NonZeroUsize::new(32).unwrap(),
        max_node_bytes: std::num::NonZeroUsize::new(32).unwrap(),
        max_writer_bytes: std::num::NonZeroUsize::new(16).unwrap(),
        max_cuts: std::num::NonZeroUsize::new(4).unwrap(),
        max_members: std::num::NonZeroUsize::new(4).unwrap(),
        max_events: std::num::NonZeroUsize::new(4).unwrap(),
        max_identity_bytes: std::num::NonZeroUsize::new(16).unwrap(),
        max_effect_bytes: std::num::NonZeroUsize::new(64).unwrap(),
    }
}

#[test]
fn phase_limits_name_each_bound_its_field_cannot_carry() {
    assert_eq!(limits().validate(), Ok(()));
    assert_eq!(PhaseLimits::default().validate(), Ok(()));
    let wide = nonzero(usize::from(u16::MAX) + 1);
    let cases = [
        (
            PhaseLimits {
                max_node_bytes: wide,
                ..limits()
            },
            PhaseLimitsError::NodeAboveLengthField,
        ),
        (
            PhaseLimits {
                max_writer_bytes: wide,
                ..limits()
            },
            PhaseLimitsError::WriterAboveLengthField,
        ),
        (
            PhaseLimits {
                max_identity_bytes: wide,
                ..limits()
            },
            PhaseLimitsError::IdentityAboveLengthField,
        ),
        (
            PhaseLimits {
                max_cuts: wide,
                ..limits()
            },
            PhaseLimitsError::CutsAboveCountField,
        ),
        (
            PhaseLimits {
                max_members: wide,
                ..limits()
            },
            PhaseLimitsError::MembersAboveCountField,
        ),
        (
            PhaseLimits {
                max_events: wide,
                ..limits()
            },
            PhaseLimitsError::EventsAboveCountField,
        ),
    ];
    for (limits, expected) in cases {
        assert_eq!(limits.validate(), Err(expected));
    }
    // The widest bound is a u32 length; it only binds on 64-bit targets.
    if let Ok(beyond) = usize::try_from(u64::from(u32::MAX) + 1) {
        assert_eq!(
            PhaseLimits {
                max_effect_bytes: nonzero(beyond),
                ..limits()
            }
            .validate(),
            Err(PhaseLimitsError::EffectAboveLengthField)
        );
    }
}

fn identities() -> (Correlation, CaptureId, ReservationId, BarrierReceipt) {
    let scope = BootstrapScope {
        domain: "origin".into(),
        partition: "bucket".into(),
    };
    let donor = ClaimIdentity {
        node: NodeId::from("donor"),
        incarnation: BootId(11),
        session: 12,
        attempt: 13,
    };
    let follower = ClaimIdentity {
        node: NodeId::from("follower"),
        incarnation: BootId(21),
        session: 22,
        attempt: 23,
    };
    let correlation = Correlation {
        scope: scope.clone(),
        donor: donor.clone(),
        follower: follower.clone(),
        parent: BootstrapOperation {
            incarnation: BootId(21),
            session: 22,
            generation: 1,
            token: 31,
        },
        child: BootstrapOperation {
            incarnation: BootId(21),
            session: 22,
            generation: 1,
            token: 32,
        },
    };
    let capture = CaptureId {
        scope,
        donor,
        recovery_generation: 2,
        serial: 3,
    };
    let reservation = ReservationId {
        capture: capture.clone(),
        follower: follower.clone(),
        serial: 4,
    };
    let barrier = BarrierReceipt {
        reservation: reservation.clone(),
        attach_operation: 5,
        barrier_operation: 6,
        cursor: JournalCursor {
            capture: capture.clone(),
            position: 7,
        },
        covered_cuts: vec![NativeCut {
            writer: b"writer".to_vec(),
            epoch: 1,
            sequence: 0,
        }],
        members: vec![
            BootstrapMemberIdentity {
                node: follower.node.clone(),
                presence: Some(PresenceIdentity {
                    node: follower.node,
                    boot: follower.incarnation,
                    session: follower.session,
                }),
                member_incarnation: 1,
                status: Status::Alive,
            },
            BootstrapMemberIdentity {
                node: NodeId::from("quiet"),
                presence: None,
                member_incarnation: 4,
                status: Status::Suspect,
            },
        ],
    };
    (correlation, capture, reservation, barrier)
}

#[test]
fn all_nine_request_bodies_round_trip_with_exact_nested_identity() {
    let (correlation, capture, reservation, barrier) = identities();
    let requests = [
        DonorRequest::Offer {
            max_metadata_bytes: nonzero(100),
        },
        DonorRequest::Reserve {
            follower: correlation.follower.clone(),
            cut: JournalCursor {
                capture: capture.clone(),
                position: 0,
            },
        },
        DonorRequest::Chunk {
            reservation: reservation.clone(),
            sequence: 0,
            max_bytes: nonzero(40),
        },
        DonorRequest::Attach {
            reservation: reservation.clone(),
        },
        DonorRequest::Barrier {
            attachment: AttachToken {
                reservation: reservation.clone(),
                operation: 5,
            },
            max_metadata_bytes: nonzero(100),
        },
        DonorRequest::AdvanceBarrier {
            expected: barrier.clone(),
            max_metadata_bytes: nonzero(100),
        },
        DonorRequest::Batch {
            barrier,
            max_bytes: nonzero(50),
            max_events: nonzero(2),
        },
        DonorRequest::Ack {
            reservation: reservation.clone(),
            batch_operation: 8,
            through: JournalCursor {
                capture,
                position: 7,
            },
        },
        DonorRequest::Release { reservation },
    ];
    for request in requests {
        let frame = encode_request(&request, correlation.clone(), limits()).unwrap();
        assert_eq!(decode_request(&frame, limits()).unwrap(), request);
    }
}

#[test]
fn stale_nested_capture_and_noncanonical_barrier_fail_closed() {
    let (correlation, capture, reservation, mut barrier) = identities();
    let mut wrong = capture.clone();
    wrong.serial += 1;
    wrong.scope.partition = "other".into();
    let request = DonorRequest::Reserve {
        follower: correlation.follower.clone(),
        cut: JournalCursor {
            capture: wrong,
            position: 0,
        },
    };
    assert_eq!(
        encode_request(&request, correlation.clone(), limits()),
        Err(WireError::Invalid)
    );
    barrier.covered_cuts.push(barrier.covered_cuts[0].clone());
    assert_eq!(
        encode_request(
            &DonorRequest::Batch {
                barrier,
                max_bytes: NonZeroUsize::MIN,
                max_events: NonZeroUsize::MIN,
            },
            correlation.clone(),
            limits(),
        ),
        Err(WireError::Invalid)
    );
    let mut frame = encode_request(
        &DonorRequest::Release { reservation },
        correlation,
        limits(),
    )
    .unwrap();
    if let Message::Request(body) = &mut frame.message {
        body.push(0);
    }
    assert_eq!(decode_request(&frame, limits()), Err(WireError::Invalid));
}

/// Every reply budget decodes as nonzero: the boundary value 1 round-trips,
/// and a peer that zeroes any budget field is refused before a request exists.
#[test]
fn zero_reply_budgets_fail_closed_at_decode() {
    let (correlation, _, reservation, barrier) = identities();
    let one = NonZeroUsize::MIN;
    // Each request, with its budget words' offsets back from the body's end.
    let cases = [
        (
            DonorRequest::Offer {
                max_metadata_bytes: one,
            },
            &[8][..],
        ),
        (
            DonorRequest::Chunk {
                reservation: reservation.clone(),
                sequence: 0,
                max_bytes: one,
            },
            &[8][..],
        ),
        (
            DonorRequest::Barrier {
                attachment: AttachToken {
                    reservation,
                    operation: 5,
                },
                max_metadata_bytes: one,
            },
            &[8][..],
        ),
        (
            DonorRequest::AdvanceBarrier {
                expected: barrier.clone(),
                max_metadata_bytes: one,
            },
            &[8][..],
        ),
        (
            DonorRequest::Batch {
                barrier,
                max_bytes: one,
                max_events: one,
            },
            &[16, 8][..],
        ),
    ];
    for (request, budgets) in cases {
        let frame = encode_request(&request, correlation.clone(), limits()).unwrap();
        assert_eq!(decode_request(&frame, limits()), Ok(request));
        for from_end in budgets {
            let mut zeroed = frame.clone();
            let Message::Request(body) = &mut zeroed.message else {
                panic!("an encoded request frame");
            };
            let start = body.len() - from_end;
            assert_eq!(body[start..start + 8], 1_u64.to_be_bytes());
            body[start..start + 8].fill(0);
            assert_eq!(decode_request(&zeroed, limits()), Err(WireError::Invalid));
        }
    }
}
