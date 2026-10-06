//! Candidate lists and session-bound check authentication regressions.

use super::*;

fn packet(body: Body<'_>) -> Packet<'_> {
    Packet {
        sender: "a",
        session: [1; 16],
        sequence: 1,
        body,
    }
}

#[test]
fn full_ipv6_candidate_list_round_trips_in_both_authentication_modes() {
    let mut candidates = Candidates::default();
    for port in 1..=8 {
        assert!(candidates.insert(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), port)));
    }
    assert!(!candidates.insert("[::1]:9".parse().unwrap()));
    let key = NetworkKey::from_bytes([1; 32]);
    for mode in [None, Some(&key)] {
        for body in [
            Body::Candidates {
                proof: [2; 16],
                candidates: (&candidates).into(),
            },
            Body::Offer {
                proof: [2; 16],
                peer: "b",
                session: [3; 16],
                address: Some("[::1]:1".parse().unwrap()),
                relay_only: false,
                candidates: (&candidates).into(),
                secret: [4; 16],
            },
        ] {
            let mut bytes = [0; MAX_PACKET];
            let length = encode_mode(packet(body), mode, &mut bytes).unwrap();
            match decode_mode(&bytes[..length], mode).unwrap().body {
                Body::Candidates {
                    candidates: received,
                    ..
                }
                | Body::Offer {
                    candidates: received,
                    ..
                } => assert!(received.iter().eq(candidates.iter())),
                _ => panic!("candidate body changed"),
            }
        }
    }
}

#[test]
fn advertised_endpoint_observing_a_check_never_receives_the_pair_key() {
    let secret = [7; 16];
    let check = packet(Body::Probe {
        target: [3; 16],
        nonce: [4; 16],
        secret,
    });
    let signed = sign_check(check);
    let Body::Probe {
        secret: observed, ..
    } = signed.body
    else {
        unreachable!()
    };
    assert_ne!(observed, secret);
    assert_eq!(observed, check_proof(secret, signed));
    for changed in [
        Packet {
            sequence: 2,
            ..signed
        },
        Packet {
            session: [8; 16],
            ..signed
        },
        Packet {
            sender: "other",
            ..signed
        },
        Packet {
            body: Body::Probe {
                target: [8; 16],
                nonce: [4; 16],
                secret: observed,
            },
            ..signed
        },
    ] {
        assert_ne!(observed, check_proof(secret, changed));
    }
    assert_ne!(observed, check_proof([8; 16], signed));
    let ack = packet(Body::ProbeAck {
        target: [3; 16],
        nonce: [4; 16],
        capability: [9; 16],
        secret,
    });
    assert_ne!(check_proof(secret, ack), observed);
    assert_ne!(check_proof(observed, ack), check_proof(secret, ack));
}

#[test]
fn oversized_and_duplicate_candidate_lists_fail_closed() {
    let mut candidates = Candidates::default();
    candidates.insert("127.0.0.1:9".parse().unwrap());
    let mut bytes = [0; MAX_PACKET];
    let length = encode_mode(
        packet(Body::Candidates {
            proof: [2; 16],
            candidates: (&candidates).into(),
        }),
        None,
        &mut bytes,
    )
    .unwrap();
    // Header is 4 magic + kind + name length/name + session + sequence, then proof.
    let count = 4 + 1 + 2 + 16 + 8 + 16;
    bytes[count] = 9;
    assert!(decode_mode(&bytes[..length], None).is_none());
    bytes[count] = 2;
    let address_size = length - count - 1;
    bytes.copy_within(count + 1..length, length);
    assert!(decode_mode(&bytes[..length + address_size], None).is_none());
}

#[test]
fn complete_direct_and_confirm_proofs_bind_payload_capability_names_sessions_and_sequence() {
    let secret = [7; 16];
    let value = packet(Body::Direct {
        peer: "b",
        target: [3; 16],
        capability: [4; 16],
        message: b"original",
        secret,
    });
    let signed = sign_check(value);
    let Body::Direct { secret: proof, .. } = signed.body else {
        unreachable!()
    };
    assert_eq!(proof, check_proof(secret, signed));
    for body in [
        Body::Direct {
            peer: "b",
            target: [3; 16],
            capability: [4; 16],
            message: b"injected",
            secret: proof,
        },
        Body::Direct {
            peer: "other",
            target: [3; 16],
            capability: [4; 16],
            message: b"original",
            secret: proof,
        },
        Body::Direct {
            peer: "b",
            target: [8; 16],
            capability: [4; 16],
            message: b"original",
            secret: proof,
        },
        Body::Direct {
            peer: "b",
            target: [3; 16],
            capability: [8; 16],
            message: b"original",
            secret: proof,
        },
        Body::Confirm {
            target: [3; 16],
            capability: [4; 16],
            secret: proof,
        },
    ] {
        assert_ne!(proof, check_proof(secret, Packet { body, ..signed }));
    }
    assert_ne!(
        proof,
        check_proof(
            secret,
            Packet {
                sequence: u64::MAX,
                ..signed
            }
        )
    );
    assert_ne!(
        proof,
        check_proof(
            secret,
            Packet {
                sender: "other",
                ..signed
            }
        )
    );
    assert_ne!(
        proof,
        check_proof(
            secret,
            Packet {
                session: [8; 16],
                ..signed
            }
        )
    );
}

#[test]
fn confirmation_proof_binds_its_sequence() {
    let secret = [7; 16];
    let confirmation = sign_check(packet(Body::Confirm {
        target: [3; 16],
        capability: [4; 16],
        secret,
    }));
    let Body::Confirm {
        secret: confirmed_proof,
        ..
    } = confirmation.body
    else {
        unreachable!()
    };
    assert_eq!(confirmed_proof, check_proof(secret, confirmation));
    assert_ne!(
        confirmed_proof,
        check_proof(
            secret,
            Packet {
                sequence: 2,
                ..confirmation
            }
        )
    );
}

#[test]
fn maximum_direct_payload_and_identity_names_still_fit_with_pair_and_fabric_macs() {
    let name = "x".repeat(64);
    let message = [0; super::super::MAX_MESSAGE];
    let key = NetworkKey::from_bytes([8; 32]);
    let value = sign_check(Packet {
        sender: &name,
        session: [1; 16],
        sequence: 1,
        body: Body::Direct {
            peer: &name,
            target: [3; 16],
            capability: [4; 16],
            message: &message,
            secret: [7; 16],
        },
    });
    let mut bytes = [0; MAX_PACKET];
    let length = encode_mode(value, Some(&key), &mut bytes).unwrap();
    assert_eq!(length, 1199);
    assert!(
        matches!(decode_mode(&bytes[..length], Some(&key)).unwrap().body,
        Body::Direct { message: received, .. } if received == message)
    );
}
