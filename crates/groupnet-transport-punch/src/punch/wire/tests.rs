//! Session datagram codec and payload-bound regressions.

use super::*;

fn key() -> NetworkKey {
    NetworkKey::from_bytes([4; 32])
}

fn packet(body: Body<'_>) -> Packet<'_> {
    Packet {
        sender: "alpha",
        session: [2; 16],
        sequence: 7,
        body,
    }
}

fn control_variants() -> [Body<'static>; 12] {
    let address = SocketAddr::from(([127, 0, 0, 1], 1234));
    [
        Body::Hello { nonce: [3; 16] },
        Body::Challenge {
            nonce: [3; 16],
            cookie: [4; 16],
        },
        Body::Register {
            nonce: [3; 16],
            cookie: [4; 16],
            relay_only: true,
            credential: &[],
        },
        Body::Registered { proof: [9; 16] },
        Body::Discover { proof: [9; 16] },
        Body::Heartbeat { proof: [9; 16] },
        Body::Depart { proof: [9; 16] },
        Body::Denied { proof: [9; 16] },
        Body::Query {
            proof: [9; 16],
            peer: "beta",
        },
        Body::Offer {
            proof: [9; 16],
            peer: "beta",
            session: [5; 16],
            address: Some(address),
            relay_only: false,
            candidates: super::super::wire::CandidateList::empty(),
            secret: [7; 16],
        },
        Body::Offer {
            proof: [9; 16],
            peer: "beta",
            session: [5; 16],
            address: Some("[::1]:1234".parse().unwrap()),
            relay_only: false,
            candidates: super::super::wire::CandidateList::empty(),
            secret: [7; 16],
        },
        Body::Offer {
            proof: [9; 16],
            peer: "beta",
            session: [5; 16],
            address: None,
            relay_only: true,
            candidates: super::super::wire::CandidateList::empty(),
            secret: [7; 16],
        },
    ]
}

fn data_variants(message: &[u8]) -> [Body<'_>; 6] {
    [
        Body::Probe {
            target: [5; 16],
            nonce: [3; 16],
            secret: [7; 16],
        },
        Body::ProbeAck {
            target: [5; 16],
            nonce: [3; 16],
            capability: [8; 16],
            secret: [7; 16],
        },
        Body::Confirm {
            target: [5; 16],
            capability: [8; 16],
            secret: [7; 16],
        },
        Body::Direct {
            peer: "beta",
            target: [5; 16],
            capability: [8; 16],
            message,
            secret: [7; 16],
        },
        Body::Relay {
            proof: [9; 16],
            peer: "beta",
            target: [5; 16],
            message,
        },
        Body::Delivered {
            proof: [9; 16],
            peer: "beta",
            session: [5; 16],
            message: &[],
        },
    ]
}

#[test]
fn every_typed_variant_round_trips_and_truncation_fails_closed() {
    let message = [6; super::super::MAX_MESSAGE];
    for body in control_variants()
        .into_iter()
        .chain(data_variants(&message))
    {
        let mut open = [0; MAX_PACKET];
        let length = encode_mode(packet(body), None, &mut open).unwrap();
        let decoded = decode_mode(&open[..length], None).unwrap();
        let mut round_trip = [0; MAX_PACKET];
        assert_eq!(encode_mode(decoded, None, &mut round_trip), Some(length));
        assert_eq!(&open[..length], &round_trip[..length]);
        let mut buffer = [0; MAX_PACKET];
        let length = encode(packet(body), &key(), &mut buffer).unwrap();
        assert!(length <= MAX_PACKET);
        let decoded = decode(&buffer[..length], &key()).unwrap();
        assert_eq!(decoded.sender, "alpha");
        assert_eq!(decoded.session, [2; 16]);
        assert_eq!(decoded.sequence, 7);
        let mut round_trip = [0; MAX_PACKET];
        assert_eq!(encode(decoded, &key(), &mut round_trip), Some(length));
        assert_eq!(&buffer[..length], &round_trip[..length]);
        for prefix in 0..length {
            assert!(decode(&buffer[..prefix], &key()).is_none());
        }
    }
}

#[test]
fn bad_tags_wrong_keys_and_every_single_byte_tamper_are_rejected() {
    let mut bytes = [0; MAX_PACKET];
    let length = encode(packet(Body::Hello { nonce: [7; 16] }), &key(), &mut bytes).unwrap();
    assert!(decode(&bytes[..length], &NetworkKey::from_bytes([9; 32])).is_none());
    for index in 0..length {
        bytes[index] ^= 1;
        assert!(decode(&bytes[..length], &key()).is_none());
        bytes[index] ^= 1;
    }
    assert!(decode(&[0; MAX_PACKET + 1], &key()).is_none());
}

fn signed(payload: &[u8]) -> Vec<u8> {
    let mut bytes = payload.to_vec();
    bytes.extend_from_slice(hmac::sign(&key().auth, payload).as_ref());
    bytes
}

#[test]
fn authenticated_malformed_payloads_unknown_kinds_flags_and_utf8_fail_closed() {
    let mut buffer = [0; MAX_PACKET];
    let length = encode(
        packet(Body::Register {
            nonce: [7; 16],
            cookie: [8; 16],
            relay_only: false,
            credential: &[],
        }),
        &key(),
        &mut buffer,
    )
    .unwrap();
    let payload = &buffer[..length - TAG];
    for kind in [0, 17, 255] {
        let mut malformed = payload.to_vec();
        malformed[4] = kind;
        assert!(decode(&signed(&malformed), &key()).is_none());
    }
    for name_length in [0, 65, 255] {
        let mut malformed = payload.to_vec();
        malformed[5] = name_length;
        assert!(decode(&signed(&malformed), &key()).is_none());
    }
    let mut malformed = payload.to_vec();
    malformed[6] = 255;
    assert!(decode(&signed(&malformed), &key()).is_none());
    let mut malformed = payload.to_vec();
    malformed[67] = 2; // Relay-only flag follows nonce and cookie.
    assert!(decode(&signed(&malformed), &key()).is_none());
    let mut malformed = payload.to_vec();
    malformed.extend_from_slice(&[0]);
    assert!(decode(&signed(&malformed), &key()).is_none());
    let mut malformed = payload.to_vec();
    malformed[27..35].fill(0); // Sequence after the five-byte name and session.
    assert!(decode(&signed(&malformed), &key()).is_none());
}

#[test]
fn exact_limits_accept_unicode_and_reject_oversized_messages_and_names() {
    let name = "é".repeat(32);
    let message = [0; super::super::MAX_MESSAGE];
    let mut buffer = [0; MAX_PACKET];
    let value = Packet {
        sender: &name,
        session: [1; 16],
        sequence: 1,
        body: Body::Relay {
            proof: [9; 16],
            peer: &name,
            target: [2; 16],
            message: &message,
        },
    };
    let length = encode(value, &key(), &mut buffer).unwrap();
    assert_eq!(decode(&buffer[..length], &key()).unwrap().sender, name);
    let oversized = [0; super::super::MAX_MESSAGE + 1];
    assert!(
        encode(
            packet(Body::Direct {
                peer: "beta",
                target: [0; 16],
                capability: [8; 16],
                message: &oversized,
                secret: [7; 16],
            }),
            &key(),
            &mut buffer
        )
        .is_none()
    );
    let long = "x".repeat(65);
    assert!(
        encode(
            Packet {
                sender: &long,
                ..value
            },
            &key(),
            &mut buffer
        )
        .is_none()
    );
    // Produce an authenticated oversized body manually, without the encoder.
    let length = encode(
        packet(Body::Direct {
            peer: "beta",
            target: [0; 16],
            capability: [8; 16],
            message: &message,
            secret: [7; 16],
        }),
        &key(),
        &mut buffer,
    )
    .unwrap();
    let mut payload = buffer[..length - TAG].to_vec();
    payload.push(0);
    assert!(decode(&signed(&payload), &key()).is_none());
}

#[test]
fn clean_wire_cutover_and_keyed_parsing_never_downgrade() {
    let mut bytes = [0; MAX_PACKET];
    let length = encode_mode(packet(Body::Hello { nonce: [7; 16] }), None, &mut bytes).unwrap();
    assert!(decode_mode(&bytes[..length], Some(&key())).is_none());
    let length = encode(packet(Body::Hello { nonce: [7; 16] }), &key(), &mut bytes).unwrap();
    assert!(decode_mode(&bytes[..length], None).is_none());
    let mut old = bytes[..length - TAG].to_vec();
    old[..4].copy_from_slice(b"GNP2");
    assert!(decode(&signed(&old), &key()).is_none());
    assert!(decode_mode(&old, None).is_none());
}

#[test]
fn maximum_identity_and_payload_fit_all_data_paths_in_both_modes() {
    let name = "x".repeat(64);
    let message = [42; super::super::MAX_MESSAGE];
    let bodies = [
        Body::Direct {
            peer: &name,
            target: [2; 16],
            capability: [3; 16],
            message: &message,
            secret: [7; 16],
        },
        Body::Relay {
            proof: [4; 16],
            peer: &name,
            target: [2; 16],
            message: &message,
        },
        Body::Delivered {
            proof: [4; 16],
            peer: &name,
            session: [2; 16],
            message: &message,
        },
    ];
    for body in bodies {
        for keyed in [false, true] {
            let key = key();
            let mode = keyed.then_some(&key);
            let mut bytes = [0; MAX_PACKET];
            let length = encode_mode(
                Packet {
                    sender: &name,
                    ..packet(body)
                },
                mode,
                &mut bytes,
            )
            .unwrap();
            assert!(length <= MAX_PACKET);
            let decoded = decode_mode(&bytes[..length], mode).unwrap();
            assert!(matches!(decoded.body, Body::Direct { message: got, .. }
                | Body::Relay { message: got, .. }
                | Body::Delivered { message: got, .. } if got == message));
        }
    }
}
