//! Controlled-Instant regressions for relay budget, capacity, and attempt ownership.

use groupnet_transport::link::LinkFuture;
use tokio::time::timeout;

use super::*;

fn loopback() -> SocketAddr {
    ([127, 0, 0, 1], 0).into()
}

fn registration(now: Instant, address: SocketAddr) -> Registration {
    Registration {
        address,
        session: [7; 16],
        proof: [9; 16],
        sequence: 1,
        seen: now,
        relay_only: true,
        candidates: Candidates::default(),
    }
}

#[test]
fn invalid_session_traffic_never_spends_any_established_session_allowance() {
    let now = Instant::now();
    let address: SocketAddr = ([127, 0, 0, 1], 1234).into();
    let mut entry = Entry::new(now);
    entry.active = Some(registration(now, address));
    let packet = Packet {
        sender: "incumbent",
        session: [7; 16],
        sequence: 2,
        body: Body::Heartbeat { proof: [9; 16] },
    };
    for _ in 0..4096 {
        assert!(
            entry
                .authenticate(
                    Packet {
                        session: [8; 16],
                        ..packet
                    },
                    address,
                    now
                )
                .is_none()
        );
        assert!(entry.authenticate(packet, loopback(), now).is_none());
    }
    assert_eq!(entry.rate_count, 0);
    for sequence in 2..=u64::from(PACKETS_PER_INTERVAL) + 1 {
        assert!(
            entry
                .authenticate(Packet { sequence, ..packet }, address, now)
                .is_some()
        );
    }
    assert!(
        entry
            .authenticate(
                Packet {
                    sequence: 258,
                    ..packet
                },
                address,
                now
            )
            .is_none()
    );
    assert_eq!(entry.rate_count, PACKETS_PER_INTERVAL);
    assert!(
        entry
            .authenticate(
                Packet {
                    sequence: 258,
                    ..packet
                },
                address,
                now + RATE_INTERVAL
            )
            .is_some()
    );
}

#[test]
fn renewable_unproved_pool_is_bounded_and_evictable_without_consuming_peer_slots() {
    let now = Instant::now();
    let address: SocketAddr = ([127, 0, 0, 1], 1234).into();
    let mut state = ServerState {
        entries: HashMap::new(),
        challenges: Challenges::default(),
        decisions: JoinSet::new(),
        pre_admission: Entry::new(now),
    };
    for sequence in [1, 2] {
        let issued = now + Duration::from_secs(sequence - 1);
        for index in 0..MAX_PEERS {
            let name = format!("unproved-{index}");
            let packet = Packet {
                sender: &name,
                session: [1; 16],
                sequence,
                body: Body::Hello { nonce: [2; 16] },
            };
            assert!(
                state
                    .challenges
                    .issue(packet, address, [2; 16], issued)
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(state.challenges.pending.len(), MAX_PEERS);
        assert!(state.entries.is_empty());
    }
    let issued = now + Duration::from_secs(2);
    let hello = Packet {
        sender: "legitimate",
        session: [3; 16],
        sequence: 1,
        body: Body::Hello { nonce: [4; 16] },
    };
    let Body::Challenge { nonce, cookie } = state
        .challenges
        .issue(hello, address, [4; 16], issued)
        .unwrap()
        .unwrap()
    else {
        panic!("expected challenge");
    };
    assert_eq!(state.challenges.pending.len(), MAX_PEERS);
    let proven = state
        .challenges
        .prove(
            Packet {
                sequence: 2,
                body: Body::Register {
                    nonce,
                    cookie,
                    relay_only: true,
                    credential: &[],
                },
                ..hello
            },
            address,
            issued,
        )
        .unwrap();
    assert_eq!(proven.session, [3; 16]);
    assert!(state.entries.is_empty());
    assert_eq!(state.challenges.pending.len(), MAX_PEERS - 1);
}

#[tokio::test]
async fn late_completion_with_reused_wire_session_cannot_clear_a_new_attempt() {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    let recipient = UdpSocket::bind(loopback()).await.unwrap();
    let address = recipient.local_addr().unwrap();
    let now = Instant::now();
    let old_issued = now - CHALLENGE_TTL - Duration::from_secs(1);
    let new_issued = now - Duration::from_secs(1);
    let mut entries = HashMap::new();
    let mut old = Entry::new(old_issued);
    old.admitting = Some(([7; 16], old_issued));
    entries.insert("same-id".to_owned(), old);
    expire_entries(&mut entries, old_issued + CHALLENGE_TTL);
    assert!(entries.is_empty());
    let mut replacement = Entry::new(new_issued);
    replacement.admitting = Some(([7; 16], new_issued));
    entries.insert("same-id".to_owned(), replacement);
    let old_decision = Decision {
        node: "same-id".into(),
        registration: registration(old_issued, address),
        accepted: Ok(AcceptedPeer {
            node: "same-id".into(),
        }),
    };
    apply_decision(&socket, None, &mut entries, old_decision).await;
    assert_eq!(entries["same-id"].admitting, Some(([7; 16], new_issued)));
    assert!(entries["same-id"].active.is_none());
    let new_decision = Decision {
        node: "same-id".into(),
        registration: registration(new_issued, address),
        accepted: Ok(AcceptedPeer {
            node: "same-id".into(),
        }),
    };
    apply_decision(&socket, None, &mut entries, new_decision).await;
    assert!(entries["same-id"].admitting.is_none());
    assert!(entries["same-id"].active.is_some());
    let mut bytes = [0; MAX_PACKET];
    let (length, _) = timeout(Duration::from_secs(1), recipient.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        wire::decode_mode(&bytes[..length], None).unwrap().body,
        Body::Registered { .. }
    ));
}

#[derive(Debug)]
struct Block;

impl Admission for Block {
    fn admit<'a>(&'a self, _: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn delayed_policy_task_uses_the_received_attempt_deadline() {
    let now = Instant::now();
    let registration = registration(now - CHALLENGE_TTL, loopback());
    let packet = Packet {
        sender: "delayed",
        session: registration.session,
        sequence: 1,
        body: Body::Register {
            nonce: [1; 16],
            cookie: [2; 16],
            relay_only: true,
            credential: &[],
        },
    };
    let policy: Arc<dyn Admission> = Arc::new(Block);
    let mut decisions = JoinSet::new();
    spawn_admission(&mut decisions, &policy, packet, registration, &[]);
    let decision = timeout(Duration::from_secs(1), decisions.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        decision.accepted.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[tokio::test]
async fn public_session_and_spoofed_source_do_not_authorize_depart_relay_or_sequence_poisoning() {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    let incumbent = UdpSocket::bind(loopback()).await.unwrap();
    let recipient = UdpSocket::bind(loopback()).await.unwrap();
    let address = incumbent.local_addr().unwrap();
    let now = Instant::now();
    let mut entries = HashMap::new();
    let mut entry = Entry::new(now);
    entry.active = Some(registration(now, address));
    entries.insert("incumbent".to_owned(), entry);
    let mut target = Entry::new(now);
    target.active = Some(Registration {
        session: [8; 16],
        proof: [10; 16],
        ..registration(now, recipient.local_addr().unwrap())
    });
    entries.insert("recipient".to_owned(), target);
    for body in [
        Body::Depart { proof: [7; 16] },
        Body::Heartbeat { proof: [7; 16] },
        Body::Discover { proof: [7; 16] },
        Body::Query {
            proof: [7; 16],
            peer: "recipient",
        },
        Body::Relay {
            proof: [7; 16],
            peer: "recipient",
            target: [8; 16],
            message: b"forged",
        },
    ] {
        handle_established(
            &socket,
            None,
            &mut entries,
            Packet {
                sender: "incumbent",
                session: [7; 16],
                sequence: u64::MAX,
                body,
            },
            address,
            now + Duration::from_secs(1),
        )
        .await;
        let entry = &entries["incumbent"];
        assert_eq!(entry.rate_count, 0);
        assert_eq!(entry.active.unwrap().sequence, 1);
        assert_eq!(entry.active.unwrap().seen, now);
    }
    let mut bytes = [0; MAX_PACKET];
    assert!(
        timeout(Duration::from_millis(50), recipient.recv_from(&mut bytes))
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(50), incumbent.recv_from(&mut bytes))
            .await
            .is_err()
    );
    handle_established(
        &socket,
        None,
        &mut entries,
        Packet {
            sender: "incumbent",
            session: [7; 16],
            sequence: 2,
            body: Body::Relay {
                proof: [9; 16],
                peer: "recipient",
                target: [8; 16],
                message: b"legitimate",
            },
        },
        address,
        now + Duration::from_secs(1),
    )
    .await;
    let (length, _) = timeout(Duration::from_secs(1), recipient.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(wire::decode_mode(&bytes[..length], None).unwrap().body,
        Body::Delivered { proof, message: b"legitimate", .. } if proof == [10; 16])
    );
    assert_eq!(entries["incumbent"].active.unwrap().sequence, 2);
}

#[tokio::test]
async fn discovery_only_discloses_addresses_when_both_participants_allow_direct_paths() {
    let socket = UdpSocket::bind(loopback()).await.unwrap();
    let requester = UdpSocket::bind(loopback()).await.unwrap();
    let peer_address: SocketAddr = ([127, 0, 0, 1], 4567).into();
    for requester_relay in [false, true] {
        for peer_relay in [false, true] {
            let now = Instant::now();
            let mut entries = HashMap::new();
            let mut entry = Entry::new(now);
            entry.active = Some(Registration {
                relay_only: requester_relay,
                ..registration(now, requester.local_addr().unwrap())
            });
            entries.insert("requester".to_owned(), entry);
            let mut peer = Entry::new(now);
            peer.active = Some(Registration {
                session: [8; 16],
                proof: [10; 16],
                relay_only: peer_relay,
                ..registration(now, peer_address)
            });
            entries.insert("peer".to_owned(), peer);
            handle_established(
                &socket,
                None,
                &mut entries,
                Packet {
                    sender: "requester",
                    session: [7; 16],
                    sequence: 2,
                    body: Body::Discover { proof: [9; 16] },
                },
                requester.local_addr().unwrap(),
                now,
            )
            .await;
            let mut bytes = [0; MAX_PACKET];
            let (length, _) = timeout(Duration::from_secs(1), requester.recv_from(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            let Body::Offer {
                proof,
                session,
                address,
                relay_only,
                ..
            } = wire::decode_mode(&bytes[..length], None).unwrap().body
            else {
                panic!("expected pair discovery offer");
            };
            assert_eq!(proof, [9; 16]); // Recipient's private proof, not peer session or proof.
            assert_eq!(session, [8; 16]);
            assert_eq!(relay_only, requester_relay || peer_relay);
            assert_eq!(
                address,
                (!(requester_relay || peer_relay)).then_some(peer_address)
            );
        }
    }
}
