//! Deterministic session/capability regressions over socket-owning endpoint state.

use std::time::Duration;

use tokio::time::timeout;

use super::super::{DIRECT_LEASE, PeerPath};
use super::*;

const SILENCE: Duration = Duration::from_millis(50);

pub(super) fn loopback() -> SocketAddr {
    ([127, 0, 0, 1], 0).into()
}

pub(super) async fn endpoint(
    policy: PathPolicy,
) -> (
    Endpoint,
    UdpSocket,
    UdpSocket,
    mpsc::Receiver<AdmittedInbound>,
) {
    let relay = UdpSocket::bind(loopback()).await.unwrap();
    let remote = UdpSocket::bind(loopback()).await.unwrap();
    let mut config = PunchConfig::open("local".into(), relay.local_addr().unwrap());
    config.policy = policy;
    let (incoming, receiver) = mpsc::channel(4);
    let mut endpoint = Endpoint {
        config,
        socket: UdpSocket::bind(loopback()).await.unwrap(),
        extra_sockets: Vec::new(),
        local_candidates: super::super::candidates::Candidates::default(),
        observed: Arc::new(Mutex::new(None)),
        announcement: 0,
        session: [1; 16],
        nonce: [2; 16],
        sequence: 0,
        hello: 0,
        registration: 0,
        queries: HashMap::new(),
        discovery: 7,
        heartbeat: 0,
        registered: Some(Instant::now()),
        sessions: SessionRegistry::new(4).unwrap(),
        ready: None,
        peers: Arc::new(Mutex::new(HashMap::new())),
        incoming,
    };
    endpoint.accept_offer(
        control(7, Body::Discover { proof: [2; 16] }),
        "remote",
        [3; 16],
        (policy != PathPolicy::RelayOnly).then(|| remote.local_addr().unwrap()),
        policy == PathPolicy::RelayOnly,
        (wire::CandidateList::empty(), [7; 16]),
    );
    (endpoint, remote, relay, receiver)
}

pub(super) fn packet(sequence: u64, body: Body<'_>) -> Packet<'_> {
    wire::sign_check(Packet {
        sender: "remote",
        session: [3; 16],
        sequence,
        body,
    })
}

pub(super) fn control(sequence: u64, body: Body<'_>) -> Packet<'_> {
    Packet {
        sender: "local",
        session: [1; 16],
        sequence,
        body,
    }
}

fn data(capability: Session) -> Body<'static> {
    Body::Direct {
        peer: "local",
        target: [1; 16],
        capability,
        message: b"payload",
        secret: [7; 16],
    }
}

fn confirmation(capability: Session) -> Body<'static> {
    Body::Confirm {
        target: [1; 16],
        capability,
        secret: [7; 16],
    }
}

fn delivered(proof: Session, message: &[u8]) -> Body<'_> {
    Body::Delivered {
        proof,
        peer: "remote",
        session: [3; 16],
        message,
    }
}

fn challenge(endpoint: &mut Endpoint, address: SocketAddr, sequence: u64) -> Session {
    endpoint.receive(
        packet(
            sequence,
            Body::Probe {
                target: [1; 16],
                nonce: [4; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    lock(&endpoint.peers)["remote"].checks[0]
        .pending
        .unwrap()
        .capability
        .token
}

#[tokio::test]
async fn public_offer_never_authorizes_data_and_forged_packets_do_not_poison_sequences() {
    let (mut endpoint, remote, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let address = remote.local_addr().unwrap();
    endpoint.receive(packet(u64::MAX, data([9; 16])), address);
    assert!(incoming.try_recv().is_err());
    assert_eq!(lock(&endpoint.peers)["remote"].sequence, 0);
    let capability = challenge(&mut endpoint, address, 1);
    // Source, sender identity and both sessions are checked before capability use.
    for forged in [
        Packet {
            sender: "unknown",
            ..packet(2, data(capability))
        },
        Packet {
            session: [8; 16],
            ..packet(2, data(capability))
        },
        packet(
            2,
            Body::Direct {
                peer: "local",
                target: [8; 16],
                capability,
                message: b"wrong session",
                secret: [7; 16],
            },
        ),
        packet(
            2,
            Body::Direct {
                peer: "other",
                target: [1; 16],
                capability,
                message: b"wrong target",
                secret: [7; 16],
            },
        ),
    ] {
        endpoint.receive(forged, address);
    }
    endpoint.receive(packet(2, data(capability)), loopback());
    assert!(incoming.try_recv().is_err());
    endpoint.receive(packet(2, data(capability)), address);
    let received = incoming.try_recv().unwrap();
    assert_eq!(received.packet.msg.as_ref(), b"payload");
    assert!(received.session.is_some());
    assert!(lock(&endpoint.peers)["remote"].checks[0].pending.is_none());
    endpoint.receive(packet(2, data(capability)), address);
    endpoint.receive(packet(3, data([9; 16])), address);
    assert!(incoming.try_recv().is_err());
    assert_eq!(lock(&endpoint.peers)["remote"].sequence, 2);
}

#[tokio::test]
async fn acks_require_fresh_outstanding_nonce_source_and_current_sessions() {
    let (mut endpoint, remote, _, _) = endpoint(PathPolicy::DirectPreferred).await;
    let address = remote.local_addr().unwrap();
    let ack = Body::ProbeAck {
        target: [1; 16],
        nonce: [5; 16],
        capability: [6; 16],
        secret: [7; 16],
    };
    endpoint.receive(packet(100, ack), address);
    assert_eq!(
        lock(&endpoint.peers)["remote"].path(Instant::now()),
        Some(PeerPath::Relay)
    );
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[0].probe = Some(Capability {
        token: [5; 16],
        issued: Instant::now() - DIRECT_LEASE,
    });
    endpoint.receive(packet(100, ack), address);
    assert!(lock(&endpoint.peers)["remote"].checks[0].direct.is_none());
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[0].probe = Some(Capability {
        token: [5; 16],
        issued: Instant::now(),
    });
    endpoint.receive(packet(100, ack), loopback());
    endpoint.receive(
        Packet {
            session: [8; 16],
            ..packet(100, ack)
        },
        address,
    );
    endpoint.receive(
        packet(
            100,
            Body::ProbeAck {
                target: [8; 16],
                nonce: [5; 16],
                capability: [6; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    endpoint.receive(
        packet(
            100,
            Body::ProbeAck {
                target: [1; 16],
                nonce: [9; 16],
                capability: [6; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    assert!(lock(&endpoint.peers)["remote"].checks[0].direct.is_none());
    endpoint.receive(packet(100, ack), address);
    let issued = lock(&endpoint.peers)["remote"].checks[0]
        .direct
        .unwrap()
        .issued;
    assert_eq!(
        lock(&endpoint.peers)["remote"].path(Instant::now()),
        Some(PeerPath::Direct)
    );
    endpoint.receive(packet(101, ack), address);
    assert_eq!(
        lock(&endpoint.peers)["remote"].checks[0]
            .direct
            .unwrap()
            .issued,
        issued
    );
    assert_eq!(lock(&endpoint.peers)["remote"].sequence, 0);
}

#[tokio::test]
async fn refresh_is_bounded_and_unproved_probes_cannot_replace_confirmed_capabilities() {
    let (mut endpoint, remote, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let address = remote.local_addr().unwrap();
    let first = challenge(&mut endpoint, address, 1);
    endpoint.receive(packet(2, confirmation(first)), address);
    endpoint.receive(
        packet(
            3,
            Body::Probe {
                target: [1; 16],
                nonce: [5; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    let pending = lock(&endpoint.peers)["remote"].checks[0].pending.unwrap();
    for sequence in [4, 100, u64::MAX] {
        endpoint.receive(
            packet(
                sequence,
                Body::Probe {
                    target: [1; 16],
                    nonce: [7; 16],
                    secret: [7; 16],
                },
            ),
            address,
        );
    }
    assert_eq!(
        lock(&endpoint.peers)["remote"].checks[0]
            .pending
            .unwrap()
            .capability
            .token,
        pending.capability.token
    );
    assert_eq!(
        lock(&endpoint.peers)["remote"].checks[0]
            .confirmed
            .unwrap()
            .token,
        first
    );
    assert_eq!(lock(&endpoint.peers)["remote"].sequence, 0);
    endpoint.receive(packet(4, data(first)), address);
    assert_eq!(incoming.try_recv().unwrap().packet.msg.as_ref(), b"payload");
    endpoint.receive(packet(5, confirmation([9; 16])), address);
    assert_eq!(
        lock(&endpoint.peers)["remote"].checks[0]
            .confirmed
            .unwrap()
            .token,
        first
    );
    endpoint.receive(packet(6, confirmation(pending.capability.token)), address);
    endpoint.receive(packet(7, data(first)), address);
    assert!(incoming.try_recv().is_err());
    endpoint.receive(packet(7, data(pending.capability.token)), address);
    assert!(incoming.try_recv().is_ok());
    // Consumed confirmation and stale probes cannot renew or replace state.
    endpoint.receive(packet(8, confirmation(first)), address);
    endpoint.receive(
        packet(
            1,
            Body::Probe {
                target: [1; 16],
                nonce: [4; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    assert!(lock(&endpoint.peers)["remote"].checks[0].pending.is_none());
    assert_eq!(
        lock(&endpoint.peers)["remote"].checks[0]
            .confirmed
            .unwrap()
            .token,
        pending.capability.token
    );
}

#[tokio::test]
async fn first_unproven_high_sequence_probe_cannot_poison_a_working_receive_path() {
    let (mut endpoint, remote, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let address = remote.local_addr().unwrap();
    let confirmed = challenge(&mut endpoint, address, 1);
    endpoint.receive(
        packet(
            2,
            Body::Confirm {
                target: [1; 16],
                capability: confirmed,
                secret: [7; 16],
            },
        ),
        address,
    );
    endpoint.receive(
        packet(
            u64::MAX,
            Body::Probe {
                target: [1; 16],
                nonce: [7; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    assert_eq!(
        lock(&endpoint.peers)["remote"].checks[0]
            .confirmed
            .unwrap()
            .token,
        confirmed
    );
    assert_eq!(lock(&endpoint.peers)["remote"].checks[0].confirmed_probe, 1);
    assert_eq!(lock(&endpoint.peers)["remote"].sequence, 0);
    endpoint.receive(packet(3, data(confirmed)), address);
    assert!(incoming.try_recv().is_ok());
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[0]
        .pending
        .as_mut()
        .unwrap()
        .capability
        .issued = Instant::now() - DIRECT_LEASE;
    endpoint.receive(
        packet(
            4,
            Body::Probe {
                target: [1; 16],
                nonce: [8; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    let pending = lock(&endpoint.peers)["remote"].checks[0].pending.unwrap();
    assert_eq!(pending.sequence, 4);
    endpoint.receive(
        packet(
            5,
            Body::Confirm {
                target: [1; 16],
                capability: pending.capability.token,
                secret: [7; 16],
            },
        ),
        address,
    );
    assert_eq!(lock(&endpoint.peers)["remote"].checks[0].confirmed_probe, 4);
    endpoint.receive(packet(6, data(pending.capability.token)), address);
    assert!(incoming.try_recv().is_ok());
}

#[tokio::test]
async fn expired_pending_and_confirmed_tokens_and_replaced_sessions_fail_closed() {
    let (mut endpoint, remote, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let address = remote.local_addr().unwrap();
    let capability = challenge(&mut endpoint, address, 1);
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[0]
        .pending
        .as_mut()
        .unwrap()
        .capability
        .issued = Instant::now() - DIRECT_LEASE;
    endpoint.receive(
        packet(
            2,
            Body::Confirm {
                target: [1; 16],
                capability,
                secret: [7; 16],
            },
        ),
        address,
    );
    endpoint.receive(packet(3, data(capability)), address);
    assert!(incoming.try_recv().is_err());
    endpoint.receive(
        packet(
            1,
            Body::Probe {
                target: [1; 16],
                nonce: [4; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    assert_eq!(
        lock(&endpoint.peers)["remote"].checks[0]
            .pending
            .unwrap()
            .capability
            .token,
        capability
    );
    endpoint.receive(
        packet(
            4,
            Body::Probe {
                target: [1; 16],
                nonce: [5; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    let fresh = lock(&endpoint.peers)["remote"].checks[0]
        .pending
        .unwrap()
        .capability
        .token;
    endpoint.receive(
        packet(
            5,
            Body::Confirm {
                target: [1; 16],
                capability: fresh,
                secret: [7; 16],
            },
        ),
        address,
    );
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[0]
        .confirmed
        .as_mut()
        .unwrap()
        .issued = Instant::now() - DIRECT_LEASE;
    endpoint.receive(packet(6, data(fresh)), address);
    assert!(incoming.try_recv().is_err());
    let old = lock(&endpoint.peers)["remote"].lease.clone();
    endpoint.accept_offer(
        control(7, Body::Discover { proof: [2; 16] }),
        "remote",
        [8; 16],
        Some(address),
        false,
        (wire::CandidateList::empty(), [7; 16]),
    );
    assert!(!old.is_active());
    assert!(
        lock(&endpoint.peers)["remote"].checks[0]
            .confirmed
            .is_none()
    );
    endpoint.receive(packet(7, data(fresh)), address);
    assert!(incoming.try_recv().is_err());
}

#[tokio::test]
async fn private_control_proof_is_required_even_with_public_identity_session_and_correlation() {
    let (mut endpoint, remote, relay, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let address = relay.local_addr().unwrap();
    endpoint.heartbeat = 10;
    endpoint.registration = 11;
    endpoint.receive(control(11, Body::Denied { proof: [1; 16] }), address);
    assert_eq!(endpoint.registration, 11);
    endpoint.receive(
        control(
            6,
            Body::Offer {
                proof: [2; 16],
                peer: "stale",
                session: [8; 16],
                address: Some(remote.local_addr().unwrap()),
                relay_only: false,
                candidates: super::super::wire::CandidateList::empty(),
                secret: [7; 16],
            },
        ),
        address,
    );
    assert!(!lock(&endpoint.peers).contains_key("stale"));
    let seen = endpoint.registered.unwrap();
    endpoint.receive(control(10, Body::Registered { proof: [1; 16] }), address);
    assert_eq!(endpoint.registered, Some(seen));
    assert_eq!(endpoint.heartbeat, 10);
    endpoint.receive(
        control(
            7,
            Body::Offer {
                proof: [1; 16],
                peer: "forged",
                session: [8; 16],
                address: Some(remote.local_addr().unwrap()),
                relay_only: false,
                candidates: super::super::wire::CandidateList::empty(),
                secret: [7; 16],
            },
        ),
        address,
    );
    assert!(!lock(&endpoint.peers).contains_key("forged"));
    endpoint.receive(control(100, delivered([1; 16], b"forged")), address);
    assert!(incoming.try_recv().is_err());
    assert_eq!(lock(&endpoint.peers)["remote"].sequence, 0);
    endpoint.receive(control(10, Body::Registered { proof: [2; 16] }), address);
    assert_eq!(endpoint.heartbeat, 0);
    endpoint.receive(control(1, delivered([2; 16], b"real")), address);
    assert_eq!(incoming.try_recv().unwrap().packet.msg.as_ref(), b"real");
    endpoint.receive(control(1, delivered([2; 16], b"replay")), address);
    assert!(incoming.try_recv().is_err());
    endpoint.nonce = [9; 16];
    endpoint.heartbeat = 10;
    endpoint.receive(control(10, Body::Registered { proof: [2; 16] }), address);
    assert_eq!(endpoint.heartbeat, 10);
}

#[tokio::test]
async fn relay_only_and_absent_offers_never_send_or_accept_direct_packets() {
    let (mut endpoint, remote, _, mut incoming) = endpoint(PathPolicy::RelayOnly).await;
    let address = remote.local_addr().unwrap();
    endpoint.probe(&"remote".into());
    endpoint.receive(
        packet(
            1,
            Body::Probe {
                target: [1; 16],
                nonce: [4; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    endpoint.receive(packet(2, data([5; 16])), address);
    assert!(incoming.try_recv().is_err());
    assert!(lock(&endpoint.peers)["remote"].checks.is_empty());
    let mut bytes = [0; MAX_PACKET];
    assert!(
        timeout(SILENCE, remote.recv_from(&mut bytes))
            .await
            .is_err()
    );
    endpoint.accept_offer(
        control(7, Body::Discover { proof: [2; 16] }),
        "bad",
        [8; 16],
        Some(address),
        false,
        (wire::CandidateList::empty(), [7; 16]),
    );
    endpoint.accept_offer(
        control(7, Body::Discover { proof: [2; 16] }),
        "bad",
        [8; 16],
        None,
        false,
        (wire::CandidateList::empty(), [7; 16]),
    );
    assert!(!lock(&endpoint.peers).contains_key("bad"));
    endpoint.config.policy = PathPolicy::DirectPreferred;
    endpoint.probe(&"remote".into());
    assert!(
        timeout(SILENCE, remote.recv_from(&mut bytes))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn direct_expiry_routes_the_next_message_to_the_relay_without_a_new_key_or_session() {
    let (mut endpoint, remote, relay, _) = endpoint(PathPolicy::DirectPreferred).await;
    let lease = lock(&endpoint.peers)["remote"].lease.id();
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[0].direct = Some(Capability {
        token: [6; 16],
        issued: Instant::now(),
    });
    let message = Outbound {
        to: "remote".into(),
        session: lease,
        target: [3; 16],
        message: Bytes::from_static(b"payload"),
    };
    endpoint.send_message(&message);
    let mut bytes = [0; MAX_PACKET];
    let (length, _) = timeout(Duration::from_secs(1), remote.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(wire::decode_mode(&bytes[..length], None).unwrap().body, Body::Direct { capability, .. } if capability == [6; 16])
    );
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[0]
        .direct
        .as_mut()
        .unwrap()
        .issued = Instant::now() - DIRECT_LEASE;
    assert_eq!(
        lock(&endpoint.peers)["remote"].path(Instant::now()),
        Some(PeerPath::Relay)
    );
    endpoint.send_message(&message);
    let (length, _) = timeout(Duration::from_secs(1), relay.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(wire::decode_mode(&bytes[..length], None).unwrap().body, Body::Relay { target, message: b"payload", .. } if target == [3; 16])
    );
    assert!(
        timeout(SILENCE, remote.recv_from(&mut bytes))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn configured_peer_cap_preserves_existing_peer_and_reclaims_revoked_slot() {
    let (mut endpoint, _, _, _) = endpoint(PathPolicy::RelayOnly).await;
    endpoint.config.max_peers = 1;
    let offer = control(7, Body::Discover { proof: [2; 16] });
    endpoint.accept_offer(
        offer,
        "overflow",
        [8; 16],
        None,
        true,
        (wire::CandidateList::empty(), [9; 16]),
    );
    assert_eq!(lock(&endpoint.peers).len(), 1);
    assert!(lock(&endpoint.peers).contains_key("remote"));
    lock(&endpoint.peers)["remote"].lease.revoke();
    endpoint.accept_offer(
        offer,
        "overflow",
        [8; 16],
        None,
        true,
        (wire::CandidateList::empty(), [9; 16]),
    );
    assert_eq!(lock(&endpoint.peers).len(), 1);
    assert!(lock(&endpoint.peers).contains_key("overflow"));
}

#[tokio::test]
async fn inbound_backpressure_drops_without_replaying_or_stalling_the_next_packet() {
    let (mut endpoint, _, relay, _) = endpoint(PathPolicy::RelayOnly).await;
    let (incoming, mut receiver) = mpsc::channel(1);
    endpoint.incoming = incoming;
    let address = relay.local_addr().unwrap();
    endpoint.receive(control(1, delivered([2; 16], b"first")), address);
    endpoint.receive(control(2, delivered([2; 16], b"dropped")), address);
    let first = receiver.try_recv().unwrap();
    assert_eq!(first.packet.msg.as_ref(), b"first");
    assert!(first.session.is_some());
    endpoint.receive(control(2, delivered([2; 16], b"replayed")), address);
    assert!(receiver.try_recv().is_err());
    endpoint.receive(control(3, delivered([2; 16], b"next")), address);
    assert_eq!(receiver.try_recv().unwrap().packet.msg.as_ref(), b"next");
}
