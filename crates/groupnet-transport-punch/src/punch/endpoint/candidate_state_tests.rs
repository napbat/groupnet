//! Candidate selection, failover, rate bounds, and admission lease regressions.

use super::super::{DIRECT_LEASE, PeerPath};
use super::tests::{control, endpoint, loopback, packet};
use super::*;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn candidate_checks_use_independent_nonces_and_additional_socket_then_fail_over() {
    let (mut endpoint, silent, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let reachable = UdpSocket::bind(loopback()).await.unwrap();
    let extra = UdpSocket::bind(loopback()).await.unwrap();
    let extra_address = extra.local_addr().unwrap();
    endpoint.extra_sockets.push(extra);
    let mut candidates = super::super::candidates::Candidates::default();
    candidates.insert(silent.local_addr().unwrap());
    candidates.insert(reachable.local_addr().unwrap());
    let checks = endpoint.make_checks(candidates);
    lock(&endpoint.peers).get_mut("remote").unwrap().checks = checks;
    endpoint.probe(&"remote".into());
    let nonce = lock(&endpoint.peers)["remote"].checks[3]
        .probe
        .unwrap()
        .token;
    let first_nonce = lock(&endpoint.peers)["remote"].checks[0]
        .probe
        .unwrap()
        .token;
    assert_ne!(first_nonce, nonce);
    endpoint.receive_on(
        packet(
            8,
            Body::ProbeAck {
                target: [1; 16],
                nonce,
                capability: [6; 16],
                secret: [7; 16],
            },
        ),
        reachable.local_addr().unwrap(),
        1,
    );
    {
        let peers = lock(&endpoint.peers);
        let peer = &peers["remote"];
        assert_eq!(peer.selected, Some(3));
        assert_eq!(peer.direct_path(Instant::now()).unwrap().socket, 1);
        assert!(peer.checks[0].direct.is_none());
    }
    let lease = lock(&endpoint.peers)["remote"].lease.id();
    endpoint.send_message(&Outbound {
        to: "remote".into(),
        session: lease,
        target: [3; 16],
        message: Bytes::from_static(b"extra socket"),
    });
    let mut bytes = [0; MAX_PACKET + 1];
    timeout(Duration::from_secs(1), async {
        loop {
            let (length, source) = reachable.recv_from(&mut bytes).await.unwrap();
            if matches!(wire::decode_mode(&bytes[..length], None).unwrap().body, Body::Direct { message, .. } if message == b"extra socket") {
                assert_eq!(source, extra_address);
                break;
            }
        }
    }).await.unwrap();
    // A later healthy alternative cannot oscillate the current selection.
    let alternative_nonce = lock(&endpoint.peers)["remote"].checks[0]
        .probe
        .unwrap()
        .token;
    endpoint.receive(
        packet(
            9,
            Body::ProbeAck {
                target: [1; 16],
                nonce: alternative_nonce,
                capability: [8; 16],
                secret: [7; 16],
            },
        ),
        silent.local_addr().unwrap(),
    );
    assert_eq!(lock(&endpoint.peers)["remote"].selected, Some(3));
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[3]
        .direct
        .as_mut()
        .unwrap()
        .issued = Instant::now() - DIRECT_LEASE;
    endpoint.probe(&"remote".into());
    assert_eq!(lock(&endpoint.peers)["remote"].selected, Some(0));
    assert_eq!(
        lock(&endpoint.peers)["remote"].path(Instant::now()),
        Some(PeerPath::Direct)
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
    assert!(incoming.try_recv().is_err());
}

#[tokio::test]
async fn untrusted_candidate_cannot_turn_a_seen_probe_into_an_authorized_ack_or_scan() {
    let (mut endpoint, remote, _, _) = endpoint(PathPolicy::DirectPreferred).await;
    endpoint.probe(&"remote".into());
    let nonce = lock(&endpoint.peers)["remote"].checks[0]
        .probe
        .unwrap()
        .token;
    let sent = Packet {
        sender: "local",
        session: [1; 16],
        sequence: endpoint.sequence,
        body: Body::Probe {
            target: [3; 16],
            nonce,
            secret: [7; 16],
        },
    };
    let leaked_tag = wire::check_proof([7; 16], sent);
    assert_ne!(leaked_tag, [7; 16]);
    // Even knowing the outgoing nonce and MAC is not knowing the private pair key.
    endpoint.receive(
        packet(
            20,
            Body::ProbeAck {
                target: [1; 16],
                nonce,
                capability: [9; 16],
                secret: leaked_tag,
            },
        ),
        remote.local_addr().unwrap(),
    );
    assert!(lock(&endpoint.peers)["remote"].checks[0].direct.is_none());
    let unknown: SocketAddr = ([127, 0, 0, 1], 39999).into();
    endpoint.receive(
        packet(
            21,
            Body::Probe {
                target: [1; 16],
                nonce,
                secret: [9; 16],
            },
        ),
        unknown,
    );
    assert_eq!(lock(&endpoint.peers)["remote"].checks.len(), 1);
    assert_eq!(lock(&endpoint.peers)["remote"].sequence, 0);
}

#[tokio::test]
async fn authenticated_peer_reflexive_candidates_are_bounded_and_fenced_on_replacement() {
    let (mut endpoint, remote, _, _) = endpoint(PathPolicy::DirectPreferred).await;
    for index in 0..40_u16 {
        let address = SocketAddr::from(([127, 0, 0, 1], 40000 + index));
        endpoint.receive(
            packet(
                u64::from(index) + 1,
                Body::Probe {
                    target: [1; 16],
                    nonce: [4; 16],
                    secret: [7; 16],
                },
            ),
            address,
        );
    }
    assert_eq!(lock(&endpoint.peers)["remote"].checks.len(), 32);
    let old = packet(
        99,
        Body::Probe {
            target: [1; 16],
            nonce: [4; 16],
            secret: [7; 16],
        },
    );
    endpoint.accept_offer(
        control(7, Body::Discover { proof: [2; 16] }),
        "remote",
        [8; 16],
        Some(remote.local_addr().unwrap()),
        false,
        (wire::CandidateList::empty(), [9; 16]),
    );
    endpoint.receive(old, ([127, 0, 0, 1], 40001).into());
    let forged = wire::sign_check(Packet {
        session: [8; 16],
        body: Body::Probe {
            target: [1; 16],
            nonce: [4; 16],
            secret: [7; 16],
        },
        ..old
    });
    endpoint.receive(forged, ([127, 0, 0, 1], 40001).into());
    assert_eq!(lock(&endpoint.peers)["remote"].checks.len(), 1);
}

#[tokio::test]
async fn late_local_candidate_announcement_preserves_live_selection_and_admission_lease() {
    let (mut endpoint, remote, _, _) = endpoint(PathPolicy::DirectPreferred).await;
    endpoint.probe(&"remote".into());
    let nonce = lock(&endpoint.peers)["remote"].checks[0]
        .probe
        .unwrap()
        .token;
    let address = remote.local_addr().unwrap();
    endpoint.receive(
        packet(
            8,
            Body::ProbeAck {
                target: [1; 16],
                nonce,
                capability: [6; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    let lease = lock(&endpoint.peers)["remote"].lease.clone();
    let extra = UdpSocket::bind(loopback()).await.unwrap();
    let mut candidates = super::super::candidates::Candidates::default();
    candidates.insert(extra.local_addr().unwrap());
    endpoint.accept_offer(
        control(7, Body::Discover { proof: [2; 16] }),
        "remote",
        [3; 16],
        Some(address),
        false,
        ((&candidates).into(), [7; 16]),
    );
    let peers = lock(&endpoint.peers);
    let peer = &peers["remote"];
    assert!(lease.is_active());
    assert_eq!(peer.lease.id(), lease.id());
    assert_eq!(peer.checks.len(), 2);
    assert_eq!(peer.direct_path(Instant::now()).unwrap().address, address);
    assert_eq!(
        peer.direct_path(Instant::now())
            .unwrap()
            .direct
            .unwrap()
            .token,
        [6; 16]
    );
}

#[tokio::test]
async fn candidate_nonce_deadlines_and_failed_bursts_bound_third_party_probing() {
    let (mut endpoint, _, _, _) = endpoint(PathPolicy::DirectPreferred).await;
    endpoint.probe(&"remote".into());
    let sequence = endpoint.sequence;
    let nonce = lock(&endpoint.peers)["remote"].checks[0]
        .probe
        .unwrap()
        .token;
    endpoint.probe(&"remote".into());
    assert_eq!(endpoint.sequence, sequence);
    assert_eq!(
        lock(&endpoint.peers)["remote"].checks[0]
            .probe
            .unwrap()
            .token,
        nonce
    );
    for _ in 0..2 {
        {
            let mut peers = lock(&endpoint.peers);
            let check = &mut peers.get_mut("remote").unwrap().checks[0];
            check.probe.as_mut().unwrap().issued = Instant::now() - DIRECT_LEASE;
            check.next_probe = Instant::now();
        }
        endpoint.probe(&"remote".into());
    }
    {
        let mut peers = lock(&endpoint.peers);
        let check = &mut peers.get_mut("remote").unwrap().checks[0];
        assert_eq!(check.attempts, 3);
        check.probe.as_mut().unwrap().issued = Instant::now() - DIRECT_LEASE;
        check.next_probe = Instant::now();
    }
    let sequence = endpoint.sequence;
    endpoint.probe(&"remote".into());
    assert_eq!(endpoint.sequence, sequence);
    assert!(lock(&endpoint.peers)["remote"].checks[0].next_probe > Instant::now());
    lock(&endpoint.peers).get_mut("remote").unwrap().checks[0].next_probe = Instant::now();
    endpoint.probe(&"remote".into());
    assert!(endpoint.sequence > sequence);
}

#[tokio::test]
async fn candidate_pair_debug_never_exposes_private_introduction_key() {
    let (endpoint, _, _, _) = endpoint(PathPolicy::DirectPreferred).await;
    let peers = lock(&endpoint.peers);
    let debug = format!("{:?}", peers["remote"]);
    assert!(debug.contains("secret: \"[REDACTED]\""));
    assert!(!debug.contains(&format!("{:?}", [7_u8; 16])));
}
