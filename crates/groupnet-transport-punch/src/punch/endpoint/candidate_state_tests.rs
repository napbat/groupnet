//! Candidate selection, failover, rate bounds, and admission lease regressions.

use super::super::candidates::Candidates;
use super::super::{DIRECT_LEASE, PeerPath};
use super::tests::{
    challenge, confirmation, data, endpoint, fill_sockets, loopback, offer, packet,
};
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
    let mut candidates = Candidates::default();
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
    offer(
        &mut endpoint,
        [8; 16],
        remote.local_addr().unwrap(),
        &[],
        [9; 16],
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
    offer(
        &mut endpoint,
        [3; 16],
        address,
        &[extra.local_addr().unwrap()],
        [7; 16],
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

fn confirm_receive_path(endpoint: &mut Endpoint, address: SocketAddr, sequence: u64) -> Session {
    let capability = challenge(endpoint, address, sequence);
    endpoint.receive(packet(sequence + 1, confirmation(capability)), address);
    capability
}

fn select_transmit_path(endpoint: &mut Endpoint, address: SocketAddr, sequence: u64) {
    endpoint.probe(&"remote".into());
    let nonce = lock(&endpoint.peers)["remote"]
        .checks
        .iter()
        .find(|check| check.tuple() == (address, 0))
        .unwrap()
        .probe
        .unwrap()
        .token;
    endpoint.receive(
        packet(
            sequence,
            Body::ProbeAck {
                target: [1; 16],
                nonce,
                capability: [6; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
}

#[tokio::test]
async fn same_session_candidate_refresh_preserves_validated_peer_reflexive_path() {
    let (mut endpoint, remote, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let primary = remote.local_addr().unwrap();
    let learned = UdpSocket::bind(loopback()).await.unwrap();
    let address = learned.local_addr().unwrap();
    let capability = confirm_receive_path(&mut endpoint, address, 8);
    select_transmit_path(&mut endpoint, address, 10);
    endpoint.receive(packet(11, data(capability)), address);
    assert_eq!(incoming.try_recv().unwrap().packet.msg, b"payload"[..]);
    let (lease, direct_issued, confirmed_issued) = {
        let peers = lock(&endpoint.peers);
        let peer = &peers["remote"];
        assert!(!peer.addresses.contains(address));
        let check = &peer.checks[peer.selected.unwrap()];
        assert_eq!(check.address, address);
        (
            peer.lease.clone(),
            check.direct.unwrap().issued,
            check.confirmed.unwrap().issued,
        )
    };
    let first = UdpSocket::bind(loopback()).await.unwrap();
    let second = UdpSocket::bind(loopback()).await.unwrap();
    for pair in [
        [first.local_addr().unwrap(), second.local_addr().unwrap()],
        [second.local_addr().unwrap(), first.local_addr().unwrap()],
    ] {
        offer(&mut endpoint, [3; 16], primary, &pair, [7; 16]);
        let peers = lock(&endpoint.peers);
        let peer = &peers["remote"];
        let check = &peer.checks[peer.selected.unwrap()];
        assert_eq!(check.tuple(), (address, 0));
        assert_eq!(check.direct.unwrap().issued, direct_issued);
        assert_eq!(check.confirmed.unwrap().issued, confirmed_issued);
        assert_eq!(peer.session, [3; 16]);
        assert_eq!(peer.secret, [7; 16]);
        assert_eq!(peer.sequence, 11);
        assert_eq!(peer.lease.id(), lease.id());
        assert!(lease.is_active());
    }
    endpoint.receive(packet(11, data(capability)), address);
    assert!(incoming.try_recv().is_err());
    endpoint.receive(packet(12, data(capability)), address);
    let delivered = incoming.try_recv().unwrap();
    assert_eq!(delivered.session, Some(lease.id()));
    assert_eq!(delivered.packet.msg, b"payload"[..]);
    endpoint.send_message(&Outbound {
        to: "remote".into(),
        session: lease.id(),
        target: [3; 16],
        message: Bytes::from_static(b"selected learned path"),
    });
    let mut bytes = [0; MAX_PACKET + 1];
    timeout(Duration::from_secs(1), async {
        loop {
            let (length, source) = learned.recv_from(&mut bytes).await.unwrap();
            if matches!(wire::decode_mode(&bytes[..length], None).unwrap().body, Body::Direct { capability, message, .. } if capability == [6; 16] && message == b"selected learned path") {
                assert_eq!(source, endpoint.socket.local_addr().unwrap());
                break;
            }
        }
    }).await.unwrap();
}

#[tokio::test]
async fn full_candidate_refresh_retains_validated_learned_paths_but_replacement_fences_them() {
    let (mut endpoint, remote, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    fill_sockets(&mut endpoint).await;
    let primary = remote.local_addr().unwrap();
    let learned = UdpSocket::bind(loopback()).await.unwrap();
    let address = learned.local_addr().unwrap();
    let capability = confirm_receive_path(&mut endpoint, address, 8);
    select_transmit_path(&mut endpoint, address, 10);
    let receive_only = UdpSocket::bind(loopback()).await.unwrap();
    let receive_address = receive_only.local_addr().unwrap();
    let receive_capability = confirm_receive_path(&mut endpoint, receive_address, 11);
    let lease = lock(&endpoint.peers)["remote"].lease.clone();
    let advertised: Vec<SocketAddr> = (41000..41008)
        .map(|port| ([127, 0, 0, 1], port).into())
        .collect();
    offer(&mut endpoint, [3; 16], primary, &advertised, [7; 16]);
    {
        let peers = lock(&endpoint.peers);
        let peer = &peers["remote"];
        assert_eq!(peer.checks.len(), 32);
        assert_eq!(peer.addresses.len(), 9);
        assert_eq!(peer.checks[peer.selected.unwrap()].address, address);
        let check = peer
            .checks
            .iter()
            .find(|check| check.address == receive_address)
            .unwrap();
        assert!(check.direct.is_none());
        assert_eq!(check.confirmed.unwrap().token, receive_capability);
        assert_eq!(peer.lease.id(), lease.id());
    }
    endpoint.receive(packet(13, data(capability)), address);
    assert_eq!(incoming.try_recv().unwrap().session, Some(lease.id()));
    endpoint.receive(packet(14, data(receive_capability)), receive_address);
    assert_eq!(incoming.try_recv().unwrap().session, Some(lease.id()));
    offer(&mut endpoint, [8; 16], primary, &advertised, [9; 16]);
    assert!(!lease.is_active());
    endpoint.receive(packet(15, data(capability)), address);
    let forged = wire::sign_check(Packet {
        session: [8; 16],
        ..packet(16, data(capability))
    });
    endpoint.receive(forged, address);
    assert!(incoming.try_recv().is_err());
    let peers = lock(&endpoint.peers);
    let peer = &peers["remote"];
    assert_eq!(peer.checks.len(), 32);
    assert!(peer.selected.is_none());
    assert_eq!(peer.sequence, 0);
    assert!(
        peer.checks
            .iter()
            .all(|check| check.direct.is_none() && check.confirmed.is_none())
    );
    assert!(
        !peer
            .checks
            .iter()
            .any(|check| check.address == address || check.address == receive_address)
    );
}

#[tokio::test]
async fn candidate_refresh_preserves_matching_pending_and_probe_state() {
    let (mut endpoint, remote, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let address = remote.local_addr().unwrap();
    endpoint.probe(&"remote".into());
    challenge(&mut endpoint, address, 8);
    let (probe, pending) = {
        let peers = lock(&endpoint.peers);
        let check = &peers["remote"].checks[0];
        (check.probe.unwrap(), check.pending.unwrap())
    };
    let new_primary = UdpSocket::bind(loopback()).await.unwrap();
    offer(
        &mut endpoint,
        [3; 16],
        new_primary.local_addr().unwrap(),
        &[address],
        [7; 16],
    );
    {
        let peers = lock(&endpoint.peers);
        let check = peers["remote"]
            .checks
            .iter()
            .find(|check| check.address == address)
            .unwrap();
        let (kept_probe, kept_pending) = (check.probe.unwrap(), check.pending.unwrap());
        assert_eq!(
            (kept_probe.token, kept_probe.issued),
            (probe.token, probe.issued)
        );
        assert_eq!(
            (
                kept_pending.capability.token,
                kept_pending.capability.issued,
                kept_pending.sequence
            ),
            (
                pending.capability.token,
                pending.capability.issued,
                pending.sequence
            )
        );
    }
    endpoint.receive(
        packet(
            9,
            Body::ProbeAck {
                target: [1; 16],
                nonce: probe.token,
                capability: [6; 16],
                secret: [7; 16],
            },
        ),
        address,
    );
    endpoint.receive(packet(10, confirmation(pending.capability.token)), address);
    endpoint.receive(packet(11, data(pending.capability.token)), address);
    assert_eq!(incoming.try_recv().unwrap().packet.msg, b"payload"[..]);
    let peers = lock(&endpoint.peers);
    let peer = &peers["remote"];
    assert_eq!(peer.checks[peer.selected.unwrap()].address, address);
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
