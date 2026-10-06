//! Regressions for transferable probes, complete candidate sets, and full tables.

use std::time::Duration;
use tokio::time::timeout;

use super::super::{Capability, PeerPath, candidates::Candidates};
use super::tests::{control, endpoint, loopback, packet};
use super::*;

async fn receive_one(endpoint: &mut Endpoint) {
    let mut bytes = [0; MAX_PACKET + 1];
    let (length, source) = timeout(
        Duration::from_secs(1),
        endpoint.socket.recv_from(&mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    let decoded = wire::decode_mode(&bytes[..length], None).unwrap();
    endpoint.receive(decoded, source);
}

async fn inject(endpoint: &mut Endpoint, source: &UdpSocket, value: Packet<'_>) {
    let mut bytes = [0; MAX_PACKET];
    let length = wire::encode_mode(value, None, &mut bytes).unwrap();
    source
        .send_to(&bytes[..length], endpoint.socket.local_addr().unwrap())
        .await
        .unwrap();
    receive_one(endpoint).await;
}

async fn capability(socket: &UdpSocket) -> Session {
    let mut bytes = [0; MAX_PACKET + 1];
    let (length, _) = timeout(Duration::from_secs(1), socket.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    match wire::decode_mode(&bytes[..length], None).unwrap().body {
        Body::ProbeAck { capability, .. } => capability,
        _ => panic!("expected session-authenticated challenge acknowledgement"),
    }
}

#[tokio::test]
async fn forwarding_a_genuine_probe_cannot_authorize_third_party_confirm_data_or_sequence_poisoning()
 {
    let (mut b, a, relay, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    let attacker = UdpSocket::bind(loopback()).await.unwrap();
    let genuine_probe = packet(
        1,
        Body::Probe {
            target: [1; 16],
            nonce: [4; 16],
            secret: [7; 16],
        },
    );
    let mut bytes = [0; MAX_PACKET];
    let length = wire::encode_mode(genuine_probe, None, &mut bytes).unwrap();
    // A sends a genuine probe to an untrusted candidate for B. C learns only
    // its MAC, not the pair key, and forwards the exact bytes unchanged to B.
    a.send_to(&bytes[..length], attacker.local_addr().unwrap())
        .await
        .unwrap();
    let mut captured = [0; MAX_PACKET + 1];
    let (length, _) = timeout(Duration::from_secs(1), attacker.recv_from(&mut captured))
        .await
        .unwrap()
        .unwrap();
    let Body::Probe {
        secret: captured_mac,
        ..
    } = wire::decode_mode(&captured[..length], None).unwrap().body
    else {
        panic!("expected captured probe");
    };
    attacker
        .send_to(&captured[..length], b.socket.local_addr().unwrap())
        .await
        .unwrap();
    receive_one(&mut b).await;
    let stolen_capability = capability(&attacker).await;
    assert!(
        lock(&b.peers)["remote"]
            .checks
            .iter()
            .any(|check| check.address == attacker.local_addr().unwrap())
    );
    reject_forwarded_credentials(
        &mut b,
        &attacker,
        captured_mac,
        stolen_capability,
        &mut incoming,
    )
    .await;
    // Legitimate relay traffic is not fenced out by the forged maximum sequence.
    b.receive(
        control(
            3,
            Body::Delivered {
                proof: [2; 16],
                peer: "remote",
                session: [3; 16],
                message: b"legitimate relay",
            },
        ),
        relay.local_addr().unwrap(),
    );
    assert_eq!(
        incoming.try_recv().unwrap().packet.msg.as_ref(),
        b"legitimate relay"
    );
    // A's own address obtains its own capability and authenticates complete data.
    inject(
        &mut b,
        &a,
        packet(
            4,
            Body::Probe {
                target: [1; 16],
                nonce: [5; 16],
                secret: [7; 16],
            },
        ),
    )
    .await;
    let legitimate_capability = capability(&a).await;
    inject(
        &mut b,
        &a,
        packet(
            5,
            Body::Direct {
                peer: "local",
                target: [1; 16],
                capability: legitimate_capability,
                message: b"legitimate direct",
                secret: [7; 16],
            },
        ),
    )
    .await;
    assert_eq!(
        (incoming.try_recv().unwrap().packet.msg).as_ref(),
        b"legitimate direct"
    );
}

async fn fill_sockets(endpoint: &mut Endpoint) {
    endpoint.config.candidate_binds = vec![loopback(); 3];
    for _ in 0..3 {
        endpoint
            .extra_sockets
            .push(UdpSocket::bind(loopback()).await.unwrap());
    }
}

#[tokio::test]
async fn eighth_advertisement_plus_distinct_observed_mapping_is_checked_with_full_cartesian_table()
{
    let (mut endpoint, observed, _, _) = endpoint(PathPolicy::DirectPreferred).await;
    fill_sockets(&mut endpoint).await;
    let reachable = UdpSocket::bind(loopback()).await.unwrap();
    let mut candidates = Candidates::default();
    for port in 45000..45007 {
        assert!(candidates.insert(([127, 0, 0, 1], port).into()));
    }
    assert!(candidates.insert(reachable.local_addr().unwrap()));
    endpoint.accept_offer(
        control(7, Body::Discover { proof: [2; 16] }),
        "remote",
        [3; 16],
        Some(observed.local_addr().unwrap()),
        false,
        ((&candidates).into(), [7; 16]),
    );
    {
        let peers = lock(&endpoint.peers);
        assert_eq!(peers["remote"].addresses.iter().count(), 9);
        assert_eq!(peers["remote"].checks.len(), 32);
        assert!(
            !peers["remote"]
                .checks
                .iter()
                .any(|check| check.address == reachable.local_addr().unwrap())
        );
    }
    endpoint.probe(&"remote".into());
    let (socket, nonce) = {
        let peers = lock(&endpoint.peers);
        let check = peers["remote"]
            .checks
            .iter()
            .find(|check| check.address == reachable.local_addr().unwrap())
            .unwrap();
        assert_eq!(peers["remote"].checks.len(), 32);
        (check.socket, check.probe.unwrap().token)
    };
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
        socket,
    );
    let peers = lock(&endpoint.peers);
    assert_eq!(peers["remote"].path(Instant::now()), Some(PeerPath::Direct));
    assert_eq!(
        peers["remote"].direct_path(Instant::now()).unwrap().address,
        reachable.local_addr().unwrap()
    );
}

#[tokio::test]
async fn full_table_adopts_authenticated_new_nat_tuple_without_evicting_healthy_selected_path() {
    let (mut endpoint, observed, _, mut incoming) = endpoint(PathPolicy::DirectPreferred).await;
    fill_sockets(&mut endpoint).await;
    let mut candidates = Candidates::default();
    for port in 46000..46007 {
        candidates.insert(([127, 0, 0, 1], port).into());
    }
    endpoint.accept_offer(
        control(7, Body::Discover { proof: [2; 16] }),
        "remote",
        [3; 16],
        Some(observed.local_addr().unwrap()),
        false,
        ((&candidates).into(), [7; 16]),
    );
    {
        let mut peers = lock(&endpoint.peers);
        let peer = peers.get_mut("remote").unwrap();
        assert_eq!(peer.checks.len(), 32);
        peer.selected = Some(0);
        peer.checks[0].direct = Some(Capability {
            token: [6; 16],
            issued: Instant::now(),
        });
        peer.checks[0].confirmed = Some(Capability {
            token: [6; 16],
            issued: Instant::now(),
        });
    }
    let nat = UdpSocket::bind(loopback()).await.unwrap();
    inject(
        &mut endpoint,
        &nat,
        packet(
            1,
            Body::Probe {
                target: [1; 16],
                nonce: [4; 16],
                secret: [7; 16],
            },
        ),
    )
    .await;
    let token = capability(&nat).await;
    {
        let peers = lock(&endpoint.peers);
        let peer = &peers["remote"];
        assert_eq!(peer.checks.len(), 32);
        assert_eq!(peer.selected, Some(0));
        assert_eq!(peer.checks[0].direct.unwrap().token, [6; 16]);
        assert!(
            peer.checks
                .iter()
                .any(|check| check.address == nat.local_addr().unwrap())
        );
    }
    inject(
        &mut endpoint,
        &nat,
        packet(
            2,
            Body::Direct {
                peer: "local",
                target: [1; 16],
                capability: token,
                message: b"new NAT tuple",
                secret: [7; 16],
            },
        ),
    )
    .await;
    assert_eq!(
        incoming.try_recv().unwrap().packet.msg.as_ref(),
        b"new NAT tuple"
    );
}

async fn reject_forwarded_credentials(
    endpoint: &mut Endpoint,
    attacker: &UdpSocket,
    captured_mac: Session,
    capability: Session,
    incoming: &mut mpsc::Receiver<AdmittedInbound>,
) {
    for fake_key in [captured_mac, capability, [0; 16]] {
        inject(
            endpoint,
            attacker,
            packet(
                2,
                Body::Confirm {
                    target: [1; 16],
                    capability,
                    secret: fake_key,
                },
            ),
        )
        .await;
        inject(
            endpoint,
            attacker,
            packet(
                u64::MAX,
                Body::Direct {
                    peer: "local",
                    target: [1; 16],
                    capability,
                    message: b"injected",
                    secret: fake_key,
                },
            ),
        )
        .await;
        assert!(incoming.try_recv().is_err());
        let peers = lock(&endpoint.peers);
        assert_eq!(peers["remote"].sequence, 0);
        let check = peers["remote"]
            .checks
            .iter()
            .find(|check| check.address == attacker.local_addr().unwrap())
            .unwrap();
        assert!(check.confirmed.is_none());
    }
}
