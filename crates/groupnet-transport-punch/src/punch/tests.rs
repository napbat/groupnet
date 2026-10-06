use std::net::SocketAddr;
use std::time::Duration;

use groupnet_testkit::cluster::eventually_within;
use tokio::net::UdpSocket;
use tokio::time::timeout;

use super::wire::{Body, MAX_PACKET, Packet, Session};
use super::*;

const SETTLE: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(150);

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn key() -> NetworkKey {
    NetworkKey::from_bytes([23; 32])
}

struct RawPeer {
    name: &'static str,
    session: Session,
    proof: Session,
    sequence: u64,
    socket: UdpSocket,
}

impl RawPeer {
    async fn new(name: &'static str, session: Session) -> Self {
        Self {
            name,
            session,
            proof: [0; 16],
            sequence: 0,
            socket: UdpSocket::bind(loopback()).await.unwrap(),
        }
    }

    async fn send(&mut self, address: SocketAddr, body: Body<'_>) -> Vec<u8> {
        self.sequence += 1;
        let mut bytes = [0; MAX_PACKET];
        let length = wire::encode(
            Packet {
                sender: self.name,
                session: self.session,
                sequence: self.sequence,
                body,
            },
            &key(),
            &mut bytes,
        )
        .unwrap();
        self.socket
            .send_to(&bytes[..length], address)
            .await
            .unwrap();
        bytes[..length].to_vec()
    }

    async fn receive(&self) -> Vec<u8> {
        let mut bytes = [0; MAX_PACKET + 1];
        let (length, _) = timeout(SETTLE, self.socket.recv_from(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        bytes[..length].to_vec()
    }

    async fn silent(&self) {
        let mut bytes = [0; MAX_PACKET + 1];
        assert!(
            timeout(SILENCE, self.socket.recv_from(&mut bytes))
                .await
                .is_err()
        );
    }

    async fn challenge(&mut self, server: SocketAddr) -> (Session, Session) {
        let nonce = random().unwrap();
        self.send(server, Body::Hello { nonce }).await;
        let bytes = self.receive().await;
        let packet = wire::decode(&bytes, &key()).unwrap();
        match packet.body {
            Body::Challenge {
                nonce: echoed,
                cookie,
            } => {
                assert_eq!(echoed, nonce);
                self.proof = nonce;
                (nonce, cookie)
            }
            other => panic!("expected challenge, got {other:?}"),
        }
    }

    async fn register(&mut self, server: SocketAddr, policy: PathPolicy) -> Vec<u8> {
        let (nonce, cookie) = self.challenge(server).await;
        let registration = self
            .send(
                server,
                Body::Register {
                    nonce,
                    cookie,
                    relay_only: policy == PathPolicy::RelayOnly,
                    credential: &[],
                },
            )
            .await;
        let bytes = self.receive().await;
        assert!(matches!(
            wire::decode(&bytes, &key()).unwrap().body,
            Body::Registered { .. }
        ));
        registration
    }

    async fn observed_peer(
        &mut self,
        server: SocketAddr,
        name: &str,
    ) -> (Option<SocketAddr>, Session) {
        self.send(
            server,
            Body::Query {
                proof: self.proof,
                peer: name,
            },
        )
        .await;
        timeout(SETTLE, async {
            loop {
                let bytes = self.receive().await;
                if let Body::Offer {
                    peer,
                    address,
                    session,
                    ..
                } = wire::decode(&bytes, &key()).unwrap().body
                {
                    assert_eq!(peer, name);
                    break (address, session);
                }
            }
        })
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn captured_registration_cannot_redirect_from_a_different_udp_address() {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let mut a = RawPeer::new("a", [1; 16]).await;
    let mut b = RawPeer::new("b", [2; 16]).await;
    let mut attacker = RawPeer::new("a", [1; 16]).await;
    b.register(address, PathPolicy::DirectPreferred).await;
    let (nonce, cookie) = a.challenge(address).await;
    // Even an authentic registration needs the challenge's observed address.
    let captured = a
        .send(
            address,
            Body::Register {
                nonce,
                cookie,
                relay_only: false,
                credential: &[],
            },
        )
        .await;
    attacker.socket.send_to(&captured, address).await.unwrap();
    attacker.silent().await;
    assert!(matches!(
        wire::decode(&a.receive().await, &key()).unwrap().body,
        Body::Registered { .. }
    ));
    assert_eq!(
        b.observed_peer(address, "a").await.0,
        Some(a.socket.local_addr().unwrap())
    );
    // Even a fresh duplicate Hello from another address cannot challenge an incumbent.
    attacker.send(address, Body::Hello { nonce: [7; 16] }).await;
    attacker.silent().await; // Sequence one is older than the accepted registration.
    attacker.sequence = 100;
    attacker.send(address, Body::Hello { nonce }).await;
    attacker.silent().await;
    attacker.socket.send_to(&captured, address).await.unwrap();
    attacker.silent().await;
    assert_eq!(
        b.observed_peer(address, "a").await.0,
        Some(a.socket.local_addr().unwrap())
    );
    relay.close().await;
}

#[tokio::test]
async fn consumed_and_old_session_registration_replays_cannot_replace_a_new_session() {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let mut a = RawPeer::new("a", [1; 16]).await;
    let mut b = RawPeer::new("b", [2; 16]).await;
    b.register(address, PathPolicy::RelayOnly).await;
    let captured = a.register(address, PathPolicy::RelayOnly).await;
    a.socket.send_to(&captured, address).await.unwrap();
    a.silent().await;
    a.send(address, Body::Depart { proof: a.proof }).await;
    a.session = [3; 16];
    a.sequence = 0;
    a.register(address, PathPolicy::RelayOnly).await;
    a.socket.send_to(&captured, address).await.unwrap();
    a.silent().await;
    assert_eq!(b.observed_peer(address, "a").await.1, [3; 16]);
    // Replaying an old session's Hello cannot displace the current session.
    let current = a.session;
    a.session = [1; 16];
    a.send(address, Body::Hello { nonce: [9; 16] }).await;
    a.silent().await;
    a.socket.send_to(&captured, address).await.unwrap();
    a.silent().await;
    assert_eq!(b.observed_peer(address, "a").await.1, current);
    relay.close().await;
}

#[tokio::test]
async fn expired_challenge_and_unregistered_relay_are_rejected() {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let mut a = RawPeer::new("a", [1; 16]).await;
    let mut b = RawPeer::new("b", [2; 16]).await;
    b.register(address, PathPolicy::RelayOnly).await;
    a.send(
        address,
        Body::Relay {
            proof: a.proof,
            peer: "b",
            target: b.session,
            message: b"not registered",
        },
    )
    .await;
    b.silent().await;
    let (nonce, cookie) = a.challenge(address).await;
    let issued = Instant::now();
    eventually_within("challenge expires", SETTLE, || {
        issued.elapsed() >= Duration::from_millis(3100)
    })
    .await;
    a.send(
        address,
        Body::Register {
            nonce,
            cookie,
            relay_only: true,
            credential: &[],
        },
    )
    .await;
    a.silent().await;
    b.send(
        address,
        Body::Query {
            proof: b.proof,
            peer: "a",
        },
    )
    .await;
    b.silent().await;
    a.register(address, PathPolicy::RelayOnly).await;
    assert_eq!(b.observed_peer(address, "a").await.1, a.session);
    relay.close().await;
}

#[tokio::test]
async fn replayed_relay_datagrams_and_wrong_recipient_sessions_are_not_forwarded() {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into(), "b".into()])
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let mut a = RawPeer::new("a", [1; 16]).await;
    let mut b = RawPeer::new("b", [2; 16]).await;
    a.register(address, PathPolicy::RelayOnly).await;
    b.register(address, PathPolicy::RelayOnly).await;
    a.send(
        address,
        Body::Relay {
            proof: a.proof,
            peer: "b",
            target: [7; 16],
            message: b"stale session",
        },
    )
    .await;
    b.silent().await;
    let captured = a
        .send(
            address,
            Body::Relay {
                proof: a.proof,
                peer: "b",
                target: b.session,
                message: b"once",
            },
        )
        .await;
    let delivered = b.receive().await;
    match wire::decode(&delivered, &key()).unwrap().body {
        Body::Delivered {
            peer,
            session,
            message,
            ..
        } => {
            assert_eq!(peer, "a");
            assert_eq!(session, a.session);
            assert_eq!(message, b"once");
        }
        other => panic!("expected relay delivery, got {other:?}"),
    }
    a.socket.send_to(&captured, address).await.unwrap();
    b.silent().await;
    relay.close().await;
}

#[tokio::test]
async fn unknown_names_and_bad_authentication_cannot_obtain_reflections() {
    let relay = Rendezvous::bind(loopback(), key(), vec!["a".into()])
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let mut unknown = RawPeer::new("unknown", [1; 16]).await;
    unknown.send(address, Body::Hello { nonce: [1; 16] }).await;
    unknown.silent().await;
    let a = RawPeer::new("a", [2; 16]).await;
    let mut bytes = [0; MAX_PACKET];
    let length = wire::encode(
        Packet {
            sender: "a",
            session: a.session,
            sequence: 1,
            body: Body::Hello { nonce: [1; 16] },
        },
        &NetworkKey::from_bytes([9; 32]),
        &mut bytes,
    )
    .unwrap();
    a.socket.send_to(&bytes[..length], address).await.unwrap();
    a.silent().await;
    a.socket
        .send_to(&[0; MAX_PACKET + 100], address)
        .await
        .unwrap();
    a.silent().await;
    let mut a = a;
    a.register(address, PathPolicy::RelayOnly).await;
    relay.close().await;
}

#[tokio::test]
async fn direct_packet_replays_bad_keys_and_unknown_sources_do_not_enter_receive_queue() {
    let relay = Rendezvous::bind(
        loopback(),
        key(),
        vec!["a".into(), "b".into(), "unknown".into()],
    )
    .await
    .unwrap();
    let address = relay.local_addr().unwrap();
    let mut config = PunchConfig::new("a".into(), address, key(), vec!["b".into()]);
    config.bind = loopback();
    let a = PunchTransport::bind(config).await.unwrap();
    let mut b = RawPeer::new("b", [2; 16]).await;
    b.register(address, PathPolicy::DirectPreferred).await;
    eventually_within("raw peer discovered", SETTLE, || {
        a.path_to(&"b".into()).is_some()
    })
    .await;
    let a_session = b.observed_peer(address, "a").await.1;
    let destination = a.local_addr().unwrap();
    b.socket
        .send_to(&[0; MAX_PACKET + 100], destination)
        .await
        .unwrap();
    assert!(timeout(SILENCE, a.recv()).await.is_err());
    let nonce = random().unwrap();
    b.send(
        destination,
        Body::Probe {
            target: a_session,
            nonce,
        },
    )
    .await;
    let capability = timeout(SETTLE, async {
        loop {
            let received = b.receive().await;
            if let Body::ProbeAck {
                nonce: echoed,
                capability,
                ..
            } = wire::decode(&received, &key()).unwrap().body
                && echoed == nonce
            {
                break capability;
            }
        }
    })
    .await
    .unwrap();
    let packet = Packet {
        sender: "b",
        session: b.session,
        sequence: 100,
        body: Body::Direct {
            peer: "a",
            target: a_session,
            capability,
            message: b"authenticated",
        },
    };
    let mut bytes = [0; MAX_PACKET];
    let length = wire::encode(packet, &NetworkKey::from_bytes([9; 32]), &mut bytes).unwrap();
    b.socket
        .send_to(&bytes[..length], destination)
        .await
        .unwrap();
    assert!(timeout(SILENCE, a.recv()).await.is_err());
    let length = wire::encode(
        Packet {
            sender: "unknown",
            ..packet
        },
        &key(),
        &mut bytes,
    )
    .unwrap();
    b.socket
        .send_to(&bytes[..length], destination)
        .await
        .unwrap();
    assert!(timeout(SILENCE, a.recv()).await.is_err());
    let length = wire::encode(packet, &key(), &mut bytes).unwrap();
    b.socket
        .send_to(&bytes[..length], destination)
        .await
        .unwrap();
    assert_eq!(
        timeout(SETTLE, a.recv()).await.unwrap().unwrap().msg,
        b"authenticated"
    );
    b.socket
        .send_to(&bytes[..length], destination)
        .await
        .unwrap();
    assert!(timeout(SILENCE, a.recv()).await.is_err());
    a.close().await;
    relay.close().await;
}
