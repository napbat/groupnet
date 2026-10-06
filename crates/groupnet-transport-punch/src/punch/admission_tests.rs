//! Real-socket return-routability, resource bounds, and stale queue regressions.

use std::sync::atomic::{AtomicUsize, Ordering};

use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::admission::{AcceptedPeer, Admission, JoinRequest};
use groupnet_transport::link::LinkFuture;
use tokio::time::timeout;

use super::wire::{Body, MAX_PACKET, Packet, Session};
use super::*;

const SETTLE: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(100);

fn loopback() -> SocketAddr {
    ([127, 0, 0, 1], 0).into()
}

struct Raw {
    socket: UdpSocket,
    node: String,
    session: Session,
    proof: Session,
    sequence: u64,
}

impl Raw {
    async fn new(node: String) -> Self {
        Self {
            socket: UdpSocket::bind(loopback()).await.unwrap(),
            node,
            session: random().unwrap(),
            proof: [0; 16],
            sequence: 0,
        }
    }

    async fn send(&mut self, address: SocketAddr, body: Body<'_>) -> Vec<u8> {
        self.sequence += 1;
        let mut bytes = [0; MAX_PACKET];
        let length = wire::encode_mode(
            Packet {
                sender: &self.node,
                session: self.session,
                sequence: self.sequence,
                body,
            },
            None,
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

    async fn challenge(&mut self, address: SocketAddr) -> (Session, Session) {
        let nonce = random().unwrap();
        self.send(address, Body::Hello { nonce }).await;
        let bytes = self.receive().await;
        match wire::decode_mode(&bytes, None).unwrap().body {
            Body::Challenge {
                nonce: echoed,
                cookie,
            } => {
                assert_eq!(nonce, echoed);
                self.proof = nonce;
                (nonce, cookie)
            }
            other => panic!("expected challenge, got {other:?}"),
        }
    }

    async fn register(&mut self, address: SocketAddr) -> Vec<u8> {
        let (nonce, cookie) = self.challenge(address).await;
        self.send(
            address,
            Body::Register {
                nonce,
                cookie,
                relay_only: true,
                credential: b"opaque",
            },
        )
        .await
    }
}

#[derive(Debug)]
struct CountPolicy(Arc<AtomicUsize>);

impl Admission for CountPolicy {
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.credential, b"opaque");
            Ok(AcceptedPeer {
                node: request.claimed.clone(),
            })
        })
    }
}

#[tokio::test]
async fn open_policy_is_not_invoked_before_address_proof_and_proof_is_single_use() {
    let calls = Arc::new(AtomicUsize::new(0));
    let relay =
        Rendezvous::bind_with_admission(loopback(), None, Arc::new(CountPolicy(calls.clone())))
            .await
            .unwrap();
    let address = relay.local_addr().unwrap();
    let mut raw = Raw::new("unknown-before-bind".into()).await;
    let thief = Raw::new(raw.node.clone()).await;
    raw.socket
        .send_to(&[0; MAX_PACKET + 1], address)
        .await
        .unwrap();
    raw.send(
        address,
        Body::Register {
            nonce: [1; 16],
            cookie: [2; 16],
            relay_only: true,
            credential: b"opaque",
        },
    )
    .await;
    raw.silent().await;
    raw.send(address, Body::Discover { proof: raw.proof }).await;
    raw.silent().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (nonce, cookie) = raw.challenge(address).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let registration = raw
        .send(
            address,
            Body::Register {
                nonce,
                cookie,
                relay_only: true,
                credential: b"opaque",
            },
        )
        .await;
    thief.socket.send_to(&registration, address).await.unwrap();
    thief.silent().await;
    let bytes = raw.receive().await;
    assert!(matches!(
        wire::decode_mode(&bytes, None).unwrap().body,
        Body::Registered { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    raw.socket.send_to(&registration, address).await.unwrap();
    raw.silent().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    raw.send(
        address,
        Body::Query {
            proof: raw.proof,
            peer: "not-admitted",
        },
    )
    .await;
    raw.silent().await;
    // A new session at the SAME source socket cannot overwrite the incumbent.
    let original = raw.session;
    raw.session = random().unwrap();
    raw.send(address, Body::Hello { nonce }).await;
    raw.silent().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    raw.session = original;
    raw.send(address, Body::Heartbeat { proof: raw.proof })
        .await;
    assert!(matches!(
        wire::decode_mode(&raw.receive().await, None).unwrap().body,
        Body::Registered { .. }
    ));
    relay.close().await;
}

#[derive(Debug)]
struct BlockPolicy {
    active: Arc<AtomicUsize>,
    started: Arc<AtomicUsize>,
}

struct Evaluation(Arc<AtomicUsize>);

impl Drop for Evaluation {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Admission for BlockPolicy {
    fn admit<'a>(&'a self, _: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            self.active.fetch_add(1, Ordering::SeqCst);
            self.started.fetch_add(1, Ordering::SeqCst);
            let _guard = Evaluation(self.active.clone());
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn policy_concurrency_is_bounded_and_shutdown_drains_every_evaluation() {
    let active = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let policy = Arc::new(BlockPolicy {
        active: active.clone(),
        started: started.clone(),
    });
    let relay = Rendezvous::bind_with_admission(loopback(), None, policy)
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let mut sockets = Vec::new();
    for index in 0..32 {
        let mut raw = Raw::new(format!("pending-{index}")).await;
        raw.register(address).await;
        sockets.push(raw);
    }
    eventually_within("bounded policy tasks entered", SETTLE, || {
        started.load(Ordering::SeqCst) == 32
    })
    .await;
    let mut overflow = Raw::new("overflow".into()).await;
    overflow.register(address).await;
    let bytes = overflow.receive().await;
    assert!(matches!(
        wire::decode_mode(&bytes, None).unwrap().body,
        Body::Denied { .. }
    ));
    assert_eq!(active.load(Ordering::SeqCst), 32);
    relay.close().await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(started.load(Ordering::SeqCst), 32);
}

#[tokio::test]
async fn stale_queue_generation_cannot_be_reauthorized_using_reused_identity() {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let address = relay.local_addr().unwrap();
    let a = UdpConnection::bind(PunchConfig::open("a".into(), address))
        .await
        .unwrap();
    let b = UdpConnection::bind(PunchConfig::open("b".into(), address))
        .await
        .unwrap();
    eventually_within("peer discovery", SETTLE, || {
        a.path_to(&"b".into()).is_some() && b.path_to(&"a".into()).is_some()
    })
    .await;
    assert_eq!(a.local_id(), &NodeId::from("a"));
    assert_eq!(b.local_id(), &NodeId::from("b"));
    let registry = b.sessions();
    let same_registry = b.clone().sessions();
    a.send(&"b".into(), b"queued old generation").await.unwrap();
    timeout(SETTLE, async {
        loop {
            if !b.inner.inbound.lock().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let old = lock(&b.inner.peers).get("a").unwrap().lease.clone();
    old.revoke();
    let replacement = registry
        .try_admit(AcceptedPeer { node: "a".into() })
        .unwrap();
    assert_ne!(old.id(), replacement.id());
    assert!(timeout(SILENCE, b.recv_admitted()).await.is_err());
    assert!(replacement.is_active());
    assert!(same_registry.is_active(&NodeId::from("a"), replacement.id()));
    drop(old);
    assert!(replacement.is_active());
    a.close().await;
    b.close().await;
    assert!(!registry.is_active(&NodeId::from("a"), replacement.id()));
    assert!(!same_registry.is_active(&NodeId::from("a"), replacement.id()));
    assert_eq!(b.local_id(), &NodeId::from("b"));
    relay.close().await;
}

#[test]
fn maximum_credential_with_maximum_identity_fits_keyed_and_open_datagrams() {
    let name = "x".repeat(64);
    let credential = [42; groupnet_transport::admission::MAX_CREDENTIAL_BYTES];
    let mut bytes = [0; MAX_PACKET];
    let packet = Packet {
        sender: &name,
        session: [1; 16],
        sequence: 1,
        body: Body::Register {
            nonce: [2; 16],
            cookie: [3; 16],
            relay_only: true,
            credential: &credential,
        },
    };
    for key in [None, Some(NetworkKey::from_bytes([8; 32]))] {
        let length = wire::encode_mode(packet, key.as_ref(), &mut bytes).unwrap();
        assert!(length <= MAX_PACKET);
        let decoded = wire::decode_mode(&bytes[..length], key.as_ref()).unwrap();
        assert!(
            matches!(decoded.body, Body::Register { credential: got, .. } if got == credential)
        );
        assert!(wire::decode_mode(&bytes[..length - 1], key.as_ref()).is_none());
        bytes[length] = 0;
        assert!(wire::decode_mode(&bytes[..=length], key.as_ref()).is_none());
    }
}

#[tokio::test]
async fn selected_outbound_generation_cannot_send_to_a_replacement_lease() {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let address = relay.local_addr().unwrap();
    let a = UdpConnection::bind(PunchConfig::open("a".into(), address))
        .await
        .unwrap();
    let b = UdpConnection::bind(PunchConfig::open("b".into(), address))
        .await
        .unwrap();
    eventually_within("outbound generation discovery", SETTLE, || {
        a.path_to(&"b".into()).is_some() && b.path_to(&"a".into()).is_some()
    })
    .await;
    let old = lock(&a.inner.peers).get("b").unwrap().lease.clone();
    // The future is immediately ready: the datagram remains queued until this
    // current-thread test yields, so replacement happens before socket dequeue.
    a.send_admitted(&"b".into(), b"queued old session", Some(old.id()))
        .await
        .unwrap();
    old.revoke();
    let replacement = a
        .inner
        .sessions
        .try_admit(AcceptedPeer { node: "b".into() })
        .unwrap();
    lock(&a.inner.peers).get_mut("b").unwrap().lease = replacement.clone();
    a.send_admitted(&"b".into(), b"stale route selection", Some(old.id()))
        .await
        .unwrap();
    assert!(timeout(SILENCE, b.recv()).await.is_err());
    assert_eq!(
        a.send_admitted(&"b".into(), b"untagged", None)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    a.send_admitted(&"b".into(), b"current selection", Some(replacement.id()))
        .await
        .unwrap();
    assert_eq!(
        timeout(SETTLE, b.recv()).await.unwrap().unwrap().msg,
        b"current selection"
    );
    drop(old);
    assert!(replacement.is_active());
    a.close().await;
    b.close().await;
    relay.close().await;
}

#[tokio::test]
async fn unadmitted_session_flood_cannot_starve_live_heartbeats_or_relay() {
    let calls = Arc::new(AtomicUsize::new(0));
    let relay =
        Rendezvous::bind_with_admission(loopback(), None, Arc::new(CountPolicy(calls.clone())))
            .await
            .unwrap();
    let address = relay.local_addr().unwrap();
    let mut incumbent = Raw::new("incumbent".into()).await;
    incumbent.register(address).await;
    assert!(matches!(
        wire::decode_mode(&incumbent.receive().await, None)
            .unwrap()
            .body,
        Body::Registered { .. }
    ));
    let mut recipient = Raw::new("recipient".into()).await;
    recipient.register(address).await;
    recipient.receive().await;
    let mut attacker = Raw::new("incumbent".into()).await;
    for _ in 0..8 {
        for _ in 0..40 {
            attacker
                .send(
                    address,
                    Body::Discover {
                        proof: attacker.proof,
                    },
                )
                .await;
        }
        incumbent
            .send(
                address,
                Body::Heartbeat {
                    proof: incumbent.proof,
                },
            )
            .await;
        assert!(matches!(
            wire::decode_mode(&incumbent.receive().await, None)
                .unwrap()
                .body,
            Body::Registered { .. }
        ));
    }
    incumbent
        .send(
            address,
            Body::Relay {
                proof: incumbent.proof,
                peer: "recipient",
                target: recipient.session,
                message: b"valid traffic after flood",
            },
        )
        .await;
    let bytes = recipient.receive().await;
    assert!(matches!(wire::decode_mode(&bytes, None).unwrap().body,
        Body::Delivered { message, .. } if message == b"valid traffic after flood"));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    relay.close().await;
}

#[tokio::test]
async fn unproved_challenge_saturation_does_not_reserve_established_peer_capacity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let relay =
        Rendezvous::bind_with_admission(loopback(), None, Arc::new(CountPolicy(calls.clone())))
            .await
            .unwrap();
    let address = relay.local_addr().unwrap();
    let mut attacker = Raw::new("unproved-0".into()).await;
    for index in 0..MAX_PEERS {
        attacker.node = format!("unproved-{index}");
        attacker.challenge(address).await;
    }
    for index in 0..16 {
        attacker.node = format!("unproved-{index}");
        attacker.challenge(address).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let admitted = UdpConnection::bind(PunchConfig::dynamic(
        "legitimate".into(),
        address,
        None,
        b"opaque".to_vec(),
    ))
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    admitted.close().await;
    relay.close().await;
}
