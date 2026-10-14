//! Configurable capacities remain independent of authentication and wire bounds.

use std::num::NonZeroUsize;

use std::error::Error as _;

use groupnet_transport::NodeIdError;
use groupnet_transport::admission::OpenAdmission;

use super::*;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn with_policy(
    bind: SocketAddr,
    key: Option<NetworkKey>,
    peers: PeerPolicy,
) -> UdpRendezvousConfig {
    UdpRendezvousConfig::new(bind, key, peers, Arc::new(OpenAdmission))
}

fn open(bind: SocketAddr) -> UdpRendezvousConfig {
    with_policy(bind, None, PeerPolicy::Dynamic)
}

fn keyed(bind: SocketAddr, peers: Vec<NodeId>) -> UdpRendezvousConfig {
    with_policy(
        bind,
        Some(NetworkKey::from_bytes([1; 32])),
        PeerPolicy::Static(peers),
    )
}

#[test]
fn configured_allowlist_capacity_is_not_a_protocol_peer_bound() {
    let bind = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut peers: Vec<NodeId> = (0..129)
        .map(|index| NodeId::from(format!("peer-{index}")))
        .collect();
    let mut config = keyed(bind, peers.clone());
    assert_eq!(
        config.validate(),
        Err(UdpRendezvousConfigError::PeersExceedMaxPeers {
            peers: 129,
            max_peers: nonzero(128)
        })
    );
    config.limits.max_peers = nonzero(129);
    config.validate().unwrap();
    config.limits.max_peers = nonzero(5000);
    config.limits.max_challenges = nonzero(5000);
    config.limits.max_pending_admissions = nonzero(5000);
    config.validate().unwrap();
    peers.push(NodeId::from("x".repeat(65)));
    config.peers = PeerPolicy::Static(peers);
    assert_eq!(
        config.validate(),
        Err(UdpRendezvousConfigError::Peer {
            index: 129,
            error: NodeIdError::TooLong { len: 65, max: 64 }
        })
    );
}

#[test]
fn allowlist_and_bind_rules_name_their_field() {
    let bind = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = keyed(bind, vec!["a".into(), "a".into()]);
    assert_eq!(
        config.validate(),
        Err(UdpRendezvousConfigError::DuplicatePeer { index: 1 })
    );
    let config = keyed(bind, vec!["".into()]);
    assert_eq!(
        config.validate(),
        Err(UdpRendezvousConfigError::Peer {
            index: 0,
            error: NodeIdError::Empty
        })
    );
    let config = open(SocketAddr::from(([224, 0, 0, 1], 0)));
    assert_eq!(
        config.validate(),
        Err(UdpRendezvousConfigError::MulticastBind)
    );
    assert!(
        UdpRendezvousConfigError::MulticastBind
            .to_string()
            .starts_with("bind")
    );
}

#[test]
fn dynamic_policy_skips_the_static_list_checks() {
    let bind = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut config = open(bind);
    assert_eq!(config.peers.static_peers(), &[] as &[NodeId]);
    config.limits.max_peers = nonzero(1);
    config.validate().unwrap();
    let key = Some(NetworkKey::from_bytes([1; 32]));
    let mut config = with_policy(bind, key, PeerPolicy::Static(vec!["a".into(), "b".into()]));
    config.limits.max_peers = nonzero(1);
    assert_eq!(
        config.validate(),
        Err(UdpRendezvousConfigError::PeersExceedMaxPeers {
            peers: 2,
            max_peers: nonzero(1)
        })
    );
    config.peers = PeerPolicy::Dynamic;
    config.validate().unwrap();
}

#[test]
fn source_is_the_wrapped_identity_error() {
    let peer = UdpRendezvousConfigError::Peer {
        index: 0,
        error: NodeIdError::Empty,
    };
    assert_eq!(
        peer.source()
            .and_then(|source| source.downcast_ref::<NodeIdError>()),
        Some(&NodeIdError::Empty)
    );
    assert!(UdpRendezvousConfigError::MulticastBind.source().is_none());
}

#[tokio::test]
async fn bind_surfaces_the_typed_error_as_invalid_input() {
    let config = open(SocketAddr::from(([224, 0, 0, 1], 0)));
    let error = UdpRendezvous::bind(config).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<UdpRendezvousConfigError>()),
        Some(&UdpRendezvousConfigError::MulticastBind)
    );
}

#[test]
fn configured_challenge_pool_evicts_without_consuming_peer_or_policy_slots() {
    let now = Instant::now();
    let address = SocketAddr::from(([127, 0, 0, 1], 1234));
    let mut challenges = Challenges {
        pending: HashMap::new(),
        capacity: 2,
    };
    for (index, sender) in ["first", "second", "third"].into_iter().enumerate() {
        let packet = Packet {
            sender,
            session: [1; 16],
            sequence: 1,
            body: Body::Hello { nonce: [2; 16] },
        };
        challenges
            .issue(
                packet,
                address,
                [2; 16],
                now + Duration::from_millis(u64::try_from(index).unwrap()),
            )
            .unwrap();
        assert!(challenges.pending.len() <= 2);
    }
    assert_eq!(challenges.pending.len(), 2);
    assert!(!challenges.pending.contains_key("first"));
    let pending = &challenges.pending["third"];
    let packet = Packet {
        sender: "third",
        session: pending.session,
        sequence: 2,
        body: Body::Register {
            nonce: pending.nonce,
            cookie: pending.cookie,
            relay_only: true,
            credential: &[],
        },
    };
    assert!(
        challenges
            .prove(
                packet,
                SocketAddr::from(([127, 0, 0, 1], 1235)),
                now + Duration::from_millis(3)
            )
            .is_none()
    );
    assert!(
        challenges
            .prove(
                packet,
                address,
                now + CHALLENGE_TTL + Duration::from_millis(2)
            )
            .is_none()
    );
    assert_eq!(challenges.pending.len(), 2);
    challenges.expire(now + CHALLENGE_TTL + Duration::from_millis(2));
    assert!(challenges.pending.is_empty());
}

#[tokio::test]
async fn configured_open_and_keyed_rendezvous_bind_with_custom_capacities() {
    let bind = SocketAddr::from(([127, 0, 0, 1], 0));
    for mut config in [open(bind), keyed(bind, vec!["peer".into()])] {
        config.limits = UdpRendezvousLimits {
            max_peers: nonzero(1),
            max_challenges: nonzero(2),
            max_pending_admissions: nonzero(1),
        };
        let rendezvous = UdpRendezvous::bind(config).await.unwrap();
        let address = rendezvous.local_addr();
        assert_ne!(address.port(), 0);
        rendezvous.close().await;
        assert_eq!(rendezvous.local_addr(), address);
    }
}
