//! Configurable capacities remain independent of authentication and wire bounds.

use super::*;

#[tokio::test]
async fn zero_operational_limits_fail_before_socket_binding() {
    let bind = SocketAddr::from(([127, 0, 0, 1], 0));
    for limits in [
        RendezvousLimits {
            max_peers: 0,
            ..RendezvousLimits::default()
        },
        RendezvousLimits {
            max_challenges: 0,
            ..RendezvousLimits::default()
        },
        RendezvousLimits {
            max_pending_admissions: 0,
            ..RendezvousLimits::default()
        },
    ] {
        let mut config = RendezvousConfig::open(bind);
        config.limits = limits;
        assert_eq!(
            Rendezvous::bind_config(config).await.unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn configured_allowlist_capacity_is_not_a_protocol_peer_bound() {
    let bind = SocketAddr::from(([127, 0, 0, 1], 0));
    let peers = (0..129)
        .map(|index| NodeId::from(format!("peer-{index}")))
        .collect();
    let mut config = RendezvousConfig::new(bind, NetworkKey::from_bytes([1; 32]), peers);
    assert!(config.validate().is_err());
    config.limits.max_peers = 129;
    config.validate().unwrap();
    config.limits.max_peers = 5000;
    config.limits.max_challenges = 5000;
    config.limits.max_pending_admissions = 5000;
    config.validate().unwrap();
    config
        .peers
        .as_mut()
        .unwrap()
        .push(NodeId::from("x".repeat(65)));
    assert!(config.validate().is_err());
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
    for mut config in [
        RendezvousConfig::open(bind),
        RendezvousConfig::new(bind, NetworkKey::from_bytes([1; 32]), vec!["peer".into()]),
    ] {
        config.limits = RendezvousLimits {
            max_peers: 1,
            max_challenges: 2,
            max_pending_admissions: 1,
        };
        let rendezvous = Rendezvous::bind_config(config).await.unwrap();
        assert_ne!(rendezvous.local_addr().unwrap().port(), 0);
        rendezvous.close().await;
    }
}
