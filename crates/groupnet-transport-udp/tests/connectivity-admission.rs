#![cfg(feature = "connectivity")]
//! Dynamic relay admission through real UDP sockets and the real router.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_network::{Router, RouterConfig};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport::admission::{AcceptedPeer, Admission, JoinRequest, MAX_CREDENTIAL_BYTES};
use groupnet_transport::link::{LinkFuture, LinkProvider};
use groupnet_transport_punch::{NetworkKey, PathPolicy, PeerPath, PunchConfig, Rendezvous};
use groupnet_transport_udp::{UdpLink, UdpTransport};
use tokio::time::timeout;

const SETTLE: Duration = Duration::from_secs(15);

fn loopback() -> SocketAddr {
    ([127, 0, 0, 1], 0).into()
}

async fn attach(router: &Router, address: SocketAddr) {
    let mut config = PunchConfig::open(router.local_id().clone(), address);
    config.bind = loopback();
    let link = Box::new(UdpLink::connectivity(config))
        .bind(router.local_id().clone())
        .await
        .unwrap();
    router.add_link(link).await.unwrap();
}

#[tokio::test]
async fn keyless_unknown_identities_discover_route_relay_withdraw_and_reconnect() {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let address = relay.local_addr().unwrap();
    let a = Router::new(
        "6adfa0df-40af-4470-a125-7c4c1f122989".into(),
        RouterConfig::default(),
    )
    .unwrap();
    let b = Router::new("application/string-id".into(), RouterConfig::default()).unwrap();
    attach(&a, address).await;
    attach(&b, address).await;
    eventually_within("dynamic open routing convergence", SETTLE, || {
        a.route_to(b.local_id()).is_some() && b.route_to(a.local_id()).is_some()
    })
    .await;
    a.send(b.local_id(), b"keyless routed message")
        .await
        .unwrap();
    let received = timeout(SETTLE, b.recv()).await.unwrap().unwrap();
    assert_eq!(received.from, *a.local_id());
    assert_eq!(received.msg, b"keyless routed message");
    b.send(a.local_id(), b"server-initiated reverse direction")
        .await
        .unwrap();
    assert_eq!(
        timeout(SETTLE, a.recv()).await.unwrap().unwrap().msg,
        b"server-initiated reverse direction"
    );
    b.close().await;
    eventually_within(
        "expired admission withdraws router neighbor",
        SETTLE,
        || a.route_to(b.local_id()).is_none(),
    )
    .await;
    let replacement = Router::new(b.local_id().clone(), RouterConfig::default()).unwrap();
    attach(&replacement, address).await;
    eventually_within("fresh generation rejoins", SETTLE, || {
        a.route_to(replacement.local_id()).is_some() && replacement.route_to(a.local_id()).is_some()
    })
    .await;
    replacement
        .send(a.local_id(), b"fresh generation")
        .await
        .unwrap();
    assert_eq!(
        timeout(SETTLE, a.recv()).await.unwrap().unwrap().msg,
        b"fresh generation"
    );
    replacement.close().await;
    a.close().await;
    relay.close().await;
}

#[derive(Debug)]
struct AccountPolicy;

impl Admission for AccountPolicy {
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            assert!(request.remote.is_some());
            if request.credential != b"account-proof" {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "account denied",
                ));
            }
            // Canonical identities cannot silently rename a managed Node.
            Ok(AcceptedPeer {
                node: if request.claimed.as_str() == "second-account" {
                    request.claimed.clone()
                } else {
                    "canonical-account".into()
                },
            })
        })
    }
}

async fn custom_accounts(policy: PathPolicy, expected: PeerPath) {
    let relay = Rendezvous::bind_with_admission(loopback(), None, Arc::new(AccountPolicy))
        .await
        .unwrap();
    let address = relay.local_addr().unwrap();
    let config_for = |identity: &str, credential: Vec<u8>| {
        let mut config = PunchConfig::dynamic(identity.into(), address, None, credential);
        config.bind = loopback();
        config.policy = policy;
        config
    };
    for (identity, credential) in [
        ("canonical-account", b"wrong".to_vec()),
        ("claimed-spoof", b"account-proof".to_vec()),
    ] {
        let config = config_for(identity, credential);
        assert_eq!(
            UdpTransport::bind_connectivity(config)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }
    let config = config_for("canonical-account", b"account-proof".to_vec());
    assert!(!format!("{config:?}").contains("account-proof"));
    let accepted = UdpTransport::bind_connectivity(config).await.unwrap();
    let peer =
        UdpTransport::bind_connectivity(config_for("other", b"account-proof".to_vec())).await;
    assert_eq!(peer.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    let second =
        UdpTransport::bind_connectivity(config_for("second-account", b"account-proof".to_vec()))
            .await
            .unwrap();
    eventually_within(
        "custom admitted accounts discover selected paths",
        SETTLE,
        || {
            accepted.path_to(&"second-account".into()) == Some(expected)
                && second.path_to(&"canonical-account".into()) == Some(expected)
        },
    )
    .await;
    // Policy approval alone is enough; no provisioned keys or keypairs are used.
    if expected == PeerPath::Direct {
        relay.close().await;
    }
    second
        .send(&"canonical-account".into(), b"policy admitted traffic")
        .await
        .unwrap();
    let received = timeout(SETTLE, accepted.recv()).await.unwrap().unwrap();
    assert_eq!(received.from, NodeId::from("second-account"));
    assert_eq!(received.msg, b"policy admitted traffic");
    accepted
        .send(&"second-account".into(), b"reverse account traffic")
        .await
        .unwrap();
    let received = timeout(SETTLE, second.recv()).await.unwrap().unwrap();
    assert_eq!(received.from, NodeId::from("canonical-account"));
    assert_eq!(received.msg, b"reverse account traffic");
    second.connection().unwrap().close().await;
    accepted.connection().unwrap().close().await;
    relay.close().await;
}

#[tokio::test]
async fn custom_credentials_and_canonical_identity_are_enforced_without_keypairs() {
    custom_accounts(PathPolicy::RelayOnly, PeerPath::Relay).await;
}

#[tokio::test]
async fn custom_credentials_and_canonical_identity_enforce_keyless_direct_admission() {
    custom_accounts(PathPolicy::DirectPreferred, PeerPath::Direct).await;
}

async fn duplicate_incumbent(policy: PathPolicy, expected: PeerPath) {
    let relay = Rendezvous::bind_open(loopback()).await.unwrap();
    let address = relay.local_addr().unwrap();
    let config = |local: &str| {
        let mut config = PunchConfig::open(local.into(), address);
        config.bind = loopback();
        config.policy = policy;
        config
    };
    let a = UdpTransport::bind_connectivity(config("a")).await.unwrap();
    let b = UdpTransport::bind_connectivity(config("b")).await.unwrap();
    eventually_within("open peers discover incumbent", SETTLE, || {
        a.path_to(&"b".into()) == Some(expected) && b.path_to(&"a".into()) == Some(expected)
    })
    .await;
    assert_eq!(
        UdpTransport::bind_connectivity(config("a"))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(a.path_to(&"b".into()), Some(expected));
    assert_eq!(b.path_to(&"a".into()), Some(expected));
    a.send(&"b".into(), b"incumbent still owns ID")
        .await
        .unwrap();
    let received = timeout(SETTLE, b.recv()).await.unwrap().unwrap();
    assert_eq!(received.from, NodeId::from("a"));
    assert_eq!(received.msg, b"incumbent still owns ID");
    b.send(&"a".into(), b"incumbent still receives")
        .await
        .unwrap();
    let received = timeout(SETTLE, a.recv()).await.unwrap().unwrap();
    assert_eq!(received.from, NodeId::from("b"));
    assert_eq!(received.msg, b"incumbent still receives");
    a.connection().unwrap().close().await;
    b.connection().unwrap().close().await;
    relay.close().await;
}

#[tokio::test]
async fn duplicate_dynamic_identity_never_evicts_the_incumbent() {
    duplicate_incumbent(PathPolicy::RelayOnly, PeerPath::Relay).await;
}

#[tokio::test]
async fn duplicate_keyless_direct_identity_never_evicts_the_incumbent() {
    duplicate_incumbent(PathPolicy::DirectPreferred, PeerPath::Direct).await;
}

#[tokio::test]
async fn explicit_keyed_dynamic_mode_never_downgrades_and_credentials_remain_bounded() {
    let key = NetworkKey::from_bytes([41; 32]);
    let relay = Rendezvous::bind_with_admission(
        loopback(),
        Some(key.clone()),
        Arc::new(groupnet_transport::admission::OpenAdmission),
    )
    .await
    .unwrap();
    let address = relay.local_addr().unwrap();
    for policy in [PathPolicy::RelayOnly, PathPolicy::DirectPreferred] {
        let mut config = PunchConfig::open("open".into(), address);
        config.policy = policy;
        assert_eq!(
            UdpTransport::bind_connectivity(config)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
    }
    let mut config = PunchConfig::dynamic("keyed".into(), address, Some(key), Vec::new());
    config.policy = PathPolicy::RelayOnly;
    let accepted = UdpTransport::bind_connectivity(config).await.unwrap();
    let mut oversized = PunchConfig::open(NodeId::from("oversized"), address);
    oversized.credential = vec![0; MAX_CREDENTIAL_BYTES + 1];
    assert_eq!(
        UdpTransport::bind_connectivity(oversized)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    accepted.connection().unwrap().close().await;
    relay.close().await;
}
