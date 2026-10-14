use super::*;
use crate::{TcpAdmissionError, TcpMsgConfigError};
use groupnet_transport::framing::MAX_FRAME_BYTES;
use groupnet_transport::link::LinkConfigError;
use std::num::NonZeroUsize;
use std::time::Duration;

fn typed<E: std::error::Error + 'static>(error: &io::Error) -> &E {
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<E>())
        .expect("typed inner error")
}

#[tokio::test]
async fn provider_drains_listener_on_close() {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let address = reservation.local_addr().expect("address");
    drop(reservation);
    let peer = NodeId::new("tcp-peer");
    let link = TcpLink::new(
        address,
        vec![PeerEndpoint::new(
            peer,
            "127.0.0.1:12345".parse().expect("peer address"),
        )],
    )
    .with_cost(NonZeroU32::new(7).unwrap());
    let provider: Box<dyn LinkProvider> = Box::new(link);
    let bound = provider.bind(NodeId::new("tcp-local")).await.expect("bind");
    assert!(std::net::TcpListener::bind(address).is_err());
    tokio::time::timeout(Duration::from_secs(5), bound.driver.close())
        .await
        .expect("drain listener");
    let _replacement = std::net::TcpListener::bind(address).expect("listener released");
}

#[tokio::test]
async fn binding_is_the_validation_point_and_names_the_field() {
    let address: SocketAddr = "127.0.0.1:0".parse().expect("address");
    let invalid_msg = TcpLink::new(address, Vec::new()).with_msg_config(TcpMsgConfig {
        max_frame_bytes: NonZeroUsize::new(MAX_FRAME_BYTES + 1).unwrap(),
        ..TcpMsgConfig::default()
    });
    let error = Box::new(invalid_msg)
        .bind(NodeId::new("tcp-local"))
        .await
        .expect_err("invalid message config rejected at bind");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        typed::<TcpMsgConfigError>(&error),
        &TcpMsgConfigError::MaxFrameBytesAboveCeiling {
            max_frame_bytes: NonZeroUsize::new(MAX_FRAME_BYTES + 1).unwrap()
        }
    );

    let oversized = TcpLink::new(address, Vec::new()).with_credentials(vec![
        0;
        groupnet_transport::admission::MAX_CREDENTIAL_BYTES
            + 1
    ]);
    let error = Box::new(oversized)
        .bind(NodeId::new("tcp-local"))
        .await
        .expect_err("oversized credential rejected at bind");
    assert_eq!(
        typed::<TcpAdmissionError>(&error),
        &TcpAdmissionError::CredentialTooLong {
            len: groupnet_transport::admission::MAX_CREDENTIAL_BYTES + 1
        }
    );

    let zero_deadline =
        TcpLink::new(address, Vec::new()).with_admission_config(TcpAdmissionConfig {
            handshake_timeout: Duration::ZERO,
            ..TcpAdmissionConfig::default()
        });
    let error = Box::new(zero_deadline)
        .bind(NodeId::new("tcp-local"))
        .await
        .expect_err("zero handshake timeout rejected at bind");
    assert_eq!(
        typed::<TcpAdmissionError>(&error),
        &TcpAdmissionError::ZeroHandshakeTimeout
    );

    let link = TcpLink::new(address, Vec::new()).with_msg_config(TcpMsgConfig {
        max_frame_bytes: NonZeroUsize::new(512).unwrap(),
        ..TcpMsgConfig::default()
    });
    let bound = Box::new(link)
        .bind(NodeId::new("tcp-local"))
        .await
        .expect("bind");
    assert_eq!(
        bound.config.mtu.get(),
        512,
        "the stored config reached the transport"
    );
    tokio::time::timeout(Duration::from_secs(5), bound.driver.close())
        .await
        .expect("drain listener");
}

#[tokio::test]
async fn provider_propagates_listener_bind_errors() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let link = TcpLink::new(listener.local_addr().expect("address"), Vec::new());
    assert!(Box::new(link).bind(NodeId::new("tcp-local")).await.is_err());
}

#[tokio::test]
async fn invalid_peer_list_is_rejected_before_the_listener_opens() {
    // An occupied address would fail with `AddrInUse` if the listener opened first.
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let address = occupied.local_addr().expect("address");
    let endpoint: SocketAddr = "127.0.0.1:9".parse().expect("peer address");
    let local = NodeId::new("tcp-local");
    let cases = [
        (
            vec![
                PeerEndpoint::new(NodeId::new("tcp-peer"), endpoint),
                PeerEndpoint::new(local.clone(), endpoint),
            ],
            LinkConfigError::LocalPeer { index: 1 },
        ),
        (
            vec![
                PeerEndpoint::new(NodeId::new("tcp-peer"), endpoint),
                PeerEndpoint::new(NodeId::new("tcp-peer"), endpoint),
            ],
            LinkConfigError::DuplicatePeer { index: 1 },
        ),
    ];
    for (peers, expected) in cases {
        let error = Box::new(TcpLink::new(address, peers))
            .bind(local.clone())
            .await
            .expect_err("invalid peer list rejected at bind");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(typed::<LinkConfigError>(&error), &expected);
    }
}

#[tokio::test]
async fn dropping_unregistered_link_cancels_listener() {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let address = reservation.local_addr().expect("address");
    drop(reservation);
    let bound = Box::new(TcpLink::new(address, Vec::new()))
        .bind(NodeId::new("tcp-local"))
        .await
        .expect("bind");
    drop(bound);
    let _replacement = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match tokio::net::TcpListener::bind(address).await {
                Ok(listener) => break listener,
                Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("rebind failed: {error}"),
            }
        }
    })
    .await
    .expect("dropped provider released listener");
}

#[tokio::test]
async fn configured_admission_denial_names_the_claim() {
    let policy = ConfiguredAdmission(vec![NodeId::new("known")]);
    let stranger = NodeId::new("stranger");
    let error = policy
        .admit(JoinRequest::new(&stranger, &[], None))
        .await
        .expect_err("unconfigured peer denied");
    assert!(
        matches!(&error, AdmissionError::UnknownPeer { claimed } if *claimed == stranger),
        "{error:?}"
    );
    assert!(error.to_string().contains("stranger"), "{error}");
}

async fn dial_finished(transport: &TcpMsgTransport) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while transport.outbound_connections() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dial attempt drained");
}

#[tokio::test]
async fn static_admission_rejects_unknown_and_bootstrap_recovers_late_listener() {
    use groupnet_transport::Transport;
    use groupnet_transport::admission::OpenAdmission;

    let a_id = NodeId::new("static-a");
    let b_id = NodeId::new("static-b");
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve");
    let b_addr = reservation.local_addr().expect("address");
    drop(reservation);
    let a = TcpMsgTransport::bind_admitted(
        a_id.clone(),
        "127.0.0.1:0",
        TcpMsgConfig::default(),
        TcpAdmission::new(Arc::new(ConfiguredAdmission(vec![b_id.clone()]))),
    )
    .await
    .expect("a");
    a.register_peer(b_id.clone(), b_addr).unwrap();
    a.connect_peer(&b_id)
        .expect("best-effort unavailable bootstrap");
    // Observe the refused attempt before bringing up the configured peer.
    dial_finished(&a).await;
    a.bootstrap(vec![b_id.clone()]);
    let b = TcpMsgTransport::bind_admitted(
        b_id.clone(),
        b_addr,
        TcpMsgConfig::default(),
        TcpAdmission::new(Arc::new(ConfiguredAdmission(vec![a_id]))),
    )
    .await
    .expect("late b");
    tokio::time::timeout(Duration::from_secs(5), async {
        while a
            .sessions()
            .expect("sessions")
            .subscribe()
            .borrow()
            .is_empty()
            || b.sessions()
                .expect("sessions")
                .subscribe()
                .borrow()
                .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late bootstrap connected");
    b.send(a.local_id(), b"static topology")
        .await
        .expect("send");
    assert_eq!(
        (tokio::time::timeout(Duration::from_secs(5), a.recv())
            .await
            .expect("receive")
            .expect("frame")
            .msg)
            .as_ref(),
        b"static topology"
    );
    let unknown = TcpMsgTransport::bind_admitted(
        NodeId::new("unknown"),
        "127.0.0.1:0",
        TcpMsgConfig::default(),
        TcpAdmission::new(Arc::new(OpenAdmission)),
    )
    .await
    .expect("unknown");
    unknown
        .register_peer(a.local_id().clone(), a.local_addr())
        .unwrap();
    unknown.connect_peer(a.local_id()).expect("attempt");
    dial_finished(&unknown).await;
    assert!(
        unknown
            .sessions()
            .expect("sessions")
            .subscribe()
            .borrow()
            .is_empty()
    );
    assert_eq!(
        a.sessions().expect("sessions").subscribe().borrow().len(),
        1
    );
    unknown.close().await;
    a.close().await;
    b.close().await;
}

#[cfg(feature = "connectivity")]
#[tokio::test]
async fn native_link_rejects_a_mismatched_identity_before_binding() {
    use crate::{NetworkKey, PeerPolicy, TcpPunchConfig};

    let config = TcpPunchConfig::new(
        NodeId::new("configured-local"),
        "127.0.0.1:9".parse().expect("rendezvous"),
        Some(NetworkKey::from_bytes([3; 32])),
        PeerPolicy::Static(Vec::new()),
    );
    let link = TcpLink::connectivity(config).with_cost(NonZeroU32::new(4).unwrap());
    let error = Box::new(link)
        .bind(NodeId::new("different-local"))
        .await
        .expect_err("identity mismatch");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("TcpPunchConfig.local"),
        "{error}"
    );
}
