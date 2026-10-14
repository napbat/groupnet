//! Ordered endpoint lifetime is independent of the shared node tunnel transport,
//! and each stream protocol has its own isolated accept queue.

use std::{io, time::Duration};

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network::{
    TunnelConfig,
    tunnel::{PeerIdentity, TlsIdentity},
};
// The network types this crate's signatures take come from its own re-exports.
use groupnet_streams::{
    OrderedConfig, OrderedConfigError, OrderedProtocol, QueueCapacity, Router, SessionProtocol,
    StreamProtocolId, TunnelTransport, TunneledStream,
};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::{bulk::BulkTransport, link::LinkConfig};
use groupnet_transport_mem::Network;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(10);

struct Fabric {
    left: Router,
    right: Router,
    sender: TunnelTransport,
    receiver: TunnelTransport,
}

impl Fabric {
    async fn new() -> Self {
        let network = Network::new();
        let left = Router::new(NodeId::new("left")).unwrap();
        let right = Router::new(NodeId::new("right")).unwrap();
        left.add_transport(
            network.endpoint(left.local_id().clone()),
            LinkConfig::new(vec![right.local_id().clone()]),
        )
        .unwrap();
        right
            .add_transport(
                network.endpoint(right.local_id().clone()),
                LinkConfig::new(vec![left.local_id().clone()]),
            )
            .unwrap();
        eventually_within("bidirectional route", DEADLINE, || {
            left.route_to(right.local_id()).is_some() && right.route_to(left.local_id()).is_some()
        })
        .await;
        let ((left_identity, left_leaf), (right_identity, right_leaf)) = credentials();
        let sender = TunnelTransport::with_config(
            left.clone(),
            TunnelConfig::new(
                left_identity,
                vec![PeerIdentity::new(right.local_id().clone(), &right_leaf).unwrap()],
            ),
        )
        .unwrap();
        let receiver = TunnelTransport::with_config(
            right.clone(),
            TunnelConfig::new(
                right_identity,
                vec![PeerIdentity::new(left.local_id().clone(), &left_leaf).unwrap()],
            ),
        )
        .unwrap();
        Self {
            left,
            right,
            sender,
            receiver,
        }
    }

    async fn close(self) {
        self.sender.close().await;
        self.receiver.close().await;
        self.left.close().await;
        self.right.close().await;
    }
}

fn credentials() -> ((TlsIdentity, Vec<u8>), (TlsIdentity, Vec<u8>)) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    let make = || {
        let mut params = CertificateParams::new(vec!["groupnet.peer".to_owned()]).unwrap();
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate().unwrap();
        let leaf = params.signed_by(&key, &ca).unwrap().der().to_vec();
        let identity = TlsIdentity::from_der(
            vec![ca.der().to_vec()],
            vec![leaf.clone()],
            key.serialize_der(),
        )
        .unwrap();
        (identity, leaf)
    };
    (make(), make())
}

async fn connect<P: SessionProtocol<Session = TunneledStream, ConnectOptions = ()>>(
    endpoint: &P,
    peer: &NodeId,
) -> io::Result<TunneledStream> {
    endpoint.connect(peer, ()).await
}

#[tokio::test]
async fn ordered_shutdown_cancels_only_its_streams_and_blocked_calls() {
    let fabric = Fabric::new().await;
    let sender = OrderedProtocol::new(fabric.sender.clone()).unwrap();
    let receiver = OrderedProtocol::new(fabric.receiver.clone()).unwrap();
    let (client, server) = timeout(DEADLINE, async {
        tokio::join!(connect(&sender, fabric.right.local_id()), receiver.accept())
    })
    .await
    .unwrap();
    let mut client = client.unwrap();
    let (peer, mut server) = server.unwrap();
    assert_eq!(peer, *fabric.left.local_id());
    client.write_all(b"ordered bytes").await.unwrap();
    let mut bytes = [0; 13];
    server.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"ordered bytes");
    sender.shutdown();
    sender.closed().await;
    assert!(client.write_all(b"closed").await.is_err());
    assert!(sender.connect(fabric.right.local_id(), ()).await.is_err());
    assert!(sender.accept().await.is_err());

    // The node's tunnel plane and unordered setup plane remain usable: a new
    // endpoint on the same tunnels reaches the receiver, and setup channels
    // still flow.
    let sender_again = OrderedProtocol::new(fabric.sender.clone()).unwrap();
    timeout(DEADLINE, async {
        let (new, accepted, control, control_accepted) = tokio::join!(
            connect(&sender_again, fabric.right.local_id()),
            receiver.accept(),
            fabric.sender.connect_control(fabric.right.local_id()),
            fabric.receiver.accept_control(),
        );
        let mut new = new.unwrap();
        let mut accepted = accepted.unwrap().1;
        let _control = control.unwrap();
        let _control_accepted = control_accepted.unwrap();
        new.write_all(b"node still alive").await.unwrap();
        let mut bytes = [0; 16];
        accepted.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"node still alive");
    })
    .await
    .unwrap();

    timeout(DEADLINE, async {
        let shutdown = async {
            receiver.shutdown();
        };
        let (accepted, ()) = tokio::join!(receiver.accept(), shutdown);
        assert!(accepted.is_err());
        receiver.closed().await;
        assert!(server.read(&mut [0]).await.is_err());
    })
    .await
    .unwrap();
    fabric.close().await;
}

#[tokio::test]
async fn clones_share_lifetime_but_last_endpoint_drop_does_not_close_transport() {
    let fabric = Fabric::new().await;
    let endpoint = OrderedProtocol::new(fabric.sender.clone()).unwrap();
    let clone = endpoint.clone();
    let receiver = OrderedProtocol::new(fabric.receiver.clone()).unwrap();
    let (client, server) = timeout(DEADLINE, async {
        tokio::join!(
            endpoint.connect(fabric.right.local_id(), ()),
            receiver.accept()
        )
    })
    .await
    .unwrap();
    let mut client = client.unwrap();
    let mut server = server.unwrap().1;
    drop(endpoint);
    client.write_all(b"clone alive").await.unwrap();
    let mut bytes = [0; 11];
    server.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"clone alive");
    drop(clone);
    assert!(client.cancellation().is_cancelled());
    assert!(client.read(&mut [0]).await.is_err());
    let fresh = OrderedProtocol::new(fabric.sender.clone()).unwrap();
    let (connected, accepted) = timeout(DEADLINE, async {
        tokio::join!(
            fresh.connect(fabric.right.local_id(), ()),
            receiver.accept()
        )
    })
    .await
    .unwrap();
    assert!(connected.is_ok());
    assert!(accepted.is_ok());
    fabric.close().await;
}

#[tokio::test]
async fn node_shutdown_notifies_endpoint_and_interrupts_accept() {
    let fabric = Fabric::new().await;
    let endpoint = OrderedProtocol::new(fabric.receiver.clone()).unwrap();
    timeout(DEADLINE, async {
        let (accepted, ()) = tokio::join!(endpoint.accept(), fabric.receiver.close());
        assert!(accepted.is_err());
        endpoint.closed().await;
    })
    .await
    .unwrap();
    fabric.close().await;
}

#[tokio::test]
async fn cancelled_connect_releases_endpoint_sessions_without_cancelling_shared_transport() {
    let ((identity, _), (_, peer_leaf)) = credentials();
    let router = Router::new(NodeId::new("isolated")).unwrap();
    let peer = NodeId::new("unreachable");
    let tunnels = TunnelTransport::with_config(
        router.clone(),
        TunnelConfig::new(
            identity,
            vec![PeerIdentity::new(peer.clone(), &peer_leaf).unwrap()],
        ),
    )
    .unwrap();
    let endpoint = OrderedProtocol::new(tunnels.clone()).unwrap();
    for _ in 0..16 {
        assert!(
            timeout(Duration::from_millis(25), endpoint.connect(&peer, ()))
                .await
                .is_err()
        );
        tokio::task::yield_now().await;
    }
    let shutdown = async {
        endpoint.shutdown();
    };
    let (connected, ()) = tokio::join!(endpoint.connect(&peer, ()), shutdown);
    assert!(connected.is_err());
    assert!(!tunnels.cancellation().is_cancelled());
    tunnels.close().await;
    router.close().await;
}

/// Writes `tag` on a fresh stream of `endpoint`'s protocol.
async fn tagged(endpoint: &OrderedProtocol, peer: &NodeId, tag: u8) -> TunneledStream {
    let mut stream = endpoint.connect(peer, ()).await.unwrap();
    stream.write_all(&[tag]).await.unwrap();
    stream.flush().await.unwrap();
    stream
}

/// Accepts one stream from `endpoint` and returns its first byte.
async fn first_byte(endpoint: &OrderedProtocol) -> u8 {
    let (_, mut stream) = endpoint.accept().await.unwrap();
    let mut tag = [0];
    stream.read_exact(&mut tag).await.unwrap();
    tag[0]
}

#[tokio::test]
async fn protocol_scoped_consumers_never_receive_each_others_streams() {
    const RPC: StreamProtocolId = StreamProtocolId::new(1);
    const BLOBS: StreamProtocolId = StreamProtocolId::new(2);
    const STREAMS: u8 = 4;

    let fabric = Fabric::new().await;
    let receiver = OrderedProtocol::new(fabric.receiver.clone()).unwrap();
    let (rpc_in, blobs_in) = (receiver.scoped(RPC), receiver.scoped(BLOBS));
    assert_eq!(rpc_in.protocol(), RPC);
    assert_eq!(receiver.protocol(), StreamProtocolId::DEFAULT);
    let sender = OrderedProtocol::new(fabric.sender.clone()).unwrap();
    let (rpc_out, blobs_out) = (sender.scoped(RPC), sender.scoped(BLOBS));
    let peer = fabric.right.local_id().clone();

    // Interleaved connects of both protocols, all queued before any accept.
    let mut held = Vec::new();
    timeout(DEADLINE, async {
        for _ in 0..STREAMS {
            held.push(tagged(&rpc_out, &peer, b'r').await);
            held.push(tagged(&blobs_out, &peer, b'b').await);
        }
    })
    .await
    .unwrap();

    // Each independent accept loop sees exactly its own protocol's streams.
    timeout(DEADLINE, async {
        for _ in 0..STREAMS {
            assert_eq!(first_byte(&blobs_in).await, b'b');
        }
        for _ in 0..STREAMS {
            assert_eq!(first_byte(&rpc_in).await, b'r');
        }
    })
    .await
    .unwrap();
    for (name, endpoint) in [
        ("rpc", &rpc_in),
        ("blobs", &blobs_in),
        ("default", &receiver),
    ] {
        assert!(
            timeout(Duration::from_millis(100), endpoint.accept())
                .await
                .is_err(),
            "{name} accepted a stream that was never connected to it"
        );
    }

    // A protocol the receiver never bound is refused at setup.
    let unbound = sender.scoped(StreamProtocolId::new(3));
    let refused = timeout(DEADLINE, unbound.connect(&peer, ()))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(refused.kind(), io::ErrorKind::ConnectionRefused);
    fabric.close().await;
}

#[tokio::test]
async fn a_full_protocol_queue_refuses_without_touching_its_neighbour() {
    const FULL: StreamProtocolId = StreamProtocolId::new(9);
    let fabric = Fabric::new().await;
    let config = OrderedConfig {
        accept_queue: QueueCapacity::MIN,
        ..OrderedConfig::default()
    };
    let receiver = OrderedProtocol::with_config(fabric.receiver.clone(), config).unwrap();
    let full = receiver.scoped(FULL);
    let sender = OrderedProtocol::new(fabric.sender.clone()).unwrap();
    let peer = fabric.right.local_id().clone();
    let queued = timeout(DEADLINE, tagged(&sender.scoped(FULL), &peer, 1))
        .await
        .unwrap();
    let refused = timeout(DEADLINE, sender.scoped(FULL).connect(&peer, ()))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(refused.kind(), io::ErrorKind::WouldBlock);

    // The default protocol's queue is its own.
    let (default_out, default_in) = timeout(DEADLINE, async {
        tokio::join!(tagged(&sender, &peer, 2), first_byte(&receiver))
    })
    .await
    .unwrap();
    assert_eq!(default_in, 2);
    assert_eq!(timeout(DEADLINE, first_byte(&full)).await.unwrap(), 1);
    drop((queued, default_out));
    fabric.close().await;
}

#[tokio::test]
async fn invalid_config_is_rejected_before_any_dispatcher_starts() {
    let fabric = Fabric::new().await;
    let error = OrderedProtocol::with_config(
        fabric.receiver.clone(),
        OrderedConfig {
            setup_timeout: Duration::ZERO,
            ..OrderedConfig::default()
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<OrderedConfigError>()),
        Some(&OrderedConfigError::ZeroSetupTimeout)
    );
    // Nothing claimed the receiver's ordered queue: a raw tunnel accept still
    // receives the stream.
    let (connected, accepted) = timeout(DEADLINE, async {
        tokio::join!(
            fabric.sender.connect(fabric.right.local_id()),
            fabric.receiver.accept()
        )
    })
    .await
    .unwrap();
    assert!(connected.is_ok() && accepted.is_ok());
    fabric.close().await;
}
