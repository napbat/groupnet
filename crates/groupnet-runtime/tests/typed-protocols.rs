//! Typed peer lifetimes, shared configuration, and node-wide endpoint shutdown.

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network::{
    TunnelConfig,
    tunnel::{PeerIdentity, TlsIdentity},
};
use groupnet_runtime::messaging::{Bytes, Delivery, SendOptions};
use groupnet_runtime::{
    MessageProtocol, Messages, Messaging, Node, Ordered, PeerImplementation, SessionProtocol,
    Unordered, UnorderedConfig, UnorderedDelivery,
};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::link::LinkConfig;
use groupnet_transport_mem::Network;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const WAIT: Duration = Duration::from_secs(5);

fn identities(count: usize) -> Vec<(TlsIdentity, Vec<u8>)> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    (0..count)
        .map(|_| {
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
        })
        .collect()
}

#[tokio::test]
async fn typed_message_peer_retains_node_and_existing_receive_owner() -> io::Result<()> {
    let node = Node::builder(NodeId::new("typed-local")).start().await?;
    let peer = node.peer(node.id().clone(), Messages::best_effort())?;
    let router = node.router().clone();
    drop(node);
    assert!(!router.is_closed());
    let id = peer.send(Bytes::from_static(b"typed")).await?;
    let (context, payload) = tokio::time::timeout(WAIT, peer.node().recv()).await??;
    assert_eq!(context.id, id);
    assert_eq!(context.from, *peer.id());
    assert_eq!(context.group, None);
    assert_eq!(payload.as_ref(), b"typed");
    context.applied()?;
    peer.node().close().await;
    assert_eq!(
        peer.send(Bytes::new()).await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert!(router.is_closed());
    Ok(())
}

#[tokio::test]
async fn cached_session_endpoints_reject_policy_changes_and_wake_on_node_close() -> io::Result<()> {
    let node = Node::builder(NodeId::new("sessions-local"))
        .tunnels(TunnelConfig::new(identities(1).remove(0).0, []))
        .start()
        .await?;
    let ordered = node.endpoint(Ordered::new())?;
    let ordered_again = node.endpoint(Ordered::new())?;
    let config = UnorderedConfig::default();
    node.configure_unordered(config.clone())?;
    let unordered = node.endpoint(Unordered::reliable())?;
    let unordered_again = node.endpoint(Unordered::reliable())?;
    node.configure_unordered(config.clone())?;
    let mut changed = config;
    changed.allow_unreliable = false;
    assert_eq!(
        node.configure_unordered(changed).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let ordered_recv = tokio::spawn(async move { ordered.accept().await });
    let unordered_recv = tokio::spawn(async move { unordered.accept().await });
    tokio::time::timeout(WAIT, node.close()).await?;
    // Endpoint cancellation can precede router cancellation during node close.
    // Both blocked accepts must terminate with failure, whichever wakes first.
    assert!(tokio::time::timeout(WAIT, ordered_recv).await??.is_err());
    assert!(tokio::time::timeout(WAIT, unordered_recv).await??.is_err());
    tokio::time::timeout(WAIT, ordered_again.protocol().closed()).await?;
    tokio::time::timeout(WAIT, unordered_again.protocol().closed()).await?;
    assert_eq!(
        node.endpoint(Ordered::new()).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        node.endpoint(Unordered::reliable()).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    Ok(())
}

#[tokio::test]
async fn session_conveniences_require_configured_tls_without_altering_messages() -> io::Result<()> {
    let node = Node::builder(NodeId::new("messages-only")).start().await?;
    assert_eq!(
        node.peer(node.id().clone(), Ordered::new())
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    assert_eq!(
        node.peer(node.id().clone(), Unordered::reliable())
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    node.send(node.id(), b"still available").await?;
    assert_eq!(
        tokio::time::timeout(WAIT, node.recv()).await??.1.as_ref(),
        b"still available"
    );
    node.close().await;
    Ok(())
}

#[tokio::test]
async fn temporary_peer_drop_does_not_cancel_returned_ordered_stream() -> io::Result<()> {
    let mut credentials = identities(2);
    let (b_identity, b_leaf) = credentials.remove(1);
    let (a_identity, a_leaf) = credentials.remove(0);
    let a_id = NodeId::new("temporary-a");
    let b_id = NodeId::new("temporary-b");
    let a = Node::builder(a_id.clone())
        .tunnels(TunnelConfig::new(
            a_identity,
            [PeerIdentity::new(b_id.clone(), &b_leaf)?],
        ))
        .start()
        .await?;
    let b = Node::builder(b_id.clone())
        .tunnels(TunnelConfig::new(
            b_identity,
            [PeerIdentity::new(a_id.clone(), &a_leaf)?],
        ))
        .start()
        .await?;
    let memory = Network::new();
    for (local, remote) in [(&a, &b), (&b, &a)] {
        local.router().add_transport(
            memory.endpoint(local.id().clone()),
            LinkConfig {
                peers: vec![remote.id().clone()],
                cost: 1,
                mtu: 1200,
            },
        )?;
    }
    eventually_within("ordered peer routes", WAIT, || {
        a.router().route_to(&b_id).is_some() && b.router().route_to(&a_id).is_some()
    })
    .await;
    let (outgoing, incoming) = tokio::time::timeout(WAIT, async {
        tokio::join!(
            async { a.peer(b_id, Ordered::new())?.connect().await },
            async { b.endpoint(Ordered::new())?.accept().await },
        )
    })
    .await?;
    let mut outgoing = outgoing?;
    let (origin, mut incoming) = incoming?;
    assert_eq!(origin, a_id);
    // Both temporary peer/endpoint clones are gone before the first write.
    tokio::time::timeout(WAIT, async {
        outgoing.write_all(b"still alive").await?;
        outgoing.flush().await?;
        let mut bytes = [0; 11];
        incoming.read_exact(&mut bytes).await?;
        assert_eq!(&bytes, b"still alive");
        incoming.write_all(b"reply").await?;
        incoming.flush().await?;
        let mut reply = [0; 5];
        outgoing.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"reply");
        Ok::<_, io::Error>(())
    })
    .await??;
    drop(outgoing);
    drop(incoming);
    a.close().await;
    b.close().await;
    Ok(())
}

async fn messaging_nodes() -> io::Result<(Node, Node)> {
    let mut credentials = identities(2);
    let (b_identity, b_leaf) = credentials.remove(1);
    let (a_identity, a_leaf) = credentials.remove(0);
    let a_id = NodeId::new("messages-a");
    let b_id = NodeId::new("messages-b");
    let memory = Network::new();
    let a = Node::builder(a_id.clone())
        .link(groupnet_transport_mem::MemLink::new(
            memory.endpoint(a_id.clone()),
            vec![b_id.clone()],
        ))
        .tunnels(TunnelConfig::new(
            a_identity,
            [PeerIdentity::new(b_id.clone(), &b_leaf)?],
        ))
        .gossip_interval_ms(20)
        .start()
        .await?;
    let b = Node::builder(b_id.clone())
        .link(groupnet_transport_mem::MemLink::new(
            memory.endpoint(b_id.clone()),
            vec![a_id.clone()],
        ))
        .tunnels(TunnelConfig::new(
            b_identity,
            [PeerIdentity::new(a_id.clone(), &a_leaf)?],
        ))
        .gossip_interval_ms(20)
        .start()
        .await?;
    eventually_within("typed protocol routes", WAIT, || {
        a.router().route_to(&b_id).is_some() && b.router().route_to(&a_id).is_some()
    })
    .await;
    Ok((a, b))
}

#[tokio::test]
async fn messaging_shutdown_keeps_coordination_and_existing_and_new_ordered_streams_usable()
-> io::Result<()> {
    let (a, b) = messaging_nodes().await?;
    let b_id = b.id().clone();
    let a_group = a.join_group("independent");
    let b_group = b.join_group("independent");
    eventually_within("independent group membership", WAIT, || {
        a_group.members().len() == 2 && b_group.members().len() == 2
    })
    .await;
    let a_ordered = a.endpoint(Ordered::new())?;
    let b_ordered = b.endpoint(Ordered::new())?;
    let (outgoing, incoming) = tokio::time::timeout(WAIT, async {
        tokio::join!(a_ordered.connect(&b_id), b_ordered.accept())
    })
    .await?;
    let mut outgoing = outgoing?;
    let mut incoming = incoming?.1;
    let node_callback = a.on_recv(|_, _| async {
        panic!("idle node callback unexpectedly received a frame");
    })?;
    let group_callback = a_group.on_frame(|_| async {
        panic!("idle group callback unexpectedly received a frame");
    })?;
    assert_eq!(
        a.recv().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        a_group.recv_frame().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let mut node_wait = Box::pin(node_callback.wait());
    let mut group_wait = Box::pin(group_callback.wait());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut node_wait)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut group_wait)
            .await
            .is_err()
    );
    a.endpoint(Messages::best_effort())?.protocol().shutdown();
    assert_eq!(
        tokio::time::timeout(WAIT, &mut node_wait)
            .await?
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        tokio::time::timeout(WAIT, &mut group_wait)
            .await?
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert!(!a.router().is_closed());
    a_group.sync(|state| state.update_metadata("after-messaging", "alive"));
    eventually_within("coordination after messaging shutdown", WAIT, || {
        a_group.metadata("after-messaging").as_deref() == Some("alive")
            && b_group.metadata("after-messaging").as_deref() == Some("alive")
    })
    .await;
    tokio::time::timeout(WAIT, async {
        outgoing.write_all(b"existing").await?;
        outgoing.flush().await?;
        let mut bytes = [0; 8];
        incoming.read_exact(&mut bytes).await?;
        assert_eq!(&bytes, b"existing");
        let (new_outgoing, new_incoming) =
            tokio::join!(a_ordered.connect(&b_id), b_ordered.accept(),);
        let mut new_outgoing = new_outgoing?;
        let mut new_incoming = new_incoming?.1;
        new_outgoing.write_all(b"new").await?;
        new_outgoing.flush().await?;
        let mut bytes = [0; 3];
        new_incoming.read_exact(&mut bytes).await?;
        assert_eq!(&bytes, b"new");
        Ok::<_, io::Error>(())
    })
    .await??;
    drop(outgoing);
    drop(incoming);
    a.close().await;
    b.close().await;
    Ok(())
}

#[tokio::test]
async fn bound_message_policies_preserve_node_inbox_and_application_boundary() -> io::Result<()> {
    let node = Node::builder(NodeId::new("bound-policies")).start().await?;
    let receiver = node.endpoint(Messages::best_effort())?;
    let applied = node.peer(node.id().clone(), Messages::applied(WAIT))?;
    let delivered = node.peer(node.id().clone(), Messages::delivered(WAIT))?;
    assert_eq!(applied.options().delivery, Delivery::Applied);
    assert_eq!(delivered.options().delivery, Delivery::Delivered);
    let callback = receiver.on_recv(|_, _| async { Ok(()) })?;
    assert_eq!(
        node.recv().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        receiver.recv().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    callback.close().await?;

    let mut sent = Box::pin(applied.send(Bytes::from_static(b"applied")));
    let (context, bytes) = tokio::time::timeout(WAIT, async {
        tokio::select! {
            result = &mut sent => panic!("application send completed before processing: {result:?}"),
            frame = receiver.recv() => frame,
        }
    }).await??;
    assert_eq!(bytes.as_ref(), b"applied");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut sent)
            .await
            .is_err()
    );
    context.applied()?;
    assert_eq!(tokio::time::timeout(WAIT, sent).await??, context.id);

    // Delivered completes after queue acceptance even though no application
    // receipt is completed and no node receiver has fetched the queued frame.
    let delivered_id =
        tokio::time::timeout(WAIT, delivered.send(Bytes::from_static(b"delivered"))).await??;
    let (context, bytes) = receiver.recv().await?;
    assert_eq!(context.id, delivered_id);
    assert_eq!(bytes.as_ref(), b"delivered");
    node.close().await;
    Ok(())
}

#[tokio::test]
async fn unordered_bindings_share_node_settings_and_accept_only_the_selected_policy()
-> io::Result<()> {
    let (a, b) = messaging_nodes().await?;
    let reliable = b.endpoint(Unordered::reliable())?;
    let unreliable = b.endpoint(Unordered::unreliable())?;
    let (outgoing_reliable, outgoing_unreliable, incoming_reliable, incoming_unreliable) =
        tokio::time::timeout(WAIT, async {
            tokio::join!(
                async {
                    a.peer(b.id().clone(), Unordered::reliable())?
                        .connect()
                        .await
                },
                async {
                    a.peer(b.id().clone(), Unordered::unreliable())?
                        .connect()
                        .await
                },
                reliable.accept(),
                unreliable.accept(),
            )
        })
        .await?;
    let outgoing_reliable = outgoing_reliable?;
    let outgoing_unreliable = outgoing_unreliable?;
    let (reliable_origin, incoming_reliable) = incoming_reliable?;
    let (unreliable_origin, incoming_unreliable) = incoming_unreliable?;
    assert_eq!(reliable_origin, *a.id());
    assert_eq!(unreliable_origin, *a.id());
    assert_eq!(incoming_reliable.delivery(), UnorderedDelivery::Reliable);
    assert_eq!(
        incoming_unreliable.delivery(),
        UnorderedDelivery::Unreliable
    );
    // Temporary outgoing peers have been dropped; node caches keep both sessions alive.
    tokio::time::timeout(WAIT, async {
        outgoing_reliable
            .send(Bytes::from_static(b"reliable"))
            .await?;
        outgoing_unreliable
            .send(Bytes::from_static(b"unreliable"))
            .await?;
        assert_eq!(incoming_reliable.recv().await?.as_ref(), b"reliable");
        assert_eq!(incoming_unreliable.recv().await?.as_ref(), b"unreliable");
        Ok::<_, io::Error>(())
    })
    .await??;
    a.close().await;
    b.close().await;
    Ok(())
}

#[tokio::test]
async fn node_unordered_settings_are_fixed_before_endpoint_or_peer_creation() -> io::Result<()> {
    let node = Node::builder(NodeId::new("configured-policy"))
        .tunnels(TunnelConfig::new(identities(1).remove(0).0, []))
        .start()
        .await?;
    let config = UnorderedConfig {
        allow_unreliable: false,
        max_sessions: 2,
        sessions_per_peer: 1,
        ..UnorderedConfig::default()
    };
    node.configure_unordered(config.clone())?;
    // No route or admitted remote exists; binding still succeeds without connecting.
    let peer = node.peer(NodeId::new("absent"), Unordered::reliable())?;
    let ordered = node.peer(NodeId::new("absent"), Ordered::new())?;
    assert_eq!(peer.options().delivery, UnorderedDelivery::Reliable);
    assert_eq!(
        node.peer(NodeId::new("absent"), Unordered::unreliable())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput,
    );
    node.configure_unordered(config.clone())?;
    let changed = UnorderedConfig {
        max_sessions: 3,
        ..config
    };
    assert_eq!(
        node.configure_unordered(changed).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    node.close().await;
    assert_eq!(
        ordered.connect().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    Ok(())
}

#[derive(Debug)]
struct CustomMessages {
    bindings: Arc<AtomicUsize>,
    sends: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct CountingMessages {
    protocol: Messaging,
    sends: Arc<AtomicUsize>,
}

impl PeerImplementation for CustomMessages {
    type Protocol = CountingMessages;
    type Options = SendOptions;

    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)> {
        self.bindings.fetch_add(1, Ordering::SeqCst);
        Ok((
            CountingMessages {
                protocol: node.endpoint(Messages::best_effort())?.protocol().clone(),
                sends: self.sends,
            },
            SendOptions::default(),
        ))
    }
}

impl MessageProtocol for CountingMessages {
    const ID: groupnet_network::ProtocolId = Messaging::ID;
    type SendOptions = SendOptions;
    type Receipt = groupnet_runtime::messaging::Receipt;

    fn send(
        &self,
        to: &NodeId,
        group: Option<&groupnet_core::GroupId>,
        payload: Bytes,
        options: SendOptions,
    ) -> impl Future<Output = io::Result<groupnet_runtime::messaging::MessageId>> + Send {
        // Intentionally eager: peer cancellation must win before constructing
        // this future, not merely before polling it.
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.protocol.send(to, group, payload, options)
    }

    async fn recv(&self) -> io::Result<groupnet_runtime::messaging::Frame<Self::Receipt>> {
        self.protocol.recv().await
    }

    fn shutdown(&self) {
        self.protocol.shutdown();
    }

    async fn closed(&self) {
        self.protocol.closed().await;
    }
}

#[derive(Debug)]
struct CustomOrdered {
    connects: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct CountingOrdered {
    protocol: groupnet_streams::OrderedProtocol,
    connects: Arc<AtomicUsize>,
}

impl PeerImplementation for CustomOrdered {
    type Protocol = CountingOrdered;
    type Options = ();

    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)> {
        Ok((
            CountingOrdered {
                protocol: node.endpoint(Ordered::new())?.protocol().clone(),
                connects: self.connects,
            },
            (),
        ))
    }
}

impl SessionProtocol for CountingOrdered {
    type Session = groupnet_streams::TunneledStream;
    type ConnectOptions = ();

    fn connect(
        &self,
        to: &NodeId,
        options: (),
    ) -> impl Future<Output = io::Result<Self::Session>> + Send {
        self.connects.fetch_add(1, Ordering::SeqCst);
        self.protocol.connect(to, options)
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Session)> {
        self.protocol.accept().await
    }
}

#[tokio::test]
async fn custom_binding_resolves_once_and_closed_node_prevents_eager_protocol_calls()
-> io::Result<()> {
    let node = Node::builder(NodeId::new("custom-binding"))
        .tunnels(TunnelConfig::new(identities(1).remove(0).0, []))
        .start()
        .await?;
    let bindings = Arc::new(AtomicUsize::new(0));
    let sends = Arc::new(AtomicUsize::new(0));
    let connects = Arc::new(AtomicUsize::new(0));
    let peer = node.peer(
        node.id().clone(),
        CustomMessages {
            bindings: bindings.clone(),
            sends: sends.clone(),
        },
    )?;
    // Selectors need not implement Clone; the resolved protocol and options do.
    let cloned_peer = peer.clone();
    assert_eq!(bindings.load(Ordering::SeqCst), 1);
    cloned_peer.send(Bytes::from_static(b"custom")).await?;
    assert_eq!(node.recv().await?.1.as_ref(), b"custom");
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    let session_peer = node.peer(
        NodeId::new("absent"),
        CustomOrdered {
            connects: connects.clone(),
        },
    )?;
    node.close().await;
    assert_eq!(
        peer.send(Bytes::new()).await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        session_peer.connect().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    assert_eq!(connects.load(Ordering::SeqCst), 0);
    assert_eq!(bindings.load(Ordering::SeqCst), 1);
    assert!(
        node.peer(
            node.id().clone(),
            CustomMessages {
                bindings: bindings.clone(),
                sends
            }
        )
        .is_err()
    );
    assert_eq!(bindings.load(Ordering::SeqCst), 1);
    Ok(())
}
