//! Integration test: **the runtime client role** (`docs/client-role.md`,
//! section 6.2).
//!
//! A [`Client`] binds the same `Endpoint`/`Peer` handles as a [`Node`], but
//! joins no group, gossips nothing and forwards nothing. A hub is an ordinary
//! `Node` that admits the client through its link.
//!
//! * `memory` runs over the in-memory transport: messages, ordered streams and
//!   unordered sessions in both directions, the client absent from every
//!   group of a two-member cluster, no transit through a client between two
//!   hubs (and, as the control, transit through a node), and close.
//! * `ipc` runs over native local IPC, with a dial-only client link and a hub
//!   link whose admission policy accepts peers it was never configured with.
//!
//! All waiting is a bounded poll on a predicate (`eventually_within`) or a
//! bounded timeout, never a bare sleep.

use std::io;
use std::time::Duration;

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_runtime::messaging::Bytes;
use groupnet_runtime::{
    Client, ClientBuilder, Messages, Node, NodeBuilder, Ordered, PeerIdentity, RouterConfig,
    TlsIdentity, TunnelConfig, Unordered,
};
use groupnet_testkit::cluster::eventually_within;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

mod ipc;
mod memory;

/// The bound of every wait.
const WAIT: Duration = Duration::from_secs(10);

/// The reserved routing group every node joins.
const ROUTING_GROUP: &str = "__groupnet_routing__";

/// Brisk routing timers, so routes form and expire in milliseconds.
fn brisk(forwarding: bool) -> RouterConfig {
    RouterConfig {
        forwarding,
        announce_interval: Duration::from_millis(30),
        route_ttl: Duration::from_millis(600),
        ..RouterConfig::default()
    }
}

/// One TLS identity per name, all issued by one test CA, with each leaf
/// certificate for pinning.
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

/// Tunnel configurations for a hub and a client that pin each other.
fn pinned_pair(hub: &NodeId, client: &NodeId) -> io::Result<(TunnelConfig, TunnelConfig)> {
    let mut credentials = identities(2);
    let (client_identity, client_leaf) = credentials.remove(1);
    let (hub_identity, hub_leaf) = credentials.remove(0);
    Ok((
        TunnelConfig::new(
            hub_identity,
            [PeerIdentity::new(client.clone(), &client_leaf)?],
        ),
        TunnelConfig::new(
            client_identity,
            [PeerIdentity::new(hub.clone(), &hub_leaf)?],
        ),
    ))
}

/// A node with brisk gossip and routing.
fn hub_builder(id: &NodeId) -> NodeBuilder {
    Node::builder(id.clone())
        .routing(brisk(true))
        .gossip_interval(Duration::from_millis(20))
}

/// A client with brisk routing.
fn client_builder(id: &NodeId) -> ClientBuilder {
    Client::builder(id.clone()).routing(brisk(false))
}

/// Waits until `hub` and `client` route to each other.
async fn routed(hub: &Node, client: &Client) {
    eventually_within("hub and client route to each other", WAIT, || {
        hub.router().route_to(client.id()).is_some() && client.router().route_to(hub.id()).is_some()
    })
    .await;
}

/// Applied messages from the client to the hub and back.
async fn messages_both_ways(hub: &Node, client: &Client) -> io::Result<()> {
    let to_hub = client.peer(hub.id().clone(), Messages::applied(WAIT))?;
    let (sent, received) = tokio::join!(to_hub.send(Bytes::from_static(b"ping")), async {
        let (context, payload) = hub.recv().await?;
        assert_eq!(context.from, *client.id());
        assert_eq!(context.group, None);
        assert_eq!(payload.as_ref(), b"ping");
        context.applied()
    });
    sent?;
    received?;

    let to_client = hub.peer(client.id().clone(), Messages::applied(WAIT))?;
    let (sent, received) = tokio::join!(to_client.send(Bytes::from_static(b"pong")), async {
        let (context, payload) = client.recv().await?;
        assert_eq!(context.from, *hub.id());
        assert_eq!(payload.as_ref(), b"pong");
        context.applied()
    });
    sent?;
    received
}

/// An ordered stream the client opens and the hub accepts, carrying bytes
/// both ways.
async fn ordered_stream(hub: &Node, client: &Client) -> io::Result<()> {
    let (outgoing, incoming) = tokio::join!(
        async {
            client
                .peer(hub.id().clone(), Ordered::new())?
                .connect()
                .await
        },
        async { hub.endpoint(Ordered::new())?.accept().await },
    );
    let mut outgoing = outgoing?;
    let (origin, mut incoming) = incoming?;
    assert_eq!(origin, *client.id());
    outgoing.write_all(b"request").await?;
    outgoing.flush().await?;
    let mut request = [0; 7];
    incoming.read_exact(&mut request).await?;
    assert_eq!(&request, b"request");
    incoming.write_all(b"reply").await?;
    incoming.flush().await?;
    let mut reply = [0; 5];
    outgoing.read_exact(&mut reply).await?;
    assert_eq!(&reply, b"reply");
    Ok(())
}

/// A reliable unordered session the hub opens and the client accepts.
async fn unordered_session(hub: &Node, client: &Client) -> io::Result<()> {
    let (outgoing, incoming) = tokio::join!(
        async {
            hub.peer(client.id().clone(), Unordered::reliable())?
                .connect()
                .await
        },
        async { client.endpoint(Unordered::reliable())?.accept().await },
    );
    let outgoing = outgoing?;
    let (origin, incoming) = incoming?;
    assert_eq!(origin, *hub.id());
    outgoing.send(Bytes::from_static(b"event")).await?;
    assert_eq!(incoming.recv().await?.as_ref(), b"event");
    incoming.send(Bytes::from_static(b"seen")).await?;
    assert_eq!(outgoing.recv().await?.as_ref(), b"seen");
    outgoing.close().await
}

/// Asserts `client` is in no group `hub` runs, the reserved routing group
/// included, and waits until the hub has gossiped while the client was
/// reachable, so the client had every chance to be learned.
async fn absent_from_membership(hub: &Node, groups: &[&str], client: &Client) {
    let routing = hub.join_group(ROUTING_GROUP);
    let digests = routing.net_stats().digests_built;
    eventually_within("the hub gossiped several rounds", WAIT, || {
        routing.net_stats().digests_built >= digests + 5
    })
    .await;
    for group in groups.iter().copied().chain([ROUTING_GROUP]) {
        let members = hub.join_group(group).members();
        assert!(
            !members.contains(client.id()),
            "{} joined {group}: {members:?}",
            client.id()
        );
    }
}
