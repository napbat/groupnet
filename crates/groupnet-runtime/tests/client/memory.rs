//! The client role over the in-memory transport.

use std::io;

use groupnet_core::{GroupId, NodeId};
use groupnet_runtime::messaging::{Bytes, MessageProtocol, SendOptions};
use groupnet_runtime::{Client, ClientConfigError, Messages, Node, RouterConfig};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport_mem::{MemLink, Network};

use super::{
    WAIT, absent_from_membership, client_builder, hub_builder, messages_both_ways, ordered_stream,
    pinned_pair, routed, unordered_session,
};

/// How long a hub watches for a route that only transit could create: many
/// announce intervals of the middle router.
const TRANSIT_WINDOW: std::time::Duration = std::time::Duration::from_millis(600);

/// A hub and a client that pin each other's tunnel identities. The hub also
/// links `members`.
async fn hub_and_client(memory: &Network, members: &[NodeId]) -> io::Result<(Node, Client)> {
    let (hub_id, client_id) = (NodeId::new("hub"), NodeId::new("client"));
    let (hub_tunnels, client_tunnels) = pinned_pair(&hub_id, &client_id)?;
    let mut peers = vec![client_id.clone()];
    peers.extend_from_slice(members);
    let hub = hub_builder(&hub_id)
        .link(MemLink::new(memory.endpoint(hub_id.clone()), peers))
        .tunnels(hub_tunnels)
        .start()
        .await?;
    let client = client_builder(&client_id)
        .link(MemLink::new(memory.endpoint(client_id), vec![hub_id]))
        .tunnels(client_tunnels)
        .start()
        .await?;
    routed(&hub, &client).await;
    Ok((hub, client))
}

#[tokio::test]
async fn a_client_exchanges_messages_streams_and_sessions_with_its_hub() -> io::Result<()> {
    let memory = Network::new();
    let (hub, client) = hub_and_client(&memory, &[]).await?;
    tokio::time::timeout(WAIT, async {
        messages_both_ways(&hub, &client).await?;
        ordered_stream(&hub, &client).await?;
        unordered_session(&hub, &client).await
    })
    .await??;
    client.close().await;
    hub.close().await;
    Ok(())
}

#[tokio::test]
async fn a_client_is_in_no_group_of_the_cluster_it_reaches() -> io::Result<()> {
    let memory = Network::new();
    let member_id = NodeId::new("member");
    let (hub, client) = hub_and_client(&memory, std::slice::from_ref(&member_id)).await?;
    let member = hub_builder(&member_id)
        .link(MemLink::new(
            memory.endpoint(member_id.clone()),
            vec![hub.id().clone()],
        ))
        .start()
        .await?;
    let (hub_shard, member_shard) = (hub.join_group("shard"), member.join_group("shard"));
    eventually_within("the two members converge", WAIT, || {
        hub_shard.members().len() == 2 && member_shard.members().len() == 2
    })
    .await;
    tokio::time::timeout(WAIT, messages_both_ways(&hub, &client)).await??;
    absent_from_membership(&hub, &["shard"], &client).await;
    absent_from_membership(&member, &["shard"], &client).await;
    // A group-addressed frame finds no group at the client, which refuses it.
    let refused = hub
        .endpoint(Messages::best_effort())?
        .protocol()
        .send(
            client.id(),
            Some(&GroupId::new("shard")),
            Bytes::from_static(b"group"),
            SendOptions::Delivered { timeout: WAIT },
        )
        .await
        .unwrap_err();
    // The wire carries a missing group as a permission rejection, as for a
    // node that has not joined the group.
    assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
    client.close().await;
    member.close().await;
    hub.close().await;
    Ok(())
}

/// Two hubs that only share `middle` as a neighbor.
async fn two_hubs(memory: &Network, middle: &NodeId) -> io::Result<(Node, Node)> {
    let mut hubs = Vec::new();
    for name in ["hub-a", "hub-b"] {
        let id = NodeId::new(name);
        hubs.push(
            hub_builder(&id)
                .link(MemLink::new(memory.endpoint(id), vec![middle.clone()]))
                .start()
                .await?,
        );
    }
    let b = hubs.pop().expect("two hubs");
    let a = hubs.pop().expect("two hubs");
    Ok((a, b))
}

#[tokio::test]
async fn a_client_between_two_hubs_forwards_nothing() -> io::Result<()> {
    let memory = Network::new();
    let middle = NodeId::new("middle");
    let (a, b) = two_hubs(&memory, &middle).await?;
    let client = client_builder(&middle)
        .link(MemLink::new(
            memory.endpoint(middle.clone()),
            vec![a.id().clone(), b.id().clone()],
        ))
        .start()
        .await?;
    routed(&a, &client).await;
    routed(&b, &client).await;
    let transit = tokio::time::timeout(TRANSIT_WINDOW, async {
        while a.router().route_to(b.id()).is_none() && b.router().route_to(a.id()).is_none() {
            tokio::time::sleep(groupnet_testkit::cluster::POLL_INTERVAL).await;
        }
    })
    .await;
    assert!(transit.is_err(), "a hub learned a route through the client");
    client.close().await;
    a.close().await;
    b.close().await;
    Ok(())
}

#[tokio::test]
async fn a_node_between_two_hubs_forwards_as_the_control() -> io::Result<()> {
    let memory = Network::new();
    let middle = NodeId::new("middle");
    let (a, b) = two_hubs(&memory, &middle).await?;
    let relay = hub_builder(&middle)
        .link(MemLink::new(
            memory.endpoint(middle.clone()),
            vec![a.id().clone(), b.id().clone()],
        ))
        .start()
        .await?;
    eventually_within(
        "each hub routes to the other through the node",
        TRANSIT_WINDOW * 4,
        || a.router().route_to(b.id()).is_some() && b.router().route_to(a.id()).is_some(),
    )
    .await;
    relay.close().await;
    a.close().await;
    b.close().await;
    Ok(())
}

#[tokio::test]
async fn closing_a_client_stops_every_handle() -> io::Result<()> {
    let memory = Network::new();
    let (hub, client) = hub_and_client(&memory, &[]).await?;
    let messages = client.peer(hub.id().clone(), Messages::best_effort())?;
    let router = client.router().clone();
    client.close().await;
    assert!(router.is_closed());
    assert_eq!(
        messages
            .send(Bytes::from_static(b"late"))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        client.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        client.endpoint(Messages::best_effort()).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    hub.close().await;
    Ok(())
}

#[tokio::test]
async fn a_forwarding_client_is_refused_before_binding() {
    let memory = Network::new();
    let id = NodeId::new("forwarder");
    let error = Client::builder(id.clone())
        .routing(RouterConfig::default())
        .link(MemLink::new(memory.endpoint(id), Vec::new()))
        .start()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<ClientConfigError>()),
        Some(&ClientConfigError::Forwarding)
    );
}
