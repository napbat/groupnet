//! Route changes must preserve the same authenticated application stream.

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::bulk::BulkTransport;
use groupnet_transport::link::LinkConfig;
use groupnet_transport_mem::Network;
use tokio::time::timeout;

use super::{
    DEADLINE, binary,
    fixtures::{Fabric, credentials},
};

#[tokio::test]
async fn active_tls_stream_survives_direct_link_removal_and_uses_tcp_bridge() {
    let fabric = Fabric::new(false, true).await;
    let shortcut = Network::new();
    let left_link = fabric
        .a
        .add_transport(
            shortcut.endpoint(fabric.a.local_id().clone()),
            LinkConfig::new(vec![fabric.c.local_id().clone()]),
        )
        .unwrap();
    let right_link = fabric
        .c
        .add_transport(
            shortcut.endpoint(fabric.c.local_id().clone()),
            LinkConfig::new(vec![fabric.a.local_id().clone()]),
        )
        .unwrap();
    eventually_within("direct route selected", DEADLINE, || {
        fabric
            .a
            .route_to(fabric.c.local_id())
            .is_some_and(|route| route.transport == left_link)
            && fabric
                .c
                .route_to(fabric.a.local_id())
                .is_some_and(|route| route.transport == right_link)
    })
    .await;
    let (left, right, _) = credentials();
    let (outbound, inbound) = fabric.tunnels(left, right);
    timeout(DEADLINE, async {
        let (client, server) =
            tokio::join!(outbound.connect(fabric.c.local_id()), inbound.accept());
        let mut client = client.unwrap();
        let (peer, mut server) = server.unwrap();
        assert_eq!(&peer, fabric.a.local_id());
        let payload = binary(98_321);
        client.write_all(&payload[..777]).await.unwrap();
        let mut prefix = [0; 777];
        server.read_exact(&mut prefix).await.unwrap();
        assert_eq!(prefix, payload[..777]);
        fabric.a.remove_transport(left_link);
        fabric.c.remove_transport(right_link);
        eventually_within(
            "bridge selected after direct link removal",
            DEADLINE,
            || {
                fabric
                    .a
                    .route_to(fabric.c.local_id())
                    .is_some_and(|route| route.next_hop == *fabric.bridge.local_id())
                    && fabric
                        .c
                        .route_to(fabric.a.local_id())
                        .is_some_and(|route| route.next_hop == *fabric.bridge.local_id())
            },
        )
        .await;
        let send = async {
            client.write_all(&payload[777..]).await.unwrap();
            client.close().await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"same TLS stream after reroute");
        };
        let receive = async {
            let mut suffix = Vec::new();
            server.read_to_end(&mut suffix).await.unwrap();
            assert_eq!(suffix, payload[777..]);
            server
                .write_all(b"same TLS stream after reroute")
                .await
                .unwrap();
            server.close().await.unwrap();
        };
        tokio::join!(send, receive);
    })
    .await
    .unwrap();
    outbound.close().await;
    inbound.close().await;
    fabric.close().await;
}
