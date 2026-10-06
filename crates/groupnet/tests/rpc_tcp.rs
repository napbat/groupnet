//! RPC over **real TCP data-plane streams**, through the facade: concurrent
//! multi-megabyte calls share one socket split into a reader and a writer,
//! and a server restarted on a new port is reached once the caller
//! re-registers its address.

#![cfg(all(feature = "rpc", feature = "tcp"))]

use std::time::Duration;

use bytes::Bytes;
use groupnet::core::NodeId;
use groupnet::rpc::{RpcClient, RpcConfig, RpcError, RpcServer, RpcServerHandle, RpcStatus};
use groupnet::transport::bulk::DataPlane;
use groupnet::transport::tcp::TcpBulkTransport;
use tokio::task::JoinSet;

/// Loopback sockets settle fast; this only bounds a hung regression.
const CALL: Duration = Duration::from_secs(10);

/// Binds an echo server for `id` on an ephemeral loopback port.
async fn echo_server(id: &NodeId) -> (RpcServerHandle, std::net::SocketAddr) {
    let transport = TcpBulkTransport::bind(id.clone(), "127.0.0.1:0")
        .await
        .expect("bind server");
    let addr = transport.local_addr().expect("server addr");
    let server = RpcServer::spawn(
        DataPlane::new(transport),
        |_from: NodeId, request: Bytes| async move { Ok::<_, RpcStatus>(request) },
    );
    (server, addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_large_calls_share_one_tcp_stream_and_survive_a_server_restart() {
    const CALLS: usize = 16;
    const SIZE: usize = 1 << 20;

    let server_id = NodeId::new("rpc-s");
    let (server, addr) = echo_server(&server_id).await;
    let transport = TcpBulkTransport::bind(NodeId::new("rpc-c"), "127.0.0.1:0")
        .await
        .expect("bind client");
    transport.register_peer(server_id.clone(), addr);
    // Keep a handle on the plane: its transport is where addresses are taught.
    let plane = DataPlane::new(transport);
    let client = RpcClient::new(plane.clone(), RpcConfig::default());

    let mut calls = JoinSet::new();
    for n in 0..CALLS {
        let (client, to) = (client.clone(), server_id.clone());
        calls.spawn(async move {
            let payload = Bytes::from(vec![u8::try_from(n).expect("small"); SIZE]);
            (client.call(&to, payload.clone(), CALL).await, payload)
        });
    }
    while let Some(joined) = calls.join_next().await {
        let (reply, sent) = joined.expect("call task");
        assert_eq!(reply.expect("echoed"), sent);
    }
    assert_eq!(server.connections(), 1, "every call shared one TCP stream");

    // The server goes away; its replacement listens elsewhere, and the caller
    // re-teaches the address on the transport the client dials through.
    drop(server);
    let (_restarted, new_addr) = echo_server(&server_id).await;
    assert_ne!(new_addr, addr);
    plane.transport().register_peer(server_id.clone(), new_addr);

    // TCP learns of a dead peer on use: a call that races the old socket's
    // close may still be sent on it and come back `ConnectionLost`. The next
    // one dials the new address.
    let again = Bytes::from_static(b"again");
    let mut reply = client.call(&server_id, again.clone(), CALL).await;
    if reply == Err(RpcError::ConnectionLost) {
        reply = client.call(&server_id, again.clone(), CALL).await;
    }
    assert_eq!(reply, Ok(again), "the restarted server is reached");
}
