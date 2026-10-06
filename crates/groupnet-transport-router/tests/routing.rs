//! Consumer-visible bridging, fragmentation, route failover, and ownership.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport_mem::Network;
use groupnet_transport_router::ipc::{IpcAddress, IpcTransport};
use groupnet_transport_router::{LinkConfig, Router, RouterConfig, TransportId};
use groupnet_transport_tcp::TcpMsgTransport;

const WAIT: Duration = Duration::from_secs(10);
static UNIQUE: AtomicU64 = AtomicU64::new(0);

fn router(name: &str) -> io::Result<Router> {
    Router::new(
        NodeId::new(name),
        RouterConfig {
            announce_interval: Duration::from_millis(30),
            route_ttl: Duration::from_millis(600),
            ..RouterConfig::default()
        },
    )
}

fn connect(
    a: &Router,
    b: &Router,
    cost: u32,
    mtu: usize,
) -> io::Result<(TransportId, TransportId)> {
    let net = Network::new();
    let aid = a.add_transport(
        net.endpoint(a.local_id().clone()),
        LinkConfig {
            peers: vec![b.local_id().clone()],
            cost,
            mtu,
        },
    )?;
    let bid = b.add_transport(
        net.endpoint(b.local_id().clone()),
        LinkConfig {
            peers: vec![a.local_id().clone()],
            cost,
            mtu,
        },
    )?;
    Ok((aid, bid))
}

#[tokio::test]
async fn fragmented_packets_cross_a_bridge_and_preserve_origin() -> io::Result<()> {
    let a = router("a")?;
    let b = router("b")?;
    let c = router("c")?;
    connect(&a, &b, 1, 256)?;
    connect(&b, &c, 1, 512)?;
    eventually_within("two-hop path", WAIT, || a.route_to(c.local_id()).is_some()).await;
    assert_eq!(
        a.route_to(c.local_id()).expect("route").path,
        vec![NodeId::new("a"), NodeId::new("b"), NodeId::new("c")]
    );
    let payload: Vec<u8> = (0_u8..=255).cycle().take(12_000).collect();
    a.send(c.local_id(), &payload).await?;
    let received = tokio::time::timeout(WAIT, c.recv()).await??;
    assert_eq!(received.from, *a.local_id());
    assert_eq!(received.msg, payload);
    a.close().await;
    b.close().await;
    c.close().await;
    Ok(())
}

#[tokio::test]
async fn route_failover_and_stale_transport_handles_do_not_disconnect_replacements()
-> io::Result<()> {
    let a = router("a")?;
    let b = router("b")?;
    let c = router("c")?;
    let d = router("d")?;
    let (ab, ba) = connect(&a, &b, 1, 1200)?;
    connect(&b, &c, 1, 1200)?;
    connect(&a, &d, 5, 1200)?;
    connect(&d, &c, 1, 1200)?;
    eventually_within("preferred path", WAIT, || {
        a.route_to(c.local_id())
            .is_some_and(|route| route.next_hop == *b.local_id())
    })
    .await;
    a.remove_transport(ab);
    b.remove_transport(ba);
    eventually_within("alternate path", WAIT, || {
        a.route_to(c.local_id())
            .is_some_and(|route| route.next_hop == *d.local_id())
    })
    .await;
    a.send(c.local_id(), b"rerouted without changing destination")
        .await?;
    assert_eq!(
        tokio::time::timeout(WAIT, c.recv()).await??.msg,
        b"rerouted without changing destination"
    );
    let (replacement, _) = connect(&a, &b, 1, 1200)?;
    assert_ne!(ab, replacement);
    a.remove_transport(ab);
    eventually_within("replacement remains reachable", WAIT, || {
        a.route_to(b.local_id())
            .is_some_and(|route| route.transport == replacement)
    })
    .await;
    a.send(b.local_id(), b"replacement survives stale handle")
        .await?;
    assert_eq!(
        tokio::time::timeout(WAIT, b.recv()).await??.msg,
        b"replacement survives stale handle"
    );
    for node in [&a, &b, &c, &d] {
        node.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn endpoint_only_mode_blocks_transit_and_shutdown_wakes_receivers() -> io::Result<()> {
    let a = router("a")?;
    let b = Router::new(
        NodeId::new("b"),
        RouterConfig {
            forwarding: false,
            announce_interval: Duration::from_millis(30),
            ..RouterConfig::default()
        },
    )?;
    let c = router("c")?;
    connect(&a, &b, 1, 1200)?;
    connect(&b, &c, 1, 1200)?;
    eventually_within("adjacent routes", WAIT, || {
        a.route_to(b.local_id()).is_some() && b.route_to(c.local_id()).is_some()
    })
    .await;
    assert!(a.route_to(c.local_id()).is_none());
    a.send(c.local_id(), b"not authorized for transit").await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), c.recv())
            .await
            .is_err()
    );
    c.close().await;
    assert_eq!(
        c.recv().await.expect_err("closed").kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        c.send(a.local_id(), b"closed")
            .await
            .expect_err("closed")
            .kind(),
        io::ErrorKind::NotConnected
    );
    a.close().await;
    b.close().await;
    Ok(())
}

#[cfg(windows)]
fn ipc_addresses() -> (IpcAddress, IpcAddress, Option<std::path::PathBuf>) {
    let token = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let name = format!("groupnet-bridge-{}-{token}", std::process::id());
    (
        IpcAddress::NamedPipe(format!(r"\\.\pipe\{name}-a")),
        IpcAddress::NamedPipe(format!(r"\\.\pipe\{name}-b")),
        None,
    )
}

#[cfg(unix)]
fn ipc_addresses() -> (IpcAddress, IpcAddress, Option<std::path::PathBuf>) {
    use std::os::unix::fs::DirBuilderExt;
    let token = UNIQUE.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!("gn-bridge-{}-{token}", std::process::id()));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .expect("create private IPC fixture directory");
    (
        IpcAddress::Unix(directory.join("a")),
        IpcAddress::Unix(directory.join("b")),
        Some(directory),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ipc_only_peer_reaches_tcp_only_peer_through_a_two_adapter_bridge() -> io::Result<()> {
    let a = router("ipc-only")?;
    let b = router("bridge")?;
    let c = router("tcp-only")?;
    let (a_addr, b_addr, directory) = ipc_addresses();
    let ipc_a = IpcTransport::bind(a.local_id().clone(), &a_addr)?;
    let ipc_b = IpcTransport::bind(b.local_id().clone(), &b_addr)?;
    ipc_a.register_peer(b.local_id().clone(), b_addr)?;
    ipc_b.register_peer(a.local_id().clone(), a_addr)?;
    a.add_transport(ipc_a.clone(), LinkConfig::new(vec![b.local_id().clone()]))?;
    b.add_transport(ipc_b.clone(), LinkConfig::new(vec![a.local_id().clone()]))?;
    let tcp_b = TcpMsgTransport::bind(b.local_id().clone(), "127.0.0.1:0").await?;
    let tcp_c = TcpMsgTransport::bind(c.local_id().clone(), "127.0.0.1:0").await?;
    tcp_b.register_peer(c.local_id().clone(), tcp_c.local_addr());
    tcp_c.register_peer(b.local_id().clone(), tcp_b.local_addr());
    b.add_transport(tcp_b, LinkConfig::new(vec![c.local_id().clone()]))?;
    c.add_transport(tcp_c, LinkConfig::new(vec![b.local_id().clone()]))?;
    eventually_within("heterogeneous bidirectional paths", WAIT, || {
        a.route_to(c.local_id()).is_some() && c.route_to(a.local_id()).is_some()
    })
    .await;
    a.send(c.local_id(), b"IPC -> bridge -> TCP").await?;
    let received = tokio::time::timeout(WAIT, c.recv()).await??;
    assert_eq!(received.from, *a.local_id());
    assert_eq!(received.msg, b"IPC -> bridge -> TCP");
    c.send(a.local_id(), b"TCP -> bridge -> IPC").await?;
    let received = tokio::time::timeout(WAIT, a.recv()).await??;
    assert_eq!(received.from, *c.local_id());
    assert_eq!(received.msg, b"TCP -> bridge -> IPC");
    for node in [&a, &b, &c] {
        node.close().await;
    }
    ipc_a.close().await;
    ipc_b.close().await;
    if let Some(directory) = directory {
        std::fs::remove_dir(directory)?;
    }
    Ok(())
}

#[tokio::test]
async fn default_router_forwards_between_neighbors_on_the_same_adapter() -> io::Result<()> {
    let network = Network::new();
    let a = router("a")?;
    let b = router("b")?;
    let c = router("c")?;
    a.add_transport(
        network.endpoint(a.local_id().clone()),
        LinkConfig::new(vec![b.local_id().clone()]),
    )?;
    b.add_transport(
        network.endpoint(b.local_id().clone()),
        LinkConfig::new(vec![a.local_id().clone(), c.local_id().clone()]),
    )?;
    c.add_transport(
        network.endpoint(c.local_id().clone()),
        LinkConfig::new(vec![b.local_id().clone()]),
    )?;
    eventually_within("same-adapter transit routes", WAIT, || {
        a.route_to(c.local_id())
            .is_some_and(|route| route.next_hop == *b.local_id())
            && c.route_to(a.local_id())
                .is_some_and(|route| route.next_hop == *b.local_id())
    })
    .await;
    c.send(a.local_id(), b"same-adapter transit").await?;
    let packet = tokio::time::timeout(WAIT, a.recv()).await??;
    assert_eq!(packet.from, *c.local_id());
    assert_eq!(packet.msg, b"same-adapter transit");
    a.send(c.local_id(), b"return transit").await?;
    let packet = tokio::time::timeout(WAIT, c.recv()).await??;
    assert_eq!(packet.from, *a.local_id());
    assert_eq!(packet.msg, b"return transit");
    for node in [&a, &b, &c] {
        node.close().await;
    }
    Ok(())
}
