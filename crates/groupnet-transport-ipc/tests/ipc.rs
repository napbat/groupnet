//! Real local-IPC integration tests; no mocked streams or fixed names.

use std::io;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_network::{Router, RouterConfig};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport::link::{LinkLifecycle, LinkProvider, PeerEndpoint};
use groupnet_transport_ipc::{IpcAddress, IpcLink, IpcTransport, MAX_FRAME};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct Address {
    ipc: IpcAddress,
    #[cfg(unix)]
    directory: std::path::PathBuf,
}

impl Address {
    fn new() -> Self {
        use std::fmt::Write;
        let mut random = [0_u8; 16];
        SystemRandom::new().fill(&mut random).unwrap();
        let mut suffix = String::with_capacity(32);
        for byte in random {
            write!(suffix, "{byte:02x}").unwrap();
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            let directory = std::env::temp_dir().join(format!("groupnet-ipc-{suffix}"));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&directory)
                .unwrap();
            Self {
                ipc: IpcAddress::Unix(directory.join("socket")),
                directory,
            }
        }
        #[cfg(windows)]
        {
            Self {
                ipc: IpcAddress::NamedPipe(format!(r"\\.\pipe\groupnet-ipc-{suffix}")),
            }
        }
    }
}

#[cfg(unix)]
impl Drop for Address {
    fn drop(&mut self) {
        // Only the empty, uniquely created directory is removed. A leftover
        // socket or unrelated replacement is deliberately not recursively erased.
        let _ = std::fs::remove_dir(&self.directory);
    }
}

async fn receive(transport: &IpcTransport) -> groupnet_transport::Inbound {
    timeout(DEADLINE, transport.recv()).await.unwrap().unwrap()
}

#[tokio::test]
async fn persistent_messages_and_reverse_without_address_registration() {
    let left_address = Address::new();
    let right_address = Address::new();
    let left = IpcTransport::bind(NodeId::new("left"), &left_address.ipc).unwrap();
    let right = IpcTransport::bind(NodeId::new("right"), &right_address.ipc).unwrap();
    left.register_peer(NodeId::new("right"), right_address.ipc.clone())
        .unwrap();
    for msg in [
        Vec::new(),
        b"first".to_vec(),
        vec![0x5a; MAX_FRAME],
        b"again".to_vec(),
    ] {
        left.send(&NodeId::new("right"), &msg).await.unwrap();
        let inbound = receive(&right).await;
        assert_eq!(inbound.from, NodeId::new("left"));
        assert_eq!(inbound.msg, msg);
        // The accepting side has no address for left. This can only work by
        // reusing the established full-duplex connection.
        right.send(&inbound.from, &inbound.msg).await.unwrap();
        let reply = receive(&left).await;
        assert_eq!(reply.from, NodeId::new("right"));
        assert_eq!(reply.msg, msg);
    }
    let error = left
        .send(&NodeId::new("right"), &vec![0; MAX_FRAME + 1])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    left.send(&NodeId::new("right"), b"still alive")
        .await
        .unwrap();
    assert_eq!(receive(&right).await.msg, b"still alive");
    timeout(DEADLINE, left.close()).await.unwrap();
    timeout(DEADLINE, right.close()).await.unwrap();
}

#[tokio::test]
async fn close_cancels_receives_and_every_clone() {
    let address = Address::new();
    let transport = IpcTransport::bind(NodeId::new("closing"), &address.ipc).unwrap();
    let clone = transport.clone();
    let pending = tokio::spawn(async move { clone.recv().await });
    timeout(DEADLINE, transport.close()).await.unwrap();
    assert_eq!(
        timeout(DEADLINE, pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        transport
            .send(&NodeId::new("unknown"), b"x")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    timeout(DEADLINE, transport.close()).await.unwrap();
    // close waits until the native listener has actually been released.
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &address.ipc).unwrap();
    replacement.close().await;
}

#[tokio::test]
async fn bounded_peer_book_and_node_ids() {
    let address = Address::new();
    let transport = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    for index in 0..128 {
        transport
            .register_peer(NodeId::new(format!("peer-{index}")), address.ipc.clone())
            .unwrap();
    }
    assert!(
        transport
            .register_peer(NodeId::new("overflow"), address.ipc.clone())
            .is_err()
    );
    transport
        .register_peer(NodeId::new("peer-0"), address.ipc.clone())
        .unwrap();
    assert!(
        transport
            .register_peer(NodeId::new("local"), address.ipc.clone())
            .is_err()
    );
    assert!(
        transport
            .register_peer(NodeId::new("x".repeat(65)), address.ipc.clone())
            .is_err()
    );
    assert!(
        transport
            .register_peer(NodeId::new(""), address.ipc.clone())
            .is_err()
    );
    transport
        .send(&NodeId::new("unknown"), b"drop")
        .await
        .unwrap();
    transport.close().await;
    assert!(IpcTransport::bind(NodeId::new(""), &address.ipc).is_err());
    assert!(IpcTransport::bind(NodeId::new("x".repeat(65)), &address.ipc).is_err());
}

#[cfg(unix)]
type RawStream = tokio::net::UnixStream;
#[cfg(windows)]
type RawStream = tokio::net::windows::named_pipe::NamedPipeClient;

async fn raw_connect(address: &IpcAddress) -> RawStream {
    #[cfg(unix)]
    {
        let IpcAddress::Unix(path) = address;
        tokio::net::UnixStream::connect(path).await.unwrap()
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        let IpcAddress::NamedPipe(name) = address;
        timeout(DEADLINE, async {
            loop {
                match ClientOptions::new().open(name) {
                    Ok(pipe) => break pipe,
                    Err(error) if error.raw_os_error() == Some(231) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("opening local test pipe: {error}"),
                }
            }
        })
        .await
        .unwrap()
    }
}

async fn introduction(stream: &mut RawStream) {
    stream.write_all(b"GNI1\x03raw").await.unwrap();
    let mut header = [0_u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(&header[..4], b"GNI1");
    let mut id = vec![0; usize::from(header[4])];
    stream.read_exact(&mut id).await.unwrap();
    assert_eq!(id, b"listener");
}

async fn assert_closed(stream: &mut RawStream) {
    let mut tail = Vec::new();
    let result = timeout(DEADLINE, stream.read_to_end(&mut tail))
        .await
        .unwrap();
    // Unix commonly returns EOF; Windows may report ERROR_BROKEN_PIPE or
    // ERROR_NO_DATA when the server disconnects a malformed client.
    if let Err(error) = result {
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::UnexpectedEof
            ) || matches!(error.raw_os_error(), Some(109 | 232 | 233)),
            "{error}"
        );
    }
    assert!(tail.len() <= 69);
}

#[tokio::test]
async fn malformed_introductions_and_oversize_lengths_are_closed() {
    let address = Address::new();
    let transport = IpcTransport::bind(NodeId::new("listener"), &address.ipc).unwrap();
    for invalid in [
        b"NOPE\x01x".as_slice(),
        b"GNI1\x00".as_slice(),
        b"GNI1\x41".as_slice(),
        b"GNI1\x01\xff".as_slice(),
    ] {
        let mut stream = raw_connect(&address.ipc).await;
        stream.write_all(invalid).await.unwrap();
        assert_closed(&mut stream).await;
    }
    for length in [65_001_u32, u32::MAX] {
        let mut stream = raw_connect(&address.ipc).await;
        introduction(&mut stream).await;
        // No body is supplied: rejection must happen at the length prefix,
        // before allocation and without waiting for attacker-specified bytes.
        stream.write_u32_le(length).await.unwrap();
        assert_closed(&mut stream).await;
    }
    let mut healthy = raw_connect(&address.ipc).await;
    introduction(&mut healthy).await;
    healthy.write_u32_le(2).await.unwrap();
    healthy.write_all(b"ok").await.unwrap();
    let inbound = receive(&transport).await;
    assert_eq!(inbound.from, NodeId::new("raw"));
    assert_eq!(inbound.msg, b"ok");
    transport.send(&inbound.from, b"back").await.unwrap();
    assert_eq!(
        timeout(DEADLINE, healthy.read_u32_le())
            .await
            .unwrap()
            .unwrap(),
        4
    );
    let mut reply = [0; 4];
    timeout(DEADLINE, healthy.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply, b"back");
    transport.close().await;
    assert_closed(&mut healthy).await;
}

#[tokio::test]
async fn occupied_listener_cannot_be_replaced() {
    let address = Address::new();
    let first = IpcTransport::bind(NodeId::new("first"), &address.ipc).unwrap();
    assert!(IpcTransport::bind(NodeId::new("second"), &address.ipc).is_err());
    first.close().await;
}

#[cfg(windows)]
#[tokio::test]
async fn windows_rejects_remote_and_non_pipe_addresses() {
    let address = Address::new();
    let transport = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    for name in [
        r"\\remote\pipe\groupnet",
        r"C:\groupnet",
        r"\\.\pipe\",
        "\\\\.\\pipe\\bad\0name",
    ] {
        let invalid = IpcAddress::NamedPipe(name.to_owned());
        assert!(IpcTransport::bind(NodeId::new("bad"), &invalid).is_err());
        assert!(
            transport
                .register_peer(NodeId::new("peer"), invalid)
                .is_err()
        );
    }
    transport.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn unix_permissions_and_existing_files_are_preserved() {
    use std::os::unix::fs::PermissionsExt;
    let address = Address::new();
    let IpcAddress::Unix(path) = &address.ipc;
    std::fs::write(path, b"not a socket").unwrap();
    assert!(IpcTransport::bind(NodeId::new("local"), &address.ipc).is_err());
    assert_eq!(std::fs::read(path).unwrap(), b"not a socket");
    std::fs::remove_file(path).unwrap();
    std::fs::set_permissions(&address.directory, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(IpcTransport::bind(NodeId::new("local"), &address.ipc).is_err());
    assert!(!path.exists());
    std::fs::set_permissions(&address.directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    let transport = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    // Replacement after bind must survive close: cleanup is inode-specific.
    std::fs::remove_file(path).unwrap();
    std::fs::write(path, b"replacement").unwrap();
    transport.close().await;
    assert_eq!(std::fs::read(path).unwrap(), b"replacement");
    std::fs::remove_file(path).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_last_handle_drop_releases_only_its_socket() {
    let address = Address::new();
    let IpcAddress::Unix(path) = &address.ipc;
    let transport = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    let clone = transport.clone();
    drop(transport);
    assert!(path.exists());
    drop(clone);
    timeout(DEADLINE, async {
        while path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn close_cancels_an_incomplete_connection_introduction() {
    let address = Address::new();
    let transport = IpcTransport::bind(NodeId::new("listener"), &address.ipc).unwrap();
    let mut stream = raw_connect(&address.ipc).await;
    stream.write_all(b"GNI1").await.unwrap();
    // Missing ID length/body must not delay cancellation until the setup timeout.
    timeout(Duration::from_secs(1), transport.close())
        .await
        .unwrap();
    assert_closed(&mut stream).await;
}

#[cfg(windows)]
#[tokio::test]
async fn windows_last_handle_drop_releases_the_reserved_pipe_name() {
    let address = Address::new();
    let transport = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    let clone = transport.clone();
    drop(transport);
    assert!(IpcTransport::bind(NodeId::new("other"), &address.ipc).is_err());
    drop(clone);
    let replacement = timeout(DEADLINE, async {
        loop {
            if let Ok(transport) = IpcTransport::bind(NodeId::new("replacement"), &address.ipc) {
                break transport;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    replacement.close().await;
}

#[tokio::test]
async fn provider_routes_bounded_frames_and_router_drains_listener() {
    let left_address = Address::new();
    let right_address = Address::new();
    let left = Router::new(NodeId::new("left"), RouterConfig::default()).unwrap();
    let right = Router::new(NodeId::new("right"), RouterConfig::default()).unwrap();
    let left_provider = IpcLink::new(
        left_address.ipc.clone(),
        vec![PeerEndpoint::new(
            right.local_id().clone(),
            right_address.ipc.clone(),
        )],
    )
    .with_cost(7);
    let right_provider = IpcLink::new(
        right_address.ipc.clone(),
        vec![PeerEndpoint::new(
            left.local_id().clone(),
            left_address.ipc.clone(),
        )],
    );
    left.add_link(
        Box::new(left_provider)
            .bind(left.local_id().clone())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    right
        .add_link(
            Box::new(right_provider)
                .bind(right.local_id().clone())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    eventually_within("IPC provider routes established", DEADLINE, || {
        left.route_to(right.local_id()).is_some() && right.route_to(left.local_id()).is_some()
    })
    .await;
    assert_eq!(left.route_to(right.local_id()).unwrap().cost, 7);
    assert_eq!(
        left.send(right.local_id(), &vec![0x5a; MAX_FRAME + 1])
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let payload = vec![0x5a; 60_000];
    left.send(right.local_id(), &payload).await.unwrap();
    let received = timeout(DEADLINE, right.recv()).await.unwrap().unwrap();
    assert_eq!(received.from, *left.local_id());
    assert_eq!(received.msg, payload);
    left.close().await;
    right.close().await;
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &left_address.ipc).unwrap();
    replacement.close().await;
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &right_address.ipc).unwrap();
    replacement.close().await;
}

#[tokio::test]
async fn provider_peer_registration_failure_drains_bound_listener() {
    let address = Address::new();
    let local = NodeId::new("local");
    let provider = IpcLink::new(
        address.ipc.clone(),
        vec![PeerEndpoint::new(local.clone(), address.ipc.clone())],
    );
    assert_eq!(
        Box::new(provider).bind(local).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &address.ipc).unwrap();
    replacement.close().await;
}

#[tokio::test]
async fn rejected_router_registration_closes_provider_listener_before_returning() {
    let address = Address::new();
    let router = Router::new(NodeId::new("local"), RouterConfig::default()).unwrap();
    router.close().await;
    let bound = Box::new(IpcLink::new(address.ipc.clone(), Vec::new()))
        .bind(router.local_id().clone())
        .await
        .unwrap();
    assert!(router.add_link(bound).await.is_err());
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &address.ipc).unwrap();
    replacement.close().await;
}

#[tokio::test]
async fn bound_provider_drop_releases_native_listener() {
    let address = Address::new();
    let bound = Box::new(IpcLink::new(address.ipc.clone(), Vec::new()))
        .bind(NodeId::new("local"))
        .await
        .unwrap();
    drop(bound);
    timeout(DEADLINE, async {
        loop {
            if let Ok(replacement) = IpcTransport::bind(NodeId::new("replacement"), &address.ipc) {
                replacement.close().await;
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn lifecycle_shutdown_synchronously_cancels_all_handles_then_close_drains() {
    let address = Address::new();
    let transport = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    let clone = transport.clone();
    LinkLifecycle::shutdown(&transport);
    assert_eq!(
        clone.recv().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    timeout(DEADLINE, LinkLifecycle::close(&transport))
        .await
        .unwrap();
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &address.ipc).unwrap();
    replacement.close().await;
}
