//! Real local-IPC integration tests; no mocked streams or fixed names.

use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_network::{RoutedSendError, Router};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::Transport;
use groupnet_transport::link::{LinkConfigError, LinkLifecycle, LinkProvider};
// The adapter re-exports the shared types its own signatures name.
use groupnet_transport_ipc::{
    IpcAddress, IpcAddressError, IpcConfig, IpcConfigError, IpcLink, IpcPeerError, IpcTransport,
    MAX_FRAME, NodeIdError, PeerEndpoint, QueueCapacity,
};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(5);

/// Recognizes the expected typed rejection in a table-driven test.
type PeerCheck = fn(&IpcPeerError) -> bool;

/// Recognizes the expected typed address rejection in a table-driven test.
#[cfg(windows)]
type AddressCheck = fn(&IpcAddressError) -> bool;

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
        assert_eq!(inbound.msg.as_ref(), msg);
        // The accepting side has no address for left. This can only work by
        // reusing the established full-duplex connection.
        right.send(&inbound.from, &inbound.msg).await.unwrap();
        let reply = receive(&left).await;
        assert_eq!(reply.from, NodeId::new("right"));
        assert_eq!(reply.msg.as_ref(), msg);
    }
    let error = left
        .send(&NodeId::new("right"), &vec![0; MAX_FRAME + 1])
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    left.send(&NodeId::new("right"), b"still alive")
        .await
        .unwrap();
    assert_eq!(receive(&right).await.msg.as_ref(), b"still alive");
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
    let peer_error = |error: io::Error| {
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        *error
            .into_inner()
            .and_then(|inner| inner.downcast::<IpcPeerError>().ok())
            .expect("typed peer error")
    };
    assert!(matches!(
        peer_error(
            transport
                .register_peer(NodeId::new("overflow"), address.ipc.clone())
                .unwrap_err()
        ),
        IpcPeerError::PeerLimit { max_peers } if max_peers.get() == 128
    ));
    transport
        .register_peer(NodeId::new("peer-0"), address.ipc.clone())
        .unwrap();
    assert!(matches!(
        peer_error(
            transport
                .register_peer(NodeId::new("local"), address.ipc.clone())
                .unwrap_err()
        ),
        IpcPeerError::Local
    ));
    assert!(matches!(
        peer_error(
            transport
                .register_peer(NodeId::new("x".repeat(65)), address.ipc.clone())
                .unwrap_err()
        ),
        IpcPeerError::Identity(NodeIdError::TooLong { len: 65, max: 64 })
    ));
    assert!(matches!(
        peer_error(
            transport
                .register_peer(NodeId::new(""), address.ipc.clone())
                .unwrap_err()
        ),
        IpcPeerError::Identity(NodeIdError::Empty)
    ));
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
    assert_eq!(inbound.msg.as_ref(), b"ok");
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
    let cases: [(String, AddressCheck); 5] = [
        (r"\\remote\pipe\groupnet".to_owned(), |error| {
            matches!(error, IpcAddressError::NotLocalPipe)
        }),
        (r"C:\groupnet".to_owned(), |error| {
            matches!(error, IpcAddressError::NotLocalPipe)
        }),
        (r"\\.\pipe\".to_owned(), |error| {
            matches!(error, IpcAddressError::EmptyPipeName)
        }),
        ("\\\\.\\pipe\\bad\0name".to_owned(), |error| {
            matches!(error, IpcAddressError::PipeNameCharacter)
        }),
        (format!(r"\\.\pipe\{}", "x".repeat(248)), |error| {
            matches!(error, IpcAddressError::PipeNameTooLong { len: 257 })
        }),
    ];
    for (name, expected) in cases {
        let invalid = IpcAddress::NamedPipe(name);
        let error = IpcTransport::bind(NodeId::new("bad"), &invalid).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let typed = error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<IpcAddressError>())
            .expect("typed address error");
        assert!(expected(typed), "{typed:?}");
        let error = transport
            .register_peer(NodeId::new("peer"), invalid)
            .unwrap_err();
        assert!(
            matches!(
                error.get_ref().and_then(|inner| inner.downcast_ref::<IpcPeerError>()),
                Some(IpcPeerError::Address(cause)) if expected(cause)
            ),
            "{error:?}"
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
    let error = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(matches!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<IpcAddressError>()),
        Some(IpcAddressError::ParentNotPrivate { mode: 0o755 })
    ));
    let missing = IpcAddress::Unix(address.directory.join("missing").join("socket"));
    let error = IpcTransport::bind(NodeId::new("local"), &missing).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert!(matches!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<IpcAddressError>()),
        Some(IpcAddressError::ParentInaccessible(cause)) if cause.kind() == io::ErrorKind::NotFound
    ));
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
    let left = Router::new(NodeId::new("left")).unwrap();
    let right = Router::new(NodeId::new("right")).unwrap();
    let left_provider = IpcLink::new(
        left_address.ipc.clone(),
        vec![PeerEndpoint::new(
            right.local_id().clone(),
            right_address.ipc.clone(),
        )],
    )
    .with_cost(NonZeroU32::new(7).unwrap());
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
    let oversized = left
        .send(right.local_id(), &vec![0x5a; MAX_FRAME + 1])
        .await
        .unwrap_err();
    assert_eq!(oversized.kind(), io::ErrorKind::InvalidInput);
    assert!(matches!(
        oversized
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<RoutedSendError>()),
        Some(RoutedSendError::PayloadTooLarge { .. })
    ));
    let payload = vec![0x5a; 60_000];
    left.send(right.local_id(), &payload).await.unwrap();
    let received = timeout(DEADLINE, right.recv()).await.unwrap().unwrap();
    assert_eq!(received.from, *left.local_id());
    assert_eq!(received.msg.as_ref(), payload);
    left.close().await;
    right.close().await;
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &left_address.ipc).unwrap();
    replacement.close().await;
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &right_address.ipc).unwrap();
    replacement.close().await;
}

#[tokio::test]
async fn provider_preflights_every_peer_before_opening_a_listener() {
    let address = Address::new();
    let peer_address = Address::new();
    let local = NodeId::new("local");
    // An occupied listener address proves no bind was attempted: a provider
    // reaching the listener would fail with the platform's address error.
    let occupant = IpcTransport::bind(NodeId::new("occupant"), &address.ipc).unwrap();
    let peer = |node: &str| PeerEndpoint::new(NodeId::new(node), peer_address.ipc.clone());
    let link_cases = [
        (
            vec![
                peer("a"),
                PeerEndpoint::new(local.clone(), peer_address.ipc.clone()),
            ],
            LinkConfigError::LocalPeer { index: 1 },
        ),
        (
            vec![peer("a"), peer("a")],
            LinkConfigError::DuplicatePeer { index: 1 },
        ),
        (
            vec![peer("")],
            LinkConfigError::Peer {
                index: 0,
                error: NodeIdError::Empty,
            },
        ),
    ];
    for (peers, expected) in link_cases {
        let error = Box::new(IpcLink::new(address.ipc.clone(), peers))
            .bind(local.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<LinkConfigError>()),
            Some(&expected)
        );
    }
    let one_peer = IpcConfig {
        max_peers: NonZeroUsize::MIN,
        ..IpcConfig::default()
    };
    let malformed = PeerEndpoint::new(NodeId::new("a"), malformed_address());
    let ipc_cases: [(IpcLink, PeerCheck); 3] = [
        (
            IpcLink::new(address.ipc.clone(), vec![peer(&"x".repeat(65))]),
            |error| {
                matches!(
                    error,
                    IpcPeerError::Identity(NodeIdError::TooLong { len: 65, max: 64 })
                )
            },
        ),
        (
            IpcLink::new(address.ipc.clone(), vec![peer("a"), peer("b")]).with_config(one_peer),
            |error| matches!(error, IpcPeerError::PeerLimit { max_peers } if *max_peers == NonZeroUsize::MIN),
        ),
        (
            IpcLink::new(address.ipc.clone(), vec![malformed]),
            |error| matches!(error, IpcPeerError::Address(address) if is_malformed(address)),
        ),
    ];
    for (provider, expected) in ipc_cases {
        let error = Box::new(provider).bind(local.clone()).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let typed = error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<IpcPeerError>())
            .expect("typed peer error");
        assert!(expected(typed), "{typed:?}");
    }
    occupant.close().await;
    let replacement = IpcTransport::bind(NodeId::new("replacement"), &address.ipc).unwrap();
    replacement.close().await;
}

/// A peer address this platform rejects as malformed, before any I/O.
fn malformed_address() -> IpcAddress {
    #[cfg(unix)]
    {
        IpcAddress::Unix("relative/socket".into())
    }
    #[cfg(windows)]
    {
        IpcAddress::NamedPipe(r"\\remote\pipe\groupnet".to_owned())
    }
}

/// Whether `error` is the reason [`malformed_address`] is rejected.
fn is_malformed(error: &IpcAddressError) -> bool {
    #[cfg(unix)]
    {
        matches!(error, IpcAddressError::RelativePath)
    }
    #[cfg(windows)]
    {
        matches!(error, IpcAddressError::NotLocalPipe)
    }
}

#[tokio::test]
async fn malformed_addresses_are_typed_for_bind_and_registration() {
    use std::error::Error as _;

    let error = IpcTransport::bind(NodeId::new("local"), &malformed_address()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(is_malformed(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<IpcAddressError>())
            .expect("typed address error")
    ));
    let address = Address::new();
    let transport = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    let error = transport
        .register_peer(NodeId::new("peer"), malformed_address())
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    let typed = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<IpcPeerError>())
        .expect("typed peer error");
    let IpcPeerError::Address(cause) = typed else {
        panic!("expected an address error, got {typed:?}");
    };
    assert!(is_malformed(cause));
    assert!(
        typed
            .source()
            .and_then(|source| source.downcast_ref::<IpcAddressError>())
            .is_some_and(is_malformed)
    );
    transport.close().await;
}

#[tokio::test]
async fn rejected_router_registration_closes_provider_listener_before_returning() {
    let address = Address::new();
    let router = Router::new(NodeId::new("local")).unwrap();
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

#[tokio::test]
async fn configured_peer_limit_allows_replacement_but_rejects_growth() {
    let address = Address::new();
    let peer_address = Address::new();
    let another_address = Address::new();
    let config = IpcConfig {
        max_peers: NonZeroUsize::MIN,
        ..IpcConfig::default()
    };
    let transport = IpcTransport::bind_with(NodeId::new("local"), &address.ipc, config).unwrap();
    transport
        .register_peer(NodeId::new("peer"), peer_address.ipc.clone())
        .unwrap();
    transport
        .register_peer(NodeId::new("peer"), another_address.ipc.clone())
        .unwrap();
    assert!(
        transport
            .register_peer(NodeId::new("other"), peer_address.ipc.clone())
            .is_err()
    );
    transport.close().await;
}

#[tokio::test]
async fn invalid_limits_do_not_reserve_native_listener() {
    let address = Address::new();
    let config = IpcConfig {
        setup_timeout: Duration::ZERO,
        ..IpcConfig::default()
    };
    let error = IpcTransport::bind_with(NodeId::new("local"), &address.ipc, config).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<IpcConfigError>()),
        Some(&IpcConfigError::ZeroSetupTimeout)
    );
    let transport = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    transport.close().await;
    let provider = IpcLink::new(address.ipc.clone(), Vec::new()).with_config(config);
    let error = Box::new(provider)
        .bind(NodeId::new("local"))
        .await
        .unwrap_err();
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<IpcConfigError>()),
        Some(&IpcConfigError::ZeroSetupTimeout),
        "the link validates once, at bind"
    );
    let replacement = IpcTransport::bind(NodeId::new("local"), &address.ipc).unwrap();
    replacement.close().await;
}

#[tokio::test]
async fn owned_packets_cross_native_ipc_with_small_configured_queues() {
    let left_address = Address::new();
    let right_address = Address::new();
    let config = IpcConfig {
        max_peers: NonZeroUsize::MIN,
        max_sessions: QueueCapacity::MIN,
        session_queue: QueueCapacity::MIN,
        inbound_queue: QueueCapacity::MIN,
        ..IpcConfig::default()
    };
    let left = IpcTransport::bind_with(NodeId::new("left"), &left_address.ipc, config).unwrap();
    let right = IpcTransport::bind_with(NodeId::new("right"), &right_address.ipc, config).unwrap();
    left.register_peer(NodeId::new("right"), right_address.ipc.clone())
        .unwrap();
    let payload = bytes::Bytes::from(vec![0x5a; MAX_FRAME]);
    left.send_owned_admitted(&NodeId::new("right"), payload.clone(), None)
        .await
        .unwrap();
    assert_eq!(receive(&right).await.msg, payload);
    left.close().await;
    right.close().await;
}

// The deadline tests configure deadlines far below the test's own `DEADLINE`
// (which equals the 5 s setup default), so a session closing in time proves
// the configured value — not a hard-coded one — was applied.

#[tokio::test]
async fn configured_setup_deadline_closes_a_silent_connection() {
    let address = Address::new();
    let config = IpcConfig {
        setup_timeout: Duration::from_millis(100),
        ..IpcConfig::default()
    };
    let transport = IpcTransport::bind_with(NodeId::new("listener"), &address.ipc, config).unwrap();
    let mut stream = raw_connect(&address.ipc).await;
    // No introduction is ever sent; the listener gives up at its deadline.
    assert_closed(&mut stream).await;
    transport.close().await;
}

#[tokio::test]
async fn configured_read_deadline_closes_an_idle_session() {
    let address = Address::new();
    let config = IpcConfig {
        read_timeout: Duration::from_millis(100),
        ..IpcConfig::default()
    };
    let transport = IpcTransport::bind_with(NodeId::new("listener"), &address.ipc, config).unwrap();
    let mut stream = raw_connect(&address.ipc).await;
    introduction(&mut stream).await;
    // The session is established but never carries a frame.
    assert_closed(&mut stream).await;
    transport.close().await;
}

/// Whether the listener admits a fresh connection: it introduces itself only
/// when a session slot is free, and drops the connection otherwise.
async fn admitted(address: &IpcAddress) -> bool {
    let mut probe = raw_connect(address).await;
    if probe.write_all(b"GNI1\x05probe").await.is_err() {
        return false;
    }
    let mut header = [0_u8; 5];
    probe.read_exact(&mut header).await.is_ok()
}

#[tokio::test]
async fn configured_write_deadline_ends_a_session_whose_peer_stops_reading() {
    let address = Address::new();
    let config = IpcConfig {
        max_sessions: QueueCapacity::MIN,
        write_timeout: Duration::from_millis(100),
        ..IpcConfig::default()
    };
    let transport = IpcTransport::bind_with(NodeId::new("listener"), &address.ipc, config).unwrap();
    let mut stream = raw_connect(&address.ipc).await;
    introduction(&mut stream).await;
    // A delivered inbound frame proves the session is registered for sends.
    stream.write_u32_le(2).await.unwrap();
    stream.write_all(b"up").await.unwrap();
    assert_eq!(receive(&transport).await.msg.as_ref(), b"up");
    // The single slot is held by the live session.
    assert!(!admitted(&address.ipc).await);

    let raw = NodeId::new("raw");
    let frame = vec![0x5a; MAX_FRAME];
    // Far more than any OS buffer holds while `stream` is never read: the
    // writer blocks, and full-queue sends are best-effort drops meanwhile.
    for _ in 0..256 {
        transport.send(&raw, &frame).await.unwrap();
        tokio::task::yield_now().await;
    }
    // The write deadline ends the stalled session and frees its slot.
    timeout(DEADLINE, async {
        while !admitted(&address.ipc).await {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the write deadline must end the stalled session");
    drop(stream);
    transport.close().await;
}
