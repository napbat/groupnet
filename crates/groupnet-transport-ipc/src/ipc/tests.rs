use super::*;
use groupnet_transport::QueueCapacity;

fn transport(config: IpcConfig) -> (IpcTransport, mpsc::Receiver<runtime::Dial>) {
    config.validate().unwrap();
    let (incoming, inbox) = mpsc::channel(config.inbound_queue.get());
    let (commands, receiver) = mpsc::channel(config.max_sessions.get());
    let (_, done) = watch::channel(false);
    let state = Arc::new(State {
        local: NodeId::new("local"),
        config,
        book: Mutex::new(Book::default()),
        slots: Arc::new(Semaphore::new(config.max_sessions.get())),
        inbox: AsyncMutex::new(inbox),
        incoming,
        commands,
        cancel: CancellationToken::new(),
    });
    (
        IpcTransport {
            handle: Arc::new(Handle { state, done }),
        },
        receiver,
    )
}

fn address() -> IpcAddress {
    #[cfg(unix)]
    {
        IpcAddress::Unix(PathBuf::from("/unused-peer-socket"))
    }
    #[cfg(windows)]
    {
        IpcAddress::NamedPipe(r"\\.\pipe\unused-peer-pipe".to_owned())
    }
}

#[tokio::test]
async fn owned_packets_keep_storage_and_full_queue_drops_without_copying() {
    let config = IpcConfig {
        session_queue: QueueCapacity::MIN,
        ..IpcConfig::default()
    };
    let (transport, _commands) = transport(config);
    let remote = NodeId::new("remote");
    let (sender, mut frames) = mpsc::channel(config.session_queue.get());
    transport
        .handle
        .state
        .book
        .lock()
        .expect("book")
        .sessions
        .insert(
            remote.clone(),
            Session {
                generation: 1,
                sender,
            },
        );
    let packet = Bytes::from(vec![0x5a; 4096]);
    let pointer = packet.as_ptr();
    transport
        .send_owned_admitted(&remote, packet, None)
        .await
        .unwrap();
    transport
        .enqueue(&remote, 1, || {
            panic!("full queues must not allocate borrowed storage")
        })
        .unwrap();
    let frame = frames.recv().await.unwrap();
    assert_eq!(frame.as_ptr(), pointer);
    assert_eq!(frame.len(), 4096);
    assert!(frames.try_recv().is_err());
}

#[tokio::test]
async fn setup_limit_and_session_queue_follow_configuration() {
    let config = IpcConfig {
        max_sessions: QueueCapacity::MIN,
        session_queue: QueueCapacity::of(2),
        ..IpcConfig::default()
    };
    let (transport, mut commands) = transport(config);
    let first = NodeId::new("first");
    let second = NodeId::new("second");
    {
        // Enqueue tests never dial these addresses; native validation is covered
        // by the real-listener integration tests.
        let mut book = transport.handle.state.book.lock().expect("book");
        book.peers.insert(first.clone(), address());
        book.peers.insert(second.clone(), address());
    }
    transport.send(&first, b"one").await.unwrap();
    transport.send(&first, b"two").await.unwrap();
    transport
        .enqueue(&first, 1, || panic!("full session queue copied a packet"))
        .unwrap();
    transport
        .enqueue(&second, 1, || {
            panic!("exhausted setup slots copied a packet")
        })
        .unwrap();
    let mut command = commands.try_recv().unwrap();
    assert_eq!(command.node, first);
    assert_eq!(command.frames.recv().await.unwrap(), b"one".as_slice());
    assert_eq!(command.frames.recv().await.unwrap(), b"two".as_slice());
    assert!(command.frames.try_recv().is_err());
    assert!(commands.try_recv().is_err());
    assert_eq!(
        transport
            .handle
            .state
            .book
            .lock()
            .expect("book")
            .sessions
            .len(),
        1
    );
    drop(command);
    transport.send(&second, b"three").await.unwrap();
    assert_eq!(commands.try_recv().unwrap().node, second);
}

#[tokio::test]
async fn static_owned_send_drops_admitted_session_packets() {
    use groupnet_transport::admission::{AcceptedPeer, SessionRegistry};

    let (transport, _commands) = transport(IpcConfig::default());
    let remote = NodeId::new("remote");
    let (sender, mut frames) = mpsc::channel(1);
    transport
        .handle
        .state
        .book
        .lock()
        .expect("book")
        .sessions
        .insert(
            remote.clone(),
            Session {
                generation: 1,
                sender,
            },
        );
    let registry = SessionRegistry::new(1).unwrap();
    let lease = registry
        .try_admit(AcceptedPeer::new(remote.clone()))
        .unwrap();
    transport
        .send_owned_admitted(
            &remote,
            Bytes::from_static(b"wrong-lifetime"),
            Some(lease.id()),
        )
        .await
        .unwrap();
    transport
        .send_owned_admitted(&remote, Bytes::from_static(b"static"), None)
        .await
        .unwrap();
    assert_eq!(frames.recv().await.unwrap(), b"static".as_slice());
    assert!(frames.try_recv().is_err());
}
