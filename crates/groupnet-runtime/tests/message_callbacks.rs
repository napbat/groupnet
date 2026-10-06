//! Application callback ownership, completion and cancellation through public APIs.

use std::{io, sync::Arc, time::Duration};

use groupnet_core::NodeId;
use groupnet_runtime::{
    Messages, Node,
    messaging::{Bytes, Delivery, SendOptions},
};
use tokio::{sync::Notify, time::timeout};

const WAIT: Duration = Duration::from_secs(5);

fn applied() -> SendOptions {
    SendOptions {
        delivery: Delivery::Applied,
        timeout: WAIT,
    }
}

#[tokio::test]
async fn callback_acknowledges_only_after_success_and_owns_the_inbox() {
    let node = Node::builder(NodeId::new("callback"))
        .start()
        .await
        .unwrap();
    let endpoint = node.endpoint(Messages::applied(WAIT)).unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let started = entered.clone();
    let gate = release.clone();
    let (seen, mut received_ids) = tokio::sync::mpsc::unbounded_channel();
    let handle = endpoint
        .on_recv(move |context, payload| {
            let started = started.clone();
            let gate = gate.clone();
            let seen = seen.clone();
            async move {
                assert_eq!(context.from, NodeId::new("callback"));
                assert_eq!(context.group, None);
                assert_eq!(context.delivery(), Delivery::Applied);
                assert_eq!(payload.as_ref(), b"apply me");
                seen.send(context.id).unwrap();
                drop(context);
                drop(payload);
                started.notify_one();
                gate.notified().await;
                Ok(())
            }
        })
        .unwrap();
    assert_eq!(
        node.recv().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        node.on_recv(|_, _| async { Ok(()) }).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        node.recv_frame().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        node.on_frame(|_| async { Ok(()) }).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let mut sent = Box::pin(endpoint.send(node.id(), Bytes::from_static(b"apply me")));
    tokio::select! {
        result = &mut sent => panic!("applied send completed before processing: {result:?}"),
        () = async { timeout(WAIT, entered.notified()).await.unwrap(); } => {}
    }
    assert!(timeout(Duration::from_millis(50), &mut sent).await.is_err());
    release.notify_one();
    let id = timeout(WAIT, &mut sent).await.unwrap().unwrap();
    assert_eq!(id, received_ids.recv().await.unwrap());
    handle.close().await.unwrap();
    node.send(node.id(), b"manual again").await.unwrap();
    let frame = timeout(WAIT, endpoint.recv_frame()).await.unwrap().unwrap();
    assert_eq!(frame.payload.as_ref(), b"manual again");
    node.close().await;
}

#[tokio::test]
async fn callback_failure_rejects_application_and_releases_receive_ownership() {
    let node = Node::builder(NodeId::new("failure")).start().await.unwrap();
    let endpoint = node.endpoint(Messages::applied(WAIT)).unwrap();
    let handle = endpoint
        .on_frame(|frame| async move {
            let (context, payload) = frame.into_parts();
            assert_eq!(payload.as_ref(), b"reject me");
            assert_eq!(context.from, NodeId::new("failure"));
            assert_eq!(context.group, None);
            assert_eq!(context.delivery(), Delivery::Applied);
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "application refused",
            ))
        })
        .unwrap();
    let result = timeout(
        WAIT,
        endpoint.send(node.id(), Bytes::from_static(b"reject me")),
    )
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(
        handle.wait().await.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    node.send(node.id(), b"after rejection").await.unwrap();
    let (context, payload) = timeout(WAIT, endpoint.recv()).await.unwrap().unwrap();
    assert_eq!(payload.as_ref(), b"after rejection");
    assert_eq!(context.from, NodeId::new("failure"));
    assert_eq!(context.group, None);
    assert_eq!(context.delivery(), SendOptions::default().delivery);
    node.close().await;
}

#[tokio::test]
async fn cancelled_callback_does_not_ack_unfinished_work_and_manual_wait_is_exclusive() {
    let node = Node::builder(NodeId::new("cancel")).start().await.unwrap();
    let mut manual = Box::pin(node.recv_frame());
    assert!(
        timeout(Duration::from_millis(20), &mut manual)
            .await
            .is_err()
    );
    assert_eq!(
        node.on_recv(|_, _| async { Ok(()) }).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(manual);
    let entered = Arc::new(Notify::new());
    let started = entered.clone();
    let handle = node
        .on_recv(move |context, payload| {
            assert_eq!(context.from, NodeId::new("cancel"));
            assert_eq!(context.group, None);
            assert_eq!(context.delivery(), Delivery::Applied);
            assert_eq!(payload.as_ref(), b"unfinished");
            started.notify_one();
            std::future::pending::<io::Result<()>>()
        })
        .unwrap();
    let options = SendOptions {
        delivery: Delivery::Applied,
        timeout: Duration::from_millis(250),
    };
    let mut sent = Box::pin(node.send_frame(node.id(), Bytes::from_static(b"unfinished"), options));
    tokio::select! {
        result = &mut sent => panic!("send completed without callback application: {result:?}"),
        () = async { timeout(WAIT, entered.notified()).await.unwrap(); } => {}
    }
    drop(handle);
    assert_eq!(
        timeout(WAIT, &mut sent).await.unwrap().unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    node.send(node.id(), b"new delivery").await.unwrap();
    let frame = timeout(WAIT, node.recv_frame()).await.unwrap().unwrap();
    assert_eq!(frame.payload.as_ref(), b"new delivery");
    node.close().await;
}

#[tokio::test]
async fn final_node_owner_drop_cancels_callback_and_blocked_receive() {
    let node = Node::builder(NodeId::new("shutdown"))
        .start()
        .await
        .unwrap();
    let group = node.join_group("messages");
    let entered = Arc::new(Notify::new());
    let started = entered.clone();
    let handle = node
        .on_frame(move |_| {
            started.notify_one();
            std::future::pending::<io::Result<()>>()
        })
        .unwrap();
    node.send(node.id(), b"in flight").await.unwrap();
    timeout(WAIT, entered.notified()).await.unwrap();
    drop(node);
    assert_eq!(
        timeout(WAIT, handle.wait())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        timeout(WAIT, group.recv())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        group.on_recv(|_, _| async { Ok(()) }).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn full_application_inbox_rejects_delivery_without_blocking_metadata_or_losing_queued_frames()
{
    let node = Node::builder(NodeId::new("bounded")).start().await.unwrap();
    let group = node.join_group("coordination");
    let options = SendOptions {
        delivery: Delivery::Delivered,
        timeout: WAIT,
    };
    for value in 0_u8..64 {
        node.send_frame(node.id(), Bytes::copy_from_slice(&[value]), options)
            .await
            .unwrap();
    }
    assert_eq!(
        node.send_frame(node.id(), Bytes::from_static(b"overflow"), options)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    group.sync(|state| state.update_metadata("independent", "yes"));
    timeout(WAIT, async {
        while group.metadata("independent").as_deref() != Some("yes") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for value in 0_u8..64 {
        let frame = timeout(WAIT, node.recv_frame()).await.unwrap().unwrap();
        assert_eq!(frame.payload.as_ref(), &[value]);
    }
    node.send_frame(
        node.id(),
        Bytes::from_static(b"capacity recovered"),
        options,
    )
    .await
    .unwrap();
    assert_eq!(
        timeout(WAIT, node.recv())
            .await
            .unwrap()
            .unwrap()
            .1
            .as_ref(),
        b"capacity recovered"
    );
    node.close().await;
}

#[tokio::test]
async fn endpoint_shutdown_wakes_blocked_node_and_group_manual_receives() {
    let node = Node::builder(NodeId::new("endpoint-manual"))
        .start()
        .await
        .unwrap();
    let group = node.join_group("blocked");
    let mut node_recv = Box::pin(node.recv());
    let mut group_recv = Box::pin(group.recv_frame());
    assert!(
        timeout(Duration::from_millis(20), &mut node_recv)
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(20), &mut group_recv)
            .await
            .is_err()
    );
    node.endpoint(Messages::best_effort())
        .unwrap()
        .protocol()
        .shutdown();
    assert_eq!(
        timeout(WAIT, &mut node_recv)
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        timeout(WAIT, &mut group_recv)
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        node.recv_frame().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        group.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        node.send(node.id(), b"closed").await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        group.send(b"closed").await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    let new_group = node.join_group("after-shutdown");
    assert_eq!(
        new_group.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert!(!node.router().is_closed());
    node.close().await;
}

#[tokio::test]
async fn endpoint_shutdown_cancels_inflight_node_and_group_callbacks_without_applying() {
    let node = Node::builder(NodeId::new("endpoint-callback"))
        .start()
        .await
        .unwrap();
    let group = node.join_group("inflight");
    groupnet_testkit::cluster::eventually_within("local callback group membership", WAIT, || {
        group.members().contains(node.id())
    })
    .await;
    let node_entered = Arc::new(Notify::new());
    let group_entered = Arc::new(Notify::new());
    let started = node_entered.clone();
    let node_handle = node
        .on_recv(move |_, _| {
            started.notify_one();
            std::future::pending::<io::Result<()>>()
        })
        .unwrap();
    let started = group_entered.clone();
    let group_handle = group
        .on_frame(move |_| {
            started.notify_one();
            std::future::pending::<io::Result<()>>()
        })
        .unwrap();
    let messages = node.endpoint(Messages::applied(WAIT)).unwrap();
    let mut node_sent =
        Box::pin(node.send_frame(node.id(), Bytes::from_static(b"unfinished node"), applied()));
    let mut group_sent = Box::pin(messages.protocol().send(
        node.id(),
        Some(group.id()),
        Bytes::from_static(b"unfinished group"),
        applied(),
    ));
    tokio::select! {
        result = &mut node_sent => panic!("node applied before completion: {result:?}"),
        result = &mut group_sent => panic!("group applied before completion: {result:?}"),
        () = async {
            timeout(WAIT, async {
                tokio::join!(node_entered.notified(), group_entered.notified());
            }).await.unwrap();
        } => {}
    }
    messages.protocol().shutdown();
    for handle in [node_handle, group_handle] {
        assert_eq!(
            timeout(WAIT, handle.wait())
                .await
                .unwrap()
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotConnected
        );
    }
    assert_eq!(
        node_sent.await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        group_sent.await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        node.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        group.recv_frame().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        node.on_frame(|_| async { Ok(()) }).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(
        group.on_recv(|_, _| async { Ok(()) }).unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert!(!node.router().is_closed());
    node.close().await;
}
