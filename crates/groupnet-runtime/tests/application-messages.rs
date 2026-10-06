//! Application routing, frozen group fanout, queue failures, and buffer limits.
//! Callback execution/ownership regressions live in `message_callbacks.rs`.

use std::io;
use std::time::Duration;

use groupnet_core::{GroupId, NodeId};
use groupnet_messaging::Messaging;
use groupnet_network::NetworkConfig;
use groupnet_runtime::messaging::{Bytes, DEFAULT_MAX_MESSAGE_BYTES, Delivery, SendOptions};
use groupnet_runtime::{Group, Node};
use groupnet_testkit::cluster::{MemCluster, eventually_within};
use groupnet_transport_mem::MemLink;

const WAIT: Duration = Duration::from_secs(5);

fn delivered() -> SendOptions {
    SendOptions {
        delivery: Delivery::Delivered,
        timeout: WAIT,
    }
}

async fn receive(group: &Group) -> groupnet_runtime::messaging::Frame {
    tokio::time::timeout(WAIT, group.recv_frame())
        .await
        .expect("group receive deadline")
        .expect("group frame")
}

async fn converge(groups: &[&Group], expected: usize) {
    eventually_within("application group membership", WAIT, || {
        groups.iter().all(|group| group.members().len() == expected)
    })
    .await;
}

#[tokio::test]
async fn node_and_group_frames_are_isolated_and_preserve_identity() {
    let cluster = MemCluster::builder(&["a", "b", "c"])
        .group("first")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    converge(&cluster.groups.iter().collect::<Vec<_>>(), 3).await;
    let second: Vec<_> = cluster
        .nodes
        .iter()
        .map(|node| node.join_group("second"))
        .collect();
    converge(&second.iter().collect::<Vec<_>>(), 3).await;

    // Queue all three destinations before receiving: a shared inbox or a
    // coordination-decoder substitution would consume the wrong frame or hang.
    let node_id = cluster.nodes[0]
        .send_frame(&cluster.ids[1], Bytes::from_static(b"node"), delivered())
        .await
        .unwrap();
    let first = cluster.groups[0]
        .send_frame(Bytes::from_static(b"first"), delivered())
        .await
        .unwrap();
    let second_report = second[0]
        .send_frame(Bytes::from_static(b"second"), delivered())
        .await
        .unwrap();
    assert!(first.all_succeeded());
    assert!(second_report.all_succeeded());
    assert_eq!(
        first
            .outcomes
            .iter()
            .map(|outcome| &outcome.node)
            .collect::<Vec<_>>(),
        vec![&cluster.ids[1], &cluster.ids[2]]
    );

    let node_frame = tokio::time::timeout(WAIT, cluster.nodes[1].recv_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(node_frame.id, node_id);
    assert_eq!(node_frame.from, cluster.ids[0]);
    assert_eq!(node_frame.group, None);
    assert_eq!(node_frame.payload.as_ref(), b"node");
    for index in 1..3 {
        let frame = receive(&cluster.groups[index]).await;
        assert_eq!(frame.from, cluster.ids[0]);
        assert_eq!(frame.group.as_ref(), Some(cluster.groups[0].id()));
        assert_eq!(frame.payload.as_ref(), b"first");
        assert_eq!(
            frame.id,
            first.outcomes[index - 1].result.as_ref().copied().unwrap()
        );
        let frame = receive(&second[index]).await;
        assert_eq!(frame.group.as_ref(), Some(second[0].id()));
        assert_eq!(frame.payload.as_ref(), b"second");
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), cluster.groups[0].recv())
            .await
            .is_err(),
        "group fanout must exclude self"
    );

    // Existing coordination and per-node metadata keep their own data paths.
    cluster.groups[0].sync(|ctx| ctx.update_metadata("independent", "value"));
    eventually_within("metadata remains independent", WAIT, || {
        cluster
            .groups
            .iter()
            .all(|group| group.metadata("independent").as_deref() == Some("value"))
    })
    .await;
}

#[tokio::test]
async fn applied_fanout_freezes_recipients_across_departure_and_late_join() {
    let cluster = MemCluster::builder(&["a", "b", "c", "d"])
        .group("background")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    let a = cluster.nodes[0].join_group("snapshot");
    let b = cluster.nodes[1].join_group("snapshot");
    let c = cluster.nodes[2].join_group("snapshot");
    converge(&[&a, &b, &c], 3).await;
    let sender = a.clone();
    let sending = tokio::spawn(async move {
        sender
            .send_frame(
                Bytes::from_static(b"frozen"),
                SendOptions {
                    delivery: Delivery::Applied,
                    timeout: WAIT,
                },
            )
            .await
    });
    // Both frames must arrive before either is applied: sends are concurrent,
    // not one peer's deadline multiplied by the number of members.
    let b_frame = receive(&b).await;
    let c_frame = receive(&c).await;
    c.leave();
    let d = cluster.nodes[3].join_group("snapshot");
    eventually_within("departed and late membership view", WAIT, || {
        [&a, &b, &d].iter().all(|group| {
            let members = group.members();
            members.len() == 3
                && [0, 1, 3]
                    .into_iter()
                    .all(|index| members.contains(&cluster.ids[index]))
        })
    })
    .await;
    b_frame.applied().unwrap();
    assert!(
        !sending.is_finished(),
        "departed recipient still owes its original receipt"
    );
    c_frame.applied().unwrap();
    let report = tokio::time::timeout(WAIT, sending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(report.all_succeeded());
    assert_eq!(
        report
            .outcomes
            .iter()
            .map(|outcome| &outcome.node)
            .collect::<Vec<_>>(),
        vec![&cluster.ids[1], &cluster.ids[2]]
    );
    assert_eq!(
        report.outcomes[0].result.as_ref().copied().unwrap(),
        b_frame.id
    );
    assert_eq!(
        report.outcomes[1].result.as_ref().copied().unwrap(),
        c_frame.id
    );

    let next = a
        .send_frame(Bytes::from_static(b"new snapshot"), delivered())
        .await
        .unwrap();
    assert!(next.all_succeeded(), "{next:?}");
    assert_eq!(
        receive(&d).await.payload.as_ref(),
        b"new snapshot",
        "late joiner must not receive an implicit replay"
    );
    assert_eq!(receive(&b).await.payload.as_ref(), b"new snapshot");
}

#[tokio::test]
async fn full_group_inbox_is_reported_without_hiding_other_recipient_success() {
    let cluster = MemCluster::builder(&["a", "b", "c"])
        .group("bounded")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    converge(&cluster.groups.iter().collect::<Vec<_>>(), 3).await;
    for _ in 0..64 {
        let report = cluster.groups[0]
            .send_frame(Bytes::from_static(b"fill"), delivered())
            .await
            .unwrap();
        assert!(report.all_succeeded());
        let _ = receive(&cluster.groups[2]).await;
    }
    let report = cluster.groups[0]
        .send_frame(Bytes::from_static(b"overflow"), delivered())
        .await
        .unwrap();
    assert!(!report.all_succeeded());
    assert_eq!(report.outcomes.len(), 2);
    assert_eq!(report.outcomes[0].node, cluster.ids[1]);
    assert_eq!(
        report.outcomes[0].result.as_ref().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(report.outcomes[1].result.is_ok());
    assert_eq!(
        receive(&cluster.groups[2]).await.payload.as_ref(),
        b"overflow"
    );
    // The rejected frame never displaced an already accepted frame.
    for _ in 0..64 {
        assert_eq!(receive(&cluster.groups[1]).await.payload.as_ref(), b"fill");
    }
    let report = cluster.groups[0]
        .send_frame(Bytes::from_static(b"after drain"), delivered())
        .await
        .unwrap();
    assert!(report.all_succeeded());
    assert_eq!(
        receive(&cluster.groups[1]).await.payload.as_ref(),
        b"after drain"
    );
}

#[tokio::test]
async fn payload_boundaries_and_empty_fanout_validate_before_dispatch() {
    let cluster = MemCluster::builder(&["a", "b"])
        .group("limits")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    converge(&cluster.groups.iter().collect::<Vec<_>>(), 2).await;
    let maximum = Bytes::from(vec![0xa5; DEFAULT_MAX_MESSAGE_BYTES]);
    let id = cluster.nodes[0]
        .send_frame(&cluster.ids[1], maximum.clone(), delivered())
        .await
        .unwrap();
    let frame = tokio::time::timeout(WAIT, cluster.nodes[1].recv_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.id, id);
    assert_eq!(frame.payload, maximum);
    let report = cluster.groups[0]
        .send_frame(maximum.clone(), delivered())
        .await
        .unwrap();
    assert!(report.all_succeeded());
    assert_eq!(receive(&cluster.groups[1]).await.payload, maximum);
    cluster.nodes[0].send(&cluster.ids[1], []).await.unwrap();
    assert!(
        tokio::time::timeout(WAIT, cluster.nodes[1].recv())
            .await
            .unwrap()
            .unwrap()
            .1
            .is_empty()
    );

    let oversized = vec![0; DEFAULT_MAX_MESSAGE_BYTES + 1];
    assert_eq!(
        cluster.nodes[0]
            .send(&cluster.ids[1], &oversized)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        cluster.groups[0].send(&oversized).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let empty = cluster.nodes[0].join_group("empty");
    assert!(empty.send(b"valid").await.unwrap().outcomes.is_empty());
    assert_eq!(
        empty.send(&oversized).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    for timeout in [Duration::ZERO, Duration::from_secs(31)] {
        let options = SendOptions {
            delivery: Delivery::Delivered,
            timeout,
        };
        assert_eq!(
            empty
                .send_frame(Bytes::new(), options)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            cluster.groups[0]
                .send_frame(Bytes::new(), options)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    // A subsequent marker is the first queued frame: invalid attempts emitted
    // no data, even with a nonempty membership snapshot.
    cluster.nodes[0]
        .send_frame(&cluster.ids[1], Bytes::from_static(b"marker"), delivered())
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(WAIT, cluster.nodes[1].recv())
            .await
            .unwrap()
            .unwrap()
            .1
            .as_ref(),
        b"marker"
    );
    cluster.groups[0]
        .send_frame(Bytes::from_static(b"marker"), delivered())
        .await
        .unwrap();
    assert_eq!(
        receive(&cluster.groups[1]).await.payload.as_ref(),
        b"marker"
    );
    cluster.nodes[0].close().await;
    assert!(
        empty.send(b"closed").await.is_err(),
        "empty fanout must not conceal shutdown"
    );
}

#[tokio::test]
async fn dispatcher_rejects_unjoined_groups_and_unknown_group_sources() {
    let net = groupnet_transport_mem::Network::new();
    let a = NodeId::new("a");
    let b = NodeId::new("b");
    let receiver = Node::builder(a.clone())
        .link(MemLink::new(net.endpoint(a.clone()), vec![b.clone()]))
        .gossip_interval_ms(20)
        .start()
        .await
        .unwrap();
    let group = receiver.join_group("joined");
    let network = NetworkConfig::default()
        .with_link(MemLink::new(net.endpoint(b), vec![a.clone()]))
        .bind(NodeId::new("b"))
        .await
        .unwrap();
    let sender = Messaging::new(network.router()).unwrap();
    eventually_within("application routes", WAIT, || {
        receiver.router().route_to(&NodeId::new("b")).is_some()
            && network.router().route_to(&a).is_some()
    })
    .await;
    let unknown = GroupId::new("unjoined");
    let error = sender
        .send(
            &a,
            Some(&unknown),
            Bytes::from_static(b"unjoined"),
            delivered(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    let error = sender
        .send(
            &a,
            Some(group.id()),
            Bytes::from_static(b"outsider"),
            delivered(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    let node_id = sender
        .send(&a, None, Bytes::from_static(b"node allowed"), delivered())
        .await
        .unwrap();
    let frame = tokio::time::timeout(WAIT, receiver.recv_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.id, node_id);
    assert_eq!(frame.payload.as_ref(), b"node allowed");
    receiver.close().await;
    sender.close().await;
    network.close().await;
}

#[tokio::test]
async fn group_report_keeps_success_and_unknown_timeout_outcomes() {
    let cluster = MemCluster::builder(&["a", "b", "c"])
        .group("partial")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    converge(&cluster.groups.iter().collect::<Vec<_>>(), 3).await;
    let sender = cluster.groups[0].clone();
    let sending = tokio::spawn(async move {
        sender
            .send_frame(
                Bytes::from_static(b"work"),
                SendOptions {
                    delivery: Delivery::Applied,
                    timeout: Duration::from_millis(500),
                },
            )
            .await
    });
    let completed = receive(&cluster.groups[1]).await;
    let unfinished = receive(&cluster.groups[2]).await;
    completed.applied().unwrap();
    let report = tokio::time::timeout(WAIT, sending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!report.all_succeeded());
    assert_eq!(report.outcomes.len(), 2);
    assert_eq!(report.outcomes[0].node, cluster.ids[1]);
    assert_eq!(
        report.outcomes[0].result.as_ref().copied().unwrap(),
        completed.id
    );
    assert_eq!(report.outcomes[1].node, cluster.ids[2]);
    assert_eq!(
        report.outcomes[1].result.as_ref().unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    // Timeout did not retract the accepted frame or claim it was unapplied.
    unfinished.applied().unwrap();
}

#[tokio::test]
async fn manual_buffer_contexts_survive_payload_moves_and_complete_application() {
    let cluster = MemCluster::builder(&["a", "b"])
        .group("buffers")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    converge(&cluster.groups.iter().collect::<Vec<_>>(), 2).await;
    let options = SendOptions {
        delivery: Delivery::Applied,
        timeout: WAIT,
    };
    let sender = cluster.nodes[0].clone();
    let recipient = cluster.ids[1].clone();
    let mut node_sending = tokio::spawn(async move {
        sender
            .send_frame(&recipient, Bytes::from_static(b"node bytes"), options)
            .await
    });
    let sender = cluster.groups[0].clone();
    let mut group_sending = tokio::spawn(async move {
        sender
            .send_frame(Bytes::from_static(b"group bytes"), options)
            .await
    });
    let (node_context, node_bytes) = tokio::time::timeout(WAIT, cluster.nodes[1].recv())
        .await
        .unwrap()
        .unwrap();
    let (group_context, group_bytes) = tokio::time::timeout(WAIT, cluster.groups[1].recv())
        .await
        .unwrap()
        .unwrap();
    tokio::spawn(async move {
        assert_eq!(node_bytes.as_ref(), b"node bytes");
        assert_eq!(group_bytes.as_ref(), b"group bytes");
        drop(node_bytes);
        drop(group_bytes);
    })
    .await
    .unwrap();
    // Moving and dropping the buffers must neither lose identity nor apply work.
    assert_eq!(node_context.from, cluster.ids[0]);
    assert_eq!(node_context.group, None);
    assert_eq!(node_context.delivery(), Delivery::Applied);
    assert_eq!(group_context.from, cluster.ids[0]);
    assert_eq!(group_context.group.as_ref(), Some(cluster.groups[0].id()));
    assert_eq!(group_context.delivery(), Delivery::Applied);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut node_sending)
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut group_sending)
            .await
            .is_err()
    );
    node_context.applied().unwrap();
    group_context.applied().unwrap();
    let node_id = tokio::time::timeout(WAIT, node_sending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let report = tokio::time::timeout(WAIT, group_sending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(node_id, node_context.id);
    assert!(report.all_succeeded());
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].node, cluster.ids[1]);
    assert_eq!(
        report.outcomes[0].result.as_ref().copied().unwrap(),
        group_context.id
    );
}

#[tokio::test]
async fn manual_buffer_contexts_reject_after_payload_drop() {
    let cluster = MemCluster::builder(&["a", "b"])
        .group("rejected buffers")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    converge(&cluster.groups.iter().collect::<Vec<_>>(), 2).await;
    let options = SendOptions {
        delivery: Delivery::Applied,
        timeout: WAIT,
    };
    let sender = cluster.nodes[0].clone();
    let recipient = cluster.ids[1].clone();
    let node_sending = tokio::spawn(async move {
        sender
            .send_frame(&recipient, Bytes::from_static(b"reject node"), options)
            .await
    });
    let sender = cluster.groups[0].clone();
    let group_sending = tokio::spawn(async move {
        sender
            .send_frame(Bytes::from_static(b"reject group"), options)
            .await
    });
    let (node_context, node_bytes) = tokio::time::timeout(WAIT, cluster.nodes[1].recv())
        .await
        .unwrap()
        .unwrap();
    let (group_context, group_bytes) = tokio::time::timeout(WAIT, cluster.groups[1].recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(node_bytes.as_ref(), b"reject node");
    assert_eq!(group_bytes.as_ref(), b"reject group");
    drop(node_bytes);
    drop(group_bytes);
    assert_eq!(node_context.from, cluster.ids[0]);
    assert_eq!(node_context.group, None);
    assert_eq!(node_context.delivery(), Delivery::Applied);
    assert_eq!(group_context.from, cluster.ids[0]);
    assert_eq!(group_context.group.as_ref(), Some(cluster.groups[0].id()));
    assert_eq!(group_context.delivery(), Delivery::Applied);
    node_context
        .reject(io::ErrorKind::PermissionDenied)
        .unwrap();
    group_context
        .reject(io::ErrorKind::PermissionDenied)
        .unwrap();
    assert_eq!(
        tokio::time::timeout(WAIT, node_sending)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    let report = tokio::time::timeout(WAIT, group_sending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!report.all_succeeded());
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].node, cluster.ids[1]);
    assert_eq!(
        report.outcomes[0].result.as_ref().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn dropping_manual_buffer_contexts_does_not_acknowledge_application() {
    let cluster = MemCluster::builder(&["a", "b"])
        .group("abandoned buffers")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    converge(&cluster.groups.iter().collect::<Vec<_>>(), 2).await;
    let options = SendOptions {
        delivery: Delivery::Applied,
        timeout: Duration::from_millis(250),
    };
    let (node_result, group_result, ()) = tokio::join!(
        cluster.nodes[0].send_frame(&cluster.ids[1], Bytes::from_static(b"node work"), options),
        cluster.groups[0].send_frame(Bytes::from_static(b"group work"), options),
        async {
            let (node_context, node_bytes) = tokio::time::timeout(WAIT, cluster.nodes[1].recv())
                .await
                .unwrap()
                .unwrap();
            let (group_context, group_bytes) = tokio::time::timeout(WAIT, cluster.groups[1].recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(node_bytes.as_ref(), b"node work");
            assert_eq!(group_bytes.as_ref(), b"group work");
            assert_eq!(node_context.delivery(), Delivery::Applied);
            assert_eq!(group_context.delivery(), Delivery::Applied);
            drop(node_bytes);
            drop(group_bytes);
            drop(node_context);
            drop(group_context);
        }
    );
    assert_eq!(node_result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    let report = group_result.unwrap();
    assert!(!report.all_succeeded());
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].node, cluster.ids[1]);
    assert_eq!(
        report.outcomes[0].result.as_ref().unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
}

#[tokio::test]
async fn buffer_callback_retains_receipt_and_excludes_all_other_receive_variants() {
    let cluster = MemCluster::builder(&["a", "b"])
        .group("callbacks")
        .gossip_interval_ms(20)
        .spawn()
        .await;
    converge(&cluster.groups.iter().collect::<Vec<_>>(), 2).await;
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let started = entered.clone();
    let gate = release.clone();
    let group = &cluster.groups[1];
    let origin = cluster.ids[0].clone();
    let group_id = group.id().clone();
    let (seen, mut received_ids) = tokio::sync::mpsc::unbounded_channel();
    let callback = group
        .on_recv(move |context, payload| {
            let started = started.clone();
            let gate = gate.clone();
            let origin = origin.clone();
            let group_id = group_id.clone();
            let seen = seen.clone();
            async move {
                assert_eq!(payload.as_ref(), b"process buffer");
                assert_eq!(context.from, origin);
                assert_eq!(context.group, Some(group_id));
                assert_eq!(context.delivery(), Delivery::Applied);
                seen.send(context.id).unwrap();
                // Success must still be acknowledged by the callback worker
                // after the application's context and buffer are gone.
                drop(context);
                drop(payload);
                started.notify_one();
                gate.notified().await;
                Ok(())
            }
        })
        .unwrap();
    assert_eq!(
        group.recv().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        group.recv_frame().await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        group.on_recv(|_, _| async { Ok(()) }).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        group.on_frame(|_| async { Ok(()) }).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let mut sending = Box::pin(cluster.groups[0].send_frame(
        Bytes::from_static(b"process buffer"),
        SendOptions {
            delivery: Delivery::Applied,
            timeout: WAIT,
        },
    ));
    tokio::select! {
        result = &mut sending => panic!("send completed before application: {result:?}"),
        () = async { tokio::time::timeout(WAIT, entered.notified()).await.unwrap(); } => {}
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut sending)
            .await
            .is_err()
    );
    release.notify_one();
    let report = tokio::time::timeout(WAIT, &mut sending)
        .await
        .unwrap()
        .unwrap();
    assert!(report.all_succeeded());
    assert_eq!(
        report.outcomes[0].result.as_ref().copied().unwrap(),
        received_ids.recv().await.unwrap()
    );
    callback.close().await.unwrap();
    for payload in [b"buffer".as_slice(), b"frame"] {
        assert!(
            cluster.groups[0]
                .send_frame(Bytes::copy_from_slice(payload), delivered())
                .await
                .unwrap()
                .all_succeeded()
        );
    }
    assert_eq!(
        tokio::time::timeout(WAIT, group.recv())
            .await
            .unwrap()
            .unwrap()
            .1
            .as_ref(),
        b"buffer"
    );
    let frame = receive(group).await;
    assert_eq!(frame.payload.as_ref(), b"frame");
    assert_eq!(frame.from, cluster.ids[0]);
    assert_eq!(frame.group.as_ref(), Some(group.id()));
}
