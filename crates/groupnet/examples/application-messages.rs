//! Opaque application frames over memory A -> TCP edge B -> TCP C.
//!
//! Run `cargo run -p groupnet --example application-messages --features tcp-msg,connectivity`.
//! Add `-- --relay-only` to keep native TCP connectivity relayed.
//! This example uses trusted loopback peers, not end-to-end encrypted messages.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use groupnet::{
    connectivity::{PathPolicy, TcpPunchConfig, TcpRendezvous},
    core::NodeId,
    messaging::{Bytes, Delivery, Frame, MessageContext, SendOptions},
    runtime::Node,
    transport::{
        mem::{MemLink, Network},
        tcp::TcpMsgTransport,
    },
};

const WAIT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> io::Result<()> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let policy = match arguments.as_slice() {
        [] => PathPolicy::DirectPreferred,
        [argument] if argument == "--relay-only" => PathPolicy::RelayOnly,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "expected --relay-only",
            ));
        }
    };
    let relay = TcpRendezvous::bind_open("127.0.0.1:0".parse().map_err(io::Error::other)?).await?;
    let address = relay.local_addr()?;
    let b_tcp = TcpMsgTransport::bind_connectivity(config("b", address, policy)).await?;
    let c_tcp = TcpMsgTransport::bind_connectivity(config("c", address, policy)).await?;
    let memory = Network::new();
    let a = Node::builder(NodeId::new("a"))
        .link(MemLink::new(
            memory.endpoint(NodeId::new("a")),
            vec![NodeId::new("b")],
        ))
        .gossip_interval_ms(30)
        .start()
        .await?;
    let b = Node::builder(NodeId::new("b"))
        .link(MemLink::new(
            memory.endpoint(NodeId::new("b")),
            vec![NodeId::new("a")],
        ))
        .link(b_tcp.into_bound_link(1))
        .gossip_interval_ms(30)
        .start()
        .await?;
    let c = Node::builder(NodeId::new("c"))
        .link(c_tcp.into_bound_link(1))
        .gossip_interval_ms(30)
        .start()
        .await?;
    let result = tokio::time::timeout(WAIT * 4, demonstrate(&a, &b, &c))
        .await
        .map_err(io::Error::other)?;
    for node in [&a, &b, &c] {
        node.close().await;
    }
    relay.close().await;
    result
}

fn config(local: &str, address: SocketAddr, policy: PathPolicy) -> TcpPunchConfig {
    let mut config = TcpPunchConfig::open(NodeId::new(local), address);
    config.bind = "127.0.0.1:0".parse().expect("literal address");
    config.candidate_binds.push(config.bind);
    config.gather_interfaces = false;
    config.policy = policy;
    config
}

async fn settle(mut ready: impl FnMut() -> bool) -> io::Result<()> {
    tokio::time::timeout(WAIT, async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(io::Error::other)
}

fn validate(frame: &Frame, expected_group: Option<&str>, expected: &[u8]) -> io::Result<()> {
    if frame.from != NodeId::new("a")
        || frame.group.as_ref().map(groupnet::core::GroupId::as_str) != expected_group
        || frame.payload.as_ref() != expected
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sender, group or frame changed",
        ));
    }
    Ok(())
}

fn validate_context(
    context: &MessageContext,
    payload: &Bytes,
    expected_group: Option<&str>,
    expected: &[u8],
    delivery: Delivery,
) -> io::Result<()> {
    if context.from != NodeId::new("a")
        || context.group.as_ref().map(groupnet::core::GroupId::as_str) != expected_group
        || payload.as_ref() != expected
        || context.delivery() != delivery
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "origin, group, payload or receipt delivery changed",
        ));
    }
    Ok(())
}

async fn demonstrate(a: &Node, b: &Node, c: &Node) -> io::Result<()> {
    let groups = [
        a.join_group("updates"),
        b.join_group("updates"),
        c.join_group("updates"),
    ];
    settle(|| {
        groups.iter().all(|group| {
            [a.id(), b.id(), c.id()]
                .iter()
                .all(|id| group.members().contains(id))
        }) && a
            .router()
            .route_to(c.id())
            .is_some_and(|route| route.path == [a.id().clone(), b.id().clone(), c.id().clone()])
            && c.router().route_to(a.id()).is_some()
    })
    .await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut callbacks = Vec::new();
    for group in &groups[1..] {
        let calls = calls.clone();
        callbacks.push(group.on_frame(move |frame| {
            let calls = calls.clone();
            async move {
                validate(&frame, Some("updates"), b"invalidate:object-42")?;
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(()) // Acknowledges Applied only after successful processing.
            }
        })?);
    }
    let options = SendOptions {
        delivery: Delivery::Applied,
        timeout: WAIT,
    };
    let report = groups[0]
        .send_frame(Bytes::from_static(b"invalidate:object-42"), options)
        .await?;
    let recipients: Vec<_> = report
        .outcomes
        .iter()
        .map(|outcome| outcome.node.clone())
        .collect();
    if report.outcomes.len() != 2 || !recipients.contains(b.id()) || !recipients.contains(c.id()) {
        return Err(io::Error::other(
            "group did not resolve both remote recipients",
        ));
    }
    for outcome in report.outcomes {
        outcome.result?;
    }
    if calls.load(Ordering::SeqCst) != 2 {
        return Err(io::Error::other("callbacks did not apply once"));
    }
    println!("PASS: group.send_frame resolved B and C; both callbacks applied exactly once");
    node_frames(a, c).await?;
    endpoints_only(a, c).await?;
    groups[0].sync(|metadata| metadata.update_metadata("schema", "2"));
    settle(|| groups[2].metadata("schema").as_deref() == Some("2")).await?;
    println!("PASS: metadata converges independently while application callbacks are registered");
    for callback in callbacks {
        callback.close().await?;
    }
    let receive = async {
        for group in &groups[1..] {
            let (context, payload) = group.recv().await?;
            validate_context(
                &context,
                &payload,
                Some("updates"),
                b"manual group message",
                Delivery::Applied,
            )?;
            drop(payload);
            context.applied()?; // The receipt survives moving/dropping the payload.
        }
        Ok::<_, io::Error>(())
    };
    let (report, ()) = tokio::try_join!(
        groups[0].send_frame(Bytes::from_static(b"manual group message"), options),
        receive,
    )?;
    for outcome in report.outcomes {
        outcome.result?;
    }
    println!("PASS: manual group buffers retain A, group and Applied receipts after payload drop");
    Ok(())
}

async fn node_frames(a: &Node, c: &Node) -> io::Result<()> {
    let options = SendOptions {
        delivery: Delivery::Delivered,
        timeout: WAIT,
    };
    a.send_frame(
        c.id(),
        Bytes::from_static(b"node frame\0with boundary"),
        options,
    )
    .await?;
    let frame = tokio::time::timeout(WAIT, c.recv_frame())
        .await
        .map_err(io::Error::other)??;
    validate(&frame, None, b"node frame\0with boundary")?;
    let (context, payload) = frame.into_parts();
    validate_context(
        &context,
        &payload,
        None,
        b"node frame\0with boundary",
        Delivery::Delivered,
    )?;
    let handler = c.on_frame(|frame| async move { validate(&frame, None, b"node callback") })?;
    a.send_frame(
        c.id(),
        Bytes::from_static(b"node callback"),
        SendOptions {
            delivery: Delivery::Applied,
            timeout: WAIT,
        },
    )
    .await?;
    handler.close().await?;
    a.send(c.id(), b"node buffer").await?;
    let (context, payload) = tokio::time::timeout(WAIT, c.recv())
        .await
        .map_err(io::Error::other)??;
    validate_context(
        &context,
        &payload,
        None,
        b"node buffer",
        Delivery::BestEffort,
    )?;
    let receive = async {
        let (context, payload) = c.recv().await?;
        validate_context(
            &context,
            &payload,
            None,
            b"manual applied buffer",
            Delivery::Applied,
        )?;
        drop(payload);
        context.applied()
    };
    tokio::try_join!(
        a.send_frame(
            c.id(),
            Bytes::from_static(b"manual applied buffer"),
            SendOptions {
                delivery: Delivery::Applied,
                timeout: WAIT,
            },
        ),
        receive,
    )?;
    let handler = c.on_recv(|context, payload| async move {
        validate_context(
            &context,
            &payload,
            None,
            b"buffer callback",
            Delivery::Applied,
        )?;
        drop(payload);
        drop(context);
        Ok(()) // The callback worker retains its own receipt for the success ACK.
    })?;
    a.send_frame(
        c.id(),
        Bytes::from_static(b"buffer callback"),
        SendOptions {
            delivery: Delivery::Applied,
            timeout: WAIT,
        },
    )
    .await?;
    handler.close().await?;
    println!("PASS: node buffers retain A and receipts; manual/callback Applied ACKs reach A");
    Ok(())
}

async fn endpoints_only(a: &Node, c: &Node) -> io::Result<()> {
    let left = a.join_group("endpoints");
    let right = c.join_group("endpoints");
    settle(|| {
        [&left, &right]
            .iter()
            .all(|group| group.members().contains(a.id()) && group.members().contains(c.id()))
    })
    .await?;
    let callback = right.on_recv(|context, payload| async move {
        validate_context(
            &context,
            &payload,
            Some("endpoints"),
            b"bridge is not a group member",
            Delivery::Applied,
        )?;
        drop(payload);
        drop(context);
        Ok(())
    })?;
    let report = left
        .send_frame(
            Bytes::from_static(b"bridge is not a group member"),
            SendOptions {
                delivery: Delivery::Applied,
                timeout: WAIT,
            },
        )
        .await?;
    if report.outcomes.len() != 1 || report.outcomes[0].node != *c.id() {
        return Err(io::Error::other("nonmember bridge became a recipient"));
    }
    for outcome in report.outcomes {
        outcome.result?;
    }
    callback.close().await?;
    println!(
        "PASS: a nonmember bridge forwards group traffic without receiving or acknowledging it"
    );
    Ok(())
}
