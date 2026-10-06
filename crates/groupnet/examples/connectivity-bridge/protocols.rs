//! Simultaneous application protocols over the same routed bridge.

use std::io;

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet::core::NodeId;
use groupnet::messaging::Bytes;
use groupnet::runtime::{Endpoint, Messages, Node, Ordered, Unordered};
use groupnet::streams::{UnorderedDelivery, UnorderedOptions, UnorderedSession};
use groupnet::transport::bulk::BulkTransport;

use super::WAIT;

pub(super) async fn exchange(a: &Node, c: &Node) -> io::Result<()> {
    let reliable_incoming = c.endpoint(Unordered::reliable())?;
    let unreliable_incoming = c.endpoint(Unordered::unreliable())?;
    // Bound the enclosing task's stack when driving five protocol futures together.
    Box::pin(tokio::time::timeout(WAIT, async {
        let (_reliable, _unreliable, _accepted, (), ()) = tokio::try_join!(
            unordered_client(a, c.id(), UnorderedDelivery::Reliable),
            unordered_client(a, c.id(), UnorderedDelivery::Unreliable),
            unordered_server(&reliable_incoming, &unreliable_incoming, a.id()),
            ordered(a, c),
            messages(a, c),
        )?;
        println!(
            "PASS: typed messaging, ordered bytes, reliable unordered and unreliable sessions coexist across the bridge"
        );
        Ok(())
    }))
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "protocol matrix timed out"))?
}

async fn messages(a: &Node, c: &Node) -> io::Result<()> {
    let peer = a.peer(c.id().clone(), Messages::applied(WAIT))?;
    let sending = peer.send(Bytes::from_static(b"typed application command"));
    let receiving = async {
        let (context, payload) = c.recv().await?;
        require(context.from == *a.id(), "message origin changed")?;
        require(context.group.is_none(), "message gained a group")?;
        require(
            payload == b"typed application command"[..],
            "message payload changed",
        )?;
        context.applied()?;
        c.send(a.id(), b"application response").await?;
        Ok::<_, io::Error>(())
    };
    tokio::try_join!(sending, receiving)?;
    let (context, response) = a.recv().await?;
    require(context.from == *c.id(), "response origin changed")?;
    require(
        response == b"application response"[..],
        "response payload changed",
    )
}

async fn ordered(a: &Node, c: &Node) -> io::Result<()> {
    let peer = a.peer(c.id().clone(), Ordered::new())?;
    // Ordinary Node::accept remains the ordered API, not an unordered handshake reader.
    let (mut outgoing, (origin, mut incoming)) = tokio::try_join!(peer.connect(), c.accept())?;
    require(origin == *a.id(), "ordered TLS origin changed")?;
    let payload: Vec<_> = (0_u8..=250).cycle().take(131_072).collect();
    let uploading = async {
        outgoing.write_all(&payload).await?;
        outgoing.close().await?;
        let mut response = Vec::new();
        outgoing.read_to_end(&mut response).await?;
        require(response == payload, "ordered response corrupted")
    };
    let echoing = async {
        let mut received = Vec::new();
        incoming.read_to_end(&mut received).await?;
        require(received == payload, "ordered request corrupted")?;
        incoming.write_all(&received).await?;
        incoming.close().await
    };
    tokio::try_join!(uploading, echoing)?;
    Ok(())
}

async fn unordered_client(
    node: &Node,
    destination: &NodeId,
    delivery: UnorderedDelivery,
) -> io::Result<UnorderedSession> {
    let peer = node.peer(
        destination.clone(),
        Unordered::new(UnorderedOptions {
            delivery,
            timeout: WAIT,
        }),
    )?;
    let session = peer.connect().await?;
    require(session.delivery() == delivery, "unordered policy changed")?;
    let request = packet(delivery);
    session.send(request.clone()).await?;
    session.send(Bytes::new()).await?;
    let first = session.recv().await?;
    let second = session.recv().await?;
    require(
        (first == request && second.is_empty()) || (second == request && first.is_empty()),
        "unordered message boundaries or payload changed",
    )?;
    session.send(Bytes::from_static(b"finished")).await?;
    // Keep the session alive until the receiver has observed the final message.
    Ok(session)
}

async fn unordered_server(
    reliable: &Endpoint<Unordered>,
    unreliable: &Endpoint<Unordered>,
    origin: &NodeId,
) -> io::Result<(UnorderedSession, UnorderedSession)> {
    let ((first_peer, first), (second_peer, second)) =
        tokio::try_join!(reliable.accept(), unreliable.accept())?;
    require(
        first_peer == *origin && second_peer == *origin,
        "unordered TLS origin changed",
    )?;
    require(
        first.delivery() == UnorderedDelivery::Reliable
            && second.delivery() == UnorderedDelivery::Unreliable,
        "unordered policies mixed",
    )?;
    tokio::try_join!(echo_unordered(&first), echo_unordered(&second))?;
    Ok((first, second))
}

async fn echo_unordered(session: &UnorderedSession) -> io::Result<()> {
    let first = session.recv().await?;
    let second = session.recv().await?;
    let expected = packet(session.delivery());
    require(
        (first == expected && second.is_empty()) || (second == expected && first.is_empty()),
        "unordered request boundaries or payload changed",
    )?;
    session.send(first).await?;
    session.send(second).await?;
    require(
        session.recv().await? == b"finished"[..],
        "unordered completion changed",
    )
}

fn packet(delivery: UnorderedDelivery) -> Bytes {
    Bytes::from_static(match delivery {
        UnorderedDelivery::Reliable => b"reliable unordered message",
        UnorderedDelivery::Unreliable => b"unreliable unordered message",
    })
}

fn require(condition: bool, reason: &'static str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::new(io::ErrorKind::InvalidData, reason))
    }
}
