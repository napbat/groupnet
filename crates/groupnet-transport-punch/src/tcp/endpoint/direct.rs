//! Mutual fresh session-bound proofs, simultaneous open, and ranked streams.
use super::{
    DEADLINE, Event, IDLE, ProofPeer, Proofs, Rank, closed, invalid, lock, random, sockets,
    wire::{self, Auth, Duplex, Message, Token},
};
use groupnet_core::NodeId;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

fn hello(secret: &Token, node: &NodeId, session: &Token, target: &Token, nonce: &Token) -> Token {
    wire::proof(
        secret,
        b"hello",
        &[session, target, nonce, node.as_str().as_bytes()],
    )
}

fn response(
    secret: &Token,
    domain: &[u8],
    session: &Token,
    target: &Token,
    nonce: &Token,
    answer: &Token,
) -> Token {
    wire::proof(secret, domain, &[session, target, nonce, answer])
}

fn data_key(secret: &Token, one: &Token, two: &Token, local: &NodeId, remote: &NodeId) -> Duplex {
    let (first, second) = if one < two { (one, two) } else { (two, one) };
    let low_to_high = Some(wire::keyed(&wire::proof(
        secret,
        b"stream low to high",
        &[first, second],
    )));
    let high_to_low = Some(wire::keyed(&wire::proof(
        secret,
        b"stream high to low",
        &[first, second],
    )));
    if local < remote {
        Duplex {
            tx: low_to_high,
            rx: high_to_low,
        }
    } else {
        Duplex {
            tx: high_to_low,
            rx: low_to_high,
        }
    }
}

fn check_hello(
    message: Message,
    local_session: Token,
    remote_node: &NodeId,
    peer: &ProofPeer,
) -> io::Result<Token> {
    let Message::Hello {
        node,
        session,
        target,
        nonce,
        proof,
    } = message
    else {
        return Err(invalid("expected TCP direct hello"));
    };
    if node != *remote_node
        || session != peer.session
        || target != local_session
        || !wire::matches(
            &hello(&peer.secret, &node, &session, &target, &nonce),
            &proof,
        )
    {
        return Err(invalid("TCP direct introduction proof rejected"));
    }
    Ok(nonce)
}

fn check_answer(
    message: &Message,
    secret: &Token,
    session: &Token,
    target: &Token,
    nonce: &Token,
) -> io::Result<Token> {
    let Message::Answer {
        nonce: answer,
        proof,
    } = message
    else {
        return Err(invalid("expected TCP direct answer"));
    };
    if !wire::matches(
        &response(secret, b"answer", session, target, nonce, answer),
        proof,
    ) {
        return Err(invalid("TCP direct answer proof rejected"));
    }
    Ok(*answer)
}

async fn finish(
    stream: &mut TcpStream,
    peer: &ProofPeer,
    session: Token,
    nonce: Token,
    answer: Token,
    send_first: bool,
) -> io::Result<()> {
    let own = response(
        &peer.secret,
        b"finish",
        &session,
        &peer.session,
        &nonce,
        &answer,
    );
    let expected = response(
        &peer.secret,
        b"finish",
        &peer.session,
        &session,
        &answer,
        &nonce,
    );
    if send_first {
        wire::write(stream, &None, &Message::Finish(own)).await?;
    }
    let Message::Finish(proof) = wire::read(stream, &None).await? else {
        return Err(invalid("expected TCP direct finish"));
    };
    if !wire::matches(&expected, &proof) {
        return Err(invalid("TCP direct finish proof rejected"));
    }
    if !send_first {
        wire::write(stream, &None, &Message::Finish(own)).await?;
    }
    Ok(())
}

pub(super) async fn dial(
    local: NodeId,
    session: Token,
    node: NodeId,
    peer: ProofPeer,
    source: SocketAddr,
    target: SocketAddr,
) -> io::Result<Event> {
    let stream = sockets::dial(source, target).await?;
    dial_stream(local, session, node, peer, stream).await
}

async fn dial_stream(
    local: NodeId,
    session: Token,
    node: NodeId,
    peer: ProofPeer,
    mut stream: TcpStream,
) -> io::Result<Event> {
    let nonce = random()?;
    wire::write(
        &mut stream,
        &None,
        &Message::Hello {
            node: local.clone(),
            session,
            target: peer.session,
            nonce,
            proof: hello(&peer.secret, &local, &session, &peer.session, &nonce),
        },
    )
    .await?;
    let first = wire::read(&mut stream, &None).await?;
    let (answer, rank) = if matches!(first, Message::Hello { .. }) {
        // A genuine kernel simultaneous-open joins two outbound sockets. Both
        // ends sent Hello; neither is misclassified as a passive responder.
        let answer = check_hello(first, session, &node, &peer)?;
        wire::write(
            &mut stream,
            &None,
            &Message::Answer {
                nonce,
                proof: response(
                    &peer.secret,
                    b"answer",
                    &peer.session,
                    &session,
                    &answer,
                    &nonce,
                ),
            },
        )
        .await?;
        let verified = check_answer(
            &wire::read(&mut stream, &None).await?,
            &peer.secret,
            &session,
            &peer.session,
            &nonce,
        )?;
        if verified != answer {
            return Err(invalid("TCP simultaneous-open nonce mismatch"));
        }
        (
            answer,
            Rank {
                direction: 0,
                nonce: if local < node { nonce } else { answer },
            },
        )
    } else {
        let answer = check_answer(&first, &peer.secret, &session, &peer.session, &nonce)?;
        (
            answer,
            Rank {
                direction: u8::from(local > node),
                nonce,
            },
        )
    };
    finish(&mut stream, &peer, session, nonce, answer, true).await?;
    let auth = data_key(&peer.secret, &nonce, &answer, &local, &node);
    Ok(Event::Ready {
        node,
        session: peer.session,
        rank,
        auth,
        stream,
    })
}

pub(super) async fn accept(
    mut stream: TcpStream,
    local: NodeId,
    session: Token,
    proofs: Proofs,
) -> io::Result<Event> {
    let message = wire::read(&mut stream, &None).await?;
    let Message::Hello { ref node, .. } = message else {
        return Err(invalid("expected TCP direct hello"));
    };
    let node = node.clone();
    let peer = lock(&proofs)
        .get(&node)
        .cloned()
        .ok_or_else(|| invalid("unknown TCP direct introduction"))?;
    let nonce = check_hello(message, session, &node, &peer)?;
    let answer = random()?;
    wire::write(
        &mut stream,
        &None,
        &Message::Answer {
            nonce: answer,
            proof: response(
                &peer.secret,
                b"answer",
                &peer.session,
                &session,
                &nonce,
                &answer,
            ),
        },
    )
    .await?;
    finish(&mut stream, &peer, session, answer, nonce, false).await?;
    let auth = data_key(&peer.secret, &nonce, &answer, &local, &node);
    Ok(Event::Ready {
        rank: Rank {
            direction: u8::from(node > local),
            nonce,
        },
        node,
        session: peer.session,
        auth,
        stream,
    })
}

pub(super) async fn accept_loop(
    listener: TcpListener,
    local: NodeId,
    session: Token,
    proofs: Proofs,
    permits: Arc<Semaphore>,
    events: mpsc::Sender<Event>,
    cancel: CancellationToken,
) {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            Ok((stream, _)) = listener.accept() => {
                let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                let local = local.clone(); let proofs = proofs.clone(); let events = events.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    if let Ok(Ok(event)) = tokio::time::timeout(DEADLINE, accept(stream, local, session, proofs)).await { let _ = events.send(event).await; }
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

pub(super) async fn connection(
    stream: TcpStream,
    auth: Duplex,
    mut outgoing: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
    identity: (NodeId, Token, Rank),
    cancel: CancellationToken,
) {
    let (node, session, rank) = identity;
    let (mut reader, mut writer) = stream.into_split();
    let reading = async {
        loop {
            match tokio::time::timeout(IDLE, wire::read(&mut reader, &auth.rx))
                .await
                .map_err(|_| closed())??
            {
                Message::Data(data) => events
                    .send(Event::Data {
                        node: node.clone(),
                        session,
                        rank,
                        data,
                    })
                    .await
                    .map_err(|_| closed())?,
                Message::Ping => {}
                _ => return Err::<(), io::Error>(invalid("invalid established TCP direct frame")),
            }
        }
    };
    let writing = write_direct(&mut writer, &auth.tx, &mut outgoing);
    tokio::select! { () = cancel.cancelled() => {}, _ = reading => {}, _ = writing => {} }
    let _ = events
        .send(Event::Closed {
            node,
            session,
            rank,
        })
        .await;
}

async fn write_direct(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    auth: &Auth,
    outgoing: &mut mpsc::Receiver<Message>,
) -> io::Result<()> {
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let message = tokio::select! {
            message = outgoing.recv() => message.ok_or_else(closed)?,
            _ = heartbeat.tick() => Message::Ping,
        };
        tokio::time::timeout(DEADLINE, wire::write(writer, auth, &message))
            .await
            .map_err(|_| closed())??;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocking_connect(
        socket: socket2::Socket,
        target: SocketAddr,
        barrier: Arc<std::sync::Barrier>,
    ) -> tokio::task::JoinHandle<io::Result<std::net::TcpStream>> {
        tokio::task::spawn_blocking(move || {
            barrier.wait();
            socket.connect_timeout(&target.into(), DEADLINE)?;
            socket.set_nonblocking(true)?;
            Ok(socket.into())
        })
    }

    #[tokio::test]
    async fn simultaneous_open_retains_source_ports_and_mutual_proofs() {
        // Separate OS threads cross a barrier with both sockets already bound;
        // this must not serialize two loopback SYNs on one executor thread.
        let source_socket_a = sockets::bound_socket("127.0.0.1:0".parse().unwrap()).unwrap();
        let source_socket_b = sockets::bound_socket("127.0.0.1:0".parse().unwrap()).unwrap();
        let source_a = source_socket_a.local_addr().unwrap().as_socket().unwrap();
        let source_b = source_socket_b.local_addr().unwrap().as_socket().unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let a = blocking_connect(source_socket_a, source_b, barrier.clone());
        let b = blocking_connect(source_socket_b, source_a, barrier);
        let (a, b) = tokio::join!(a, b);
        let a = TcpStream::from_std(a.unwrap().unwrap()).unwrap();
        let b = TcpStream::from_std(b.unwrap().unwrap()).unwrap();
        let secret = random().unwrap();
        let sa = random().unwrap();
        let sb = random().unwrap();
        let a = dial_stream(
            NodeId::from("a"),
            sa,
            NodeId::from("b"),
            ProofPeer {
                session: sb,
                secret,
            },
            a,
        );
        let b = dial_stream(
            NodeId::from("b"),
            sb,
            NodeId::from("a"),
            ProofPeer {
                session: sa,
                secret,
            },
            b,
        );
        let (a, b) = tokio::time::timeout(DEADLINE, async { tokio::join!(a, b) })
            .await
            .unwrap();
        let Event::Ready {
            stream: mut a,
            auth: key_a,
            rank: rank_a,
            ..
        } = a.unwrap()
        else {
            panic!("missing direct a");
        };
        let Event::Ready {
            stream: mut b,
            auth: key_b,
            rank: rank_b,
            ..
        } = b.unwrap()
        else {
            panic!("missing direct b");
        };
        assert_eq!(a.local_addr().unwrap(), source_a);
        assert_eq!(b.local_addr().unwrap(), source_b);
        assert!(rank_a == rank_b);
        wire::write(&mut a, &key_a.tx, &Message::Data(b"active open".to_vec()))
            .await
            .unwrap();
        let Message::Data(data) = wire::read(&mut b, &key_b.rx).await.unwrap() else {
            panic!("missing payload");
        };
        assert_eq!(data, b"active open");
    }

    #[test]
    fn public_sessions_and_replayed_hellos_cannot_authorize_replacement_sessions() {
        let node = NodeId::from("peer");
        let peer = ProofPeer {
            session: [2; 32],
            secret: [3; 32],
        };
        let old_local = [4; 32];
        let nonce = [5; 32];
        let old_proof = hello(&peer.secret, &node, &peer.session, &old_local, &nonce);
        let replay = Message::Hello {
            node: node.clone(),
            session: peer.session,
            target: old_local,
            nonce,
            proof: old_proof,
        };
        assert!(check_hello(replay, [6; 32], &node, &peer).is_err());
        let forged = Message::Hello {
            node: node.clone(),
            session: peer.session,
            target: old_local,
            nonce,
            proof: hello(&[0; 32], &node, &peer.session, &old_local, &nonce),
        };
        assert!(check_hello(forged, old_local, &node, &peer).is_err());
        let replacement = ProofPeer {
            session: peer.session,
            secret: [7; 32],
        };
        let replay = Message::Hello {
            node: node.clone(),
            session: peer.session,
            target: old_local,
            nonce,
            proof: old_proof,
        };
        assert!(check_hello(replay, old_local, &node, &replacement).is_err());
    }

    #[tokio::test]
    async fn recorded_finish_does_not_prove_a_fresh_passive_challenge() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let local = NodeId::from("a");
        let remote = NodeId::from("b");
        let session = [1; 32];
        let peer = ProofPeer {
            session: [2; 32],
            secret: [3; 32],
        };
        let proofs = Arc::new(std::sync::Mutex::new(std::collections::HashMap::from([(
            remote.clone(),
            peer.clone(),
        )])));
        let remote_nonce = [4; 32];
        let connecting = TcpStream::connect(address);
        let accepting = listener.accept();
        let (client, accepted) = tokio::join!(connecting, accepting);
        let mut client = client.unwrap();
        let (server, _) = accepted.unwrap();
        let worker = tokio::spawn(accept(server, local, session, proofs));
        wire::write(
            &mut client,
            &None,
            &Message::Hello {
                node: remote.clone(),
                session: peer.session,
                target: session,
                nonce: remote_nonce,
                proof: hello(
                    &peer.secret,
                    &remote,
                    &peer.session,
                    &session,
                    &remote_nonce,
                ),
            },
        )
        .await
        .unwrap();
        let Message::Answer { .. } = wire::read(&mut client, &None).await.unwrap() else {
            panic!("expected answer");
        };
        let stale_finish = response(
            &peer.secret,
            b"finish",
            &peer.session,
            &session,
            &remote_nonce,
            &[5; 32],
        );
        wire::write(&mut client, &None, &Message::Finish(stale_finish))
            .await
            .unwrap();
        assert!(worker.await.unwrap().is_err());
    }
}

#[cfg(test)]
mod directional_tests {
    use super::*;

    #[tokio::test]
    async fn actual_direct_data_bytes_cannot_be_reflected_or_replayed() {
        let local = NodeId::from("a");
        let remote = NodeId::from("b");
        let sender = data_key(&[1; 32], &[2; 32], &[3; 32], &local, &remote);
        let recipient = data_key(&[1; 32], &[3; 32], &[2; 32], &remote, &local);
        wire::directional_tests::rejects_reflection_and_replay(
            &sender,
            &recipient,
            &Message::Data(b"cannot reflect as authenticated peer data".to_vec()),
        )
        .await;
    }
}
