//! Adapter regressions for native session ownership and raw-mode isolation.

use std::net::SocketAddr;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::Transport;
use groupnet_transport::admission::{SessionId, SessionRegistry};
use groupnet_transport_punch::{PunchConfig, Rendezvous};

use super::UdpTransport;
use super::framing::SendBuffer;

async fn admitted(registry: &SessionRegistry, peer: &NodeId) -> SessionId {
    let mut neighbors = registry.subscribe();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(session) = neighbors.borrow().iter().find(|entry| &entry.node == peer) {
                return session.id;
            }
            neighbors.changed().await.expect("session update");
        }
    })
    .await
    .expect("peer admission")
}

async fn connect(id: &NodeId, rendezvous: SocketAddr) -> UdpTransport {
    let mut config = PunchConfig::open(id.clone(), rendezvous);
    config.bind = "127.0.0.1:0".parse().expect("bind address");
    config.gather_interfaces = false;
    UdpTransport::bind_connectivity(config)
        .await
        .expect("connected endpoint")
}

#[tokio::test]
async fn managed_connection_preserves_socket_sessions_and_tagged_io() {
    let server = Rendezvous::bind_open("127.0.0.1:0".parse().expect("server address"))
        .await
        .expect("rendezvous");
    let receiver_id = NodeId::new("connected-receiver");
    let sender_id = NodeId::new("connected-sender");
    let receiver = connect(&receiver_id, server.local_addr().expect("server address")).await;
    let receiver_addr = receiver.local_addr().expect("actual socket address");
    let observer = receiver.clone();
    let receiver_sessions = observer.sessions().expect("native sessions");
    let bound = receiver.into_bound_link(7);
    assert!(std::net::UdpSocket::bind(receiver_addr).is_err());

    let sender = connect(&sender_id, server.local_addr().expect("server address")).await;
    let receiver_generation = admitted(&receiver_sessions, &sender_id).await;
    assert!(
        bound
            .sessions
            .as_ref()
            .expect("managed sessions")
            .is_active(&sender_id, receiver_generation)
    );
    let sender_generation =
        admitted(&sender.sessions().expect("sender sessions"), &receiver_id).await;

    // Missing generations must not become untagged native writes.
    assert_eq!(
        sender
            .send_admitted(&receiver_id, b"untagged", None)
            .await
            .expect_err("missing generation")
            .kind(),
        std::io::ErrorKind::NotConnected
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), observer.recv_admitted())
            .await
            .is_err()
    );
    sender
        .send_owned_admitted(
            &receiver_id,
            bytes::Bytes::from_static(b"native payload"),
            Some(sender_generation),
        )
        .await
        .expect("admitted send");
    let inbound = tokio::time::timeout(Duration::from_secs(2), observer.recv_admitted())
        .await
        .expect("receive deadline")
        .expect("receive");
    assert_eq!(inbound.packet.from, sender_id);
    assert_eq!(inbound.packet.msg.as_ref(), b"native payload");
    assert_eq!(inbound.session, Some(receiver_generation));

    bound.driver.close().await;
    assert!(!receiver_sessions.is_active(&sender_id, receiver_generation));
    assert_eq!(
        observer.local_addr().expect_err("closed").kind(),
        std::io::ErrorKind::NotConnected
    );
    let _replacement = std::net::UdpSocket::bind(receiver_addr).expect("actual socket released");
    sender.into_bound_link(1).driver.close().await;
    server.close().await;
}

#[tokio::test]
async fn connected_mode_ignores_address_hints_and_raw_self_attribution() {
    let server = Rendezvous::bind_open("127.0.0.1:0".parse().expect("server address"))
        .await
        .expect("rendezvous");
    let receiver_id = NodeId::new("fenced-receiver");
    let attacker_id = NodeId::new("unadmitted");
    let receiver = connect(&receiver_id, server.local_addr().expect("server address")).await;
    let raw = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("raw socket");
    let hint = raw.local_addr().expect("hint address");
    receiver.register_peer(attacker_id.clone(), hint);
    receiver.learn_peer(&attacker_id, &hint.to_string());
    assert!(!receiver.known_peers().contains(&attacker_id));
    assert!(receiver.direct_addr_to(&attacker_id).is_none());
    assert!(receiver.path_to(&attacker_id).is_none());
    let mut buffer = SendBuffer::new(&attacker_id).expect("raw sender prefix");
    raw.send_to(
        buffer.frame(b"forged raw payload").expect("raw payload"),
        receiver.local_addr().expect("socket"),
    )
    .await
    .expect("raw datagram");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), receiver.recv_admitted())
            .await
            .is_err()
    );
    assert!(
        receiver
            .sessions()
            .expect("sessions")
            .subscribe()
            .borrow()
            .is_empty()
    );
    receiver.into_bound_link(1).driver.close().await;
    server.close().await;
}
