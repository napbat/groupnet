//! Unknown clients join a managed TCP node without a preconfigured peer list.
//!
//! Run with `cargo run -p groupnet --example dynamic-admission --features tcp-msg`.
//! Add `-- --invite` to select a custom credential policy instead of open admission.
//! The example invitation is public demo data, not a production secret. TCP here
//! is plaintext; admission alone does not provide confidentiality.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::runtime::{Group, Node};
use groupnet::transport::admission::{AcceptedPeer, Admission, JoinRequest, OpenAdmission};
use groupnet::transport::link::{LinkFuture, PeerEndpoint};
use groupnet::transport::tcp::{TcpAdmissionConfig, TcpLink, TcpMsgConfig, TcpMsgTransport};

#[derive(Debug)]
struct Invitation;

impl Admission for Invitation {
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            if request.credential != b"example-invite" {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "invalid invitation",
                ));
            }
            Ok(AcceptedPeer::new(request.claimed.clone()))
        })
    }
}

async fn client(
    id: &str,
    server: &NodeId,
    address: SocketAddr,
    credential: &[u8],
) -> io::Result<Node> {
    Node::builder(NodeId::new(id))
        .link(
            TcpLink::new(
                "127.0.0.1:0".parse().expect("literal loopback address"),
                vec![PeerEndpoint::new(server.clone(), address)],
            )
            .with_admission(Arc::new(OpenAdmission))
            .with_credentials(credential.to_vec()),
        )
        .gossip_interval_ms(50)
        .start()
        .await
}

async fn converge(groups: &[Group], ids: &[NodeId]) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if groups.iter().all(|group| {
                let members = group.members();
                ids.iter().all(|id| members.contains(id))
            }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "dynamic membership did not converge",
        )
    })
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let invitation = std::env::args().any(|argument| argument == "--invite");
    let policy: Arc<dyn Admission> = if invitation {
        Arc::new(Invitation)
    } else {
        Arc::new(OpenAdmission)
    };
    let credential: &[u8] = if invitation { b"example-invite" } else { b"" };
    let server_id = NodeId::new("game-server");
    let transport = TcpMsgTransport::bind_admitted(
        server_id.clone(),
        "127.0.0.1:0",
        TcpMsgConfig::default(),
        policy,
        Vec::new(),
        TcpAdmissionConfig::default(),
    )
    .await?;
    let address = transport.local_addr();
    let server = Node::builder(server_id.clone())
        .link(transport.into_bound_link(1))
        .gossip_interval_ms(50)
        .start()
        .await?;
    let first = client("player-a", &server_id, address, credential).await?;
    let second = client("player-b", &server_id, address, credential).await?;
    let nodes = [server, first, second];
    let groups: Vec<_> = nodes
        .iter()
        .map(|node| node.join_group("game-room"))
        .collect();
    let ids = [server_id, NodeId::new("player-a"), NodeId::new("player-b")];
    converge(&groups, &ids).await?;
    for (id, group) in ids.iter().zip(&groups) {
        println!("{} sees {:?}", id.as_str(), group.members());
    }
    println!(
        "PASS: previously unknown clients joined using {} admission",
        if invitation {
            "custom invitation"
        } else {
            "open"
        }
    );
    for node in &nodes {
        node.close().await;
    }
    Ok(())
}
