//! A runtime client of a hub node, over local IPC.
//!
//! Run with `cargo run -p groupnet --example client --features ipc`.
//!
//! The hub is an ordinary managed `Node` whose IPC link admits any local peer
//! that presents one credential. The client is a `Client`: its link only
//! dials the hub, it joins no group and forwards nothing, and it uses the same
//! typed `Peer` handles as a node. The two exchange application messages
//! acknowledged on completion, and the client never appears in the hub's
//! membership. The credential is public demo data, not a production secret.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::runtime::{Client, Messages, Node};
use groupnet::transport::Bytes;
use groupnet::transport::admission::{AcceptedPeer, Admission, AdmissionError, JoinRequest};
use groupnet::transport::ipc::{IpcAddress, IpcLink, PeerEndpoint};
use groupnet::transport::link::LinkFuture;

const TOKEN: &[u8] = b"example-token";
const TIMEOUT: Duration = Duration::from_secs(5);

/// Admits any identity that presents [`TOKEN`].
#[derive(Debug)]
struct Token;

impl Admission for Token {
    fn admit<'a>(
        &'a self,
        request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async move {
            request.validate()?;
            if request.credential == TOKEN {
                Ok(AcceptedPeer::new(request.claimed.clone()))
            } else {
                Err(AdmissionError::CredentialRejected {
                    claimed: request.claimed.clone(),
                })
            }
        })
    }
}

/// A local-only listener address that no other run uses.
#[cfg_attr(
    windows,
    expect(
        clippy::unnecessary_wraps,
        reason = "the Unix form creates a private directory, which can fail"
    )
)]
fn address() -> io::Result<IpcAddress> {
    let suffix = format!("groupnet-client-example-{}", std::process::id());
    #[cfg(windows)]
    {
        Ok(IpcAddress::NamedPipe(format!(r"\\.\pipe\{suffix}")))
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let directory = std::env::temp_dir().join(suffix);
        std::fs::DirBuilder::new().mode(0o700).create(&directory)?;
        Ok(IpcAddress::Unix(directory.join("hub")))
    }
}

/// Polls `ready` for up to [`TIMEOUT`].
async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) -> io::Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, format!("{what} timed out")))
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let address = address()?;
    let hub_id = NodeId::new("hub");
    let hub = Node::builder(hub_id.clone())
        .link(IpcLink::new(address.clone(), Vec::new()).with_admission(Arc::new(Token)))
        .start()
        .await?;
    let cluster = hub.join_group("cluster");

    let client = Client::builder(NodeId::new("tool"))
        .link(
            IpcLink::dial_only(vec![PeerEndpoint::new(hub_id.clone(), address.clone())])
                .with_credentials(TOKEN.to_vec()),
        )
        .start()
        .await?;
    wait_until("the client route", || {
        client.router().route_to(&hub_id).is_some()
    })
    .await?;

    let to_hub = client.peer(hub_id.clone(), Messages::applied(TIMEOUT))?;
    let tool = async {
        to_hub.send(Bytes::from_static(b"status?")).await?;
        let (context, reply) = client.recv().await?;
        context.applied()?;
        Ok::<_, io::Error>(reply)
    };
    let serve = async {
        let (context, request) = hub.recv().await?;
        println!(
            "hub <- {}: {}",
            context.from,
            String::from_utf8_lossy(&request)
        );
        let reply = hub.peer(context.from.clone(), Messages::applied(TIMEOUT))?;
        context.applied()?;
        reply.send(Bytes::from_static(b"ready")).await
    };
    let (reply, served) = tokio::join!(tool, serve);
    served?;
    println!("tool <- hub: {}", String::from_utf8_lossy(&reply?));

    let members = cluster.members();
    assert_eq!(members, vec![hub_id], "the client joined no group");
    println!("hub members: {members:?}");

    client.close().await;
    hub.close().await;
    #[cfg(unix)]
    if let IpcAddress::Unix(path) = &address
        && let Some(directory) = path.parent()
    {
        let _ = std::fs::remove_dir(directory);
    }
    println!("PASS: a dial-only client talked to its hub without joining its membership");
    Ok(())
}
