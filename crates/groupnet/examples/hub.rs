//! A hub that serves clients it was never configured with, over local IPC.
//!
//! Run with `cargo run -p groupnet --example hub --features ipc,rpc`.
//!
//! The hub binds an IPC data plane whose admission policy accepts one
//! credential. Two clients that only dial call the hub and subscribe to its
//! events, which arrive on the streams the clients opened. A client with the
//! wrong credential is refused before any call runs. The credential is public
//! demo data, not a production secret.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use groupnet::core::NodeId;
use groupnet::rpc::{RpcClient, RpcError, RpcServer, RpcStatus, SubscriptionSink};
use groupnet::transport::Bytes;
use groupnet::transport::admission::{AcceptedPeer, Admission, AdmissionError, JoinRequest};
use groupnet::transport::bulk::DataPlane;
use groupnet::transport::ipc::{IpcAddress, IpcBulkConfig, IpcBulkDialConfig, IpcBulkTransport};
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
    let suffix = format!("groupnet-hub-example-{}", std::process::id());
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

/// A client that dials the hub and never listens.
fn client(
    name: &str,
    hub: &IpcAddress,
    credential: &[u8],
) -> io::Result<RpcClient<IpcBulkTransport>> {
    let transport = IpcBulkTransport::dial_only(
        NodeId::new(name),
        IpcBulkDialConfig {
            credential: credential.to_vec(),
            ..IpcBulkDialConfig::default()
        },
    )?;
    transport.register_peer(NodeId::new("hub"), hub.clone())?;
    Ok(RpcClient::new(DataPlane::new(transport)))
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let address = address()?;
    let hub = NodeId::new("hub");
    let plane = DataPlane::new(IpcBulkTransport::bind_with(
        hub.clone(),
        &address,
        IpcBulkConfig {
            admission: Some(Arc::new(Token)),
            ..IpcBulkConfig::default()
        },
    )?);
    let server = RpcServer::with_subscriptions(
        // A call: greets the caller by its admitted identity.
        |from: NodeId, _request: Bytes| async move {
            Ok::<_, RpcStatus>(Bytes::from(format!("hello, {from}")))
        },
        // A subscription: three events, pushed to a client that only dials.
        |from: NodeId, _request: Bytes, mut events: SubscriptionSink| async move {
            for n in 1..=3 {
                if events
                    .send(Bytes::from(format!("event {n} for {from}")))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok::<(), RpcStatus>(())
        },
    )
    .accept_on(plane);

    for name in ["alpha", "beta"] {
        let client = client(name, &address, TOKEN)?;
        let greeting = client
            .call(&hub, Bytes::new(), TIMEOUT)
            .await
            .map_err(io::Error::other)?;
        println!("{name} <- {}", String::from_utf8_lossy(&greeting));
        let mut events = client
            .subscribe(&hub, Bytes::new(), TIMEOUT)
            .await
            .map_err(io::Error::other)?;
        let mut received = 0;
        while let Some(event) = events.next().await {
            let event = event.map_err(io::Error::other)?;
            println!("{name} <- {}", String::from_utf8_lossy(&event));
            received += 1;
        }
        assert_eq!(received, 3, "every event reached {name}");
        client.shutdown();
    }

    let intruder = client("intruder", &address, b"guess")?;
    let refused = intruder.call(&hub, Bytes::new(), TIMEOUT).await;
    assert_eq!(refused, Err(RpcError::Unreachable));
    println!("intruder refused: {}", refused.unwrap_err());
    intruder.shutdown();

    server.shutdown();
    #[cfg(unix)]
    if let IpcAddress::Unix(path) = &address
        && let Some(directory) = path.parent()
    {
        let _ = std::fs::remove_dir(directory);
    }
    println!(
        "PASS: dial-only clients joined the hub at run time, called it and received pushed events"
    );
    Ok(())
}
