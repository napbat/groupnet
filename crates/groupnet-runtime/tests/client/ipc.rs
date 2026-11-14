//! The client role over native local IPC: a dial-only client link, and a hub
//! link whose admission policy accepts peers it was never configured with.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use groupnet_core::NodeId;
use groupnet_runtime::{Client, Node, TunnelConfig};
use groupnet_transport::admission::OpenAdmission;
use groupnet_transport_ipc::{IpcAddress, IpcLink, PeerEndpoint};

use super::{
    WAIT, absent_from_membership, client_builder, hub_builder, messages_both_ways, ordered_stream,
    pinned_pair, routed,
};

/// One unique listener address for one test: a fresh pipe name on Windows, a
/// socket in a fresh owner-only directory on Unix.
#[derive(Debug)]
struct Address {
    ipc: IpcAddress,
    #[cfg(unix)]
    directory: std::path::PathBuf,
}

impl Address {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos());
        let suffix = format!(
            "groupnet-client-{}-{}-{nanos}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            let directory = std::env::temp_dir().join(suffix);
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&directory)
                .unwrap();
            Self {
                ipc: IpcAddress::Unix(directory.join("hub")),
                directory,
            }
        }
        #[cfg(windows)]
        {
            Self {
                ipc: IpcAddress::NamedPipe(format!(r"\\.\pipe\{suffix}")),
            }
        }
    }
}

#[cfg(unix)]
impl Drop for Address {
    fn drop(&mut self) {
        // Only the uniquely created directory is removed, never recursively.
        let _ = std::fs::remove_dir(&self.directory);
    }
}

/// A hub that listens at `address` and admits every local peer.
async fn hub(address: &Address, tunnels: Option<TunnelConfig>) -> io::Result<Node> {
    let hub = hub_builder(&NodeId::new("hub")).link(
        IpcLink::new(address.ipc.clone(), Vec::new()).with_admission(Arc::new(OpenAdmission)),
    );
    match tunnels {
        Some(tunnels) => hub.tunnels(tunnels),
        None => hub,
    }
    .start()
    .await
}

/// A client that only dials the hub at `address`.
async fn client(
    id: &NodeId,
    address: &Address,
    tunnels: Option<TunnelConfig>,
) -> io::Result<Client> {
    let client = client_builder(id).link(IpcLink::dial_only(vec![PeerEndpoint::new(
        NodeId::new("hub"),
        address.ipc.clone(),
    )]));
    match tunnels {
        Some(tunnels) => client.tunnels(tunnels),
        None => client,
    }
    .start()
    .await
}

#[tokio::test]
async fn a_hub_admits_a_dial_only_client_it_was_never_configured_with() -> io::Result<()> {
    let address = Address::new();
    let id = NodeId::new("client");
    let (hub_tunnels, client_tunnels) = pinned_pair(&NodeId::new("hub"), &id)?;
    let hub = hub(&address, Some(hub_tunnels)).await?;
    let client = client(&id, &address, Some(client_tunnels)).await?;
    routed(&hub, &client).await;
    tokio::time::timeout(WAIT, async {
        messages_both_ways(&hub, &client).await?;
        ordered_stream(&hub, &client).await
    })
    .await??;
    absent_from_membership(&hub, &["shard"], &client).await;
    client.close().await;
    hub.close().await;
    Ok(())
}

#[tokio::test]
async fn a_closed_client_releases_its_link_and_its_identity_rejoins() -> io::Result<()> {
    let address = Address::new();
    let id = NodeId::new("client");
    let hub = hub(&address, None).await?;
    let first = client(&id, &address, None).await?;
    routed(&hub, &first).await;
    tokio::time::timeout(WAIT, messages_both_ways(&hub, &first)).await??;
    first.close().await;
    assert!(first.router().is_closed());
    // The hub admits one session per identity: the same identity joins again
    // only once the closed client's session ended.
    let second = client(&id, &address, None).await?;
    routed(&hub, &second).await;
    tokio::time::timeout(WAIT, messages_both_ways(&hub, &second)).await??;
    second.close().await;
    hub.close().await;
    Ok(())
}
