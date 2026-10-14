//! Integration test: **RPC next to another stream consumer on one runtime
//! `Node`** — two nodes on a routed in-memory network with pinned TLS
//! tunnels, whose bulk plane every stream consumer of a node shares.
//!
//! The serving node runs the only accept loop on its plane and dispatches
//! each inbound stream by the tag byte its dialer wrote first: RPC streams go
//! to an `RpcServer` through the connection-level `serve`, blob streams to a
//! framed echo. Calls and blob echoes interleave on the same node pair, the
//! handler sees the tunnel-authenticated caller, and the RPC server counts
//! only the RPC connection.

use std::io;
use std::time::Duration;

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network::TunnelConfig;
use groupnet_network::tunnel::{PeerIdentity, TlsIdentity};
use groupnet_rpc::{BulkTransport, Bytes, DataPlane, DataStream, RpcClient, RpcServer, RpcStatus};
use groupnet_runtime::Node;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport_mem::{MemLink, Network};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio::task::JoinSet;

/// Bounds route discovery and every call.
const WAIT: Duration = Duration::from_secs(10);

/// The first byte of every stream on the shared plane names its consumer.
const RPC: u8 = b'r';
const BLOB: u8 = b'b';

/// One CA and `count` leaf identities it signed, each with its leaf DER.
fn identities(count: usize) -> Vec<(TlsIdentity, Vec<u8>)> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    (0..count)
        .map(|_| {
            let mut params = CertificateParams::new(vec!["groupnet.peer".to_owned()]).unwrap();
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            let key = KeyPair::generate().unwrap();
            let leaf = params.signed_by(&key, &ca).unwrap().der().to_vec();
            let identity = TlsIdentity::from_der(
                vec![ca.der().to_vec()],
                vec![leaf.clone()],
                key.serialize_der(),
            )
            .unwrap();
            (identity, leaf)
        })
        .collect()
}

/// Two nodes linked over memory, each admitting the other's tunnel identity,
/// returned once each has a route to the other.
async fn linked_nodes(a_id: &NodeId, b_id: &NodeId) -> io::Result<(Node, Node)> {
    let mut credentials = identities(2);
    let (b_identity, b_leaf) = credentials.remove(1);
    let (a_identity, a_leaf) = credentials.remove(0);
    let memory = Network::new();
    let a = Node::builder(a_id.clone())
        .link(MemLink::new(
            memory.endpoint(a_id.clone()),
            vec![b_id.clone()],
        ))
        .tunnels(TunnelConfig::new(
            a_identity,
            [PeerIdentity::new(b_id.clone(), &b_leaf)?],
        ))
        .start()
        .await?;
    let b = Node::builder(b_id.clone())
        .link(MemLink::new(
            memory.endpoint(b_id.clone()),
            vec![a_id.clone()],
        ))
        .tunnels(TunnelConfig::new(
            b_identity,
            [PeerIdentity::new(a_id.clone(), &a_leaf)?],
        ))
        .start()
        .await?;
    eventually_within("routes between the nodes", WAIT, || {
        a.router().route_to(b_id).is_some() && b.router().route_to(a_id).is_some()
    })
    .await;
    Ok((a, b))
}

/// The node's own streams, tagged for the remote dispatcher: `connect` writes
/// `tag` before handing the stream to its consumer.
struct Tagged {
    node: Node,
    tag: u8,
}

impl BulkTransport for Tagged {
    type Error = io::Error;
    type Stream = <Node as BulkTransport>::Stream;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        let mut stream = BulkTransport::connect(&self.node, to).await?;
        stream.write_all(&[self.tag]).await?;
        stream.flush().await?;
        Ok(stream)
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        BulkTransport::accept(&self.node).await
    }
}

/// The second consumer: echoes every frame back on its stream.
async fn blob_echo<S>(mut stream: DataStream<S>)
where
    S: futures_util::io::AsyncRead + futures_util::io::AsyncWrite + Unpin,
{
    while let Ok(Some(frame)) = stream.recv().await {
        if stream.send(frame).await.is_err() {
            return;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rpc_and_a_blob_consumer_share_one_node_plane() -> io::Result<()> {
    const CALLS: usize = 16;
    const BLOBS: usize = 3;

    let server_id = NodeId::new("shared-server");
    let caller_id = NodeId::new("shared-caller");
    let (server_node, caller_node) = linked_nodes(&server_id, &caller_id).await?;

    // Serving node: one accept loop on the node's plane, RPC one consumer.
    let rpc = RpcServer::new(|from: NodeId, request: Bytes| async move {
        Ok::<_, RpcStatus>(Bytes::from(format!(
            "{from}:{}",
            String::from_utf8_lossy(&request)
        )))
    });
    let dispatcher = tokio::spawn({
        let (plane, rpc) = (DataPlane::new(server_node.clone()), rpc.clone());
        async move {
            while let Ok((from, stream)) = plane.accept().await {
                let rpc = rpc.clone();
                tokio::spawn(async move {
                    let mut raw = stream.into_inner();
                    let mut tag = [0];
                    if raw.read_exact(&mut tag).await.is_err() {
                        return;
                    }
                    match tag[0] {
                        RPC => rpc
                            .serve(from, DataStream::new(raw))
                            .expect("the RPC server admits the stream"),
                        BLOB => blob_echo(DataStream::new(raw)).await,
                        other => panic!("untagged stream {other}"),
                    }
                });
            }
        }
    });

    // Calling node: an RPC client and blob streams over the same node.
    let client = RpcClient::new(DataPlane::new(Tagged {
        node: caller_node.clone(),
        tag: RPC,
    }));
    let blobs = DataPlane::new(Tagged {
        node: caller_node.clone(),
        tag: BLOB,
    });

    let mut work = JoinSet::new();
    for n in 0..BLOBS {
        let (blobs, to) = (blobs.clone(), server_id.clone());
        work.spawn(async move {
            let mut stream = blobs.connect(&to).await?;
            let blob = Bytes::from(format!("blob-{n}"));
            stream.send(blob.clone()).await?;
            assert_eq!(stream.recv().await?, Some(blob), "blob {n} echoed");
            Ok::<_, io::Error>(())
        });
    }
    for n in 0..CALLS {
        let (client, to, caller) = (client.clone(), server_id.clone(), caller_id.clone());
        work.spawn(async move {
            let reply = client
                .call(&to, Bytes::from(format!("call-{n}")), WAIT)
                .await;
            assert_eq!(
                reply,
                Ok(Bytes::from(format!("{caller}:call-{n}"))),
                "call {n} answered for the authenticated caller"
            );
            Ok(())
        });
    }
    while let Some(done) = work.join_next().await {
        done.expect("work task")?;
    }
    assert_eq!(
        rpc.connections(),
        1,
        "every call shared one RPC connection; blob streams never reached RPC"
    );

    client.shutdown();
    dispatcher.abort();
    caller_node.close().await;
    server_node.close().await;
    Ok(())
}
