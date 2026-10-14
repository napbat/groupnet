//! Integration test: **RPC next to another stream consumer on one runtime
//! `Node`**, each with its own independent accept loop — two nodes on a
//! routed in-memory network with pinned TLS tunnels.
//!
//! Every consumer binds its own ordered stream protocol: the RPC server owns
//! the accept loop of its protocol through `RpcServer::bind`, and a blob echo
//! runs a separate accept loop on another protocol of the same node. The node
//! routes each inbound stream by the protocol its connector named in the
//! authenticated setup, so no shared dispatcher or tag byte exists. Calls and
//! blob echoes interleave on the same node pair, the handler sees the
//! tunnel-authenticated caller, the RPC server counts only the RPC connection,
//! and the node's default ordered protocol never sees either consumer's
//! streams.

use std::io;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_network::TunnelConfig;
use groupnet_network::tunnel::{PeerIdentity, TlsIdentity};
use groupnet_rpc::{BulkTransport, Bytes, DataPlane, DataStream, RpcClient, RpcServer, RpcStatus};
use groupnet_runtime::{Node, Ordered, StreamProtocolId};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport_mem::{MemLink, Network};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};
use tokio::task::JoinSet;

/// Bounds route discovery and every call.
const WAIT: Duration = Duration::from_secs(10);

/// The stream protocols every node in the cluster agrees on.
const RPC: StreamProtocolId = StreamProtocolId::new(1);
const BLOB: StreamProtocolId = StreamProtocolId::new(2);

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
async fn rpc_and_a_blob_consumer_run_independent_accept_loops_on_one_node() -> io::Result<()> {
    const CALLS: usize = 16;
    const BLOBS: usize = 3;

    let server_id = NodeId::new("shared-server");
    let caller_id = NodeId::new("shared-caller");
    let (server_node, caller_node) = linked_nodes(&server_id, &caller_id).await?;

    // Serving node: RPC owns its own protocol's accept loop...
    let rpc = RpcServer::bind(
        DataPlane::new(server_node.endpoint(Ordered::protocol(RPC))?),
        |from: NodeId, request: Bytes| async move {
            Ok::<_, RpcStatus>(Bytes::from(format!(
                "{from}:{}",
                String::from_utf8_lossy(&request)
            )))
        },
    );
    // ...and the blob consumer runs an independent one beside it.
    let blob_loop = tokio::spawn({
        let plane = DataPlane::new(server_node.endpoint(Ordered::protocol(BLOB))?);
        async move {
            let mut served = 0;
            while let Ok((_, stream)) = plane.accept().await {
                served += 1;
                tokio::spawn(blob_echo(stream));
                if served == BLOBS {
                    break;
                }
            }
            served
        }
    });

    // Calling node: an RPC client and blob streams, each on its protocol.
    let client = RpcClient::new(DataPlane::new(
        caller_node.endpoint(Ordered::protocol(RPC))?,
    ));
    let blobs = DataPlane::new(caller_node.endpoint(Ordered::protocol(BLOB))?);

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
        tokio::time::timeout(WAIT, blob_loop)
            .await
            .expect("the blob loop accepted every blob stream")
            .expect("blob loop task"),
        BLOBS,
        "the blob loop accepted exactly the blob streams"
    );
    assert_eq!(
        rpc.connections(),
        1,
        "every call shared one RPC connection; blob streams never reached RPC"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            BulkTransport::accept(&server_node)
        )
        .await
        .is_err(),
        "the node's default ordered protocol received a scoped stream"
    );

    client.shutdown();
    rpc.shutdown();
    caller_node.close().await;
    server_node.close().await;
    Ok(())
}
