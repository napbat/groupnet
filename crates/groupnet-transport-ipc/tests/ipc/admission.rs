//! Admitted IPC: peers that join at run time, dial-only clients, refusal and
//! revocation, over real native IPC and the router.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use groupnet_transport::admission::{AcceptedPeer, AdmissionError, JoinRequest};
use groupnet_transport::link::LinkFuture;
use groupnet_transport_ipc::{Admission, IpcAdmission};

use super::*;

/// Admits a claim only with the expected credential, and counts refusals.
#[derive(Debug)]
struct Token {
    expected: &'static [u8],
    refused: AtomicUsize,
}

impl Token {
    fn new(expected: &'static [u8]) -> Arc<Self> {
        Arc::new(Self {
            expected,
            refused: AtomicUsize::new(0),
        })
    }
}

impl Admission for Token {
    fn admit<'a>(
        &'a self,
        request: JoinRequest<'a>,
    ) -> LinkFuture<'a, Result<AcceptedPeer, AdmissionError>> {
        Box::pin(async move {
            request.validate()?;
            if request.credential == self.expected {
                Ok(AcceptedPeer::new(request.claimed.clone()))
            } else {
                self.refused.fetch_add(1, Ordering::SeqCst);
                Err(AdmissionError::CredentialRejected {
                    claimed: request.claimed.clone(),
                })
            }
        })
    }
}

/// A hub that admits any identity presenting `token`.
async fn hub(address: &Address, policy: Arc<Token>) -> Router {
    let router = Router::new(NodeId::new("hub")).unwrap();
    let link = IpcLink::new(address.ipc.clone(), Vec::new()).with_admission(policy);
    router
        .add_link(
            Box::new(link)
                .bind(router.local_id().clone())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    router
}

/// A client that dials the hub, accepts it by its configured identity, and
/// presents `credential`.
async fn client(name: &str, hub: &Address, credential: &[u8]) -> Router {
    let router = Router::new(NodeId::new(name)).unwrap();
    let link = IpcLink::dial_only(vec![PeerEndpoint::new(NodeId::new("hub"), hub.ipc.clone())])
        .with_credentials(credential.to_vec());
    router
        .add_link(
            Box::new(link)
                .bind(router.local_id().clone())
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    router
}

#[tokio::test]
async fn a_hub_admits_dial_only_clients_it_was_never_configured_with() {
    let address = Address::new();
    let hub = hub(&address, Token::new(b"token")).await;
    let alpha = client("alpha", &address, b"token").await;
    let beta = client("beta", &address, b"token").await;
    eventually_within("both clients routed to the hub", DEADLINE, || {
        [&alpha, &beta].iter().all(|client| {
            client.route_to(hub.local_id()).is_some() && hub.route_to(client.local_id()).is_some()
        })
    })
    .await;
    for client in [&alpha, &beta] {
        client.send(hub.local_id(), b"hello").await.unwrap();
        let received = timeout(DEADLINE, hub.recv()).await.unwrap().unwrap();
        assert_eq!(received.from, *client.local_id());
        hub.send(client.local_id(), b"welcome").await.unwrap();
        let reply = timeout(DEADLINE, client.recv()).await.unwrap().unwrap();
        assert_eq!(reply.from, *hub.local_id());
        assert_eq!(reply.msg.as_ref(), b"welcome");
    }
    for router in [alpha, beta, hub] {
        router.close().await;
    }
}

#[tokio::test]
async fn a_wrong_credential_is_refused_and_never_routed() {
    let address = Address::new();
    let policy = Token::new(b"token");
    let hub = hub(&address, Arc::clone(&policy)).await;
    let intruder = client("intruder", &address, b"guess").await;
    eventually_within("the hub refused the claim", DEADLINE, || {
        policy.refused.load(Ordering::SeqCst) > 0
    })
    .await;
    assert!(hub.route_to(intruder.local_id()).is_none());
    assert!(intruder.route_to(hub.local_id()).is_none());
    intruder.close().await;
    hub.close().await;
}

#[tokio::test]
async fn a_revoked_client_loses_its_session_and_rejoins_under_a_new_generation() {
    let address = Address::new();
    let hub = IpcTransport::bind_admitted(
        NodeId::new("hub"),
        &address.ipc,
        IpcConfig::default(),
        IpcAdmission::new(Token::new(b"token")),
    )
    .unwrap();
    let client = IpcTransport::dial_only_admitted(
        NodeId::new("client"),
        IpcConfig::default(),
        IpcAdmission {
            policy: Arc::new(groupnet_transport::admission::AllowedPeers::new(vec![
                NodeId::new("hub"),
            ])),
            credential: b"token".to_vec(),
        },
    )
    .unwrap();
    client
        .register_peer(NodeId::new("hub"), address.ipc.clone())
        .unwrap();
    let hub_sessions = hub.sessions().unwrap().clone();
    let client_sessions = client.sessions().unwrap().clone();
    let live = |registry: &groupnet_transport_ipc::SessionRegistry, node: &str| {
        registry
            .subscribe()
            .borrow()
            .iter()
            .find(|peer| peer.node.as_str() == node)
            .map(|peer| peer.id)
    };
    client.connect_peer(&NodeId::new("hub")).unwrap();
    eventually_within("client admitted on both sides", DEADLINE, || {
        live(&hub_sessions, "client").is_some() && live(&client_sessions, "hub").is_some()
    })
    .await;
    let first = live(&hub_sessions, "client").unwrap();
    hub_sessions.revoke(&NodeId::new("client"));
    // The revoked connection ends, so the client loses its session too.
    eventually_within("client session ended", DEADLINE, || {
        live(&client_sessions, "hub").is_none()
    })
    .await;
    client.connect_peer(&NodeId::new("hub")).unwrap();
    eventually_within("client admitted again", DEADLINE, || {
        live(&hub_sessions, "client").is_some_and(|id| id != first)
    })
    .await;
    client.close().await;
    hub.close().await;
}

/// Writes a join hello for `node` without a credential.
async fn join(stream: &mut RawStream, node: &str) {
    let id = node.as_bytes();
    let length = u32::try_from(id.len()).unwrap();
    stream.write_all(b"GNIJ").await.unwrap();
    stream.write_all(&length.to_be_bytes()).await.unwrap();
    stream.write_all(id).await.unwrap();
    stream.write_all(&0_u32.to_be_bytes()).await.unwrap();
}

#[tokio::test]
async fn the_default_link_policy_admits_only_configured_peers() {
    let address = Address::new();
    // The seed must stay valid while the link binds: on Unix its directory
    // exists only while its `Address` lives.
    let known_address = Address::new();
    let hub = Router::new(NodeId::new("hub")).unwrap();
    let configured = IpcLink::new(
        address.ipc.clone(),
        vec![PeerEndpoint::new(
            NodeId::new("known"),
            known_address.ipc.clone(),
        )],
    );
    hub.add_link(
        Box::new(configured)
            .bind(hub.local_id().clone())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    // The configured peer gets the hub's own hello back.
    let mut known = raw_connect(&address.ipc).await;
    join(&mut known, "known").await;
    let mut magic = [0_u8; 4];
    timeout(DEADLINE, known.read_exact(&mut magic))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&magic, b"GNIJ");
    // A peer the hub was not configured with is refused and closed.
    let mut stranger = raw_connect(&address.ipc).await;
    join(&mut stranger, "stranger").await;
    assert_closed(&mut stranger).await;
    assert!(hub.route_to(&NodeId::new("stranger")).is_none());
    hub.close().await;
}

#[tokio::test]
async fn an_admitted_listener_closes_a_static_introduction() {
    let address = Address::new();
    let transport = IpcTransport::bind_admitted(
        NodeId::new("listener"),
        &address.ipc,
        IpcConfig::default(),
        IpcAdmission::new(Arc::new(groupnet_transport::admission::OpenAdmission)),
    )
    .unwrap();
    let mut stream = raw_connect(&address.ipc).await;
    stream.write_all(b"GNI1\x03raw").await.unwrap();
    assert_closed(&mut stream).await;
    assert!(
        transport
            .sessions()
            .unwrap()
            .subscribe()
            .borrow()
            .is_empty()
    );
    transport.close().await;
}
