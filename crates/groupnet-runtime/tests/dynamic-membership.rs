//! Live admitted contacts discover participants without asserting membership.

use groupnet_core::{Config, NodeId, Status};
use groupnet_network::RouterConfig;
use groupnet_runtime::Node;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::admission::{
    Admission, JoinRequest, OpenAdmission, SessionId, SessionLease, SessionRegistry,
};
use groupnet_transport::link::{AdmittedInbound, BoundLink, LinkConfig};
use groupnet_transport::{Inbound, Transport};
use std::{io, time::Duration};
use tokio::sync::{Mutex, mpsc};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct Endpoint {
    local: NodeId,
    destination: NodeId,
    outgoing: mpsc::Sender<AdmittedInbound>,
    incoming: Mutex<mpsc::Receiver<AdmittedInbound>>,
    local_sessions: SessionRegistry,
    remote_sessions: SessionRegistry,
}

impl Transport for Endpoint {
    type Error = io::Error;

    async fn send(&self, to: &NodeId, bytes: &[u8]) -> io::Result<()> {
        let expected = self
            .local_sessions
            .subscribe()
            .borrow()
            .iter()
            .find(|session| &session.node == to)
            .map(|session| session.id);
        self.send_admitted(to, bytes, expected).await
    }

    async fn send_admitted(
        &self,
        to: &NodeId,
        bytes: &[u8],
        expected: Option<SessionId>,
    ) -> io::Result<()> {
        if to != &self.destination
            || !expected.is_some_and(|id| self.local_sessions.is_active(to, id))
        {
            return Ok(());
        }
        let session = self
            .remote_sessions
            .subscribe()
            .borrow()
            .iter()
            .find(|session| session.node == self.local)
            .map(|session| session.id);
        if let Some(session) = session {
            // Capture the receiving generation at production, never at dequeue.
            let _ = self
                .outgoing
                .send(AdmittedInbound {
                    packet: Inbound {
                        from: self.local.clone(),
                        msg: bytes.to_vec(),
                    },
                    session: Some(session),
                })
                .await;
        }
        Ok(())
    }

    async fn recv(&self) -> io::Result<Inbound> {
        self.recv_admitted().await.map(|frame| frame.packet)
    }

    async fn recv_admitted(&self) -> io::Result<AdmittedInbound> {
        self.incoming
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "physical endpoint closed"))
    }
}

async fn start(endpoint: Endpoint) -> io::Result<Node> {
    let sessions = endpoint.local_sessions.clone();
    Node::builder(endpoint.local.clone())
        .routing(RouterConfig {
            announce_interval: Duration::from_millis(20),
            route_ttl: Duration::from_millis(600),
            ..RouterConfig::default()
        })
        .config(Config {
            gossip_interval_ms: 30,
            anti_entropy_interval_ms: 30,
            full_digest_every: 1,
            probe_interval_ms: 30,
            probe_timeout_ms: 20,
            suspect_timeout_ms: 60,
            dead_timeout_ms: 30_000,
            ..Config::default()
        })
        .link(BoundLink::new(endpoint, LinkConfig::new(Vec::new())).with_sessions(sessions))
        .start()
        .await
}

#[derive(Debug)]
struct Pair {
    a: NodeId,
    b: NodeId,
    node_a: Node,
    node_b: Node,
    sessions_a: SessionRegistry,
    sessions_b: SessionRegistry,
}

impl Pair {
    async fn start() -> io::Result<Self> {
        let a = NodeId::new("keyless-a");
        let b = NodeId::new("keyless-b");
        let sessions_a = SessionRegistry::new(4)?;
        let sessions_b = SessionRegistry::new(4)?;
        let (send_a, receive_a) = mpsc::channel(32);
        let (send_b, receive_b) = mpsc::channel(32);
        let node_a = start(Endpoint {
            local: a.clone(),
            destination: b.clone(),
            outgoing: send_b,
            incoming: Mutex::new(receive_a),
            local_sessions: sessions_a.clone(),
            remote_sessions: sessions_b.clone(),
        })
        .await?;
        let node_b = start(Endpoint {
            local: b.clone(),
            destination: a.clone(),
            outgoing: send_a,
            incoming: Mutex::new(receive_b),
            local_sessions: sessions_b.clone(),
            remote_sessions: sessions_a.clone(),
        })
        .await?;
        Ok(Self {
            a,
            b,
            node_a,
            node_b,
            sessions_a,
            sessions_b,
        })
    }

    async fn admit(&self) -> io::Result<[SessionLease; 2]> {
        Ok([
            self.sessions_a.try_admit(
                OpenAdmission
                    .admit(JoinRequest::new(&self.b, &[], None))
                    .await?,
            )?,
            self.sessions_b.try_admit(
                OpenAdmission
                    .admit(JoinRequest::new(&self.a, &[], None))
                    .await?,
            )?,
        ])
    }

    async fn close(&self) {
        self.node_a.close().await;
        self.node_b.close().await;
    }
}

#[tokio::test]
async fn late_admission_bootstraps_existing_and_later_joined_groups() -> io::Result<()> {
    let pair = Pair::start().await?;
    let early_a = pair.node_a.join_group("already-joined");
    let early_b = pair.node_b.join_group("already-joined");
    assert!(!early_a.members().contains(&pair.b));
    assert!(!early_b.members().contains(&pair.a));
    let leases = pair.admit().await?;
    eventually_within("seedless admitted membership", WAIT, || {
        early_a.member_status(&pair.b) == Some(Status::Alive)
            && early_b.member_status(&pair.a) == Some(Status::Alive)
            && pair
                .node_a
                .join_group("__groupnet_routing__")
                .members()
                .contains(&pair.b)
            && pair
                .node_b
                .join_group("__groupnet_routing__")
                .members()
                .contains(&pair.a)
    })
    .await;
    let later_a = pair.node_a.join_group("later-joined");
    let later_b = pair.node_b.join_group("later-joined");
    eventually_within("later group discovers actual participants", WAIT, || {
        later_a.member_status(&pair.b) == Some(Status::Alive)
            && later_b.member_status(&pair.a) == Some(Status::Alive)
    })
    .await;
    later_a.set_entry("device", b"keyless", None).unwrap();
    eventually_within("dynamic peer receives state", WAIT, || {
        later_b.node_entry(&pair.a, "device").as_deref() == Some(b"keyless")
    })
    .await;
    for lease in leases {
        lease.revoke();
    }
    assert!(pair.node_a.router().route_to(&pair.b).is_none());
    assert!(pair.node_b.router().route_to(&pair.a).is_none());
    pair.close().await;
    Ok(())
}

#[tokio::test]
async fn reachable_nodes_never_become_members_of_disjoint_groups() -> io::Result<()> {
    let pair = Pair::start().await?;
    let only_a = pair.node_a.join_group("only-a");
    let only_b = pair.node_b.join_group("only-b");
    let _leases = pair.admit().await?;
    eventually_within(
        "actual shared routing membership and repeated disjoint contact attempts",
        WAIT,
        || {
            pair.node_a
                .join_group("__groupnet_routing__")
                .members()
                .contains(&pair.b)
                && pair
                    .node_b
                    .join_group("__groupnet_routing__")
                    .members()
                    .contains(&pair.a)
                && only_a.net_stats().digest_frames_sent >= 3
                && only_b.net_stats().digest_frames_sent >= 3
        },
    )
    .await;
    assert_eq!(only_a.members(), vec![pair.a.clone()]);
    assert_eq!(only_b.members(), vec![pair.b.clone()]);
    assert_eq!(only_a.coordinator(), Some(pair.a.clone()));
    assert_eq!(only_b.coordinator(), Some(pair.b.clone()));
    let later_a = pair.node_a.join_group("later-only-a");
    eventually_within(
        "later disjoint group attempts bounded discovery",
        WAIT,
        || later_a.net_stats().digest_frames_sent >= 3,
    )
    .await;
    assert_eq!(later_a.members(), vec![pair.a.clone()]);
    assert_eq!(later_a.coordinator(), Some(pair.a.clone()));
    pair.close().await;
    Ok(())
}

#[tokio::test]
async fn seedless_existing_groups_reconnect_with_both_dead_tombstones_retained() -> io::Result<()> {
    let pair = Pair::start().await?;
    let group_a = pair.node_a.join_group("devices");
    let group_b = pair.node_b.join_group("devices");
    let leases = pair.admit().await?;
    eventually_within("initial participants exchange membership", WAIT, || {
        group_a.member_status(&pair.b) == Some(Status::Alive)
            && group_b.member_status(&pair.a) == Some(Status::Alive)
    })
    .await;
    drop(leases);
    assert!(pair.node_a.router().route_to(&pair.b).is_none());
    assert!(pair.node_b.router().route_to(&pair.a).is_none());
    eventually_within(
        "both groups retain Dead tombstones before readmission",
        WAIT,
        || {
            group_a.member_status(&pair.b) == Some(Status::Dead)
                && group_b.member_status(&pair.a) == Some(Status::Dead)
        },
    )
    .await;
    let _replacement = pair.admit().await?;
    eventually_within(
        "actual exchange refutes both retained tombstones",
        WAIT,
        || {
            group_a.member_status(&pair.b) == Some(Status::Alive)
                && group_b.member_status(&pair.a) == Some(Status::Alive)
        },
    )
    .await;
    group_a
        .set_entry("state", b"after-reconnect", None)
        .unwrap();
    eventually_within("reconnected participant receives new state", WAIT, || {
        group_b.node_entry(&pair.a, "state").as_deref() == Some(b"after-reconnect")
    })
    .await;
    pair.close().await;
    Ok(())
}
