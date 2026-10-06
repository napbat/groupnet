//! Shared real-router fixtures for the tunnel integration suite.

use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::network::{
    Router, RouterConfig,
    tunnel::{PeerIdentity, TlsIdentity, TunnelTransport},
};
use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::link::LinkConfig;
use groupnet_transport::{Inbound, Transport};
use groupnet_transport_mem::{MemTransport, Network};
use groupnet_transport_tcp::TcpMsgTransport;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

#[derive(Debug)]
pub(super) struct Credentials {
    pub(super) identity: TlsIdentity,
    pub(super) leaf: Vec<u8>,
}

pub(super) fn credentials() -> (Credentials, Credentials, Credentials) {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    let make = || {
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
        Credentials { identity, leaf }
    };
    (make(), make(), make())
}

#[derive(Debug, Default)]
pub(super) struct Faults {
    pub(super) dropped: AtomicUsize,
    pub(super) reordered: AtomicUsize,
    pub(super) duplicated: AtomicUsize,
}

#[derive(Debug)]
struct Controlled {
    inner: MemTransport,
    sequence: AtomicUsize,
    held: Mutex<Option<(NodeId, Vec<u8>)>>,
    faults: Arc<Faults>,
}

impl Transport for Controlled {
    type Error = io::Error;

    async fn send(&self, to: &NodeId, msg: &[u8]) -> io::Result<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        if sequence.is_multiple_of(11) {
            self.faults.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let held = {
            let mut held = self
                .held
                .lock()
                .map_err(|_| io::Error::other("controlled adapter lock poisoned"))?;
            if sequence.is_multiple_of(5) && held.is_none() {
                *held = Some((to.clone(), msg.to_vec()));
                self.faults.reordered.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            held.take()
        };
        self.inner.send(to, msg).await.map_err(io::Error::other)?;
        if sequence.is_multiple_of(7) {
            self.inner.send(to, msg).await.map_err(io::Error::other)?;
            self.faults.duplicated.fetch_add(1, Ordering::Relaxed);
        }
        if let Some((node, bytes)) = held {
            self.inner
                .send(&node, &bytes)
                .await
                .map_err(io::Error::other)?;
        }
        Ok(())
    }

    async fn recv(&self) -> io::Result<Inbound> {
        self.inner.recv().await.map_err(io::Error::other)
    }
}

#[derive(Debug)]
pub(super) struct Fabric {
    pub(super) a: Router,
    pub(super) bridge: Router,
    pub(super) c: Router,
    pub(super) faults: Arc<Faults>,
}

impl Fabric {
    pub(super) async fn new(lossy: bool, tcp_last_hop: bool) -> Self {
        let a = NodeId::new("a");
        let b = NodeId::new("bridge");
        let c = NodeId::new("c");
        let config = RouterConfig {
            announce_interval: Duration::from_millis(100),
            ..RouterConfig::default()
        };
        let ar = Router::new(a.clone(), config.clone()).unwrap();
        let br = Router::new(b.clone(), config.clone()).unwrap();
        let cr = Router::new(c.clone(), config).unwrap();
        let first = Network::new();
        let faults = Arc::new(Faults::default());
        if lossy {
            ar.add_transport(
                Controlled {
                    inner: first.endpoint(a.clone()),
                    sequence: AtomicUsize::new(0),
                    held: Mutex::new(None),
                    faults: faults.clone(),
                },
                LinkConfig::new(vec![b.clone()]),
            )
            .unwrap();
            br.add_transport(
                Controlled {
                    inner: first.endpoint(b.clone()),
                    sequence: AtomicUsize::new(0),
                    held: Mutex::new(None),
                    faults: faults.clone(),
                },
                LinkConfig::new(vec![a.clone()]),
            )
            .unwrap();
        } else {
            ar.add_transport(first.endpoint(a.clone()), LinkConfig::new(vec![b.clone()]))
                .unwrap();
            br.add_transport(first.endpoint(b.clone()), LinkConfig::new(vec![a.clone()]))
                .unwrap();
        }
        if tcp_last_hop {
            let bt = TcpMsgTransport::bind(b.clone(), "127.0.0.1:0")
                .await
                .unwrap();
            let ct = TcpMsgTransport::bind(c.clone(), "127.0.0.1:0")
                .await
                .unwrap();
            bt.register_peer(c.clone(), ct.local_addr());
            ct.register_peer(b.clone(), bt.local_addr());
            br.add_transport(bt, LinkConfig::new(vec![c.clone()]))
                .unwrap();
            cr.add_transport(ct, LinkConfig::new(vec![b])).unwrap();
        } else {
            let second = Network::new();
            br.add_transport(second.endpoint(b.clone()), LinkConfig::new(vec![c.clone()]))
                .unwrap();
            cr.add_transport(second.endpoint(c.clone()), LinkConfig::new(vec![b]))
                .unwrap();
        }
        eventually_within(
            "bidirectional multi-hop routes",
            Duration::from_secs(5),
            || ar.route_to(&c).is_some() && cr.route_to(&a).is_some(),
        )
        .await;
        Self {
            a: ar,
            bridge: br,
            c: cr,
            faults,
        }
    }

    pub(super) fn tunnels(
        &self,
        a: Credentials,
        c: Credentials,
    ) -> (TunnelTransport, TunnelTransport) {
        let ap = PeerIdentity::new(self.c.local_id().clone(), &c.leaf).unwrap();
        let cp = PeerIdentity::new(self.a.local_id().clone(), &a.leaf).unwrap();
        (
            TunnelTransport::new(self.a.clone(), a.identity, vec![ap]).unwrap(),
            TunnelTransport::new(self.c.clone(), c.identity, vec![cp]).unwrap(),
        )
    }

    pub(super) async fn close(&self) {
        self.a.close().await;
        self.bridge.close().await;
        self.c.close().await;
    }
}
