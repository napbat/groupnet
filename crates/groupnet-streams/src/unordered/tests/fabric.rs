use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use groupnet_core::NodeId;
use groupnet_network::{
    Router, RouterConfig,
    tunnel::{PeerIdentity, TlsIdentity, TunnelTransport},
};
use groupnet_testkit::cluster::eventually;
use groupnet_transport::{Inbound, Transport, link::LinkConfig};
use groupnet_transport_mem::{MemTransport, Network};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair,
    KeyUsagePurpose,
};

use super::super::{
    UnorderedConfig, UnorderedDelivery, UnorderedOptions, UnorderedProtocol, UnorderedSession,
};

pub(super) const PASS: u8 = 0;
pub(super) const DROP_FIRST: u8 = 1;
pub(super) const HOLD_FIRST: u8 = 2;
pub(super) const DROP_ALL: u8 = 3;
pub(super) const TAMPER_FIRST: u8 = 4;

#[derive(Debug, Default)]
pub(super) struct Faults {
    pub(super) action: AtomicU8,
    pub(super) data: AtomicUsize,
    pub(super) heartbeats: AtomicUsize,
    pub(super) captured: Mutex<Option<Vec<u8>>>,
    held: Mutex<Option<(NodeId, Vec<u8>)>>,
    withheld_message: AtomicU64,
    blocked_kind: AtomicU8,
    blocked_message: AtomicU64,
    pub(super) blocked: AtomicUsize,
}

impl Faults {
    pub(super) fn set(&self, action: u8) {
        self.data.store(0, Ordering::SeqCst);
        self.withheld_message.store(0, Ordering::SeqCst);
        self.action.store(action, Ordering::SeqCst);
    }

    pub(super) fn block(&self, kind: u8, message: u64) {
        self.blocked.store(0, Ordering::SeqCst);
        self.blocked_message.store(message, Ordering::SeqCst);
        self.blocked_kind.store(kind, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct Controlled {
    inner: MemTransport,
    faults: Arc<Faults>,
}

impl Transport for Controlled {
    type Error = io::Error;

    async fn send(&self, to: &NodeId, packet: &[u8]) -> io::Result<()> {
        let start = packet.windows(4).position(|bytes| bytes == b"GNU1");
        if let Some(start) = start {
            let kind = packet[start + 28];
            let message = u64::from_be_bytes(packet[start + 29..start + 37].try_into().unwrap());
            if kind == self.faults.blocked_kind.load(Ordering::SeqCst)
                && message == self.faults.blocked_message.load(Ordering::SeqCst)
            {
                self.faults.blocked.fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
        }
        if start.is_some_and(|start| packet.get(start + 28) == Some(&3)) {
            self.faults.heartbeats.fetch_add(1, Ordering::SeqCst);
        }
        let data = start.filter(|start| packet.get(start + 28) == Some(&1));
        if let Some(start) = data {
            self.faults.data.fetch_add(1, Ordering::SeqCst);
            *self
                .faults
                .captured
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(packet[start..].to_vec());
            let message = u64::from_be_bytes(packet[start + 29..start + 37].try_into().unwrap());
            // Keep the selected earlier logical message unavailable until a
            // different message crosses the link, even if scheduling is slow.
            let withheld = self.faults.withheld_message.load(Ordering::SeqCst);
            if withheld != 0 {
                if withheld == message {
                    return Ok(());
                }
                self.faults.withheld_message.store(0, Ordering::SeqCst);
            }
            let action = self.faults.action.load(Ordering::SeqCst);
            match action {
                DROP_FIRST => {
                    self.faults.action.store(PASS, Ordering::SeqCst);
                    self.faults
                        .withheld_message
                        .store(message, Ordering::SeqCst);
                    return Ok(());
                }
                DROP_ALL => return Ok(()),
                HOLD_FIRST => {
                    self.faults.action.store(PASS, Ordering::SeqCst);
                    self.faults
                        .withheld_message
                        .store(message, Ordering::SeqCst);
                    *self
                        .faults
                        .held
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some((to.clone(), packet.to_vec()));
                    return Ok(());
                }
                TAMPER_FIRST => {
                    self.faults.action.store(PASS, Ordering::SeqCst);
                    let mut corrupt = packet.to_vec();
                    *corrupt.last_mut().unwrap() ^= 1;
                    return self
                        .inner
                        .send(to, &corrupt)
                        .await
                        .map_err(io::Error::other);
                }
                _ => {}
            }
            self.inner
                .send(to, packet)
                .await
                .map_err(io::Error::other)?;
            let held = self
                .faults
                .held
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some((peer, packet)) = held {
                self.inner
                    .send(&peer, &packet)
                    .await
                    .map_err(io::Error::other)?;
            }
            return Ok(());
        }
        self.inner.send(to, packet).await.map_err(io::Error::other)
    }

    async fn recv(&self) -> io::Result<Inbound> {
        self.inner.recv().await.map_err(io::Error::other)
    }
}

#[derive(Debug)]
pub(super) struct Fabric {
    pub(super) a: UnorderedProtocol,
    pub(super) b: UnorderedProtocol,
    pub(super) at: TunnelTransport,
    pub(super) bt: TunnelTransport,
    pub(super) faults: Arc<Faults>,
    pub(super) ar: Router,
    pub(super) br: Router,
    pub(super) attacker: Router,
}

impl Fabric {
    pub(super) async fn new(config: UnorderedConfig) -> Self {
        Self::with_configs(config.clone(), config).await
    }

    pub(super) async fn with_configs(a_config: UnorderedConfig, b_config: UnorderedConfig) -> Self {
        Self::build(a_config, b_config, false).await.0
    }

    pub(super) async fn with_stalling_peer(
        a_config: UnorderedConfig,
        b_config: UnorderedConfig,
    ) -> (Self, TunnelTransport) {
        let (fabric, peer) = Self::build(a_config, b_config, true).await;
        (fabric, peer.unwrap())
    }

    async fn build(
        a_config: UnorderedConfig,
        b_config: UnorderedConfig,
        admit_attacker: bool,
    ) -> (Self, Option<TunnelTransport>) {
        let a = NodeId::new("a");
        let b = NodeId::new("b");
        let evil = NodeId::new("unadmitted");
        let router_config = RouterConfig {
            announce_interval: Duration::from_millis(50),
            ..RouterConfig::default()
        };
        let ar = Router::new(a.clone(), router_config.clone()).unwrap();
        let br = Router::new(b.clone(), router_config.clone()).unwrap();
        let attacker = Router::new(evil.clone(), router_config).unwrap();
        let network = Network::new();
        let faults = Arc::new(Faults::default());
        ar.add_transport(
            Controlled {
                inner: network.endpoint(a.clone()),
                faults: faults.clone(),
            },
            LinkConfig::new(vec![b.clone()]),
        )
        .unwrap();
        br.add_transport(
            Controlled {
                inner: network.endpoint(b.clone()),
                faults: faults.clone(),
            },
            LinkConfig::new(vec![a.clone(), evil.clone()]),
        )
        .unwrap();
        attacker
            .add_transport(
                network.endpoint(evil.clone()),
                LinkConfig::new(vec![b.clone()]),
            )
            .unwrap();
        eventually("unordered fixture routes", || {
            ar.route_to(&b).is_some() && br.route_to(&a).is_some()
        })
        .await;
        let [(ai, ac), (bi, bc), (ei, ec)] = credentials();
        let at = TunnelTransport::new(
            ar.clone(),
            ai,
            vec![PeerIdentity::new(b.clone(), &bc).unwrap()],
        )
        .unwrap();
        let mut b_peers = vec![PeerIdentity::new(a.clone(), &ac).unwrap()];
        if admit_attacker {
            b_peers.push(PeerIdentity::new(evil, &ec).unwrap());
        }
        let bt = TunnelTransport::new(br.clone(), bi, b_peers).unwrap();
        let attacker_tunnel = admit_attacker.then(|| {
            TunnelTransport::new(
                attacker.clone(),
                ei,
                vec![PeerIdentity::new(b, &bc).unwrap()],
            )
            .unwrap()
        });
        let a = UnorderedProtocol::new(&ar, at.clone(), a_config).unwrap();
        let b = UnorderedProtocol::new(&br, bt.clone(), b_config).unwrap();
        let fabric = Self {
            a,
            b,
            at,
            bt,
            faults,
            ar,
            br,
            attacker,
        };
        (fabric, attacker_tunnel)
    }

    pub(super) async fn pair(
        &self,
        delivery: UnorderedDelivery,
    ) -> (UnorderedSession, UnorderedSession) {
        let (outgoing, incoming) = tokio::join!(
            self.a.connect(
                self.br.local_id(),
                UnorderedOptions {
                    delivery,
                    timeout: Duration::from_secs(3)
                }
            ),
            self.b.accept(),
        );
        let outgoing = outgoing.unwrap();
        let (peer, incoming) = incoming.unwrap();
        assert_eq!(&peer, self.ar.local_id());
        assert_eq!(outgoing.session_id(), incoming.session_id());
        assert_eq!(incoming.delivery(), delivery);
        (outgoing, incoming)
    }

    pub(super) async fn close(&self) {
        self.a.shutdown();
        self.b.shutdown();
        tokio::join!(self.a.closed(), self.b.closed());
        tokio::join!(self.at.close(), self.bt.close());
        tokio::join!(self.ar.close(), self.br.close(), self.attacker.close());
    }
}

pub(super) fn config() -> UnorderedConfig {
    UnorderedConfig {
        retry_interval: Duration::from_millis(200),
        send_timeout: Duration::from_secs(2),
        max_attempts: 10,
        heartbeat_interval: Duration::from_millis(100),
        idle_timeout: Duration::from_secs(1),
        ..UnorderedConfig::default()
    }
}

fn credentials() -> [(TlsIdentity, Vec<u8>); 3] {
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
        (identity, leaf)
    };
    [make(), make(), make()]
}
