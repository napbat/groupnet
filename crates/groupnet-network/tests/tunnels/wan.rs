//! Bulk throughput across a long-delay link, measured on a paused clock.

use std::{io, num::NonZeroU16, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network::{
    Router, RouterConfig,
    tunnel::{PeerIdentity, SegmentSize, TunnelLimits, TunnelTransport},
};
use groupnet_transport::{Inbound, Transport, bulk::BulkTransport, link::LinkConfig};
use groupnet_transport_mem::{MemTransport, Network};
use tokio::{
    sync::mpsc,
    time::{Instant, sleep_until, timeout},
};

use super::{binary, fixtures::credentials};

const ONE_WAY: Duration = Duration::from_millis(40);
const ROUND_TRIP: Duration = Duration::from_millis(80);
/// Round trips a 1 MiB transfer may take once both endpoints are connected.
const ROUND_TRIP_BUDGET: u32 = 8;
const SETTLE: Duration = Duration::from_secs(5);

type Frame = (Instant, NodeId, Bytes);

/// A propagation-delay line with unlimited bandwidth: frames keep their order
/// and arrive [`ONE_WAY`] after they are sent.
#[derive(Debug)]
struct Delayed {
    inner: Arc<MemTransport>,
    line: mpsc::UnboundedSender<Frame>,
}

impl Delayed {
    fn new(inner: MemTransport) -> Self {
        let inner = Arc::new(inner);
        let (line, mut frames) = mpsc::unbounded_channel::<Frame>();
        let output = inner.clone();
        tokio::spawn(async move {
            while let Some((due, to, frame)) = frames.recv().await {
                sleep_until(due).await;
                if output.send(&to, &frame).await.is_err() {
                    break;
                }
            }
        });
        Self { inner, line }
    }
}

impl Transport for Delayed {
    type Error = io::Error;

    fn send(&self, to: &NodeId, msg: &[u8]) -> impl Future<Output = io::Result<()>> {
        let frame = (
            Instant::now() + ONE_WAY,
            to.clone(),
            Bytes::copy_from_slice(msg),
        );
        std::future::ready(
            self.line
                .send(frame)
                .map_err(|_| io::Error::other("delay line closed")),
        )
    }

    async fn recv(&self) -> io::Result<Inbound> {
        self.inner.recv().await.map_err(io::Error::other)
    }
}

/// Connects two routers across the delayed link and returns the virtual time
/// one megabyte takes once both tunnel endpoints hold a connected stream.
async fn megabyte_transfer(limits: TunnelLimits) -> Duration {
    let network = Network::new();
    let (a, c) = (NodeId::new("a"), NodeId::new("c"));
    let config = RouterConfig {
        announce_interval: Duration::from_millis(100),
        ..RouterConfig::default()
    };
    let ar = Router::new(a.clone(), config.clone()).unwrap();
    let cr = Router::new(c.clone(), config).unwrap();
    ar.add_transport(
        Delayed::new(network.endpoint(a.clone())),
        LinkConfig::new(vec![c.clone()]),
    )
    .unwrap();
    cr.add_transport(
        Delayed::new(network.endpoint(c.clone())),
        LinkConfig::new(vec![a.clone()]),
    )
    .unwrap();
    for (router, target) in [(&ar, &c), (&cr, &a)] {
        timeout(
            SETTLE,
            router.reachable().wait_for(|peers| peers.contains(target)),
        )
        .await
        .expect("route readiness across the delayed link")
        .unwrap();
    }
    let (ac, cc, _) = credentials();
    let sender = TunnelTransport::with_limits(
        ar.clone(),
        ac.identity,
        vec![PeerIdentity::new(c.clone(), &cc.leaf).unwrap()],
        limits.clone(),
    )
    .unwrap();
    let receiver = TunnelTransport::with_limits(
        cr.clone(),
        cc.identity,
        vec![PeerIdentity::new(a.clone(), &ac.leaf).unwrap()],
        limits,
    )
    .unwrap();
    let (client, server) = tokio::join!(sender.connect(&c), receiver.accept());
    let mut client = client.unwrap();
    let (_, mut server) = server.unwrap();
    let payload = binary(1024 * 1024);
    let started = Instant::now();
    let ((), uploaded) = tokio::join!(
        async {
            client.write_all(&payload).await.unwrap();
            client.close().await.unwrap();
        },
        async {
            let mut bytes = Vec::new();
            server.read_to_end(&mut bytes).await.unwrap();
            bytes
        },
    );
    let elapsed = started.elapsed();
    assert_eq!(uploaded, payload);
    sender.close().await;
    receiver.close().await;
    ar.close().await;
    cr.close().await;
    elapsed
}

#[tokio::test(start_paused = true)]
async fn bulk_megabyte_crosses_a_long_delay_link_within_a_few_round_trips() {
    let budget = ROUND_TRIP * ROUND_TRIP_BUDGET;
    let elapsed = megabyte_transfer(TunnelLimits::default()).await;
    assert!(
        elapsed <= budget,
        "1 MiB over an {ROUND_TRIP:?} round trip took {elapsed:?}, budget {budget:?}"
    );
    // The former 512-byte segments and 32-segment window cap a round trip at
    // 16 KiB, so the same transfer cannot fit the budget.
    let legacy = megabyte_transfer(TunnelLimits {
        payload: SegmentSize::of(512),
        window: NonZeroU16::new(32).unwrap(),
        ..TunnelLimits::default()
    })
    .await;
    assert!(legacy > budget, "legacy-sized limits took {legacy:?}");
}
