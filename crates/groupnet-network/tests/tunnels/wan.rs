//! Bulk throughput and queueing across long-delay links, measured on a paused
//! clock.

use std::{
    io,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network::{
    Router, RouterConfig,
    tunnel::{PeerIdentity, TunnelLimits, TunnelTransport, TunneledStream},
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
const GIGABIT: u64 = 1_000_000_000;
/// Half-period of the square-wave delay jitter.
const JITTER_PERIOD: Duration = Duration::from_millis(40);
const SETTLE: Duration = Duration::from_secs(5);

type Frame = (Instant, NodeId, Bytes);

/// Shape of both directions of an emulated link.
#[derive(Clone, Copy, Debug)]
struct Link {
    /// Bits per second.
    rate: u64,
    /// Extra one-way delay during every other [`JITTER_PERIOD`], like a timer
    /// or scheduler with coarse granularity. Frames keep their order.
    jitter: Duration,
}

/// One direction of a link: frames serialize at the link rate, keep their
/// order and arrive [`ONE_WAY`] (plus jitter) after their last bit is sent. The
/// queue never drops; `backlog` records the longest any frame waited to finish
/// serializing.
#[derive(Debug)]
struct Delayed {
    inner: Arc<MemTransport>,
    line: mpsc::UnboundedSender<Frame>,
    link: Link,
    epoch: Instant,
    /// Nanoseconds after `epoch` when the line finishes its queued frames.
    idle_at: AtomicU64,
    backlog: Arc<AtomicU64>,
}

impl Delayed {
    fn new(inner: MemTransport, shape: Link, backlog: Arc<AtomicU64>) -> Self {
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
        Self {
            inner,
            line,
            link: shape,
            epoch: Instant::now(),
            idle_at: AtomicU64::new(0),
            backlog,
        }
    }
}

impl Transport for Delayed {
    type Error = io::Error;

    fn send(&self, to: &NodeId, msg: &[u8]) -> impl Future<Output = io::Result<()>> {
        let bits = u64::try_from(msg.len()).unwrap() * 8;
        let serialization = bits * 1_000_000_000 / self.link.rate;
        let now = u64::try_from(self.epoch.elapsed().as_nanos()).unwrap();
        let previous = self
            .idle_at
            .try_update(Ordering::AcqRel, Ordering::Acquire, |idle| {
                Some(idle.max(now) + serialization)
            })
            .unwrap();
        let finished = previous.max(now) + serialization;
        self.backlog.fetch_max(finished - now, Ordering::AcqRel);
        let period = u64::try_from(JITTER_PERIOD.as_nanos()).unwrap();
        let jitter = if (finished / period) % 2 == 1 {
            self.link.jitter
        } else {
            Duration::ZERO
        };
        let sent = self.epoch + Duration::from_nanos(finished);
        let frame = (
            sent + ONE_WAY + jitter,
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

/// Which way one transfer moves data.
#[derive(Clone, Copy, Debug)]
enum Direction {
    /// Initiator to acceptor.
    Forward,
    /// Acceptor to initiator.
    Back,
}

/// What bulk transfers measured, one entry per transfer.
#[derive(Debug)]
struct Measured {
    /// Virtual time from the first write to the last byte read.
    elapsed: Vec<Duration>,
    /// Longest wait of an initiator frame in its link queue.
    backlog: Vec<Duration>,
}

/// Moves `length` bytes from `from` to `to` on an established session.
async fn transfer(from: &mut TunneledStream, to: &mut TunneledStream, length: usize) -> Duration {
    let payload = binary(length);
    let started = Instant::now();
    let ((), received) = tokio::join!(
        async {
            from.write_all(&payload).await.unwrap();
            from.flush().await.unwrap();
        },
        async {
            let mut bytes = vec![0; length];
            to.read_exact(&mut bytes).await.unwrap();
            bytes
        },
    );
    let elapsed = started.elapsed();
    assert!(received == payload, "transfer corrupted");
    elapsed
}

/// Connects two routers across `link` and moves `length` bytes in each of
/// `transfers` in turn on one session.
async fn bulk_transfer(
    limits: TunnelLimits,
    link: Link,
    length: usize,
    transfers: &[Direction],
) -> Measured {
    let network = Network::new();
    let (a, c) = (NodeId::new("a"), NodeId::new("c"));
    let config = RouterConfig {
        announce_interval: Duration::from_millis(100),
        ..RouterConfig::default()
    };
    let ar = Router::new(a.clone(), config.clone()).unwrap();
    let cr = Router::new(c.clone(), config).unwrap();
    let backlog = Arc::new(AtomicU64::new(0));
    ar.add_transport(
        Delayed::new(network.endpoint(a.clone()), link, backlog.clone()),
        LinkConfig::new(vec![c.clone()]),
    )
    .unwrap();
    cr.add_transport(
        Delayed::new(network.endpoint(c.clone()), link, Arc::default()),
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
    let initiator = TunnelTransport::with_limits(
        ar.clone(),
        ac.identity,
        vec![PeerIdentity::new(c.clone(), &cc.leaf).unwrap()],
        limits.clone(),
    )
    .unwrap();
    let acceptor = TunnelTransport::with_limits(
        cr.clone(),
        cc.identity,
        vec![PeerIdentity::new(a.clone(), &ac.leaf).unwrap()],
        limits,
    )
    .unwrap();
    let (client, server) = tokio::join!(initiator.connect(&c), acceptor.accept());
    let mut client = client.unwrap();
    let (_, mut server) = server.unwrap();
    let mut measured = Measured {
        elapsed: Vec::new(),
        backlog: Vec::new(),
    };
    for direction in transfers {
        backlog.store(0, Ordering::Release);
        measured.elapsed.push(match direction {
            Direction::Forward => transfer(&mut client, &mut server, length).await,
            Direction::Back => transfer(&mut server, &mut client, length).await,
        });
        measured
            .backlog
            .push(Duration::from_nanos(backlog.load(Ordering::Acquire)));
    }
    initiator.close().await;
    acceptor.close().await;
    ar.close().await;
    cr.close().await;
    measured
}

#[tokio::test(start_paused = true)]
async fn bulk_transfers_fill_a_jittery_gigabit_long_delay_link_both_ways() {
    // Slow start reaches the 10 MB bandwidth-delay product within about five
    // round trips; a 1 MiB window needs at least sixteen for 16 MiB. Four
    // milliseconds of delay jitter once ended slow start a fraction of the way
    // there, in whichever direction it happened to hit, leaving the window to
    // grow by one segment per round: over twelve round trips one way.
    let link = Link {
        rate: GIGABIT,
        jitter: Duration::from_millis(4),
    };
    let budget = ROUND_TRIP * 8;
    let both = [Direction::Forward, Direction::Back];
    let measured = bulk_transfer(TunnelLimits::default(), link, 16 << 20, &both).await;
    assert!(
        measured.elapsed.iter().all(|elapsed| *elapsed <= budget),
        "16 MiB each way over 1 Gbit/s × {ROUND_TRIP:?}: {measured:?}, budget {budget:?}"
    );
    let capped = bulk_transfer(
        TunnelLimits {
            max_window: NonZeroU32::new(1 << 20).unwrap(),
            ..TunnelLimits::default()
        },
        link,
        16 << 20,
        &[Direction::Forward],
    )
    .await;
    assert!(capped.elapsed[0] > budget, "a 1 MiB window: {capped:?}");
}

#[tokio::test(start_paused = true)]
async fn a_window_beyond_path_capacity_never_builds_a_standing_queue() {
    // 100 Mbit/s × 80 ms holds 1 MB. Without delay control the 16 MiB window,
    // limited only by the receiver's growing credit, queues hundreds of
    // milliseconds. Slow start may overshoot briefly; a second transfer on the
    // warmed session shows the standing queue.
    let link = Link {
        rate: GIGABIT / 10,
        jitter: Duration::ZERO,
    };
    let length = 8 << 20;
    let twice = [Direction::Forward, Direction::Forward];
    let measured = bulk_transfer(TunnelLimits::default(), link, length, &twice).await;
    let line_rate =
        Duration::from_nanos(u64::try_from(length).unwrap() * 8 * 1_000_000_000 / link.rate);
    assert!(
        measured.backlog[0] <= ROUND_TRIP * 3,
        "slow start overshot by more than three round trips: {measured:?}"
    );
    assert!(
        measured.backlog[1] <= ROUND_TRIP,
        "a standing queue of a round trip or more: {measured:?}"
    );
    assert!(
        measured.elapsed[0] <= line_rate + ROUND_TRIP * 8
            && measured.elapsed[1] <= line_rate + ROUND_TRIP * 2,
        "link underused: {measured:?}, line rate {line_rate:?}"
    );
}
