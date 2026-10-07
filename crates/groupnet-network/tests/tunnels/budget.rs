//! The node memory budget under saturation: bounded, starvation-free, and
//! returned by idle sessions.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_network::tunnel::{PeerIdentity, TunnelLimits, TunnelTransport, TunneledStream};
use groupnet_testkit::cluster::eventually_within;
use groupnet_transport::bulk::BulkTransport;
use tokio::{task::JoinHandle, time::sleep};

use super::{binary, fixtures::Fabric, fixtures::credentials};

const SESSIONS: usize = 8;
const FLOOR: u32 = 64 * 1024;
const WINDOW: u32 = 2 << 20;
const BUDGET: usize = 4 << 20;
/// Sessions saturated at once: their windows need three times the lendable budget.
const SATURATED: usize = 6;
const SETTLE: Duration = Duration::from_secs(30);

fn limits() -> TunnelLimits {
    TunnelLimits {
        max_sessions: NonZeroUsize::new(SESSIONS).unwrap(),
        sessions_per_peer: NonZeroUsize::new(SESSIONS).unwrap(),
        min_window: NonZeroU32::new(FLOOR).unwrap(),
        max_window: NonZeroU32::new(WINDOW).unwrap(),
        memory_budget: NonZeroUsize::new(BUDGET).unwrap(),
        ..TunnelLimits::default()
    }
}

/// Floors held by `sessions` live sessions.
fn floors(sessions: usize) -> usize {
    sessions * 2 * FLOOR as usize
}

/// Bytes lent above floors once every other session's floors are set aside.
fn lendable() -> usize {
    BUDGET - floors(SESSIONS)
}

struct Pair {
    fabric: Fabric,
    sender: TunnelTransport,
    receiver: TunnelTransport,
}

impl Pair {
    async fn new() -> Self {
        let fabric = Fabric::new(false, false).await;
        let (a, c, _) = credentials();
        let ap = PeerIdentity::new(fabric.c.local_id().clone(), &c.leaf).unwrap();
        let cp = PeerIdentity::new(fabric.a.local_id().clone(), &a.leaf).unwrap();
        let sender =
            TunnelTransport::with_limits(fabric.a.clone(), a.identity, vec![ap], limits()).unwrap();
        let receiver =
            TunnelTransport::with_limits(fabric.c.clone(), c.identity, vec![cp], limits()).unwrap();
        Self {
            fabric,
            sender,
            receiver,
        }
    }

    async fn stream(&self) -> (TunneledStream, TunneledStream) {
        let (outgoing, incoming) = tokio::join!(
            self.sender.connect(self.fabric.c.local_id()),
            self.receiver.accept()
        );
        (outgoing.unwrap(), incoming.unwrap().1)
    }

    fn within_budget(&self) -> bool {
        self.sender.reserved_memory() <= BUDGET && self.receiver.reserved_memory() <= BUDGET
    }

    /// Waits until `predicate` holds, checking the budget bound at every poll.
    async fn until(&self, what: &str, predicate: impl Fn(&Self) -> bool) {
        eventually_within(what, SETTLE, || {
            assert!(self.within_budget(), "budget exceeded");
            predicate(self)
        })
        .await;
    }

    async fn close(self) {
        self.sender.close().await;
        self.receiver.close().await;
        self.fabric.close().await;
    }
}

/// Writes `payload` on a task; the stream stays open afterwards.
fn write(mut stream: TunneledStream, payload: Vec<u8>) -> JoinHandle<TunneledStream> {
    tokio::spawn(async move {
        stream.write_all(&payload).await.unwrap();
        stream.flush().await.unwrap();
        stream
    })
}

async fn read(stream: &mut TunneledStream, expected: &[u8]) {
    let mut bytes = vec![0; expected.len()];
    stream.read_exact(&mut bytes).await.unwrap();
    assert!(bytes == expected, "stream corrupted");
}

/// Opens [`SATURATED`] streams whose unread receivers fill the lendable budget.
async fn saturate(pair: &Pair) -> Vec<(JoinHandle<TunneledStream>, TunneledStream)> {
    let mut streams = Vec::new();
    for _ in 0..SATURATED {
        let (outgoing, incoming) = pair.stream().await;
        streams.push((write(outgoing, binary(WINDOW as usize)), incoming));
    }
    pair.until("receivers hold every lendable byte", |pair| {
        pair.receiver.reserved_memory() == floors(SATURATED) + lendable()
    })
    .await;
    streams
}

#[tokio::test(start_paused = true)]
async fn saturated_sessions_never_exceed_the_node_budget_and_all_complete() {
    let pair = Pair::new().await;
    let streams = saturate(&pair).await;
    for (writer, mut incoming) in streams {
        read(&mut incoming, &binary(WINDOW as usize)).await;
        assert!(pair.within_budget());
        writer.await.unwrap();
    }
    pair.close().await;
}

#[tokio::test(start_paused = true)]
async fn exhausted_budget_never_starves_a_session_and_idle_release_lets_it_grow() {
    let pair = Pair::new().await;
    let streams = saturate(&pair).await;
    // The newcomer progresses on its guaranteed floors alone.
    let (outgoing, mut newcomer) = pair.stream().await;
    let payload = binary(512 * 1024 + 9);
    let writer = write(outgoing, payload.clone());
    read(&mut newcomer, &payload).await;
    let mut outgoing = writer.await.unwrap();
    assert_eq!(
        pair.receiver.reserved_memory(),
        floors(SATURATED + 1) + lendable(),
        "the exhausted budget lent the newcomer nothing"
    );
    // Drained, the saturated sessions stay open but go idle and relinquish.
    let mut open = Vec::new();
    for (writer, mut incoming) in streams {
        read(&mut incoming, &binary(WINDOW as usize)).await;
        open.push((writer.await.unwrap(), incoming));
    }
    sleep(Duration::from_secs(3)).await;
    pair.until("idle sessions release lent memory", |pair| {
        pair.receiver.reserved_memory() == floors(SATURATED + 1)
    })
    .await;
    // Released memory lets the newcomer's unread receive window grow: holding
    // 1 MiB needs about 1 MiB lent beyond its 64 KiB floor.
    let payload = binary(1 << 20);
    outgoing.write_all(&payload).await.unwrap();
    outgoing.flush().await.unwrap();
    pair.until("the newcomer grows beyond its floor", |pair| {
        pair.receiver.reserved_memory() >= floors(SATURATED + 1) + (1 << 20) - FLOOR as usize
    })
    .await;
    read(&mut newcomer, &payload).await;
    drop(open);
    pair.close().await;
}
