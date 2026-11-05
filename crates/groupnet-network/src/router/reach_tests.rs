//! The reachable-destination set: an advert updates only the destination it
//! names, and the set always equals a full rebuild from the route table.

use std::num::NonZeroUsize;

use super::*;

/// Destinations the counting test learns routes to.
const N: usize = 200;

/// A link that sends into the void and never receives: the tests feed the
/// router's route learning directly.
#[derive(Debug)]
struct Silent;

impl Transport for Silent {
    type Error = io::Error;

    fn send(&self, _to: &NodeId, _bytes: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        std::future::pending().await
    }
}

fn config() -> RouterConfig {
    RouterConfig {
        max_routes: NonZeroUsize::new(1_024).expect("nonzero"),
        ..RouterConfig::default()
    }
}

fn destination(i: usize) -> NodeId {
    NodeId::new(format!("d{i:03}"))
}

/// Neighbors that each advertise a route to every destination, so each
/// destination has this many candidate routes — the all-seeds shape.
const NEIGHBORS: usize = 50;

/// The work counters the reachability index keeps in tests:
/// `(destinations examined, candidate routes examined, publishes)`.
fn work(shared: &Shared) -> (u64, u64, u64) {
    let table = shared.table.lock().expect("router table");
    (
        table.reach_visits,
        table.reach_candidates,
        table.reach_publishes,
    )
}

/// With 200 destinations known, each through 50 neighbors, one more
/// steady-state advert — a destination already reachable, refreshed by one
/// of its neighbors — examines no other destination and none of its other
/// candidate routes, and neither copies nor republishes the reachable set.
/// Before (5f1c9c3, a full rebuild per advert): 200 destinations examined;
/// at ccf36c7 (per-destination refresh): 1 destination and its 50
/// candidates; now: 0, 0. Publishes are 0 at ccf36c7 and now: only entering
/// or leaving the set republishes it.
#[tokio::test(start_paused = true)]
async fn a_steady_state_advert_touches_only_its_own_lapse() -> io::Result<()> {
    let neighbors: Vec<NodeId> = (0..NEIGHBORS)
        .map(|i| NodeId::new(format!("n{i:02}")))
        .collect();
    let router = Router::with_config(NodeId::new("local"), config())?;
    let link = router
        .add_link(BoundLink::new(Silent, LinkConfig::new(neighbors.clone())))
        .await?;
    let shared = &router.inner.shared;
    let advert = |neighbor: &NodeId, i: usize| {
        shared.learn(
            link,
            neighbor.clone(),
            None,
            1,
            1,
            vec![neighbor.clone(), destination(i)],
        );
    };
    for i in 0..N {
        for neighbor in &neighbors {
            advert(neighbor, i);
        }
    }
    assert_eq!(router.reachable().borrow().len(), N);
    let (_, _, publishes) = work(shared);
    assert_eq!(
        publishes,
        u64::try_from(N).expect("small"),
        "one publish per destination entering the set"
    );

    // Later than the routes it refreshes, so its lapse really moves.
    tokio::time::advance(Duration::from_millis(100)).await;
    let before = work(shared);
    advert(&neighbors[7], N / 2);
    let after = work(shared);
    let (destinations, candidates, publishes) =
        (after.0 - before.0, after.1 - before.1, after.2 - before.2);
    assert_eq!(
        (destinations, candidates, publishes),
        (0, 0, 0),
        "one steady-state advert examined {destinations} destinations and \
         {candidates} candidate routes, and republished {publishes} times"
    );
    assert_eq!(router.reachable().borrow().len(), N);
    router.close().await;
    Ok(())
}

/// A splitmix64 stream for the property test's schedules.
struct Draws(u64);

impl Draws {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) % n
    }

    fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.below(u64::try_from(len).expect("small"))).expect("small")
    }
}

/// The reachable set a full rebuild from the route table computes now.
fn rebuilt(table: &Table, ttl: Duration) -> Vec<NodeId> {
    let mut reachable: Vec<NodeId> = table
        .candidates
        .keys()
        .filter(|id| table.route(id, ttl).is_some())
        .cloned()
        .collect();
    reachable.sort();
    reachable
}

/// Random schedules of adverts — new routes, changed costs and paths, routes
/// over a neighbor the link does not admit, routes over a removed link —
/// interleaved with clock jumps that lapse routes, link losses and link
/// additions. After every step the index, once lapses are applied as the next
/// advert applies them, equals a full rebuild from the table; after every
/// advert the published set equals both.
#[tokio::test(start_paused = true)]
async fn the_index_always_equals_a_full_rebuild() -> io::Result<()> {
    const DESTINATIONS: usize = 40;
    for seed in 0..16 {
        let router = Router::with_config(NodeId::new("local"), config())?;
        let shared = &router.inner.shared;
        let ttl = shared.config.route_ttl;
        let ttl_ms = u64::try_from(ttl.as_millis()).expect("short ttl");
        let mut draws = Draws(seed);
        let mut links: Vec<(TransportId, Vec<NodeId>)> = Vec::new();
        let mut dead: Vec<TransportId> = Vec::new();
        let mut next_neighbor = 0;
        for step in 0..400 {
            let op = match (links.len(), draws.below(10)) {
                (0, _) => 9,
                // Stay well inside `max_transports`: past six links, add none.
                (7.., 9) => 0,
                (_, op) => op,
            };
            let mut advert = false;
            match op {
                0..=5 => {
                    let (link, peers) = if !dead.is_empty() && draws.below(8) == 0 {
                        (dead[draws.index(dead.len())], Vec::new())
                    } else {
                        links[draws.index(links.len())].clone()
                    };
                    // One advert in eight comes from a neighbor the link does
                    // not admit: its candidate must never count.
                    let neighbor = if peers.is_empty() || draws.below(8) == 0 {
                        NodeId::new("stranger")
                    } else {
                        peers[draws.index(peers.len())].clone()
                    };
                    let mut path = vec![neighbor.clone()];
                    if draws.below(3) == 0 {
                        path.push(NodeId::new(format!("via{}", draws.below(4))));
                    }
                    path.push(destination(draws.index(DESTINATIONS)));
                    let cost = u32::try_from(draws.below(5)).expect("small") + 1;
                    shared.learn(link, neighbor, None, 1, cost, path);
                    advert = true;
                }
                6 | 7 => {
                    let jump = draws.below(ttl_ms * 3 / 4) + 1;
                    tokio::time::advance(Duration::from_millis(jump)).await;
                }
                8 => {
                    let (link, _) = links.swap_remove(draws.index(links.len()));
                    router.remove_transport(link);
                    dead.push(link);
                }
                _ => {
                    let peers: Vec<NodeId> = (0..2)
                        .map(|i| NodeId::new(format!("n{}", next_neighbor + i)))
                        .collect();
                    next_neighbor += 2;
                    let link = router
                        .add_link(BoundLink::new(Silent, LinkConfig::new(peers.clone())))
                        .await?;
                    links.push((link, peers));
                }
            }
            let table = shared.table.lock().expect("router table");
            if advert {
                assert!(
                    router.reachable().borrow().iter().eq(table.reachable()),
                    "seed {seed} step {step}: the published set is not the index"
                );
            }
            let indexed: Vec<NodeId> = table.reachable_at(Instant::now()).cloned().collect();
            assert_eq!(
                indexed,
                rebuilt(&table, ttl),
                "seed {seed} step {step}: the index left a full rebuild"
            );
        }
        router.close().await;
    }
    Ok(())
}
