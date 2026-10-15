//! The engine's [`Config`] contract: construction refuses a config that fails
//! [`Config::validate`], and a valid one runs exactly as given — the gossip
//! round on `gossip_interval_ms`, to `gossip_fanout` peers.

use std::num::{NonZeroU64, NonZeroUsize};

use groupnet_core::{Config, ConfigError, GroupEngine, GroupId, NodeId, RecoveredGrant, Time};

fn ids(names: &[&str]) -> Vec<NodeId> {
    names.iter().map(|name| NodeId::new(*name)).collect()
}

#[test]
fn new_rejects_an_invalid_config_with_its_rule() {
    let config = Config {
        gossip_interval_ms: 0,
        ..Config::default()
    };
    let result = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [], config);
    assert_eq!(result.err(), Some(ConfigError::ZeroGossipInterval));
}

/// The restarting voter's constructor validates exactly as `new` does, before
/// any recovered state is applied: the smallest valid interval builds, one
/// below it is refused with its rule.
#[test]
fn recovering_validates_the_config_like_new() {
    let config = |gossip_interval_ms| Config {
        gossip_interval_ms,
        ..Config::default()
    };
    let refused = GroupEngine::recovering(GroupId::new("g"), NodeId::new("a"), [], config(0));
    assert_eq!(refused.err(), Some(ConfigError::ZeroGossipInterval));

    let mut engine =
        GroupEngine::recovering(GroupId::new("g"), NodeId::new("a"), ids(&["b"]), config(1))
            .expect("the smallest positive interval is valid")
            .restore(RecoveredGrant::none());
    engine.start(Time::ZERO);
    assert_eq!(engine.net_stats().digests_built, 1);
    engine.on_tick(Time(1));
    assert_eq!(
        engine.net_stats().digests_built,
        2,
        "the restored engine runs the boundary interval exactly as given"
    );
}

/// `gossip_interval_ms` is the one cadence of the digest/delta round: a round
/// fires at start, none before the interval elapses, and the next exactly
/// when it does — each to `gossip_fanout` peers.
#[test]
fn the_gossip_round_runs_on_gossip_interval_to_gossip_fanout_peers() {
    const INTERVAL_MS: u64 = 300;
    let gossip_fanout = NonZeroUsize::new(3).expect("nonzero");
    let config = Config {
        gossip_interval_ms: INTERVAL_MS,
        gossip_fanout,
        full_digest_every: NonZeroU64::MIN,
        ..Config::default()
    };
    let mut engine = GroupEngine::new(
        GroupId::new("g"),
        NodeId::new("a"),
        ids(&["b", "c", "d", "e"]),
        config,
    )
    .expect("valid config");
    let per_round = u64::try_from(gossip_fanout.get()).expect("small");

    engine.start(Time::ZERO);
    assert_eq!(engine.net_stats().digests_built, per_round);

    engine.on_tick(Time(INTERVAL_MS - 1));
    assert_eq!(
        engine.net_stats().digests_built,
        per_round,
        "no round before the gossip interval elapses"
    );

    engine.on_tick(Time(INTERVAL_MS));
    assert_eq!(
        engine.net_stats().digests_built,
        2 * per_round,
        "the next round fires exactly one gossip interval later"
    );
}
