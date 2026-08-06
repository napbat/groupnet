//! The coherence-lease tier reached through the umbrella facade (feature
//! `consistency-leases`).
//!
//! A smoke test, deliberately: the tier's behaviour is proved next door in
//! `groupnet-consistency`. What this pins is that the feature wires up — that
//! `consistency-leases` really does turn on the underlying crate's `leases`
//! (and the `acks` tier its fast path is built on), and that the shell runs
//! when reached only through `groupnet::`.

#![cfg(all(feature = "consistency-leases", feature = "mem"))]

use std::time::Duration;

use groupnet::consistency::CAP_ACKS;
use groupnet::consistency::lease::{CAP_LEASE, LeaseConfig, LeaseState, Leases};
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::mem::Network;
use groupnet_testkit::cluster::eventually;

/// The tier's vocabulary is reachable through the facade, under both the
/// module path and the crate root the other tiers use.
#[test]
fn the_lease_tier_is_reachable_through_the_facade() {
    assert_eq!(CAP_LEASE, "leases");
    assert_eq!(
        groupnet::consistency::CAP_LEASE,
        CAP_LEASE,
        "re-exported at the crate root like every other tier"
    );
    // The lease tier implies the ack tier its fast path is built on, so the
    // feature must have turned that on too.
    assert_eq!(CAP_ACKS, "acks");

    let cfg = LeaseConfig::for_duration(Duration::from_secs(1));
    assert_eq!(cfg.validate(), Ok(()));
    assert_eq!(cfg.duration_ms(), 1_000);
    assert_eq!(cfg.renew_every_ms(), 333);
}

/// …and the shell actually runs on it: a solo reader has nobody who must
/// confirm, so its own renewal is the confirmed one and it serves as soon as
/// the consumer affirms catch-up.
#[tokio::test]
async fn the_lease_shell_runs_through_the_facade() {
    let net = Network::new();
    let me = NodeId::new("facade-a");
    let node = Node::builder(me.clone(), net.endpoint(me.clone())).spawn();
    let group = node.join_group("stores");
    group
        .advertise_capabilities([CAP_ACKS, CAP_LEASE])
        .expect("the advertisement is enqueued");

    let leases = Leases::new(
        group,
        me,
        LeaseConfig::for_duration(Duration::from_millis(300)),
    );
    let view = leases.view();
    assert!(
        !view.valid(),
        "a booting reader serves nothing until it affirms catch-up"
    );
    eventually("the solo reader confirms its own renewal", || {
        view.mark_caught_up()
    })
    .await;
    assert!(view.valid());
    assert_eq!(view.state(), LeaseState::Serving);
    assert!(
        view.remaining().is_some(),
        "and it reports how much is left"
    );
}
