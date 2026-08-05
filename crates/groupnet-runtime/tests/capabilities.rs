//! Capability advertisement over real nodes on the in-memory transport:
//! dissemination to peers, replace-the-whole-set semantics, and the restart
//! case the wholesale-entry design exists for.

use std::time::Duration;

use groupnet_testkit::cluster::{
    MemCluster, NodeOpts, converged_within, eventually, spawn_mem_node,
};
use groupnet_transport_mem::Network;

/// A tighter convergence bound than the harness default, so a genuine
/// regression reports in seconds rather than at the full budget.
const SETTLE: Duration = Duration::from_secs(3);

/// The capability these tests advertise; the name is opaque to groupnet.
const CAP: &str = "acks";

/// The timings every node here runs at: fast gossip, brisk anti-entropy.
fn opts() -> NodeOpts {
    NodeOpts::new("g")
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
}

/// The base case: an advertisement reaches peers, scopes a member selection,
/// and a node that never advertised reads as making no claim at all.
#[tokio::test]
async fn an_advertisement_reaches_peers_and_scopes_the_member_set() {
    let cluster = MemCluster::builder(&["cap-a", "cap-b", "cap-c"])
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
        .spawn();
    let (a_id, c_id) = (cluster.ids[0].clone(), cluster.ids[2].clone());
    let (a, b) = (&cluster.groups[0], &cluster.groups[1]);
    converged_within(&cluster.groups.iter().collect::<Vec<_>>(), SETTLE).await;

    a.advertise_capabilities([CAP]).unwrap();

    eventually("b sees a's capability", || {
        b.node_has_capability(&a_id, CAP)
    })
    .await;
    assert_eq!(
        b.node_capabilities(&a_id),
        vec![CAP.to_owned()],
        "the advertised set arrives whole"
    );
    assert_eq!(
        b.members_with_capability(CAP),
        vec![a_id.clone()],
        "only the advertising member is selected"
    );

    // A node that never advertised makes no claim — and that is not the same
    // as a claim of absence, which is why the reads are simply empty/false.
    assert!(!b.node_has_capability(&c_id, CAP));
    assert!(b.node_capabilities(&c_id).is_empty());

    // The advertiser's own view agrees with its peers'.
    assert!(a.node_has_capability(&a_id, CAP));

    // An unadvertised capability name never matches.
    assert!(b.members_with_capability("nobody:has-this").is_empty());
}

/// Replace semantics: the set is rewritten wholesale, so re-advertising an
/// empty set retires everything the node claimed before.
#[tokio::test]
async fn re_advertising_replaces_the_whole_set() {
    let cluster = MemCluster::builder(&["cap-r-a", "cap-r-b"])
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
        .spawn();
    let a_id = cluster.ids[0].clone();
    let (a, b) = (&cluster.groups[0], &cluster.groups[1]);
    converged_within(&[a, b], SETTLE).await;

    a.advertise_capabilities([CAP, "mycrate:thing"]).unwrap();
    eventually("b sees both capabilities", || {
        b.node_capabilities(&a_id).len() == 2
    })
    .await;

    // Dropping one: the survivor stays, the retired one goes.
    a.advertise_capabilities(["mycrate:thing"]).unwrap();
    eventually("b sees the narrowed set", || {
        b.node_capabilities(&a_id) == vec!["mycrate:thing".to_owned()]
    })
    .await;
    assert!(!b.node_has_capability(&a_id, CAP));

    // Dropping everything: an empty advertisement is a real advertisement.
    a.advertise_capabilities(Vec::<&str>::new()).unwrap();
    eventually("b sees the empty set", || {
        b.node_capabilities(&a_id).is_empty()
    })
    .await;
    assert!(b.members_with_capability("mycrate:thing").is_empty());
}

/// The case the wholesale-entry design exists for: a node restarts under the
/// same id, and the engine's restart recovery would otherwise re-adopt the
/// previous life's `~caps` from a peer's echo. Advertising the (empty) set at
/// startup authors the key this boot, so the dead advertisement loses.
#[tokio::test]
async fn a_restart_retires_the_previous_lifes_advertisement() {
    let net = Network::new();
    let (a_id, a_node, a_group) = spawn_mem_node(&net, "cap-x-a", &["cap-x-b"], &opts());
    let (_b_id, _b_node, b_group) = spawn_mem_node(&net, "cap-x-b", &["cap-x-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    a_group.advertise_capabilities([CAP]).unwrap();
    eventually("b sees the first life's capability", || {
        b_group.node_has_capability(&a_id, CAP)
    })
    .await;

    // Tear the node down completely; B still holds (and will echo back) the
    // first life's advertisement.
    drop(a_group);
    drop(a_node);

    // The reborn node runs without the capability and says so at startup.
    let (_reborn_id, _reborn_node, reborn) = spawn_mem_node(&net, "cap-x-a", &["cap-x-b"], &opts());
    reborn.advertise_capabilities(Vec::<&str>::new()).unwrap();

    eventually("the previous life's advertisement dies on b", || {
        // Non-vacuous: the node is back as a live member and *still* claims
        // nothing, rather than having merely been reaped.
        b_group.members().contains(&a_id) && b_group.node_capabilities(&a_id).is_empty()
    })
    .await;
    assert!(!b_group.node_has_capability(&a_id, CAP));
    assert!(b_group.members_with_capability(CAP).is_empty());
    assert!(
        reborn.node_capabilities(&a_id).is_empty(),
        "the reborn node does not re-adopt its own dead advertisement either"
    );
}
