//! Multi-node tests over the in-memory transport: consumer-typed sequence
//! floors reach peers and list their publishers, never regress within a
//! publisher's life, fade to "no claim" when refreshes stop (and come back on
//! a republish), read as absent for unknown and undecodable entries, and stay
//! isolated between named sets.

use std::time::Duration;

use groupnet_consistency::SeqFloors;
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually, spawn_mem_node};
use groupnet_transport_mem::Network;

const GROUP: &str = "shards";

/// The convergence budget these tests carry: a genuine regression reports in
/// 3 s, not the harness default.
const SETTLE: Duration = Duration::from_secs(3);

/// Long enough that nothing expires mid-test where expiry is not the subject.
const LONG_TTL: Duration = Duration::from_secs(30);

/// The timings every node in these tests runs at: fast gossip, brisk
/// anti-entropy, one shared group.
fn opts() -> NodeOpts {
    NodeOpts::new(GROUP)
        .gossip_interval_ms(10)
        .anti_entropy_interval_ms(25)
}

/// A published floor reaches a peer, and the peer can enumerate exactly the
/// members currently advertising that key.
#[tokio::test]
async fn floors_disseminate_and_list_their_publishers() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "fl-a", &["fl-b"], &opts());
    let (_b_id, _b_node, b_group) = spawn_mem_node(&net, "fl-b", &["fl-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let writer = SeqFloors::new(a_group, LONG_TTL);
    let reader = SeqFloors::new(b_group, LONG_TTL);

    // Nothing claimed yet: absence, not zero.
    assert_eq!(reader.floor_of(&a_id, "shard-7"), None);
    assert!(reader.floors_for("shard-7").is_empty());

    writer.publish("shard-7", 4_210).expect("entry accepted");

    eventually("b sees a's floor", || {
        reader.floor_of(&a_id, "shard-7") == Some(4_210)
    })
    .await;
    assert_eq!(
        reader.floors_for("shard-7"),
        vec![(a_id.clone(), 4_210)],
        "only the member actually advertising is listed"
    );
    // A key the writer never touched stays absent — floors are per key.
    assert_eq!(reader.floor_of(&a_id, "shard-8"), None);

    // An advance propagates as an advance.
    writer.publish("shard-7", 4_999).expect("entry accepted");
    eventually("b sees the advance", || {
        reader.floor_of(&a_id, "shard-7") == Some(4_999)
    })
    .await;
}

/// Within one publisher life a lower publish refreshes the claim without ever
/// walking it backwards — a reader can never observe a regression.
#[tokio::test]
async fn floors_never_regress_within_a_publisher_life() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "mo-a", &["mo-b"], &opts());
    let (_b_id, _b_node, b_group) = spawn_mem_node(&net, "mo-b", &["mo-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let writer = SeqFloors::new(a_group, LONG_TTL);
    let reader = SeqFloors::new(b_group, LONG_TTL);

    writer.publish("shard-1", 10).expect("entry accepted");
    eventually("b sees the high floor", || {
        reader.floor_of(&a_id, "shard-1") == Some(10)
    })
    .await;

    // A stale/out-of-order call site publishes a lower floor: it must refresh
    // the entry, not regress it — on the publisher's own view and the peer's.
    writer.publish("shard-1", 7).expect("entry accepted");
    assert_eq!(
        writer.floor_of(&a_id, "shard-1"),
        Some(10),
        "the publisher's own view must not regress either"
    );
    // A bounded poll, deliberately, where the rest of the suite uses
    // `eventually`: this asserts something never *starts* being true within a
    // window, and `eventually` waits *for* a condition — it would pass the
    // instant the regression appeared, which is the opposite verdict. Half a
    // second of 20ms samples is many gossip and anti-entropy rounds at these
    // timings, so a regression that propagated at all would be seen.
    for _ in 0..25 {
        assert_eq!(
            reader.floor_of(&a_id, "shard-1"),
            Some(10),
            "a lower publish must never be observable as a regression"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The idle signal: per-entry TTL expiry. A publisher that stops refreshing
/// fades to "no claim" on the peer *and* on itself, and a republish brings
/// the claim back.
#[tokio::test]
async fn ttl_expiry_is_the_idle_signal() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "tt-a", &["tt-b"], &opts());
    let (_b_id, _b_node, b_group) = spawn_mem_node(&net, "tt-b", &["tt-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let writer = SeqFloors::new(a_group, Duration::from_millis(300));
    let reader = SeqFloors::new(b_group, LONG_TTL);

    writer.publish("shard-3", 12).expect("entry accepted");
    eventually("b sees the floor", || {
        reader.floor_of(&a_id, "shard-3") == Some(12)
    })
    .await;

    // Stop refreshing: the claim expires everywhere, with no unpublish and no
    // death notice — including on the author's own copy.
    eventually("the floor expires on b", || {
        reader.floor_of(&a_id, "shard-3").is_none()
    })
    .await;
    eventually("the floor expires on a too", || {
        writer.floor_of(&a_id, "shard-3").is_none()
    })
    .await;
    assert!(
        reader.floors_for("shard-3").is_empty(),
        "an expired claim leaves no placeholder behind"
    );

    // Republishing re-arms it: the same value, a live entry again.
    writer.publish("shard-3", 12).expect("entry accepted");
    eventually("the republished floor returns", || {
        reader.floor_of(&a_id, "shard-3") == Some(12)
    })
    .await;
}

/// Unknown-posture: everything that is not a well-formed claim reads as
/// `None`, and nothing panics on hostile bytes.
#[tokio::test]
async fn unknown_and_undecodable_floors_read_as_no_claim() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "un-a", &["un-b"], &opts());
    let (b_id, _b_node, b_group) = spawn_mem_node(&net, "un-b", &["un-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let reader = SeqFloors::new(b_group.clone(), LONG_TTL);

    // Never published, by anyone: absent, and absent for the peer that is
    // not publishing at all.
    assert_eq!(reader.floor_of(&a_id, "shard-0"), None);
    assert_eq!(reader.floor_of(&b_id, "shard-0"), None);

    // A peer writing junk under the documented entry layout (`~floor:<key>`)
    // must read as "no claim", never as a number nobody wrote.
    a_group.set_entry("~floor:short", vec![1, 2, 3], None).ok();
    a_group.set_entry("~floor:long", vec![0_u8; 9], None).ok();
    a_group.set_entry("~floor:empty", Vec::new(), None).ok();
    let writer = SeqFloors::new(a_group, LONG_TTL);
    writer.publish("good", 5).expect("entry accepted");

    // The junk really arrived (so the assertions below are not vacuous)…
    eventually("b sees the raw entries", || {
        b_group.node_entry(&a_id, "~floor:short").is_some()
            && b_group.node_entry(&a_id, "~floor:long").is_some()
            && b_group.node_entry(&a_id, "~floor:empty").is_some()
            && reader.floor_of(&a_id, "good") == Some(5)
    })
    .await;
    // …and every flavour of it decodes to no claim.
    for key in ["short", "long", "empty"] {
        assert_eq!(reader.floor_of(&a_id, key), None, "garbled {key}");
        assert!(reader.floors_for(key).is_empty(), "garbled {key}");
    }
    assert_eq!(
        reader.floors_for("good"),
        vec![(a_id, 5)],
        "a well-formed claim is unaffected by its garbled neighbours"
    );
}

/// Two subsystems sharing one group keep their floors apart by set name, and
/// the default set reads neither of them.
#[tokio::test]
async fn named_sets_do_not_cross_read() {
    let net = Network::new();
    let (a_id, _a_node, a_group) = spawn_mem_node(&net, "nm-a", &["nm-b"], &opts());
    let (_b_id, _b_node, b_group) = spawn_mem_node(&net, "nm-b", &["nm-a"], &opts());
    converged_within(&[&a_group, &b_group], SETTLE).await;

    let lsn = SeqFloors::named("lsn", a_group.clone(), LONG_TTL);
    let idx = SeqFloors::named("idx", a_group.clone(), LONG_TTL);
    let default = SeqFloors::new(a_group, LONG_TTL);
    lsn.publish("s1", 10).expect("entry accepted");
    idx.publish("s1", 99).expect("entry accepted");
    default.publish("s1", 5).expect("entry accepted");

    let read_lsn = SeqFloors::named("lsn", b_group.clone(), LONG_TTL);
    let read_idx = SeqFloors::named("idx", b_group.clone(), LONG_TTL);
    let read_default = SeqFloors::new(b_group, LONG_TTL);

    // All three converge, and each reads exactly its own set's value — the
    // same key in three sets never crosses over.
    eventually("b sees all three sets", || {
        read_lsn.floor_of(&a_id, "s1") == Some(10)
            && read_idx.floor_of(&a_id, "s1") == Some(99)
            && read_default.floor_of(&a_id, "s1") == Some(5)
    })
    .await;
    assert_eq!(read_lsn.floors_for("s1"), vec![(a_id.clone(), 10)]);
    assert_eq!(read_idx.floors_for("s1"), vec![(a_id.clone(), 99)]);
    assert_eq!(read_default.floors_for("s1"), vec![(a_id, 5)]);
}
