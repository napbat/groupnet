use crate::membership::{Member, Status};
use crate::{Command, Config, Effect, GroupEngine, GroupId, NodeId, Time};
use std::collections::VecDeque;

fn engine(local: &str) -> GroupEngine {
    GroupEngine::new(
        GroupId::new("devices"),
        NodeId::new(local),
        [],
        Config {
            anti_entropy_fanout: 1,
            full_digest_every: 1,
            ..Config::default()
        },
    )
}

fn recipients(effects: Vec<Effect>) -> Vec<NodeId> {
    effects
        .into_iter()
        .filter_map(|effect| match effect {
            Effect::Send { to, .. } => Some(to),
            _ => None,
        })
        .collect()
}

#[test]
fn discovery_contacts_are_not_members_and_retry_with_bounded_fanout() {
    let mut local = engine("z-local");
    let peers = vec![NodeId::new("a-unknown"), NodeId::new("b-unknown")];
    let coordinator = local.coordinator().cloned();
    local.apply(Command::SetBootstrapContacts(peers.clone()));
    assert_eq!(local.coordinator(), coordinator.as_ref());
    assert_eq!(
        local.members().cloned().collect::<Vec<_>>(),
        vec![NodeId::new("z-local")]
    );
    assert!(peers.iter().all(|peer| local.member_status(peer).is_none()));
    // No delivery: periodic digest attempts continue rather than treating a
    // dropped first exchange as a completed discovery or asserting Alive.
    let first = recipients(local.disseminate_digest(Time(1)));
    let second = recipients(local.disseminate_digest(Time(2)));
    let retry = recipients(local.disseminate_digest(Time(3)));
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(first, second);
    assert_eq!(first, retry);
    assert!(peers.iter().all(|peer| local.member_status(peer).is_none()));
    local.apply(Command::SetBootstrapContacts(Vec::new()));
    assert_eq!(
        recipients(local.disseminate_digest(Time(4))),
        Vec::<NodeId>::new()
    );
    assert!(local.digest_cursors.is_empty());
    assert!(local.digest_visits.is_empty());
}

#[test]
fn discovery_contacts_are_bounded_and_do_not_replace_static_seeds() {
    let static_seed = NodeId::new("configured");
    let mut local = GroupEngine::new(
        GroupId::new("devices"),
        NodeId::new("local"),
        [static_seed.clone()],
        Config::default(),
    );
    let admitted = NodeId::new("admitted");
    local.apply(Command::SetBootstrapContacts(vec![admitted.clone()]));
    local.apply(Command::SetBootstrapContacts(
        (0..4097).map(|i| NodeId::new(i.to_string())).collect(),
    ));
    assert_eq!(
        local.bootstrap_contacts.iter().cloned().collect::<Vec<_>>(),
        vec![admitted]
    );
    local.apply(Command::SetBootstrapContacts(Vec::new()));
    assert_eq!(local.dissemination_targets(), vec![static_seed]);
}

fn exchange(engines: &mut [GroupEngine; 2], now: Time) {
    let mut queue = VecDeque::new();
    for (index, engine) in engines.iter_mut().enumerate() {
        queue.extend(
            engine
                .disseminate_digest(now)
                .into_iter()
                .map(|effect| (index, effect)),
        );
    }
    for _ in 0..128 {
        let Some((sender, effect)) = queue.pop_front() else {
            return;
        };
        if let Effect::Send { to, wire } = effect {
            let receiver = 1 - sender;
            if to == *engines[receiver].local() {
                let from = engines[sender].local().clone();
                queue.extend(
                    engines[receiver]
                        .on_message(from, &wire, now)
                        .into_iter()
                        .map(|effect| (receiver, effect)),
                );
            }
        }
    }
    assert!(
        queue.is_empty(),
        "one bounded discovery exchange must drain"
    );
}

#[test]
fn seedless_contacts_recover_while_both_engines_retain_dead_tombstones() {
    let a = NodeId::new("a");
    let b = NodeId::new("b");
    let mut engines = [engine("a"), engine("b")];
    engines[0]
        .members
        .insert(b.clone(), Member::new(0, Status::Dead, Time::ZERO));
    engines[1]
        .members
        .insert(a.clone(), Member::new(0, Status::Dead, Time::ZERO));
    engines[0].apply(Command::SetBootstrapContacts(vec![b.clone()]));
    engines[1].apply(Command::SetBootstrapContacts(vec![a.clone()]));
    assert_eq!(engines[0].member_status(&b), Some(Status::Dead));
    assert_eq!(engines[1].member_status(&a), Some(Status::Dead));
    // Contact retries ignore membership's Dead target exclusion. The first
    // exchange carries each self tombstone back to its author for refutation;
    // later full digests teach the new incarnation without reaping tombstones.
    for now in 1..=8 {
        exchange(&mut engines, Time(now));
    }
    assert_eq!(engines[0].member_status(&b), Some(Status::Alive));
    assert_eq!(engines[1].member_status(&a), Some(Status::Alive));
    assert!(engines[0].member_incarnation(&a).unwrap() > 0);
    assert!(engines[1].member_incarnation(&b).unwrap() > 0);
}
