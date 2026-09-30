//! Seeded builder/follower schedules for a slow origin build: a follower waits
//! for a builder whose build keeps advancing, however long it runs, and falls
//! back to its own scan only once the build stalls for a whole donor wait.

use std::collections::{BTreeMap, VecDeque};

use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapOperation, BootstrapScope, ClaimEngine, ClaimIdentity,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

const CONFIG: BootstrapConfig = BootstrapConfig {
    max_members: 8,
    max_member_bytes: 32,
    max_scope_bytes: 64,
    settle_ms: 3,
    renew_ms: 2,
    claim_ttl_ms: 6,
    observe_ms: 3,
    donor_wait_ms: 10,
    total_ms: 30,
};

fn scope() -> BootstrapScope {
    BootstrapScope {
        domain: "origin".to_owned(),
        partition: "bucket".to_owned(),
    }
}

/// One node's engine, its source view (claim, first visible time), and the
/// in-flight local build, if it is the builder.
struct Node {
    name: NodeId,
    engine: ClaimEngine,
    view: BTreeMap<NodeId, (BootstrapClaim, u64)>,
    build: Option<(BootstrapOperation, ClaimIdentity, u64)>,
    builds: u32,
    build_started: Option<u64>,
    fallback: Option<u64>,
    donor: Option<u64>,
    progress_reports: u32,
}

enum Delivery {
    Publish(BootstrapClaim),
    Withdraw(ClaimIdentity),
}

fn observe(
    node: &mut Node,
    op: BootstrapOperation,
    names: &[NodeId],
    now: u64,
) -> Vec<BootstrapEffect> {
    let members = names
        .iter()
        .map(|name| BootstrapMember {
            node: name.clone(),
            eligible: true,
        })
        .collect();
    let claims = node
        .view
        .values()
        .filter_map(|(claim, visible)| {
            let remaining = CONFIG.claim_ttl_ms.saturating_sub(now - visible);
            (remaining > 0).then(|| BootstrapClaim {
                remaining_ms: remaining,
                ..claim.clone()
            })
        })
        .collect();
    node.engine
        .step(BootstrapEvent::ClaimsObserved {
            op,
            members,
            claims,
        })
        .effects
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded two-node schedule keeps its delivery queue and assertions together"
)]
fn follower_waits_for_a_progressing_builder_and_only_a_stall_releases_it() {
    let names = [NodeId::from("node-a"), NodeId::from("node-b")];
    let (mut waited, mut released) = (0, 0);
    for seed in 0..64_u64 {
        let mut rng = SplitMix64::new(seed);
        let stalls = seed % 2 == 1;
        let pages = 40 + rng.below(40);
        let stall_after = 1 + rng.below(pages - 1);
        let mut nodes: Vec<Node> = names
            .iter()
            .zip(1_u128..)
            .map(|(name, boot)| Node {
                name: name.clone(),
                engine: ClaimEngine::new(
                    CONFIG,
                    scope(),
                    name.clone(),
                    BootId(u128::from(seed) * 4 + boot),
                    1,
                )
                .unwrap(),
                view: BTreeMap::new(),
                build: None,
                builds: 0,
                build_started: None,
                fallback: None,
                donor: None,
                progress_reports: 0,
            })
            .collect();
        let mut queue: Vec<(u64, usize, Delivery)> = Vec::new();
        let mut effects: Vec<VecDeque<BootstrapEffect>> = nodes
            .iter_mut()
            .map(|node| node.engine.step(BootstrapEvent::Start).effects.into())
            .collect();
        let mut pages_done = 0;
        let mut built_at = None;
        let mut last_progress = None;
        let mut builder = None;
        for now in 0..2_000_u64 {
            queue.retain(|(at, to, delivery)| {
                if *at > now {
                    return true;
                }
                let view = &mut nodes[*to].view;
                match delivery {
                    Delivery::Publish(claim) => {
                        view.insert(claim.identity.node.clone(), (claim.clone(), now));
                    }
                    Delivery::Withdraw(identity) => {
                        if view
                            .get(&identity.node)
                            .is_some_and(|(claim, _)| claim.identity == *identity)
                        {
                            view.remove(&identity.node);
                        }
                    }
                }
                false
            });
            for index in 0..nodes.len() {
                effects[index].extend(
                    nodes[index]
                        .engine
                        .step(BootstrapEvent::Tick(Time(now)))
                        .effects,
                );
                // The builder's scan commits pages until it completes or stalls.
                if let Some((op, selected, at)) = nodes[index].build.clone()
                    && at == now
                {
                    pages_done += 1;
                    let event = if pages_done == pages && !stalls {
                        built_at = Some(now);
                        nodes[index].build = None;
                        BootstrapEvent::Built { op, selected }
                    } else {
                        let gap = if stalls && pages_done >= stall_after {
                            u64::MAX
                        } else {
                            3 + u64::from(rng.below(7))
                        };
                        nodes[index].build = Some((op, selected.clone(), now.saturating_add(gap)));
                        BootstrapEvent::BuildProgressed { op, selected }
                    };
                    let step = nodes[index].engine.step(event);
                    assert_eq!(step.rejection, None, "seed {seed}: live build refused");
                    if pages_done < pages {
                        last_progress = Some(now);
                    }
                    effects[index].extend(step.effects);
                }
                while let Some(effect) = effects[index].pop_front() {
                    match effect {
                        BootstrapEffect::PublishClaim(claim) => {
                            nodes[index]
                                .view
                                .insert(claim.identity.node.clone(), (claim.clone(), now));
                            let other = 1 - index;
                            queue.push((
                                now + 1 + u64::from(rng.below(2)),
                                other,
                                Delivery::Publish(claim),
                            ));
                        }
                        BootstrapEffect::WithdrawClaim(identity) => {
                            nodes[index].view.remove(&identity.node);
                            queue.push((now + 1, 1 - index, Delivery::Withdraw(identity)));
                        }
                        BootstrapEffect::ObserveClaims { op, .. } => {
                            let next = observe(&mut nodes[index], op, &names, now);
                            effects[index].extend(next);
                        }
                        BootstrapEffect::BuildOrigin { op, selected } => {
                            let node = &mut nodes[index];
                            node.builds += 1;
                            node.build_started.get_or_insert(now);
                            if builder.is_none() {
                                builder = Some(index);
                                node.build =
                                    Some((op, selected, now + 3 + u64::from(rng.below(7))));
                            }
                        }
                        BootstrapEffect::BuilderProgressed => nodes[index].progress_reports += 1,
                        BootstrapEffect::DonorAvailable { .. } => {
                            nodes[index].donor.get_or_insert(now);
                        }
                        BootstrapEffect::FallbackOrigin => {
                            nodes[index].fallback.get_or_insert(now);
                        }
                        _ => {}
                    }
                }
            }
            let done = nodes.iter().any(|node| node.donor.is_some())
                || builder.is_some_and(|b| nodes[1 - b].builds > 0);
            if done {
                break;
            }
        }
        let builder = builder.unwrap_or_else(|| panic!("seed {seed}: no builder"));
        let (lead, follower) = (&nodes[builder], &nodes[1 - builder]);
        let last_progress = last_progress.expect("build progressed");
        assert!(
            follower.progress_reports > 0,
            "seed {seed}: {} never saw progress",
            follower.name
        );
        if stalls {
            // The builder's own stall bound ends it exactly one donor wait
            // after its last committed page; the follower builds no earlier,
            // and no later than one observation after that.
            assert_eq!(
                lead.fallback,
                Some(last_progress + CONFIG.donor_wait_ms),
                "seed {seed}"
            );
            assert_eq!(follower.builds, 1, "seed {seed}: follower took over");
            let takeover = follower.build_started.expect("follower built");
            let stalled = last_progress + CONFIG.donor_wait_ms;
            assert!(
                (stalled..=stalled + CONFIG.observe_ms + 2).contains(&takeover),
                "seed {seed}: takeover at {takeover}, stall from {stalled}"
            );
            released += 1;
        } else {
            let built_at = built_at.expect("progressing build completes");
            assert!(
                built_at > 2 * CONFIG.total_ms,
                "seed {seed}: schedule must outlast every fixed bound"
            );
            assert_eq!(lead.builds, 1);
            assert_eq!(
                follower.builds, 0,
                "seed {seed}: follower rescanned a live build"
            );
            assert_eq!(lead.fallback, None);
            assert_eq!(follower.fallback, None);
            assert!(
                follower.donor.is_some_and(|at| at >= built_at),
                "seed {seed}"
            );
            waited += 1;
        }
    }
    assert_eq!(waited, 32);
    assert_eq!(released, 32);
}
