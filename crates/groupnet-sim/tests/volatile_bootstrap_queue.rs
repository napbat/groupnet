//! Source-visible TTL claims and callback queues under seeded virtual time.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::volatile_bootstrap::{
    BootstrapClaim, BootstrapConfig, BootstrapEffect, BootstrapEvent, BootstrapMember,
    BootstrapOperation, BootstrapScope, ClaimEngine, ClaimIdentity,
};
use groupnet_core::{NodeId, Time};
use groupnet_sim::SplitMix64;

#[derive(Clone)]
enum Delivery {
    Tick,
    Publish(BootstrapClaim),
    Withdraw(ClaimIdentity),
    Observe(
        BootstrapOperation,
        Vec<BootstrapMember>,
        Vec<BootstrapClaim>,
    ),
    Built(BootstrapOperation, ClaimIdentity),
}

struct Queued {
    at: u64,
    recipient: usize,
    delivery: Delivery,
}

type View = BTreeMap<ClaimIdentity, (BootstrapClaim, u64)>;

#[derive(Default)]
struct Coverage {
    connected_one_builder: usize,
    partition_two_builders: usize,
    poll_callbacks: usize,
    lost_deliveries: usize,
    duplicate_deliveries: usize,
    ready_donors: usize,
    stale_callbacks: usize,
    expired_claims: usize,
    fallbacks: usize,
    builders_ever: BTreeSet<usize>,
    ready_received_this_seed: bool,
    donor_nodes_this_seed: BTreeSet<usize>,
    timely_ready_success: usize,
    healed_full_rosters: usize,
}

fn config() -> BootstrapConfig {
    BootstrapConfig {
        max_members: 3,
        max_member_bytes: 8,
        max_scope_bytes: 16,
        settle_ms: 3,
        renew_ms: 6,
        claim_ttl_ms: 12,
        observe_ms: 2,
        donor_wait_ms: 8,
        total_ms: 23,
    }
}

fn scope() -> BootstrapScope {
    BootstrapScope {
        domain: "o".to_owned(),
        partition: "b".to_owned(),
    }
}

fn connected(a: usize, b: usize, partitioned: bool, now: u64) -> bool {
    !partitioned || now >= 9 || (a == 2) == (b == 2)
}

#[expect(
    clippy::too_many_arguments,
    reason = "one queue schedule passes its explicit source views, transport faults, and coverage ledger"
)]
fn enqueue_effects(
    queue: &mut Vec<Queued>,
    views: &mut [View; 3],
    names: &[NodeId; 3],
    effects: Vec<BootstrapEffect>,
    from: usize,
    now: u64,
    rng: &mut SplitMix64,
    partitioned: bool,
    healthy: bool,
    coverage: &mut Coverage,
) {
    for effect in effects {
        match effect {
            BootstrapEffect::PublishClaim(claim) => {
                for (recipient, view) in views.iter_mut().enumerate() {
                    if !connected(from, recipient, partitioned, now) {
                        continue;
                    }
                    if recipient == from {
                        deliver_claim(view, claim.clone(), now);
                        continue;
                    }
                    // Initial convergence is guaranteed before settle. Later
                    // source deliveries can be lost, delayed, or duplicated.
                    let delay = if healthy || claim.renewal == 1 {
                        1
                    } else {
                        rng.below(3)
                    };
                    if !healthy && recipient != from && claim.renewal > 1 && rng.below(7) == 0 {
                        coverage.lost_deliveries += 1;
                        continue;
                    }
                    queue.push(Queued {
                        at: now + u64::from(delay),
                        recipient,
                        delivery: Delivery::Publish(claim.clone()),
                    });
                    if !healthy && recipient != from && claim.renewal > 1 && rng.below(5) == 0 {
                        queue.push(Queued {
                            at: now + u64::from(delay) + 1,
                            recipient,
                            delivery: Delivery::Publish(claim.clone()),
                        });
                        coverage.duplicate_deliveries += 1;
                    }
                }
            }
            BootstrapEffect::WithdrawClaim(identity) => {
                for recipient in 0..3 {
                    if connected(from, recipient, partitioned, now) {
                        queue.push(Queued {
                            at: now + 1,
                            recipient,
                            delivery: Delivery::Withdraw(identity.clone()),
                        });
                    }
                }
            }
            BootstrapEffect::ObserveClaims { op, .. } => {
                let (members, claims) = snapshot(&views[from], names, from, now, partitioned);
                if partitioned && now >= 9 && members.len() == 3 {
                    coverage.healed_full_rosters += 1;
                }
                let delay = if healthy || op.token == 1 {
                    1
                } else {
                    1 + u64::from(rng.below(2))
                };
                queue.push(Queued {
                    at: now + delay,
                    recipient: from,
                    delivery: Delivery::Observe(op, members, claims),
                });
            }
            BootstrapEffect::BuildOrigin { op, selected } => {
                coverage.builders_ever.insert(from);
                // Some builders crash. The core's finite build timeout then
                // fences that work and permits ordinary origin fallback.
                if healthy || rng.below(6) != 0 {
                    queue.push(Queued {
                        at: now + 2,
                        recipient: from,
                        delivery: Delivery::Built(op, selected),
                    });
                }
            }
            BootstrapEffect::ArmTimer(due) => {
                if !queue.iter().any(|queued| {
                    queued.at == due.0
                        && queued.recipient == from
                        && matches!(queued.delivery, Delivery::Tick)
                }) {
                    queue.push(Queued {
                        at: due.0,
                        recipient: from,
                        delivery: Delivery::Tick,
                    });
                }
            }
            BootstrapEffect::DonorAvailable { .. } => {
                coverage.ready_donors += 1;
                coverage.donor_nodes_this_seed.insert(from);
            }
            BootstrapEffect::FallbackOrigin => coverage.fallbacks += 1,
            BootstrapEffect::CancelWork { .. }
            | BootstrapEffect::FollowBuilder { .. }
            | BootstrapEffect::ObserveSelectedClaim { .. }
            | BootstrapEffect::Transfer(_) => {}
        }
    }
}

fn deliver_claim(view: &mut View, claim: BootstrapClaim, now: u64) {
    let identity = claim.identity.clone();
    let fresh_expiry = now + claim.remaining_ms;
    match view.get(&identity) {
        Some((previous, _)) if claim.renewal < previous.renewal => {}
        Some((previous, expiry)) if claim.renewal == previous.renewal => {
            // Native duplicate observation cannot extend a TTL renewal.
            view.insert(identity, (claim, *expiry));
        }
        _ => {
            view.insert(identity, (claim, fresh_expiry));
        }
    }
}

fn snapshot(
    view: &View,
    names: &[NodeId; 3],
    recipient: usize,
    now: u64,
    partitioned: bool,
) -> (Vec<BootstrapMember>, Vec<BootstrapClaim>) {
    let members = (0..3)
        .filter(|index| connected(*index, recipient, partitioned, now))
        .map(|index| BootstrapMember {
            node: names[index].clone(),
            eligible: true,
        })
        .collect();
    let claims = view
        .values()
        .filter_map(|(claim, expiry)| {
            let publisher = names.iter().position(|node| *node == claim.identity.node)?;
            (connected(publisher, recipient, partitioned, now) && *expiry > now).then(|| {
                BootstrapClaim {
                    remaining_ms: expiry - now,
                    ..claim.clone()
                }
            })
        })
        .collect();
    (members, claims)
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one seeded source-queue schedule keeps its fault injections and earned safety/liveness floors together"
)]
fn queued_native_ttl_claims_converge_and_survive_faults() {
    let names = [NodeId::from("a"), NodeId::from("b"), NodeId::from("c")];
    let mut coverage = Coverage::default();
    for seed in 0..64 {
        let mut rng = SplitMix64::new(seed);
        let partitioned = seed % 2 == 0;
        let healthy = seed % 4 == 1;
        coverage.builders_ever.clear();
        coverage.ready_received_this_seed = false;
        coverage.donor_nodes_this_seed.clear();
        let mut engines: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                ClaimEngine::new(
                    config(),
                    scope(),
                    name.clone(),
                    groupnet_core::volatile_bootstrap::BootId(u128::try_from(index + 1).unwrap()),
                    seed + 1,
                )
                .unwrap()
            })
            .collect();
        let mut views = [View::new(), View::new(), View::new()];
        let mut queue = Vec::new();
        for (index, engine) in engines.iter_mut().enumerate() {
            let step = engine.step(BootstrapEvent::Start);
            enqueue_effects(
                &mut queue,
                &mut views,
                &names,
                step.effects,
                index,
                0,
                &mut rng,
                partitioned,
                healthy,
                &mut coverage,
            );
        }
        for now in 0..=25 {
            for view in &mut views {
                view.retain(|_, (_, expiry)| {
                    let live = *expiry > now;
                    coverage.expired_claims += usize::from(!live);
                    live
                });
            }
            while let Some(position) = queue.iter().position(|message| message.at == now) {
                let message = queue.remove(position);
                let recipient = message.recipient;
                let event = match message.delivery {
                    Delivery::Tick => Some(BootstrapEvent::Tick(Time(now))),
                    Delivery::Publish(claim) => {
                        if claim.phase == groupnet_core::volatile_bootstrap::ClaimPhase::Ready
                            && claim.identity.node != names[recipient]
                            && now <= 8
                        {
                            coverage.ready_received_this_seed = true;
                        }
                        deliver_claim(&mut views[recipient], claim, now);
                        None
                    }
                    Delivery::Withdraw(identity) => {
                        views[recipient].remove(&identity);
                        None
                    }
                    Delivery::Observe(op, members, claims) => {
                        coverage.poll_callbacks += 1;
                        Some(BootstrapEvent::ClaimsObserved {
                            op,
                            members,
                            claims,
                        })
                    }
                    Delivery::Built(op, selected) => Some(BootstrapEvent::Built { op, selected }),
                };
                if let Some(event) = event {
                    if !matches!(event, BootstrapEvent::Tick(_)) {
                        let before = engines[recipient].step(BootstrapEvent::Tick(Time(now)));
                        enqueue_effects(
                            &mut queue,
                            &mut views,
                            &names,
                            before.effects,
                            recipient,
                            now,
                            &mut rng,
                            partitioned,
                            healthy,
                            &mut coverage,
                        );
                    }
                    let step = engines[recipient].step(event);
                    if step.rejection.is_some() {
                        coverage.stale_callbacks += 1;
                    }
                    enqueue_effects(
                        &mut queue,
                        &mut views,
                        &names,
                        step.effects,
                        recipient,
                        now,
                        &mut rng,
                        partitioned,
                        healthy,
                        &mut coverage,
                    );
                }
            }
            if now == 8 {
                let builders = coverage.builders_ever.len();
                if partitioned {
                    assert_eq!(
                        builders,
                        2,
                        "seed {seed}; stages {:?}; queue {}",
                        engines.iter().map(ClaimEngine::stage).collect::<Vec<_>>(),
                        queue.len()
                    );
                    coverage.partition_two_builders += 1;
                } else {
                    assert_eq!(builders, 1, "seed {seed}");
                    coverage.connected_one_builder += 1;
                }
            }
        }
        if healthy {
            assert!(coverage.ready_received_this_seed, "seed {seed}");
            assert_eq!(
                coverage.donor_nodes_this_seed.len(),
                2,
                "seed {seed}; stages {:?}; selected {:?}",
                engines.iter().map(ClaimEngine::stage).collect::<Vec<_>>(),
                engines
                    .iter()
                    .map(|engine| engine.selected().cloned())
                    .collect::<Vec<_>>()
            );
            coverage.timely_ready_success += 1;
        }
    }
    assert_eq!(coverage.connected_one_builder, 32);
    assert_eq!(coverage.partition_two_builders, 32);
    assert!(coverage.poll_callbacks >= 100);
    assert!(coverage.lost_deliveries >= 5);
    assert!(coverage.duplicate_deliveries >= 5);
    assert!(coverage.ready_donors >= 10);
    assert!(coverage.timely_ready_success >= 10);
    assert!(coverage.healed_full_rosters >= 5);
    assert!(coverage.stale_callbacks >= 5);
    assert!(coverage.expired_claims >= 5);
    assert!(coverage.fallbacks >= 5);
}
