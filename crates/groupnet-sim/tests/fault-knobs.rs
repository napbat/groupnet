//! The fault knobs take their values as given: loss is a validated
//! [`Percent`] (no clamping exists to hide a typo), and every jitter is
//! representable — `u64::MAX` included — without overflowing the schedule.

use groupnet_core::{Config, GroupEngine, GroupId, NodeId, Time};
use groupnet_sim::{Percent, PercentError, Simulation};

fn add_pair(sim: &mut Simulation) -> (NodeId, NodeId) {
    let a = NodeId::new("a");
    let b = NodeId::new("b");
    for (local, seed) in [(&a, &b), (&b, &a)] {
        let engine = GroupEngine::new(
            GroupId::new("g"),
            local.clone(),
            [seed.clone()],
            Config::default(),
        )
        .expect("valid config");
        sim.add(engine).expect("distinct node");
    }
    (a, b)
}

/// Full loss isolates the pair exactly; dropping it back to zero lets them
/// converge, so the knob applied the given share, not a corrected one.
#[test]
fn full_loss_isolates_and_zero_loss_heals() {
    let mut sim = Simulation::new(5).with_loss(Percent::HUNDRED);
    let (a, b) = add_pair(&mut sim);
    sim.run_until(Time(3_000));
    assert_eq!(sim.is_member(&a, &b), Some(false));
    assert_eq!(sim.is_member(&b, &a), Some(false));

    sim.set_loss(Percent::ZERO);
    sim.run_until(Time(6_000));
    assert_eq!(sim.is_member(&a, &b), Some(true));
    assert_eq!(sim.is_member(&b, &a), Some(true));
}

/// An out-of-range share is refused before it reaches a knob.
#[test]
fn an_out_of_range_share_is_refused_not_clamped() {
    assert_eq!(
        Percent::try_from(101_u8),
        Err(PercentError::AboveHundred(101))
    );
    assert_eq!(Percent::try_from(100_u32), Ok(Percent::HUNDRED));
}

/// The widest jitter is a valid draw range: the run neither panics nor
/// wraps a delivery into the past.
#[test]
fn maximal_jitter_never_overflows_the_schedule() {
    let mut sim = Simulation::new(5);
    sim.set_jitter(u64::MAX);
    let (a, _) = add_pair(&mut sim);
    sim.run_until(Time(3_000));
    assert!(sim.member_count(&a).is_some());
}
