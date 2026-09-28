//! Facade exposes typed state-sync APIs only with the opt-in feature.

#![cfg(feature = "consistency-replication")]

use groupnet::consistency::replication::{CatchUp, Limits};

#[test]
fn facade_reexports_bounded_native_floor_outcomes() {
    assert!(Limits::default().validate().is_ok());
    let outcome = CatchUp::Ready(42_u64);
    assert_eq!(outcome, CatchUp::Ready(42));
}
