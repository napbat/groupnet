//! Unit tests for [`Config`](super::Config) and the Hosted-mode types.

use super::{Activation, Config, GroupMode, HostedConfig, VoterRoster};
use crate::NodeId;

fn roster(ids: &[&str]) -> VoterRoster {
    VoterRoster::new(ids.iter().map(|id| NodeId::new(*id)))
}

/// A group is a plain metadata/membership group unless it is explicitly
/// opted in — the Eventual contract is the default, in both spellings.
#[test]
fn eventual_is_the_default_mode() {
    assert_eq!(Config::default().mode, GroupMode::Eventual);
    assert_eq!(GroupMode::default(), GroupMode::Eventual);
}

/// The mode rides `Config`'s struct-update syntax like every other knob,
/// and a Hosted config compares by value.
#[test]
fn hosted_mode_carries_its_activation_and_lease() {
    let hosted = HostedConfig {
        activation: Activation::Settle {
            claim_settle_ms: 600,
        },
        lease_ms: 2_000,
    };
    let cfg = Config {
        mode: GroupMode::Hosted(hosted.clone()),
        ..Config::default()
    };
    assert_eq!(cfg.mode, GroupMode::Hosted(hosted));
    assert_ne!(cfg.mode, GroupMode::Eventual);
    // Everything else is untouched by opting in.
    assert_eq!(
        cfg.detection_window_ms(3),
        Config::default().detection_window_ms(3)
    );
}

/// A Quorum activation rides `GroupMode` exactly as a Settle one does, and
/// the two are never equal — the roster is part of the group's identity,
/// not a detail hanging off it.
#[test]
fn quorum_mode_carries_its_roster_and_lease() {
    let hosted = HostedConfig {
        activation: Activation::Quorum {
            voters: roster(&["a", "b", "c"]),
        },
        lease_ms: 2_000,
    };
    let cfg = Config {
        mode: GroupMode::Hosted(hosted.clone()),
        ..Config::default()
    };
    assert_eq!(cfg.mode, GroupMode::Hosted(hosted));
    assert_ne!(
        cfg.mode,
        GroupMode::Hosted(HostedConfig {
            activation: Activation::Settle {
                claim_settle_ms: 600
            },
            lease_ms: 2_000,
        })
    );
    assert_ne!(
        cfg.mode,
        GroupMode::Hosted(HostedConfig {
            activation: Activation::Quorum {
                voters: roster(&["a", "b", "d"]),
            },
            lease_ms: 2_000,
        }),
        "a different roster is a different mode"
    );
}

/// An External activation rides `GroupMode` exactly as the other two do,
/// and is never equal to either of them — the way an epoch is closed is
/// part of a group's identity, and a driver reading the mode must be able
/// to tell "go win an anchor round" from "wait out a window".
#[test]
fn external_mode_carries_its_steal_margin_and_lease() {
    let hosted = HostedConfig {
        activation: Activation::External {
            steal_margin_ms: 500,
        },
        lease_ms: 2_000,
    };
    let cfg = Config {
        mode: GroupMode::Hosted(hosted.clone()),
        ..Config::default()
    };
    assert_eq!(cfg.mode, GroupMode::Hosted(hosted));
    assert_ne!(cfg.mode, GroupMode::Eventual);
    assert_ne!(
        cfg.mode,
        GroupMode::Hosted(HostedConfig {
            activation: Activation::Settle {
                claim_settle_ms: 500
            },
            lease_ms: 2_000,
        })
    );
    assert_ne!(
        cfg.mode,
        GroupMode::Hosted(HostedConfig {
            activation: Activation::Quorum {
                voters: roster(&["a"]),
            },
            lease_ms: 2_000,
        })
    );
    assert_ne!(
        cfg.mode,
        GroupMode::Hosted(HostedConfig {
            activation: Activation::External {
                steal_margin_ms: 501
            },
            lease_ms: 2_000,
        }),
        "a different steal margin is a different mode"
    );
    // Opting in changes nothing about the base fabric's timings.
    assert_eq!(
        cfg.detection_window_ms(3),
        Config::default().detection_window_ms(3)
    );
}

/// A zero steal margin is a legal configuration — it says "these clocks
/// are perfectly agreed", which is unwise rather than invalid, and the
/// type must not quietly rewrite it into something else. The rule it feeds
/// is tabulated in [`crate::anchor`].
#[test]
fn a_zero_steal_margin_is_representable() {
    let cfg = Config {
        mode: GroupMode::Hosted(HostedConfig {
            activation: Activation::External { steal_margin_ms: 0 },
            lease_ms: 2_000,
        }),
        ..Config::default()
    };
    let GroupMode::Hosted(hosted) = &cfg.mode else {
        panic!("hosted");
    };
    assert_eq!(
        hosted.activation,
        Activation::External { steal_margin_ms: 0 }
    );
    assert_eq!(hosted.lease_ms, 2_000);
}

/// The majority table, spelled out: a strict majority, so any two
/// majorities of one roster share a voter.
#[test]
fn majority_is_a_strict_majority_of_the_roster() {
    let ids = ["a", "b", "c", "d", "e"];
    for (voters, want) in [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3)] {
        let roster = roster(&ids[..voters]);
        assert_eq!(roster.len(), voters);
        assert!(!roster.is_empty());
        assert_eq!(roster.majority(), want, "{voters} voters");
        // Strictly more than half, which is what makes two majorities of
        // the same roster intersect.
        assert!(2 * roster.majority() > roster.len());
    }
}

/// An empty roster asks for one grant it can never collect, so the group
/// simply never activates a host. Fail-safe by arithmetic — no panic, and
/// emphatically not a zero threshold that would activate on nobody's say-so.
#[test]
fn an_empty_roster_is_unattainable_rather_than_unanimous() {
    let empty = roster(&[]);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.majority(), 1);
    assert!(
        empty.majority() > empty.len(),
        "an empty roster's majority must be unattainable"
    );
    assert!(!empty.contains(&NodeId::new("a")));
    assert_eq!(empty.iter().count(), 0);
}

/// The roster is canonical: order-insensitive and duplicate-insensitive,
/// so every member of a group derives the same roster (and the same
/// majority) from the same set of ids however it was written down.
#[test]
fn a_roster_is_canonical_in_order_and_multiplicity() {
    let canonical = roster(&["a", "b", "c"]);
    for spelling in [
        &["a", "b", "c"][..],
        &["c", "b", "a"][..],
        &["b", "a", "c", "b"][..],
        &["a", "a", "b", "c", "c", "c"][..],
    ] {
        let built = roster(spelling);
        assert_eq!(built, canonical, "{spelling:?} is the same roster");
        assert_eq!(built.len(), 3, "{spelling:?} names three distinct voters");
        assert_eq!(built.majority(), 2);
        assert_eq!(
            built.iter().cloned().collect::<Vec<_>>(),
            ["a", "b", "c"].map(NodeId::new).to_vec(),
            "{spelling:?} iterates in canonical order"
        );
    }
    for id in &canonical {
        assert!(canonical.contains(id));
    }
    assert!(!canonical.contains(&NodeId::new("d")));
    assert_ne!(canonical, roster(&["a", "b"]));
}

/// A bigger group means a longer round-robin pass before the detector
/// reaches any one member, so the window may never shrink as members are
/// added — and at the default timings it strictly grows.
#[test]
fn detection_window_is_monotone_in_members() {
    let cfg = Config::default();
    let mut prev = cfg.detection_window_ms(0);
    for members in 1..=64usize {
        let window = cfg.detection_window_ms(members);
        assert!(
            window >= prev,
            "window shrank from {prev} to {window} at {members} members"
        );
        if members >= 2 {
            assert!(
                window > prev,
                "window did not grow from {prev} to {window} at {members} members"
            );
        }
        prev = window;
    }
}

/// The reason every step is charged the silent price: over one pass of the
/// ring, with `F` *other* members gone quiet at the same moment, the
/// detector's worst path to a verdict is `(members - 2 - F)·(i + t)` live
/// steps, `F·(i + 2t)` steps stalled behind a silent peer's direct miss
/// *and* its indirect re-arm, the target's own `i + 2t` slot, and `s`. The
/// window must cover every `F`, not only the `F = 0` single-failure case —
/// the pre-fix formula covered it only up to `F = 1`.
///
/// This is the *one-pass* algebra; whether the ring reaches the target in
/// one pass is the separate assumption the rustdoc states (and the
/// simulator probes), not something arithmetic can settle.
#[test]
fn window_covers_a_one_pass_ring_with_any_number_of_silent_members() {
    for cfg in [
        Config::default(),
        Config {
            probe_interval_ms: 40,
            probe_timeout_ms: 250, // timeout > interval: slots get skipped
            suspect_timeout_ms: 90,
            ..Config::default()
        },
    ] {
        let (i, t, s) = (
            cfg.probe_interval_ms,
            cfg.probe_timeout_ms,
            cfg.suspect_timeout_ms,
        );
        for members in 2..=32u64 {
            let window = cfg.detection_window_ms(usize::try_from(members).expect("a small count"));
            // `F` ranges over every possibility, up to everyone else being
            // silent too (`members - 2`).
            for silent in 0..=members - 2 {
                let worst =
                    (members - 2 - silent) * (i + t) + silent * (i + 2 * t) + (i + 2 * t) + s;
                assert!(
                    window >= worst,
                    "{members}-node window {window} is under the {worst}ms worst case \
                         with {silent} other members silent"
                );
            }
        }
    }
}

/// The smallest real detection: one peer, so the whole path is one probe
/// slot, the unanswered probe, and the refutation window. The bound must
/// cover at least that.
#[test]
fn two_node_window_covers_one_probe_plus_suspicion() {
    for cfg in [
        Config::default(),
        Config {
            probe_interval_ms: 40,
            probe_timeout_ms: 250, // timeout > interval: slots get skipped
            suspect_timeout_ms: 90,
            ..Config::default()
        },
    ] {
        let floor = cfg.probe_interval_ms + cfg.probe_timeout_ms + cfg.suspect_timeout_ms;
        assert!(
            cfg.detection_window_ms(2) >= floor,
            "2-node window {} is below the {floor}ms floor",
            cfg.detection_window_ms(2)
        );
    }
}

/// The defaults must land somewhere a consumer can actually use: under a
/// second for a small cluster, and still seconds — not minutes — at fifty.
#[test]
fn default_window_is_sane_at_realistic_cluster_sizes() {
    let cfg = Config::default();
    // 2 · (100 + 2·50) + 500
    assert_eq!(cfg.detection_window_ms(3), 900);
    assert!(
        (500..2_000).contains(&cfg.detection_window_ms(2)),
        "a 2-node window should be sub-2s, got {}",
        cfg.detection_window_ms(2)
    );
    assert!(
        cfg.detection_window_ms(50) < 30_000,
        "a 50-node window should stay in seconds, not minutes, got {}",
        cfg.detection_window_ms(50)
    );
}

/// Degenerate inputs never panic and never wrap into a too-small window.
#[test]
fn absurd_inputs_saturate_instead_of_wrapping() {
    let cfg = Config {
        probe_interval_ms: u64::MAX,
        probe_timeout_ms: u64::MAX,
        suspect_timeout_ms: u64::MAX,
        ..Config::default()
    };
    assert_eq!(cfg.detection_window_ms(usize::MAX), u64::MAX);
    // A group with no peers has nothing to detect: no probe steps, so the
    // bound degenerates to the refutation window rather than to garbage.
    assert_eq!(
        Config::default().detection_window_ms(1),
        Config::default().suspect_timeout_ms
    );
}
