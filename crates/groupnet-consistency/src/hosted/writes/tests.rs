//! The shell's pure corpus: the admission table, the feed names, the setup
//! errors.

use groupnet_core::NodeId;
use groupnet_runtime::{Leadership, Role};

use super::{HostedSetupError, admit, hosted_feed_name};
use crate::hosted::HostedError;

fn node(name: &str) -> NodeId {
    NodeId::new(name)
}

fn me() -> NodeId {
    node("me")
}

/// The leadership watch's report, built the way the driver builds it — so a
/// row of the table below can never disagree with the runtime about what
/// `Role::Host` means.
fn lead(epoch: u64, host: Option<&str>) -> Leadership {
    let host = host.map(node);
    let role = if host.as_ref() == Some(&me()) {
        Role::Host
    } else {
        Role::Follower
    };
    Leadership { epoch, host, role }
}

#[test]
fn the_admission_table() {
    type Case = (
        &'static str,
        Leadership,
        Option<u64>,
        bool,
        Result<u64, HostedError>,
    );
    let cases: Vec<Case> = vec![
        (
            "the host of a recovered epoch serves it",
            lead(7, Some("me")),
            Some(7),
            true,
            Ok(7),
        ),
        (
            "…and a host that has never hosted before is no different",
            lead(7, Some("me")),
            None,
            true,
            Ok(7),
        ),
        (
            "the host of an unrecovered epoch is elected, not serving",
            lead(7, Some("me")),
            None,
            false,
            Err(HostedError::Recovering),
        ),
        (
            "a node that never held the group is redirected",
            lead(7, Some("peer")),
            None,
            true,
            Err(HostedError::NotHost {
                epoch: 7,
                host: Some(node("peer")),
            }),
        ),
        (
            "…and with no host at all, that redirect is the promised NoLeader",
            lead(7, None),
            None,
            true,
            Err(HostedError::NotHost {
                epoch: 7,
                host: None,
            }),
        ),
        (
            "a host succeeded by a peer is deposed from its own epoch",
            lead(9, Some("peer")),
            Some(7),
            true,
            Err(HostedError::Deposed { epoch: 7 }),
        ),
        (
            "…and a host that lapsed into (e, None) likewise: it is fenced, \
             not merely hostless",
            lead(7, None),
            Some(7),
            true,
            Err(HostedError::Deposed { epoch: 7 }),
        ),
        (
            "a host elected, deposed, and elected again serves the new epoch",
            lead(9, Some("me")),
            Some(7),
            true,
            Ok(9),
        ),
        (
            "the recovery gate binds a re-elected host too",
            lead(9, Some("me")),
            Some(7),
            false,
            Err(HostedError::Recovering),
        ),
        (
            "before any election, a fresh node is a NoLeader at epoch zero",
            lead(0, None),
            None,
            true,
            Err(HostedError::NotHost {
                epoch: 0,
                host: None,
            }),
        ),
    ];
    for (name, leadership, last_hosted, recovered, expected) in cases {
        assert_eq!(
            admit(&leadership, &me(), last_hosted, recovered),
            expected,
            "{name}"
        );
    }
}

/// The gate is only ever consulted for the node's *own* hostship, so a
/// follower's verdict cannot depend on it — the property that lets
/// `admit_now` skip the roster sweep entirely when this node is not host.
#[test]
fn the_recovery_gate_never_changes_a_non_hosts_verdict() {
    for leadership in [lead(4, Some("peer")), lead(4, None)] {
        for last_hosted in [None, Some(3)] {
            assert_eq!(
                admit(&leadership, &me(), last_hosted, true),
                admit(&leadership, &me(), last_hosted, false),
                "{leadership:?} / {last_hosted:?}"
            );
        }
    }
}

#[test]
fn write_paths_map_to_distinct_feeds() {
    assert_eq!(hosted_feed_name(""), "hosted");
    assert_eq!(hosted_feed_name("docs"), "hosted:docs");
    assert_ne!(hosted_feed_name("a"), hosted_feed_name("b"));
    // Distinct from the session tier's default feed: a hosted path and a
    // plain one coexist on one node.
    assert_ne!(hosted_feed_name(""), "");
}

#[test]
fn setup_errors_say_which_configuration_is_wrong() {
    assert!(
        HostedSetupError::NotHosted
            .to_string()
            .contains("not a Hosted group")
    );
    assert!(
        HostedSetupError::NotQuorum
            .to_string()
            .contains("static voter roster")
    );
    let mismatch = HostedSetupError::LedgerMismatch {
        expected: "~hosted:applied:docs".to_owned(),
        found: "~hosted:applied".to_owned(),
    };
    assert_eq!(
        mismatch.to_string(),
        "the commit ledger publishes under ~hosted:applied, but this write path reads \
         ~hosted:applied:docs"
    );
    assert_ne!(mismatch, HostedSetupError::NotQuorum);
}
