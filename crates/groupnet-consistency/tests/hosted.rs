//! The Hosted write path (feature `hosted`), over real nodes on the in-memory
//! transport: the **write path's own surface**.
//!
//! The two rules are proved as pure predicates in the crate's own unit tests,
//! and S5 belongs to the simulator. What only this layer can prove is the half
//! that lives in the shells: that a real election, a real gossiped ledger and a
//! real feed compose into the bargain the tier advertises.
//!
//! * a roster elects, the host **recovers before it serves**, and a
//!   `QuorumApplied` write commits in an ack round;
//! * a voter that votes without applying is **named** by the write it holds up,
//!   and the write times out rather than resolving on its silence;
//! * the fence tracks the hostship exactly, and a starved incumbent learns it as
//!   `Deposed`;
//! * a minority of the roster never gets a host, and says so with the promised
//!   `NoLeader` shape rather than hanging.
//!
//! The **migration** family — the lineage a hand-over produces, the successor's
//! recovery gate, and the cut a serving host takes on its predecessor — is next
//! door in `hosted_migration.rs`, on its own copy of this file's `Voter`
//! harness: duplicated helpers across sibling test files is the house pattern
//! (`groupnet-sim`'s `election.rs` / `election_failover.rs`).
//!
//! Every wait is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep. The groups here run the **storage-free** Quorum posture (no
//! `GrantStore`), so every election is charged the engine's boot blackout — one
//! `LEASE_MS` — which is what makes `SETTLE` as loose as it is.

#![cfg(feature = "hosted")]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_consistency::hosted::{commit_applied_by, commit_reading};
use groupnet_consistency::{
    CAP_HOSTED, Commit, CommitLedger, CommitOutcome, Completeness, HostedError, HostedRead,
    HostedReads, HostedWrites, WriteToken,
};
use groupnet_core::{Activation, HostedConfig, NodeId, VoterRoster};
use groupnet_runtime::{Group, GroupProfile, Leadership, Node, Role};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
use groupnet_transport_mem::{MemTransport, Network};
use tokio::task::JoinHandle;

/// The poll budget for every assertion here. Looser than the harness default and
/// than `quorum.rs`'s, because the longest chain in this file is a whole
/// migration *plus* a recovery: detect the dead host, burn the voters' grant
/// promise, close a new epoch, then read a fresh majority out of gossip. A
/// genuine regression still reports in seconds.
const SETTLE: Duration = Duration::from_secs(10);

/// A brisk gossip cadence, so grant rounds, renewals and ledger republishes all
/// happen in wall-clock milliseconds. It also sets the driver's tick period
/// (~7 ms here), which [`LEASE_MS`] is two orders of magnitude above — the
/// sizing rule `HostedConfig` states.
const GOSSIP_MS: u64 = 15;

/// A host's authority after its last confirmed renewal round, and — in the
/// storage-free posture these tests run — also the boot blackout a voter sits
/// out before it will grant a new claimant. Comfortably under the 900 ms
/// detection window the default probe timings give a group of three.
const LEASE_MS: u64 = 600;

/// The hosted feed's ring. Far larger than any test's write count, so no
/// assertion here is ever confounded by a ring-overflow `Gap`.
const RING: usize = 64;

/// The deadline a healthy committed write is given. It is a *bound*, not an
/// expectation — the assertions below check the write landed well inside it.
const PATIENT: Duration = Duration::from_secs(2);

/// The deadline a write that can never commit is given. Long enough that a
/// healthy round would have closed many times over, short enough that the
/// fail-closed tests stay quick.
const IMPATIENT: Duration = Duration::from_millis(400);

const fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("nonzero")
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

/// A Quorum profile over `voters`, storage-free (see the module docs).
fn quorum_profile(voters: &[&str]) -> GroupProfile {
    GroupProfile::hosted(HostedConfig {
        activation: Activation::Quorum {
            voters: VoterRoster::new(voters.iter().map(|v| NodeId::new(*v))),
        },
        lease_ms: LEASE_MS,
    })
}

/// The leadership every live node agrees on, or `None` while it is still
/// settling: the same `(epoch, host)` everywhere, and exactly one [`Role::Host`]
/// — asserted as one indivisible predicate so a poll can never catch half of it.
fn agreed(groups: &[&Group]) -> Option<Leadership> {
    let first = groups.first()?.leadership();
    first.host.as_ref()?;
    let all: Vec<Leadership> = groups.iter().map(|g| g.leadership()).collect();
    if all
        .iter()
        .any(|l| l.epoch != first.epoch || l.host != first.host)
    {
        return None;
    }
    (all.iter().filter(|l| l.role == Role::Host).count() == 1).then_some(first)
}

/// What `member`'s gossiped ledger says it has applied of `writer`'s feed, as
/// `observer` currently sees it.
fn applied(observer: &Group, member: &NodeId, writer: &NodeId) -> Option<WriteToken> {
    commit_applied_by(observer, member, writer).map(|(_, token)| token)
}

/// The leadership epoch `member`'s ledger is stamped with, as `observer` sees
/// it — the freshness half of both rules.
fn stamp(observer: &Group, member: &NodeId) -> Option<u64> {
    commit_reading(observer, member).map(|reading| reading.lead_epoch)
}

/// One node's whole participation in the tier: the group, its commit ledger, its
/// follower loop, and its write path.
///
/// A voter that must *stop applying* here does so by dropping the loop
/// ([`Voter::stop_following`]) rather than losing its subscription: what the
/// contract is written about is a node that votes without applying, and this
/// file's families only ever need it to stop for good. (The migration file's
/// copy carries the resumable gate instead, because a stall that ends is what
/// makes a recovery gate observable.)
struct Voter {
    id: NodeId,
    net: Network,
    node: Option<Node<MemTransport>>,
    group: Option<Group>,
    ledger: Option<Arc<CommitLedger>>,
    writes: Option<Arc<HostedWrites<String>>>,
    apply: Option<JoinHandle<()>>,
}

impl Voter {
    /// Brings one node up, joined to `group` under the Quorum profile over
    /// `voters`, advertising [`CAP_HOSTED`] — but not yet following.
    fn spawn(net: &Network, group: &str, id: &str, seeds: &[&str], voters: &[&str]) -> Self {
        let opts = NodeOpts::new(group)
            .gossip_interval_ms(GOSSIP_MS)
            .group_profile(quorum_profile(voters));
        let (id, node, handle) = spawn_mem_node(net, id, seeds, &opts);
        handle
            .advertise_capabilities([CAP_HOSTED])
            .expect("the advertisement is enqueued");
        let ledger = Arc::new(CommitLedger::new(handle.clone()));
        let writes = HostedWrites::committed(
            handle.clone(),
            id.clone(),
            cap(RING),
            |key: &String| key.clone().into_bytes(),
            Arc::clone(&ledger),
        )
        .expect("a Quorum group supports the committed regime");
        Self {
            id,
            net: net.clone(),
            node: Some(node),
            group: Some(handle),
            ledger: Some(ledger),
            writes: Some(Arc::new(writes)),
            apply: None,
        }
    }

    fn group(&self) -> &Group {
        self.group.as_ref().expect("this voter is alive")
    }

    fn writes(&self) -> &HostedWrites<String> {
        self.writes.as_ref().expect("this voter is alive")
    }

    fn ledger(&self) -> &Arc<CommitLedger> {
        self.ledger.as_ref().expect("this voter is alive")
    }

    /// Starts the follower loop the deployment contract asks of every voter:
    /// apply, then [`CommitLedger::record`]; on a migration,
    /// [`CommitLedger::refresh`].
    fn follow(&mut self) {
        assert!(self.apply.is_none(), "already following");
        let mut reads = HostedReads::new(self.group().clone(), self.id.clone(), decode);
        // The builder step the contracted loop asks for: when this node is
        // admitted to serve, its own lineage is cut there, so a predecessor's
        // late tail dies instead of landing behind this host's own writes.
        self.writes().bind(&mut reads);
        let ledger = Arc::clone(self.ledger());
        self.apply = Some(tokio::spawn(async move {
            let mut host: Option<NodeId> = None;
            while let Some(event) = reads.next().await {
                match event {
                    HostedRead::Wrote {
                        host: writer,
                        token,
                        ..
                    } => {
                        // The apply happens here, before the record — that
                        // ordering is the whole meaning of a watermark.
                        ledger.record(&writer, token).await;
                    }
                    HostedRead::Gap { missed_through } => {
                        // Coarse remediation would go here; the watermark it
                        // raises belongs to the lineage's host, which the
                        // preceding `Migrated` named.
                        if let Some(host) = &host {
                            ledger.record(host, missed_through).await;
                        }
                    }
                    HostedRead::Migrated { host: adopted, .. } => {
                        host = adopted;
                        ledger.refresh().await;
                    }
                }
            }
        }));
    }

    /// Stops applying for good.
    fn stop_following(&mut self) {
        if let Some(apply) = self.apply.take() {
            apply.abort();
        }
    }

    /// Kills the node outright, endpoint and all.
    ///
    /// Dropping the handles is not enough on its own: the node's receive loop
    /// owns an `Arc` of the same inner state, so the actors keep ticking until
    /// the endpoint is evicted from the `Network` (registering the id again
    /// replaces the sender and closes the old inbox).
    async fn kill(&mut self) {
        if let Some(apply) = self.apply.take() {
            apply.abort();
            let _ = apply.await;
        }
        self.writes = None;
        self.ledger = None;
        self.group = None;
        self.node = None;
        drop(self.net.endpoint(self.id.clone()));
    }
}

/// Brings `ids` up as an all-to-all Quorum cluster, each node a voter, none of
/// them following yet.
fn spawn_roster(net: &Network, group: &str, ids: &[&str]) -> Vec<Voter> {
    ids.iter()
        .map(|id| {
            let seeds: Vec<&str> = ids.iter().copied().filter(|other| other != id).collect();
            Voter::spawn(net, group, id, &seeds, ids)
        })
        .collect()
}

/// The live members' group handles, for the convergence and agreement helpers.
fn live(voters: &[Voter]) -> Vec<&Group> {
    voters.iter().filter_map(|v| v.group.as_ref()).collect()
}

/// Brings the roster up, starts every follower loop, and waits for an epoch to
/// close and its host to finish recovering. Returns the agreed leadership and
/// the host's index.
async fn elected(voters: &mut [Voter]) -> (Leadership, usize) {
    for voter in voters.iter_mut() {
        voter.follow();
    }
    converged_within(&live(voters), SETTLE).await;
    eventually_within("every voter to see the whole hosted roster", SETTLE, || {
        live(voters)
            .iter()
            .all(|g| g.members_with_capability(CAP_HOSTED).len() == voters.len())
    })
    .await;
    eventually_within("the roster to close an epoch", SETTLE, || {
        agreed(&live(voters)).is_some()
    })
    .await;
    let lead = agreed(&live(voters)).expect("agreed just above");
    let host = lead.host.clone().expect("agreement requires a named host");
    let index = voters
        .iter()
        .position(|v| v.id == host)
        .expect("the host is one of ours");
    eventually_within("the host to finish recovering", SETTLE, || {
        voters[index].writes().fence().is_some()
    })
    .await;
    (lead, index)
}

/// The healthy path, end to end: three voters, one host, a recovery that
/// completes, and a `QuorumApplied` write that costs an ack round.
#[tokio::test]
async fn a_recovered_host_serves_and_commits_at_quorum_applied() {
    const GROUP: &str = "hosted-happy";
    const IDS: [&str; 3] = ["hh-a", "hh-b", "hh-c"];

    let net = Network::new();
    let mut voters = spawn_roster(&net, GROUP, &IDS);
    let (lead, host) = elected(&mut voters).await;
    let host_id = voters[host].id.clone();
    let others: Vec<usize> = (0..IDS.len()).filter(|i| *i != host).collect();

    // The fence is the hostship, named. Only the host has one.
    let fence = voters[host].writes().fence().expect("recovered just above");
    assert_eq!(fence.epoch, lead.epoch);
    assert_eq!(fence.host, host_id);
    assert_eq!(fence.to_string(), format!("{}:{host_id}", lead.epoch));
    for &other in &others {
        assert_eq!(
            voters[other].writes().fence(),
            None,
            "a follower holds no authority to stamp anything with"
        );
    }
    assert_eq!(
        voters[host].writes().recovery(),
        Some(Completeness::Complete)
    );

    // A `Local` write: in the feed, and nobody is waited on.
    let local = voters[host]
        .writes()
        .publish(&"local-1".to_owned())
        .await
        .expect("the host serves");
    assert_eq!(
        local,
        WriteToken {
            epoch: lead.epoch,
            seq: 1
        },
        "the feed life is the hostship: its epoch is the leadership epoch, \
         and it sequences from one"
    );

    // A `QuorumApplied` write: acknowledged once a majority of the roster has
    // applied it.
    let started = Instant::now();
    let receipt = voters[host]
        .writes()
        .publish_committed(&"k1".to_owned(), Commit::QuorumApplied, PATIENT)
        .await
        .expect("the host serves");
    let elapsed = started.elapsed();
    assert_eq!(receipt.outcome, CommitOutcome::Committed);
    assert!(receipt.is_committed());
    assert_eq!(receipt.token.seq, 2, "the same feed life, one write on");
    assert!(
        elapsed < PATIENT / 4,
        "the healthy path costs an ack round, not a deadline (took {elapsed:?})"
    );

    // …and it committed because the majority genuinely applied it: the host
    // counts its own write, and both followers converge on it.
    let observer = voters[host].group().clone();
    assert_eq!(
        applied(&observer, &host_id, &host_id),
        Some(receipt.token),
        "a host that publishes has applied — its own ledger says so"
    );
    for &other in &others {
        let member = voters[other].id.clone();
        eventually_within(
            "every follower to apply the committed write",
            SETTLE,
            || applied(&observer, &member, &host_id) == Some(receipt.token),
        )
        .await;
        assert_eq!(
            stamp(&observer, &member),
            Some(lead.epoch),
            "…stamped with the epoch it was applied under, which is what makes \
             it count"
        );
    }

    // A follower is told where to go, not left to guess.
    let follower_lead = voters[others[0]].group().leadership();
    let refused = voters[others[0]]
        .writes()
        .publish(&"not-mine".to_owned())
        .await
        .expect_err("a follower may not write");
    assert_eq!(
        refused,
        HostedError::NotHost {
            epoch: follower_lead.epoch,
            host: Some(host_id.clone()),
        }
    );
    assert_eq!(
        refused.to_string(),
        format!(
            "not the host at epoch {}: {host_id} is",
            follower_lead.epoch
        )
    );
}

/// The fail-closed surface: a voter that votes but never applies is invisible to
/// the commit rule, and the tier says so by **name** rather than resolving on
/// its silence.
///
/// Both halves of the bargain are here. `AllApplied` needs unanimity, so one
/// silent member is enough to hold a write up; `QuorumApplied` needs a majority,
/// so it tolerates exactly one straggler out of three and no more.
#[tokio::test]
async fn a_voter_that_stops_applying_is_named_by_the_write_it_holds_up() {
    const GROUP: &str = "hosted-fail-closed";
    const IDS: [&str; 3] = ["hf-a", "hf-b", "hf-c"];

    let net = Network::new();
    let mut voters = spawn_roster(&net, GROUP, &IDS);
    let (_lead, host) = elected(&mut voters).await;
    let others: Vec<usize> = (0..IDS.len()).filter(|i| *i != host).collect();
    let (quiet, still_applying) = (others[0], others[1]);

    // Baseline: with everybody applying, both levels close.
    for level in [Commit::QuorumApplied, Commit::AllApplied] {
        let receipt = voters[host]
            .writes()
            .publish_committed(&"base".to_owned(), level, PATIENT)
            .await
            .expect("the host serves");
        assert_eq!(receipt.outcome, CommitOutcome::Committed, "{level:?}");
    }

    // One voter stops applying. Its node stays up, it keeps voting, and its
    // ledger keeps its (now stale) watermark on the wire — the fail-slow shape.
    voters[quiet].stop_following();
    let quiet_id = voters[quiet].id.clone();

    // `AllApplied` is unanimity over the alive, advertised members: one silent
    // member is the whole difference, and the outcome names it.
    let receipt = voters[host]
        .writes()
        .publish_committed(&"all".to_owned(), Commit::AllApplied, IMPATIENT)
        .await
        .expect("the host serves");
    assert_eq!(
        receipt.outcome,
        CommitOutcome::TimedOut {
            waiting_on: vec![quiet_id.clone()],
        },
        "the level that waits for everyone must not resolve on a silence"
    );
    assert!(
        !receipt.is_committed(),
        "a timeout carries no guarantee, and says so"
    );

    // `QuorumApplied` over three still closes: the host counts its own write and
    // one follower is still applying, which is the majority.
    let receipt = voters[host]
        .writes()
        .publish_committed(&"quorum".to_owned(), Commit::QuorumApplied, PATIENT)
        .await
        .expect("the host serves");
    assert_eq!(
        receipt.outcome,
        CommitOutcome::Committed,
        "a majority tolerates exactly one straggler out of three — that is what \
         it is bought for"
    );

    // The second one stops too, and now there is no majority to be had.
    voters[still_applying].stop_following();
    let quiet_too = voters[still_applying].id.clone();
    let receipt = voters[host]
        .writes()
        .publish_committed(&"stuck".to_owned(), Commit::QuorumApplied, IMPATIENT)
        .await
        .expect("the host serves");
    let mut expected = vec![quiet_id, quiet_too];
    expected.sort();
    assert_eq!(
        receipt.outcome,
        CommitOutcome::TimedOut {
            waiting_on: expected,
        },
        "both silent voters are named, in id order — the operational signal"
    );
    // The write is still in the feed, and the caller still has its name: that is
    // the point of returning a receipt rather than an error.
    assert_eq!(receipt.token.epoch, voters[host].group().leadership().epoch);
}

/// The fence's whole lifecycle, on one handle: absent before an election, absent
/// while merely a follower, present and exact while serving, and gone the
/// instant the hostship is — with the writes that follow reported as `Deposed`
/// rather than politely redirected.
///
/// The deposition is arranged without a partition: starve the incumbent of its
/// voters and, under the CP posture, it cannot renew, so it steps down. That is
/// the minority side of a partition seen from the inside, and the only shape in
/// which a *live* node observes its own fencing.
#[tokio::test]
async fn the_fence_tracks_the_hostship_and_a_starved_incumbent_is_deposed() {
    const GROUP: &str = "hosted-fence";
    const IDS: [&str; 3] = ["hx-a", "hx-b", "hx-c"];

    let net = Network::new();
    let mut voters = spawn_roster(&net, GROUP, &IDS);
    // Before anything is elected there is no authority anywhere.
    for voter in &voters {
        assert_eq!(voter.writes().fence(), None);
        assert_eq!(voter.writes().recovery(), None, "nothing to recover into");
    }

    let (lead, host) = elected(&mut voters).await;
    let host_id = voters[host].id.clone();
    let others: Vec<usize> = (0..IDS.len()).filter(|i| *i != host).collect();

    let fence = voters[host].writes().fence().expect("serving");
    assert_eq!(
        fence.epoch,
        voters[host].group().leadership().epoch,
        "the fence names the epoch the watch names, and nothing else"
    );
    assert_eq!(fence.host, host_id);
    for &other in &others {
        assert_eq!(voters[other].writes().fence(), None);
    }
    // It really is authority: a write goes through under it.
    voters[host]
        .writes()
        .publish(&"under-fence".to_owned())
        .await
        .expect("the host serves");

    // --- Starve it: both voters die, so no renewal round can close. ---
    for &other in &others {
        voters[other].kill().await;
    }
    eventually_within("the starved incumbent to step down", SETTLE, || {
        voters[host].group().leadership().host.is_none()
    })
    .await;

    let after = voters[host].group().leadership();
    assert_eq!(after.epoch, lead.epoch, "a demotion keeps the epoch");
    assert_eq!(after.role, Role::Follower);
    assert_eq!(
        voters[host].writes().fence(),
        None,
        "a host that cannot renew holds no fence — the whole point of the lease"
    );
    assert_eq!(
        voters[host]
            .writes()
            .publish(&"after-the-fall".to_owned())
            .await
            .expect_err("a fenced host must not write"),
        HostedError::Deposed { epoch: lead.epoch },
        "a node that *was* the host is told it was fenced, not redirected: \
         `NotHost` would invite it to retry somewhere, and there is nowhere"
    );
    assert_eq!(
        voters[host].writes().recovery(),
        None,
        "and it has no hostship to recover into any more"
    );
}

/// The minority freeze, partition-free: a group whose roster majority is simply
/// not running never elects, and the write path says so with the promised
/// `NoLeader` shape — `NotHost { host: None }` — rather than hanging on a host
/// that a minority cannot produce.
///
/// The non-vacuity is the second half: restore the majority and the very same
/// handle gets a host.
#[tokio::test]
async fn a_minority_of_the_roster_never_gets_a_host_and_says_so() {
    const GROUP: &str = "hosted-minority";
    const IDS: [&str; 3] = ["hn-a", "hn-b", "hn-c"];
    /// Polls the freeze must survive: at 20 ms each, comfortably past the
    /// engine's boot blackout and several claim windows.
    const FROZEN_POLLS: usize = 50;

    let net = Network::new();
    // One of three. It can claim all it likes; it can never collect two grants.
    let mut alone = Voter::spawn(&net, GROUP, IDS[0], &IDS[1..], &IDS);
    alone.follow();

    let refused = alone
        .writes()
        .publish(&"nobody-home".to_owned())
        .await
        .expect_err("a minority has no host to write through");
    assert_eq!(
        refused,
        HostedError::NotHost {
            epoch: 0,
            host: None
        },
        "the `NoLeader` the activation-policy table promised a minority side"
    );
    assert_eq!(refused.to_string(), "no host at epoch 0");

    // It stays that way: not "not yet", but "not without a majority".
    let mut polls = 0usize;
    eventually_within("the minority to burn several claim windows", SETTLE, || {
        assert_eq!(
            alone.group().leadership(),
            Leadership {
                epoch: 0,
                host: None,
                role: Role::Follower,
            },
            "a minority activated a host after {polls} polls"
        );
        assert_eq!(alone.writes().fence(), None);
        polls += 1;
        polls >= FROZEN_POLLS
    })
    .await;
    assert_eq!(
        alone
            .writes()
            .publish(&"still-nobody".to_owned())
            .await
            .expect_err("still a minority"),
        HostedError::NotHost {
            epoch: 0,
            host: None
        }
    );

    // --- Restore the majority. ---
    let mut second = Voter::spawn(&net, GROUP, IDS[1], &[IDS[0]], &IDS);
    second.follow();
    let pair = [alone, second];
    converged_within(&live(&pair), SETTLE).await;
    eventually_within("two of three to close an epoch", SETTLE, || {
        agreed(&live(&pair)).is_some()
    })
    .await;

    // Whichever of them won, the handle that was frozen now has a real answer:
    // it serves, or it names the peer that does. Never `NoLeader` again.
    let lead = agreed(&live(&pair)).expect("agreed just above");
    let host = lead.host.clone().expect("a named host");
    let index = pair
        .iter()
        .position(|v| v.id == host)
        .expect("the host is one of the two");
    eventually_within("the new host to finish recovering", SETTLE, || {
        pair[index].writes().fence().is_some()
    })
    .await;
    let token = pair[index]
        .writes()
        .publish(&"quorum-restored".to_owned())
        .await
        .expect("a majority elected it");
    assert_eq!(token.epoch, lead.epoch);

    let other = 1 - index;
    assert_eq!(
        pair[other]
            .writes()
            .publish(&"redirect-me".to_owned())
            .await
            .expect_err("the other one is a follower"),
        HostedError::NotHost {
            epoch: pair[other].group().leadership().epoch,
            host: Some(host),
        },
        "a redirect, which is emphatically not the NoLeader it used to get"
    );
}
