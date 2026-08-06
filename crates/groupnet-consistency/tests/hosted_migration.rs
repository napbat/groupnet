//! The Hosted write path (feature `hosted`) across a **migration**, over real
//! nodes on the in-memory transport.
//!
//! A hand-over is where every part of this tier meets at once: the engine closes
//! a new epoch, the successor's write path refuses to serve until the recovery
//! rule is satisfied, and every subscriber has to be told that the group's
//! authority changed writer. The properties this file holds:
//!
//! * killing the host is a migration, and to a subscriber it is exactly one
//!   `Migrated`, exactly one `Gap`, then the successor's writes — and the
//!   successor recovers before it serves;
//! * a host-elect whose own apply loop is stalled answers `Recovering`, and
//!   serves the moment it is not;
//! * a host that *serves* cuts its predecessor's lineage at that instant
//!   (`HostedWrites::bind`), so the dead host's un-replicated tail dies instead
//!   of being applied behind the successor's own writes — while a voter that
//!   never served applies it, which is the drain-window divergence the tier
//!   documents and the next lineage's `Gap` is there to reconcile.
//!
//! The write path's own surface — the healthy commit, the fail-closed voter, the
//! fence lifecycle, the minority freeze — is next door in `hosted.rs`, whose
//! `Voter` harness this file copies: duplicated helpers across sibling test
//! files is the house pattern (`groupnet-sim`'s `election.rs` /
//! `election_failover.rs`).
//!
//! Every wait is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep. The groups here run the **storage-free** Quorum posture (no
//! `GrantStore`), so every election is charged the engine's boot blackout — one
//! `LEASE_MS` — which is what makes `SETTLE` as loose as it is.

#![cfg(feature = "hosted")]

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_consistency::hosted::{commit_applied_by, commit_reading, hosted_feed_name};
use groupnet_consistency::{
    CAP_HOSTED, Commit, CommitLedger, CommitOutcome, Completeness, HostedError, HostedRead,
    HostedReads, HostedWrites, WriteToken, advertised_head_named,
};
use groupnet_core::{Activation, HostedConfig, NodeId, VoterRoster, placement};
use groupnet_runtime::{Group, GroupProfile, Leadership, Node, Role};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
use groupnet_transport_mem::{MemTransport, Network};
use tokio::sync::watch;
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

const fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("nonzero")
}

fn decode(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

/// Rendezvous ranking of `ids` for `group`, best first — the order the engine's
/// claim guard reads, so `ranked(..)[0]` is the node that will bid and
/// `ranked(..)[1]` is the one that inherits when it dies.
fn ranked(group: &str, ids: &[&str]) -> Vec<NodeId> {
    let members: Vec<(NodeId, u32)> = ids.iter().map(|id| (NodeId::new(*id), 1)).collect();
    placement::owners(group, &members, ids.len())
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

/// One event a follower loop observed, flattened for assertion.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Wrote(NodeId, WriteToken),
    Gap(WriteToken),
    Migrated(u64, Option<NodeId>),
}

/// One node's whole participation in the tier: the group, its commit ledger, its
/// follower loop, and its write path.
///
/// The follower loop runs behind a **gate** rather than being spawned and
/// aborted, because a restarted [`HostedReads`] would start at every peer feed's
/// current end (history is not replayed) — so aborting it is "this node lost its
/// subscription", while closing the gate is "this node stopped applying", which
/// is the failure the tier's contract is written about.
struct Voter {
    id: NodeId,
    net: Network,
    node: Option<Node<MemTransport>>,
    group: Option<Group>,
    ledger: Option<Arc<CommitLedger>>,
    writes: Option<Arc<HostedWrites<String>>>,
    gate: watch::Sender<bool>,
    apply: Option<JoinHandle<()>>,
    log: Arc<Mutex<Vec<Seen>>>,
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
            gate: watch::channel(true).0,
            apply: None,
            log: Arc::default(),
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

    fn is_host(&self) -> bool {
        self.group().leadership().role == Role::Host
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
        let log = Arc::clone(&self.log);
        let mut gate = self.gate.subscribe();
        self.apply = Some(tokio::spawn(async move {
            let mut host: Option<NodeId> = None;
            loop {
                while !*gate.borrow_and_update() {
                    if gate.changed().await.is_err() {
                        return;
                    }
                }
                let event = tokio::select! {
                    biased;
                    // Closing the gate pre-empts an in-flight poll, so a stall
                    // stops the loop at the instant it is asked to rather than
                    // one event later. `HostedReads::next` is cancel-safe, so
                    // nothing is lost by the cancellation.
                    _ = gate.changed() => continue,
                    event = reads.next() => event,
                };
                let Some(event) = event else { return };
                match event {
                    HostedRead::Wrote {
                        host: writer,
                        token,
                        ..
                    } => {
                        log.lock()
                            .expect("log")
                            .push(Seen::Wrote(writer.clone(), token));
                        // The apply happens here, before the record — that
                        // ordering is the whole meaning of a watermark.
                        ledger.record(&writer, token).await;
                    }
                    HostedRead::Gap { missed_through } => {
                        log.lock().expect("log").push(Seen::Gap(missed_through));
                        // Coarse remediation would go here; the watermark it
                        // raises belongs to the lineage's host, which the
                        // preceding `Migrated` named.
                        if let Some(host) = &host {
                            ledger.record(host, missed_through).await;
                        }
                    }
                    HostedRead::Migrated {
                        epoch,
                        host: adopted,
                    } => {
                        log.lock()
                            .expect("log")
                            .push(Seen::Migrated(epoch, adopted.clone()));
                        host = adopted;
                        ledger.refresh().await;
                    }
                }
            }
        }));
    }

    /// Stops applying without losing the subscription — the fail-slow voter the
    /// tier's contract is written about.
    fn stall(&self) {
        self.gate.send_replace(false);
    }

    /// Resumes applying. The feed entries are state, so the loop reconciles
    /// against what is current rather than replaying a log.
    fn resume(&self) {
        self.gate.send_replace(true);
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

    /// Everything this node's follower loop has observed.
    fn seen(&self) -> Vec<Seen> {
        self.log.lock().expect("log").clone()
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
/// Killing the host is a **migration**, and to a subscriber it is exactly one
/// `Migrated`, exactly one `Gap`, and then the successor's writes. The successor
/// recovers before it serves, and the write it then publishes commits.
#[tokio::test]
async fn killing_the_host_migrates_the_lineage_and_the_successor_recovers() {
    const GROUP: &str = "hosted-migrate";
    const IDS: [&str; 3] = ["hm-a", "hm-b", "hm-c"];

    let net = Network::new();
    // Spawned in rendezvous order, so index 0 is the node that will bid and
    // index 1 is the one that inherits when it dies.
    let rank = ranked(GROUP, &IDS);
    let order: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    let mut voters = spawn_roster(&net, GROUP, &order);
    let (first, host) = elected(&mut voters).await;
    assert_eq!(
        host, 0,
        "the top-ranked live candidate is the one that bids"
    );
    let old_host = voters[0].id.clone();
    let (successor, observer) = (1usize, 2usize);

    // Three writes, committed at the level that promises they survive this.
    let mut last = WriteToken { epoch: 0, seq: 0 };
    for n in 0..3 {
        let receipt = voters[0]
            .writes()
            .publish_committed(&format!("pre-{n}"), Commit::QuorumApplied, PATIENT)
            .await
            .expect("the host serves");
        assert_eq!(receipt.outcome, CommitOutcome::Committed);
        last = receipt.token;
    }
    // Both survivors caught up before the kill, so the successor's recovery
    // target is one it already meets. (A majority commit only guarantees *one*
    // of them; this barrier is the test making the migration deterministic, not
    // the tier requiring it.)
    let watcher = voters[observer].group().clone();
    for index in [successor, observer] {
        let member = voters[index].id.clone();
        eventually_within("both survivors to apply the whole prefix", SETTLE, || {
            applied(&watcher, &member, &old_host) == Some(last)
        })
        .await;
    }
    let mark = voters[observer].seen().len();

    // --- The host dies. ---
    voters[0].kill().await;

    eventually_within("the successor to take the group", SETTLE, || {
        voters[successor].is_host()
    })
    .await;
    let second = voters[successor].group().leadership();
    assert!(
        second.epoch > first.epoch,
        "a migration takes a strictly higher epoch: {second:?} after {first:?}"
    );
    eventually_within("the successor to finish recovering", SETTLE, || {
        voters[successor].writes().fence().is_some()
    })
    .await;

    // …and then it serves, at the level that needs a majority of the roster —
    // which, with one voter dead, is the successor and the observer.
    let receipt = voters[successor]
        .writes()
        .publish_committed(&"post-1".to_owned(), Commit::QuorumApplied, PATIENT)
        .await
        .expect("the successor serves");
    assert_eq!(receipt.outcome, CommitOutcome::Committed);
    assert_eq!(
        receipt.token,
        WriteToken {
            epoch: second.epoch,
            seq: 1
        },
        "a fresh feed life at the new leadership epoch, sequencing from one"
    );

    // The subscriber's view of all that: one migration, one gap, then writes.
    eventually_within("the observer to see the successor's write", SETTLE, || {
        voters[observer]
            .seen()
            .iter()
            .any(|e| matches!(e, Seen::Wrote(_, t) if t.epoch == second.epoch))
    })
    .await;
    let after: Vec<Seen> = voters[observer].seen().split_off(mark);
    assert_clean_handover(&after, second.epoch, &voters[successor].id);
}

/// The shape a migration must have at a subscriber: the hand-over announced
/// first, then exactly one gap opening the new lineage, then nothing but the new
/// host's new-epoch writes.
///
/// The gap is `(epoch, 0)` — nothing of the *new* epoch was missed — and because
/// tokens order epoch-major, advancing a frontier or a watermark to it covers
/// every token of every earlier hostship in one step. That is what makes the
/// coarse remediation a consumer already implements for a writer restart the
/// whole of its migration handling too.
fn assert_clean_handover(after: &[Seen], epoch: u64, host: &NodeId) {
    let migrations: Vec<&Seen> = after
        .iter()
        .filter(|e| matches!(e, Seen::Migrated(..)))
        .collect();
    assert_eq!(
        migrations,
        vec![&Seen::Migrated(epoch, Some(host.clone()))],
        "exactly one hand-over, naming the successor: {after:?}"
    );
    let gaps: Vec<&Seen> = after.iter().filter(|e| matches!(e, Seen::Gap(_))).collect();
    assert_eq!(
        gaps,
        vec![&Seen::Gap(WriteToken { epoch, seq: 0 })],
        "exactly one gap, opening the new lineage: {after:?}"
    );
    assert_eq!(
        after.iter().position(|e| matches!(e, Seen::Migrated(..))),
        Some(0),
        "the migration is announced before anything is delivered under it"
    );
    assert!(
        after
            .iter()
            .skip_while(|e| !matches!(e, Seen::Gap(_)))
            .skip(1)
            .all(|e| matches!(e, Seen::Wrote(writer, t) if writer == host && t.epoch == epoch)),
        "after the gap, only the new host's new-epoch writes: {after:?}"
    );
}

/// The cut a **serving** host takes on its own lineage — the one the "first
/// delivered write of the new lineage" rule can never take for it, because
/// `HostedReads` excludes this node's own feed and its own writes therefore
/// never arrive to close the predecessor's.
///
/// The predecessor's un-replicated tail is real, gossiped state: it sits in the
/// survivors' entry views long after the write path stopped counting it, and a
/// follower loop that applied it *after* its own hostship began would order a
/// fenced epoch-`e` write behind the authority's own epoch-`e′` ones. Here the
/// successor serves first and the tail is offered second — and dies unapplied,
/// because `HostedWrites::bind` cut the lineage at the instant service opened.
///
/// The voter that never served is the control, and it diverges: it applies the
/// same tail, because its lineage is still the dead host's. That is the
/// drain-window divergence the honesty box documents — harmless for a consumer
/// that treats the next lineage's `Gap` as the authoritative rebuild it is.
/// The arrangement the cut is tested against, on a roster whose host is index 0.
///
/// Returns `(prefix, tail)`: a committed write both survivors have **applied**,
/// and a later one both merely **hold in gossip**. The two survivors stop
/// applying before the tail is authored, so it reaches their entry views and no
/// further — which is what makes it an unacked tail no recovery target will ever
/// name, and therefore state a successor is entitled to drop.
async fn commit_then_strand_a_tail(
    voters: &[Voter],
    (successor, observer): (usize, usize),
) -> (WriteToken, WriteToken) {
    let old_host = voters[0].id.clone();
    let receipt = voters[0]
        .writes()
        .publish_committed(&"pre".to_owned(), Commit::QuorumApplied, PATIENT)
        .await
        .expect("the host serves");
    assert_eq!(receipt.outcome, CommitOutcome::Committed);
    let prefix = receipt.token;
    let watcher = voters[observer].group().clone();
    for index in [successor, observer] {
        let member = voters[index].id.clone();
        eventually_within("both survivors to apply the prefix", SETTLE, || {
            applied(&watcher, &member, &old_host) == Some(prefix)
        })
        .await;
    }

    // Both survivors stop applying, and the host writes on. Those writes reach
    // the survivors' *entry views* through ordinary gossip and are delivered to
    // neither: the unacked tail a migration is entitled to lose.
    voters[successor].stall();
    voters[observer].stall();
    let mut tail = prefix;
    for n in 0..2 {
        tail = voters[0]
            .writes()
            .publish(&format!("tail-{n}"))
            .await
            .expect("the host serves");
    }
    assert!(tail > prefix, "the tail is above the committed prefix");
    for index in [successor, observer] {
        let group = voters[index].group().clone();
        eventually_within(
            "the tail to reach the survivors' entry views",
            SETTLE,
            || advertised_head_named(&hosted_feed_name(""), &group, &old_host) == Some(tail),
        )
        .await;
    }
    (prefix, tail)
}

#[tokio::test]
async fn a_serving_host_cuts_its_predecessors_late_tail() {
    const GROUP: &str = "hosted-cut";
    const IDS: [&str; 3] = ["hk-a", "hk-b", "hk-c"];
    /// The cadence the stand-in re-stamper republishes at: brisk enough that
    /// the successor's recovery is not what this test spends its time on.
    const RESTAMP: Duration = Duration::from_millis(20);

    let net = Network::new();
    // Rendezvous order again: index 0 bids, index 1 inherits.
    let rank = ranked(GROUP, &IDS);
    let order: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    let mut voters = spawn_roster(&net, GROUP, &order);
    let (first, host) = elected(&mut voters).await;
    assert_eq!(host, 0);
    let old_host = voters[0].id.clone();
    let (successor, observer) = (1usize, 2usize);
    let (prefix, tail) = commit_then_strand_a_tail(&voters, (successor, observer)).await;

    // --- The host dies with that tail unapplied everywhere. ---
    voters[0].kill().await;

    // The stalled loops cannot re-stamp themselves, so a task stands in for the
    // `Migrated` handler they are not running. `refresh` is the freshness half
    // of the deployment contract and it is all a recovering host needs from a
    // voter that has nothing new to apply — which is exactly the point: neither
    // reading names the tail, so it is no part of the recovery target.
    let restamp = tokio::spawn({
        let ledgers = [
            Arc::clone(voters[successor].ledger()),
            Arc::clone(voters[observer].ledger()),
        ];
        async move {
            loop {
                for ledger in &ledgers {
                    ledger.refresh().await;
                }
                tokio::time::sleep(RESTAMP).await;
            }
        }
    });
    eventually_within("the successor to take the group", SETTLE, || {
        voters[successor].is_host()
    })
    .await;
    let second = voters[successor].group().leadership();
    assert!(second.epoch > first.epoch);
    eventually_within("the successor to finish recovering", SETTLE, || {
        voters[successor].writes().fence().is_some()
    })
    .await;
    restamp.abort();
    assert_eq!(
        voters[successor].writes().recovery(),
        Some(Completeness::Complete),
        "it serves on two fresh readings, neither of which names the tail"
    );

    // --- Both loops resume. The tail is offered to both. ---
    let mark = voters[successor].seen().len();
    voters[successor].resume();
    voters[observer].resume();

    let observer_ledger = Arc::clone(voters[observer].ledger());
    eventually_within(
        "the voter that never served to apply the tail",
        SETTLE,
        || observer_ledger.applied(&old_host) == Some(tail),
    )
    .await;
    eventually_within(
        "the successor's own loop to turn after resuming",
        SETTLE,
        || {
            voters[successor].seen()[mark..]
                .iter()
                .any(|event| matches!(event, Seen::Migrated(epoch, _) if *epoch == second.epoch))
        },
    )
    .await;

    // The serving host dropped it where the follower applied it.
    let after: Vec<Seen> = voters[successor].seen().split_off(mark);
    assert!(
        !after
            .iter()
            .any(|event| matches!(event, Seen::Wrote(_, token) if token.epoch == first.epoch)),
        "a serving host applied its predecessor's fenced tail: {after:?}"
    );
    assert_eq!(
        voters[successor].ledger().applied(&old_host),
        Some(prefix),
        "its applied state is exactly what recovery proved, and no more"
    );
    assert_ne!(
        observer_ledger.applied(&old_host),
        voters[successor].ledger().applied(&old_host),
        "the two really were offered the same tail: one applied it, one cut it"
    );
}

/// The recovery gate, made observable. A host-elect whose own apply loop is
/// stalled cannot prove leader completeness — it has not even re-stamped its own
/// view — so it refuses service with `Recovering` rather than serializing writes
/// against state it cannot see. Unstall it and it serves.
///
/// This is the honest shape of "elected but not ready", and the reason `M4`
/// gates *hosted service* rather than the engine's activation: the engine says
/// host the instant it is one, and the write path is what waits.
#[tokio::test]
async fn a_host_elect_that_has_not_caught_up_refuses_service_until_it_has() {
    const GROUP: &str = "hosted-recovering";
    const IDS: [&str; 3] = ["hr-a", "hr-b", "hr-c"];

    let net = Network::new();
    let rank = ranked(GROUP, &IDS);
    let order: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    let mut voters = spawn_roster(&net, GROUP, &order);
    let (first, host) = elected(&mut voters).await;
    assert_eq!(host, 0);
    let old_host = voters[0].id.clone();
    let (successor, observer) = (1usize, 2usize);

    let receipt = voters[0]
        .writes()
        .publish_committed(&"pre".to_owned(), Commit::QuorumApplied, PATIENT)
        .await
        .expect("the host serves");
    assert_eq!(receipt.outcome, CommitOutcome::Committed);
    let watcher = voters[observer].group().clone();
    for index in [successor, observer] {
        let member = voters[index].id.clone();
        eventually_within("both survivors to apply the prefix", SETTLE, || {
            applied(&watcher, &member, &old_host) == Some(receipt.token)
        })
        .await;
    }

    // --- The heir stops applying, then inherits. ---
    voters[successor].stall();
    voters[0].kill().await;
    eventually_within("the stalled heir to take the group", SETTLE, || {
        voters[successor].is_host()
    })
    .await;
    let second = voters[successor].group().leadership();
    assert!(second.epoch > first.epoch);

    // The engine says host. The write path does not.
    assert_eq!(
        voters[successor]
            .writes()
            .publish(&"too-soon".to_owned())
            .await
            .expect_err("an unrecovered host must not serialize anything"),
        HostedError::Recovering,
        "the gate binds `Local` too: a host with no view of its own state has \
         nothing to order a write against"
    );
    assert_eq!(
        voters[successor].writes().fence(),
        None,
        "and it holds no fence to stamp an external store with either"
    );
    assert_eq!(
        voters[successor].writes().recovery(),
        Some(Completeness::Recovering { needed: Vec::new() }),
        "an empty `needed` is not almost-there: no fresh majority has been read \
         at all, because this node has not re-stamped its own view"
    );
    assert_eq!(
        stamp(voters[observer].group(), &voters[successor].id),
        Some(first.epoch),
        "…which is exactly what the gossiped ledger shows: a stale stamp"
    );

    // --- Unstall. It re-stamps, reads a fresh majority, and serves. ---
    voters[successor].resume();
    eventually_within("the heir to finish recovering", SETTLE, || {
        voters[successor].writes().fence().is_some()
    })
    .await;
    let fence = voters[successor].writes().fence().expect("recovered");
    assert_eq!(fence.epoch, second.epoch);
    assert_eq!(fence.host, voters[successor].id);

    let receipt = voters[successor]
        .writes()
        .publish_committed(&"served".to_owned(), Commit::QuorumApplied, PATIENT)
        .await
        .expect("the heir serves now");
    assert_eq!(receipt.outcome, CommitOutcome::Committed);
}
