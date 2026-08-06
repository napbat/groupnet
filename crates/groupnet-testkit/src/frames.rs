//! Shared fixtures for the sans-IO engine tests: build an engine, hand-assemble
//! wire frames, and read summaries back out of the effects it returns.
//!
//! Dependency-free by design — these helpers touch nothing but
//! [`groupnet_core`], so any crate can use them without dragging an async
//! runtime into its test graph.

use groupnet_core::{
    Activation, Config, Effect, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId,
    RecoveredGrant, Status, Time, VoterRoster, placement, wire,
};

/// The group every fixture frame and fixture engine belongs to. Tests are
/// single-group, so one well-known id keeps senders and receivers agreeing.
pub const TEST_GROUP: &str = "g";

/// [`TEST_GROUP`]'s placement ranking of `ids`, best-ranked first — the same
/// rendezvous order the derived coordinator, the claim guard, and the
/// equal-epoch fencing tiebreak all read.
#[must_use]
pub fn rank_by_placement(ids: &[&str]) -> Vec<NodeId> {
    let members: Vec<(NodeId, u32)> = ids.iter().map(|id| (NodeId::new(*id), 1)).collect();
    placement::owners(TEST_GROUP, &members, ids.len())
}

/// Teaches `engine` that `peers` exist and are alive, via one digest frame from
/// the first of them.
///
/// # Panics
/// If `peers` is empty — there would be no plausible sender for the digest.
pub fn learn_peers(engine: &mut GroupEngine, peers: &[NodeId], now: Time) {
    let digest = peers
        .iter()
        .map(|p| ndigest(p.as_str(), 0, Status::Alive, 0))
        .collect();
    let from = peers.first().cloned().expect("at least one peer");
    engine.on_message(from, &digest_frame(digest, vec![]), now);
}

/// The election frames `effects` sends, as `(recipient, body)` pairs. Digest
/// and probe traffic is filtered out — an election assertion reads the election
/// wire only.
#[must_use]
pub fn election_frames(effects: &[Effect]) -> Vec<(NodeId, wire::LeadBody)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Send { to, wire } => {
                let frame = wire::decode(wire)?;
                frame.lead.map(|body| (to.clone(), body))
            }
            _ => None,
        })
        .collect()
}

/// The grants `effects` puts on the wire: `(recipient, epoch, claimant,
/// granter)`.
#[must_use]
pub fn grant_frames(effects: &[Effect]) -> Vec<(NodeId, u64, NodeId, NodeId)> {
    election_frames(effects)
        .into_iter()
        .filter_map(|(to, body)| match body {
            wire::LeadBody::Grant {
                epoch,
                claimant,
                granter,
            } => Some((to, epoch, claimant, granter)),
            _ => None,
        })
        .collect()
}

/// The claims `effects` puts on the wire: `(recipient, epoch, claimant)`.
#[must_use]
pub fn claim_frames(effects: &[Effect]) -> Vec<(NodeId, u64, NodeId)> {
    election_frames(effects)
        .into_iter()
        .filter_map(|(to, body)| match body {
            wire::LeadBody::Claim { epoch, claimant } => Some((to, epoch, claimant)),
            _ => None,
        })
        .collect()
}

/// The adopted pairs `effects` puts on the wire: `(recipient, epoch, host)`.
#[must_use]
pub fn state_frames(effects: &[Effect]) -> Vec<(NodeId, u64, Option<NodeId>)> {
    election_frames(effects)
        .into_iter()
        .filter_map(|(to, body)| match body {
            wire::LeadBody::State { epoch, host } => Some((to, epoch, host)),
            _ => None,
        })
        .collect()
}

/// The write-ahead grant persists `effects` asks for, in emission order — the
/// order itself is the [`Effect::PersistGrant`] contract.
#[must_use]
pub fn persisted_grants(effects: &[Effect]) -> Vec<(u64, NodeId)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::PersistGrant { epoch, claimant } => Some((*epoch, claimant.clone())),
            _ => None,
        })
        .collect()
}

/// The leadership transitions `effects` announces, in order.
#[must_use]
pub fn leadership_changes(effects: &[Effect]) -> Vec<(u64, Option<NodeId>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::LeadershipChanged { epoch, host } => Some((*epoch, host.clone())),
            _ => None,
        })
        .collect()
}

/// An engine for node `id` seeded with `seeds`, in [`TEST_GROUP`] at the
/// default [`Config`].
#[must_use]
pub fn engine(id: &str, seeds: &[&str]) -> GroupEngine {
    GroupEngine::new(
        GroupId::new(TEST_GROUP),
        NodeId::new(id),
        seeds.iter().map(|s| NodeId::new(*s)),
        Config::default(),
    )
}

/// The default [`Config`] opted into [`GroupMode::Hosted`] with
/// [`Activation::Settle`] — what [`hosted_engine`] is built from, exposed so a
/// test can vary the constructor without re-spelling the mode.
#[must_use]
pub fn settle_config(claim_settle_ms: u64, lease_ms: u64) -> Config {
    Config {
        mode: GroupMode::Hosted(HostedConfig {
            activation: Activation::Settle { claim_settle_ms },
            lease_ms,
        }),
        ..Config::default()
    }
}

/// The default [`Config`] opted into [`GroupMode::Hosted`] with
/// [`Activation::Quorum`] over `voters` — what [`hosted_quorum_engine`] is
/// built from.
#[must_use]
pub fn quorum_config(voters: &[&str], lease_ms: u64) -> Config {
    Config {
        mode: GroupMode::Hosted(HostedConfig {
            activation: Activation::Quorum {
                voters: VoterRoster::new(voters.iter().map(|v| NodeId::new(*v))),
            },
            lease_ms,
        }),
        ..Config::default()
    }
}

/// An engine for node `id` seeded with `seeds`, in [`TEST_GROUP`], opted into
/// [`GroupMode::Hosted`] with [`Activation::Settle`] — the election fixture.
///
/// Everything else is the default [`Config`], so a hosted engine and an
/// [`engine`] differ by exactly the mode, which is what makes the "an
/// `Eventual` group runs no election" assertions meaningful.
#[must_use]
pub fn hosted_engine(id: &str, seeds: &[&str], claim_settle_ms: u64, lease_ms: u64) -> GroupEngine {
    GroupEngine::new(
        GroupId::new(TEST_GROUP),
        NodeId::new(id),
        seeds.iter().map(|s| NodeId::new(*s)),
        settle_config(claim_settle_ms, lease_ms),
    )
}

/// An engine for node `id` seeded with `seeds`, in [`TEST_GROUP`], built with
/// [`GroupEngine::with_recovered`] over `config` — the voter-durability
/// fixture.
///
/// Pair it with [`quorum_config`] for the posture `recovered` actually means
/// something in, and with [`settle_config`] or a plain [`Config::default`] to
/// pin that it means nothing anywhere else.
#[must_use]
pub fn recovered_engine(
    id: &str,
    seeds: &[&str],
    config: Config,
    recovered: RecoveredGrant,
) -> GroupEngine {
    GroupEngine::with_recovered(
        GroupId::new(TEST_GROUP),
        NodeId::new(id),
        seeds.iter().map(|s| NodeId::new(*s)),
        config,
        recovered,
    )
}

/// An engine for node `id` seeded with `seeds`, in [`TEST_GROUP`], opted into
/// [`GroupMode::Hosted`] with [`Activation::Quorum`] over `voters` — the
/// CP-activation fixture.
///
/// Differs from [`hosted_engine`] by exactly the activation policy, so a test
/// can hold everything else fixed and vary only the way an epoch is closed.
/// `lease_ms` is both the lease and — under Quorum — the claim window, the boot
/// guard, and the post-boot **grant blackout**, which is why there is no
/// separate settle knob here. A test that wants a voter able to grant from the
/// first instant builds it through [`recovered_engine`] with
/// [`RecoveredGrant::none`] instead.
#[must_use]
pub fn hosted_quorum_engine(
    id: &str,
    seeds: &[&str],
    voters: &[&str],
    lease_ms: u64,
) -> GroupEngine {
    GroupEngine::new(
        GroupId::new(TEST_GROUP),
        NodeId::new(id),
        seeds.iter().map(|s| NodeId::new(*s)),
        quorum_config(voters, lease_ms),
    )
}

/// A **started** [`hosted_quorum_engine`] for `id` over the roster `voters`,
/// already knowing `peers` as live members — the fixture every Quorum suite
/// opens with.
///
/// Started at [`Time::ZERO`], so the boot guard and the grant blackout both
/// lapse at `lease_ms`: a test that wants either of them spent ticks at or
/// past that instant, and one that wants them standing ticks before it.
#[must_use]
pub fn quorum_voter_engine(
    id: &NodeId,
    peers: &[NodeId],
    voters: &[NodeId],
    lease_ms: u64,
) -> GroupEngine {
    let ids: Vec<&str> = voters.iter().map(NodeId::as_str).collect();
    let mut engine = hosted_quorum_engine(id.as_str(), &[], &ids, lease_ms);
    if !peers.is_empty() {
        learn_peers(&mut engine, peers, Time::ZERO);
    }
    engine.start(Time::ZERO);
    engine
}

/// The member summaries listed across all digest frames in `effects`.
#[must_use]
pub fn digest_summaries(effects: &[Effect]) -> Vec<NodeId> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Send { wire, .. } => wire::decode(wire),
            _ => None,
        })
        .filter(|f| f.kind == wire::Kind::Digest)
        .flat_map(|f| f.digest.into_iter().map(|d| d.node))
        .collect()
}

/// A digest frame (liveness summaries + metadata) — how liveness and
/// metadata now disseminate.
#[must_use]
pub fn digest_frame(digest: Vec<wire::NodeDigest>, metadata: Vec<wire::MetaDelta>) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind: wire::Kind::Digest,
        group: GroupId::new(TEST_GROUP),
        target: None,
        digest,
        wants: Vec::new(),
        members: Vec::new(),
        metadata,
        lead: None,
    })
}

/// A delta frame (member entries) — how per-node state now disseminates.
#[must_use]
pub fn delta_frame(members: Vec<wire::MemberDelta>) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind: wire::Kind::Delta,
        group: GroupId::new(TEST_GROUP),
        target: None,
        digest: Vec::new(),
        wants: Vec::new(),
        members,
        metadata: Vec::new(),
        lead: None,
    })
}

/// A bare probe frame of `kind` (`Ping`, `PingReq`, `Ack`, `IndirectAck`),
/// optionally naming the probe's `target`.
#[must_use]
pub fn probe_frame(kind: wire::Kind, target: Option<NodeId>) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind,
        group: GroupId::new(TEST_GROUP),
        target,
        digest: Vec::new(),
        wants: Vec::new(),
        members: Vec::new(),
        metadata: Vec::new(),
        lead: None,
    })
}

/// An election frame of `kind` carrying `body` — the one place the fixtures
/// build a Hosted-mode frame, so the kind and the body can never disagree.
fn lead_frame(kind: wire::Kind, body: wire::LeadBody) -> Vec<u8> {
    wire::encode(&wire::Frame {
        kind,
        group: GroupId::new(TEST_GROUP),
        target: None,
        digest: Vec::new(),
        wants: Vec::new(),
        members: Vec::new(),
        metadata: Vec::new(),
        lead: Some(body),
    })
}

/// A `LeadClaim` frame: `claimant` bidding for the host role at `epoch`.
#[must_use]
pub fn lead_claim_frame(epoch: u64, claimant: &str) -> Vec<u8> {
    lead_frame(
        wire::Kind::LeadClaim,
        wire::LeadBody::Claim {
            epoch,
            claimant: NodeId::new(claimant),
        },
    )
}

/// A `LeadGrant` frame: `granter` endorsing `claimant` for `epoch`.
#[must_use]
pub fn lead_grant_frame(epoch: u64, claimant: &str, granter: &str) -> Vec<u8> {
    lead_frame(
        wire::Kind::LeadGrant,
        wire::LeadBody::Grant {
            epoch,
            claimant: NodeId::new(claimant),
            granter: NodeId::new(granter),
        },
    )
}

/// A `LeadState` repair frame: the sender's current `(epoch, host)` belief.
/// `host` is `None` when the sender holds no host for that epoch.
#[must_use]
pub fn lead_state_frame(epoch: u64, host: Option<&str>) -> Vec<u8> {
    lead_frame(
        wire::Kind::LeadState,
        wire::LeadBody::State {
            epoch,
            host: host.map(NodeId::new),
        },
    )
}

/// One liveness-only digest summary for `node`.
#[must_use]
pub fn ndigest(node: &str, inc: u64, status: Status, max_version: u64) -> wire::NodeDigest {
    wire::NodeDigest {
        node: NodeId::new(node),
        incarnation: inc,
        status: status.to_wire(),
        max_version,
        // Empty holdings hash to zero; these liveness-only digests advertise
        // no entries, so a zero here matches an empty receiver.
        content_hash: 0,
    }
}

/// One keyed state entry, as it rides a delta frame.
#[must_use]
pub fn entry(
    key: &str,
    version: u64,
    ttl_ms: u64,
    tombstone: bool,
    value: &[u8],
) -> wire::EntryDelta {
    wire::EntryDelta {
        key: key.to_owned(),
        version,
        ttl_ms,
        tombstone,
        value: value.to_vec(),
    }
}

/// A member delta carrying `entries` (a well-formed delta sets its
/// high-water to the max entry version).
#[must_use]
pub fn member_delta(node: &str, entries: Vec<wire::EntryDelta>) -> wire::MemberDelta {
    let max_version = entries.iter().map(|e| e.version).max().unwrap_or(0);
    wire::MemberDelta {
        node: NodeId::new(node),
        incarnation: 0,
        status: Status::Alive.to_wire(),
        max_version,
        entries,
    }
}

/// Decodes the single digest frame a round emits (all chunks in one, at
/// these small sizes), returning the sender's own summaries and metadata.
///
/// # Panics
/// If `effects` carries no digest [`Effect::Send`], or the one it carries does
/// not decode — either way the round under test did not do what it claimed.
#[must_use]
pub fn decode_one_digest(effects: &[Effect]) -> wire::Frame {
    let bytes = effects
        .iter()
        .find_map(|e| match e {
            Effect::Send { wire, .. } => {
                let f = wire::decode(wire)?;
                (f.kind == wire::Kind::Digest).then_some(wire.clone())
            }
            _ => None,
        })
        .expect("a digest send");
    wire::decode(&bytes).expect("decodes")
}
