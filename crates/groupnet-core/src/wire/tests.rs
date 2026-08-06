//! The wire codec's corpus: round-trips, bounds, and rejections.

use super::*;

fn digest_sample() -> Frame {
    Frame {
        kind: Kind::Digest,
        group: GroupId::new("shard-42"),
        target: None,
        digest: vec![
            NodeDigest {
                node: NodeId::new("node-a"),
                incarnation: 2,
                status: 0,
                max_version: 12,
                content_hash: 0x1234_5678_9abc_def0,
            },
            NodeDigest {
                node: NodeId::new("node-b"),
                incarnation: 5,
                status: 1,
                max_version: 0,
                content_hash: 0,
            },
        ],
        wants: vec![],
        members: vec![],
        metadata: vec![MetaDelta {
            key: "routing".into(),
            version: 3,
            writer: NodeId::new("node-a"),
            value: "v3".into(),
        }],
        lead: None,
    }
}

fn delta_sample() -> Frame {
    Frame {
        kind: Kind::Delta,
        group: GroupId::new("shard-42"),
        target: None,
        digest: vec![],
        wants: vec![],
        members: vec![MemberDelta {
            node: NodeId::new("node-a"),
            incarnation: 2,
            status: 0,
            max_version: 12,
            entries: vec![
                EntryDelta {
                    key: "~addr".into(),
                    version: 7,
                    ttl_ms: 0,
                    tombstone: false,
                    value: vec![1, 2, 3],
                },
                EntryDelta {
                    key: "hot/3".into(),
                    version: 12,
                    ttl_ms: 120_000,
                    tombstone: false,
                    value: vec![9; 40],
                },
                EntryDelta {
                    key: "old".into(),
                    version: 4,
                    ttl_ms: 0,
                    tombstone: true,
                    value: vec![],
                },
            ],
        }],
        metadata: vec![],
        lead: None,
    }
}

fn request_sample() -> Frame {
    Frame {
        kind: Kind::DeltaRequest,
        group: GroupId::new("shard-42"),
        target: None,
        digest: vec![],
        wants: vec![
            NodeWant {
                node: NodeId::new("node-a"),
                have_version: 4,
            },
            NodeWant {
                node: NodeId::new("node-c"),
                have_version: 0,
            },
        ],
        members: vec![],
        metadata: vec![],
        lead: None,
    }
}

fn ping_req_sample() -> Frame {
    Frame {
        kind: Kind::PingReq,
        group: GroupId::new("shard-42"),
        target: Some(NodeId::new("node-c")),
        digest: vec![],
        wants: vec![],
        members: vec![],
        metadata: vec![],
        lead: None,
    }
}

/// An election frame carrying `body`, on the kind that body belongs to.
fn lead_sample(body: LeadBody) -> Frame {
    Frame {
        kind: lead_kind(&body),
        group: GroupId::new("shard-42"),
        target: None,
        digest: vec![],
        wants: vec![],
        members: vec![],
        metadata: vec![],
        lead: Some(body),
    }
}

/// Every election body shape, including a `State` with and without a host —
/// the two branches of its optional-host byte.
fn lead_samples() -> Vec<Frame> {
    vec![
        lead_sample(LeadBody::Claim {
            epoch: 7,
            claimant: NodeId::new("node-a"),
        }),
        lead_sample(LeadBody::Grant {
            epoch: 7,
            claimant: NodeId::new("node-a"),
            granter: NodeId::new("node-b"),
        }),
        lead_sample(LeadBody::State {
            epoch: 7,
            host: Some(NodeId::new("node-a")),
        }),
        lead_sample(LeadBody::State {
            epoch: 0,
            host: None,
        }),
    ]
}

#[test]
fn round_trips_every_kind() {
    for frame in [
        digest_sample(),
        delta_sample(),
        request_sample(),
        ping_req_sample(),
    ]
    .into_iter()
    .chain(lead_samples())
    {
        let bytes = encode(&frame);
        assert_eq!(decode(&bytes), Some(frame.clone()));
        assert_eq!(peek_group(&bytes), Some(GroupId::new("shard-42")));
    }
}

#[test]
fn an_election_frame_decodes_to_the_body_its_kind_names() {
    // The kind tag and the `lead` variant are never allowed to disagree.
    for frame in lead_samples() {
        let decoded = decode(&encode(&frame)).expect("a well-formed election frame");
        assert_eq!(
            decoded.lead.as_ref().map(lead_kind),
            Some(decoded.kind),
            "kind {:?} decoded to a mismatched body",
            frame.kind
        );
    }
}

#[test]
fn an_election_frame_missing_its_body_decodes_to_none() {
    // A kind in 8..=10 requires a well-formed body: a bare header is not a
    // frame with an empty body, it is undecodable.
    for kind in [KIND_LEAD_CLAIM, KIND_LEAD_GRANT, KIND_LEAD_STATE] {
        let mut bytes = encode(&ping_req_sample());
        bytes[1] = kind;
        assert_eq!(decode(&bytes), None, "kind {kind} accepted a probe body");
    }
}

#[test]
fn an_unknown_kind_decodes_to_none() {
    // 11 is the first unassigned tag: an old node must drop a future kind
    // rather than misread it, which is what lets new kinds land inside v3.
    let mut bytes = encode(&digest_sample());
    bytes[1] = 11;
    assert_eq!(decode(&bytes), None);
    // ...but the group is still peekable, so a driver can demux it.
    assert_eq!(peek_group(&bytes), Some(GroupId::new("shard-42")));
}

#[test]
fn probes_carry_no_view() {
    // A bare probe encodes only version, kind, group, and the target flag —
    // never a piggybacked digest or delta.
    let ping = Frame {
        kind: Kind::Ping,
        group: GroupId::new("g"),
        target: None,
        digest: vec![],
        wants: vec![],
        members: vec![],
        metadata: vec![],
        lead: None,
    };
    assert_eq!(encode(&ping).len(), 1 + 1 + (4 + 1) + 1);
}

#[test]
fn entry_and_member_lengths_match_the_encoding() {
    // The size accounting the engine budgets against must equal what `encode`
    // actually produces.
    let frame = delta_sample();
    let member = &frame.members[0];
    let predicted = delta_frame_overhead(&frame.group)
        + member_header_len(&member.node)
        + member.entries.iter().map(entry_len).sum::<usize>();
    assert_eq!(predicted, encode(&frame).len());
}

#[test]
fn truncated_input_decodes_to_none_not_panic() {
    for frame in [digest_sample(), delta_sample(), request_sample()]
        .into_iter()
        .chain(lead_samples())
    {
        let bytes = encode(&frame);
        for cut in 0..bytes.len() {
            let _ = decode(&bytes[..cut]); // must never panic
        }
    }
    assert_eq!(decode(&[]), None);
}

#[test]
fn a_v2_framed_byte_stream_is_rejected() {
    // Hard cut: a frame stamped with the old version byte does not decode.
    let mut bytes = encode(&digest_sample());
    bytes[0] = 2;
    assert_eq!(decode(&bytes), None);
    assert_eq!(peek_group(&bytes), None);
}
