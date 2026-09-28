# Volatile peer-bootstrap membership binding

Status: accepted design contract; implementation pending.

Builder claims and membership are different lifetimes. A follower withdraws
its builder claim after transfer, while it remains a healthy Groupnet member.
Therefore a donor image cannot use the currently visible claim identities as
its complete membership roster. Requiring one claim per member would make a
later joining follower rebuild from origin even in a healthy connected fleet.

An opted-in participant publishes one separate, scoped participation entry
with its NodeId, a fresh caller-supplied 128-bit boot identity, a fresh worker
session, scope/policy binding, and strictly increasing renewal sequence.
The attempt number belongs only to transient builder claims. The same
sans-IO bootstrap engine schedules presence renewal and expiry with its
existing logical timer/operation allocator, including after the recovery
engine reaches Ready or follower transfer completes; the runtime does not
add a second application scheduler. The entry is advisory: it grants neither
serving authority nor an origin-write position. A new process using the same
NodeId has a distinct boot identity; a new worker session under the same boot
is distinct too. An old entry, reply, or image cannot be
reinterpreted as belonging to the new process. Publication uses the existing
Groupnet actor's bounded TTL entry mutation and exact readback on unknown
outcome. Shutdown withdraws the exact entry; lost withdrawal remains bounded
by native TTL. A participant cannot claim readiness for a donor image from
this entry alone.

The core owns one presence identity, renewal high-water mark and finite
next-renewal deadline per scope, not an unbounded history of old entries.
Re-observing an unchanged native renewal never extends its expiry. An
unreadable entry, backward renewal, clock arithmetic exhaustion, lost current
presence or finite capacity failure withdraws donor availability and makes
peer transfer unavailable until fresh source proof; local read authority
remains governed by its separate recovery/lease gates. The actor-owned queue,
raw response, decoded response and core-retained roster each have a checked
pre-allocation charge in the shared byte pool. A cancelled caller does not
release actor-response admission before the actor drops the response.

Presence mutation is conditional within the Group actor. The paired actor
cut returns the retained per-key revision, including a tombstone, and the
member's state-version high-water mark. A renewal compares only the exact
presence-key revision, so unrelated hot coherence entries cannot starve it.
An initial create after no retained per-key revision also compares the member
high-water mark, preventing absent-value ABA: an old queued create cannot
publish after a newer presence was installed and then withdrawn or expired.
The actor checks after advancing expiries and refuses revision exhaustion;
an unknown response requires exact readback rather than blind retry. The
queued request and exact response own admission through actor completion.

The source exposes one **complete, bounded actor cut** of native membership
status/incarnation, participation entries, and builder claims for a bootstrap
scope. Limits on member count, NodeId bytes, entry bytes, and total owned
response bytes are checked before cloning inside the actor. Its response
carries an observer-local monotonic sample instant and native remaining TTL,
which ages through the actor, adapter, and worker before a core event accepts
it. Missing, malformed, duplicate, expired, or wrong-policy participation for
an eligible member declines peer bootstrap; it never creates a synthetic boot
identity from NodeId or SWIM incarnation. A retained old-process entry
conflicting with a new-process entry also declines until the source resolves
the conflict. The source may fall back to origin when mixed-version peers lack
participation; ordinary coherence and origin reads remain available.

The narrow runtime primitive is a fixed two-key
`Group::inspect_scoped_pair(presence_key, claim_key, limits, owned_budget)`.
It returns each member's NodeId, status, native membership incarnation, two
optional raw values, their retained revisions, the member version high-water,
and both native remaining TTLs at one actor sample
instant. The actor validates the complete result and retains the supplied
owned budget through its queue and response; it never gathers two independent
snapshots and pretends they were atomic. `NativeClaimSource` decodes both
policy-bound values into a new complete observation. Claim selection may use
only claim-bearing candidates, whereas the journal continuity roster uses
all compatible participating members, including already transferred peers.
The current one-key inspection and VBC1 claim APIs remain usable unchanged.

The donor journal records a sorted, exact, count-and-byte-bounded roster of
participant identities (NodeId, fresh boot identity, worker session, and
native membership incarnation/status). Builder selection separately retains the exact
`ClaimIdentity` of donor and follower. Under the guarded index publication
lock, image C and the journal ingress become one candidate; a fresh source
roster is checked immediately before C and again before Ready and each
barrier. Changed or unprovable membership invalidates that candidate and
withdraws donor availability. Source gossip is not a linearizable admission
barrier: a new writer unseen at C can still join. The follower therefore
keeps native overlap/continuity and its independent current lease/frontier
affirmation; it must origin-route if those proofs cannot cover the join.

The scoped participation entry uses a new reserved key and body kind. The
existing VBC1 builder-claim value is unchanged. The bulk data-plane Offer
and Barrier bodies require a distinct codec version/kind because their member
roster changes from claim identities to participation identities. The
global Groupnet `FRAME_VERSION` and existing frame bodies are unchanged.
Older bulk peers refuse the new exchange and the follower uses origin
fallback. A healthy two-node run with a transferred follower must still
select the already-Ready donor for a third joining follower. Tests must prove
that schedule, old/new boot and same-boot new-session overlap, stale TTL or
renewal replay, missing participation fallback, cancellation and admission
release, and member change during C encoding or B replay.
