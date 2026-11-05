//! The group actor's published member views — live members, statuses and
//! entries — kept in [persistent maps](crate::pmap), so a batch that changes
//! `k` members republishes in `O(k log N)` and a batch that changes none
//! publishes nothing. The actor learns which members changed from the
//! engine's change clock
//! ([`GroupEngine::members_changed_since`] and
//! [`members_forgotten_since`](GroupEngine::members_forgotten_since)), and
//! which entries changed from the batch's `NodeStateChanged` effects.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use groupnet_core::{GroupEngine, NodeId, Status, Time};

use crate::pmap::PMap;

#[cfg(test)]
mod tests;

/// One node's live entries, key order.
pub(crate) type NodeEntries = Arc<BTreeMap<String, Vec<u8>>>;

/// Every node's live entries as one ordered map — the shape
/// [`Group::all_entries`](crate::Group::all_entries) returns.
pub(crate) type AllEntries = Arc<BTreeMap<NodeId, NodeEntries>>;

/// A published, read-only snapshot of a group's live members (id order).
pub(crate) type MembersSnapshot = Arc<Members>;

/// A published, read-only snapshot of every known member's status
/// (Alive/Suspect, plus not-yet-reaped Dead tombstones), so readers can
/// route *around* a suspected peer that [`MembersSnapshot`] (the not-`Dead`
/// set) still lists.
///
/// Each status carries the engine-logical [`Time`] this observer has held it
/// continuously since, so a reader can turn a status into a *duration* —
/// see [`Group::status_held_for`](crate::Group::status_held_for).
pub(crate) type StatusesSnapshot = PMap<NodeId, (Status, Time)>;

/// A published, read-only snapshot of every node's keyed app-defined state
/// (live entries only — tombstoned/expired keys are absent).
pub(crate) type NodeEntriesSnapshot = Arc<Entries>;

/// The live members, as published.
#[derive(Debug)]
pub(crate) struct Members {
    set: PMap<NodeId, ()>,
    /// The members as one shared slice, built by the first reader that asks
    /// for it ([`roster`](Self::roster)) and shared by every later one.
    flat: OnceLock<Arc<[NodeId]>>,
}

impl Members {
    fn new(set: PMap<NodeId, ()>) -> Self {
        Self {
            set,
            flat: OnceLock::new(),
        }
    }

    /// Whether `node` is a live member.
    pub(crate) fn contains(&self, node: &NodeId) -> bool {
        self.set.contains_key(node)
    }

    /// The live members, id order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &NodeId> {
        self.set.keys()
    }

    /// The live members as one shared slice, id order.
    pub(crate) fn roster(&self) -> Arc<[NodeId]> {
        self.flat
            .get_or_init(|| self.set.keys().cloned().collect())
            .clone()
    }
}

/// Every node's live entries, as published.
#[derive(Debug)]
pub(crate) struct Entries {
    map: PMap<NodeId, NodeEntries>,
    /// The entries as one ordered map, built by the first reader that asks
    /// for it ([`all`](Self::all)) and shared by every later one.
    flat: OnceLock<AllEntries>,
}

impl Entries {
    fn new(map: PMap<NodeId, NodeEntries>) -> Self {
        Self {
            map,
            flat: OnceLock::new(),
        }
    }

    /// `node`'s live entries, if it has any.
    pub(crate) fn get(&self, node: &NodeId) -> Option<&NodeEntries> {
        self.map.get(node)
    }

    /// Every node with live entries, in id order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&NodeId, &NodeEntries)> {
        self.map.iter()
    }

    /// Every node's live entries as one ordered map.
    pub(crate) fn all(&self) -> AllEntries {
        self.flat
            .get_or_init(|| {
                Arc::new(
                    self.map
                        .iter()
                        .map(|(node, entries)| (node.clone(), entries.clone()))
                        .collect(),
                )
            })
            .clone()
    }
}

/// Which published views one [`Views::update`] changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Changed {
    pub(crate) members: bool,
    pub(crate) statuses: bool,
    pub(crate) entries: bool,
}

/// The actor's mirror of the engine's member views, persistent so every
/// published snapshot shares all but the changed members with the last.
#[derive(Debug)]
pub(crate) struct Views {
    live: PMap<NodeId, ()>,
    statuses: StatusesSnapshot,
    entries: PMap<NodeId, NodeEntries>,
    /// The engine's change clock as of the last update.
    clock: u64,
}

impl Views {
    /// The views of `engine` as it stands, built in full.
    pub(crate) fn new(engine: &GroupEngine) -> Self {
        Self {
            live: engine.members().map(|node| (node.clone(), ())).collect(),
            statuses: engine
                .member_statuses_since()
                .map(|(node, status, since)| (node.clone(), (status, since)))
                .collect(),
            entries: engine
                .member_statuses()
                .filter_map(|(node, _)| node_entries(engine, node).map(|e| (node.clone(), e)))
                .collect(),
            clock: engine.change_clock(),
        }
    }

    /// The live members to publish.
    pub(crate) fn members(&self) -> MembersSnapshot {
        Arc::new(Members::new(self.live.clone()))
    }

    /// The statuses to publish.
    pub(crate) fn statuses(&self) -> StatusesSnapshot {
        self.statuses.clone()
    }

    /// The entries to publish.
    pub(crate) fn entries(&self) -> NodeEntriesSnapshot {
        Arc::new(Entries::new(self.entries.clone()))
    }

    /// Folds in what changed since the last update: the members the
    /// engine's change clock names, and the entries of `touched` (the
    /// nodes this batch's `NodeStateChanged` effects named). Writes only
    /// members whose published value moved.
    pub(crate) fn update<'a>(
        &mut self,
        engine: &GroupEngine,
        touched: impl Iterator<Item = &'a NodeId>,
    ) -> Changed {
        let since = self.clock;
        self.clock = engine.change_clock();
        let mut changed = Changed::default();
        let Some(forgotten) = engine.members_forgotten_since(since) else {
            // Too far behind to know who was forgotten: rebuild.
            *self = Self::new(engine);
            return Changed {
                members: true,
                statuses: true,
                entries: true,
            };
        };
        for node in forgotten {
            if engine.member_status(node).is_none() {
                changed.members |= self.live.remove(node);
                changed.statuses |= self.statuses.remove(node);
                changed.entries |= self.entries.remove(node);
            }
        }
        for node in engine.members_changed_since(since) {
            let Some((status, held)) = engine.member_status_since(node) else {
                continue;
            };
            if self.statuses.get(node) != Some(&(status, held)) {
                self.statuses.insert(node.clone(), (status, held));
                changed.statuses = true;
            }
            let live = status != Status::Dead;
            if live != self.live.contains_key(node) {
                if live {
                    self.live.insert(node.clone(), ());
                } else {
                    self.live.remove(node);
                }
                changed.members = true;
            }
        }
        for node in touched {
            match node_entries(engine, node) {
                Some(entries) => self.entries.insert(node.clone(), entries),
                None => {
                    self.entries.remove(node);
                }
            }
            changed.entries = true;
        }
        changed
    }
}

/// `node`'s live entries, or `None` when it has none.
fn node_entries(engine: &GroupEngine, node: &NodeId) -> Option<NodeEntries> {
    let entries: BTreeMap<String, Vec<u8>> = engine
        .node_entries(node)
        .map(|(key, value)| (key.to_owned(), value.to_vec()))
        .collect();
    (!entries.is_empty()).then(|| Arc::new(entries))
}
