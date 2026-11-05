//! Dynamic contact discovery, separate from group participation.

use super::state::GroupEngine;
use crate::NodeId;
use std::collections::BTreeSet;

impl GroupEngine {
    pub(super) fn set_bootstrap_contacts(&mut self, contacts: Vec<NodeId>) {
        if contacts.len() > 4096 {
            return;
        }
        let contacts: BTreeSet<_> = contacts
            .into_iter()
            .filter(|node| node != &self.local)
            .collect();
        if contacts == self.bootstrap_contacts {
            return;
        }
        for removed in self.bootstrap_contacts.difference(&contacts) {
            if !self.seeds.contains(removed) && !self.members.contains_key(removed) {
                self.digest_peers.remove(removed);
            }
        }
        for added in contacts.difference(&self.bootstrap_contacts) {
            // First contact after discovery or reconnect must include a full
            // digest. Later full rounds repair loss without unbounded retries.
            // The rolling window keeps its place, so a contact that keeps
            // reconnecting still sees a new slice of the membership each time.
            if let Some(peer) = self.digest_peers.get_mut(added) {
                peer.visits = 0;
                peer.cursor = 0;
            }
        }
        self.bootstrap_contacts = contacts;
        self.targets = None;
        self.nudge_anti_entropy();
    }
}

#[cfg(test)]
mod tests;
