//! Explicit bootstrap contacts and bounded managed address learning.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;

use groupnet_core::NodeId;

/// Dial addresses by identity: explicit registrations take precedence over
/// learned contacts.
#[derive(Debug)]
pub(super) struct AddressBook {
    /// Registered contacts; raw endpoints also record every learned address here.
    registered: HashMap<NodeId, SocketAddr>,
    /// Managed endpoints' bounded learned contacts. `None` for raw endpoints,
    /// whose learning is unbounded and last-write-wins.
    learned: Option<Learned>,
}

/// Learned contacts, evicting the least recently learned at capacity.
#[derive(Debug)]
struct Learned {
    addresses: HashMap<NodeId, SocketAddr>,
    /// Each learned identity exactly once, from least to most recently learned.
    order: VecDeque<NodeId>,
    capacity: usize,
}

impl AddressBook {
    pub(super) fn new(learned_capacity: Option<usize>) -> Self {
        Self {
            registered: HashMap::new(),
            learned: learned_capacity.map(Learned::new),
        }
    }

    /// Sets `node`'s configured address, superseding anything learned for it.
    pub(super) fn register(&mut self, node: NodeId, address: SocketAddr) {
        if let Some(learned) = &mut self.learned {
            learned.remove(&node);
        }
        self.registered.insert(node, address);
    }

    /// Records an introduced or hinted address. Managed books never override a
    /// registered contact.
    pub(super) fn learn(&mut self, node: &NodeId, address: SocketAddr) {
        let Some(learned) = &mut self.learned else {
            self.registered.insert(node.clone(), address);
            return;
        };
        if !self.registered.contains_key(node) {
            learned.insert(node, address);
        }
    }

    pub(super) fn get(&self, node: &NodeId) -> Option<SocketAddr> {
        self.registered
            .get(node)
            .or_else(|| self.learned.as_ref()?.addresses.get(node))
            .copied()
    }

    #[cfg(feature = "connectivity")]
    pub(super) fn keys(&self) -> impl Iterator<Item = &NodeId> {
        self.registered.keys().chain(
            self.learned
                .iter()
                .flat_map(|learned| learned.addresses.keys()),
        )
    }
}

impl Learned {
    fn new(capacity: usize) -> Self {
        Self {
            addresses: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn remove(&mut self, node: &NodeId) {
        if self.addresses.remove(node).is_some()
            && let Some(index) = self.order.iter().position(|entry| entry == node)
        {
            self.order.remove(index);
        }
    }

    /// Updates or adds `node` as the most recently learned contact.
    fn insert(&mut self, node: &NodeId, address: SocketAddr) {
        if let Some(existing) = self.addresses.get_mut(node) {
            *existing = address;
            // Refreshes usually hit recent contacts: search from the back.
            if let Some(index) = self.order.iter().rposition(|entry| entry == node)
                && let Some(entry) = self.order.remove(index)
            {
                self.order.push_back(entry);
            }
            return;
        }
        if self.addresses.len() >= self.capacity
            && let Some(oldest) = self.order.pop_front()
        {
            self.addresses.remove(&oldest);
        }
        self.addresses.insert(node.clone(), address);
        self.order.push_back(node.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn managed_learning_refreshes_without_duplicates_and_protects_registered_contacts() {
        let mut book = AddressBook::new(Some(2));
        let seed = NodeId::new("seed");
        let first = NodeId::new("first");
        let second = NodeId::new("second");
        let third = NodeId::new("third");
        book.register(seed.clone(), address(1));
        book.learn(&seed, address(2));
        book.learn(&first, address(3));
        book.learn(&second, address(4));
        for port in 5..10 {
            book.learn(&first, address(port));
        }
        let learned = book.learned.as_ref().expect("managed learning");
        assert_eq!(learned.addresses.len(), 2);
        assert_eq!(
            learned.order,
            VecDeque::from([second.clone(), first.clone()])
        );
        book.learn(&third, address(10));
        assert_eq!(book.get(&seed), Some(address(1)));
        assert_eq!(book.get(&first), Some(address(9)));
        assert_eq!(book.get(&second), None);
        assert_eq!(book.get(&third), Some(address(10)));
        book.register(first.clone(), address(11));
        book.learn(&first, address(12));
        let learned = book.learned.as_ref().expect("managed learning");
        assert_eq!(learned.addresses.len(), 1);
        assert_eq!(learned.order, VecDeque::from([third]));
        assert_eq!(book.get(&first), Some(address(11)));
    }

    #[test]
    fn raw_learning_remains_unbounded_and_last_write_wins() {
        let mut book = AddressBook::new(None);
        let seed = NodeId::new("seed");
        book.register(seed.clone(), address(1));
        book.learn(&seed, address(2));
        for port in 3..10 {
            book.learn(&NodeId::new(port.to_string()), address(port));
        }
        assert_eq!(book.get(&seed), Some(address(2)));
        assert_eq!(book.registered.len(), 8);
        assert!(book.learned.is_none());
    }
}
