//! A persistent ordered map: an AVL tree of `Arc`-shared nodes. Cloning it
//! is one reference-count increment, and an insert or a removal copies only
//! the `O(log N)` nodes on its path — every other subtree stays shared with
//! the clones that came before. The group actor keeps its published views
//! in these, so republishing after a change to `k` members costs
//! `O(k log N)`, not a copy of the roster.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

type Link<K, V> = Option<Arc<Node<K, V>>>;

struct Node<K, V> {
    key: K,
    value: V,
    height: u8,
    left: Link<K, V>,
    right: Link<K, V>,
}

/// A persistent ordered map (see the [module docs](self)).
pub(crate) struct PMap<K, V> {
    root: Link<K, V>,
    len: usize,
}

impl<K, V> Clone for PMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            len: self.len,
        }
    }
}

impl<K, V> Default for PMap<K, V> {
    fn default() -> Self {
        Self { root: None, len: 0 }
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for PMap<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<K: Ord + Clone, V: Clone> FromIterator<(K, V)> for PMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let mut map = Self::default();
        for (key, value) in iter {
            map.insert(key, value);
        }
        map
    }
}

impl<K, V> PMap<K, V> {
    /// How many keys the map holds.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The entries in key order.
    pub(crate) fn iter(&self) -> Iter<'_, K, V> {
        let mut iter = Iter { stack: Vec::new() };
        iter.descend(self.root.as_deref());
        iter
    }

    /// The keys in order.
    pub(crate) fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(key, _)| key)
    }
}

impl<K: Ord + Clone, V: Clone> PMap<K, V> {
    /// The value stored under `key`.
    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut link = self.root.as_deref();
        while let Some(node) = link {
            link = match key.cmp(node.key.borrow()) {
                Ordering::Less => node.left.as_deref(),
                Ordering::Greater => node.right.as_deref(),
                Ordering::Equal => return Some(&node.value),
            };
        }
        None
    }

    /// Whether `key` is stored.
    pub(crate) fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Stores `value` under `key`, copying the path to it.
    pub(crate) fn insert(&mut self, key: K, value: V) {
        let (root, added) = insert(self.root.as_ref(), key, value);
        self.root = Some(root);
        self.len += usize::from(added);
    }

    /// Removes `key`, copying the path to it; returns whether it was there.
    pub(crate) fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match remove(self.root.as_ref(), key) {
            Some(root) => {
                self.root = root;
                self.len -= 1;
                true
            }
            None => false,
        }
    }
}

/// In-order iteration over a [`PMap`].
pub(crate) struct Iter<'a, K, V> {
    stack: Vec<&'a Node<K, V>>,
}

impl<'a, K, V> Iter<'a, K, V> {
    fn descend(&mut self, mut link: Option<&'a Node<K, V>>) {
        while let Some(node) = link {
            self.stack.push(node);
            link = node.left.as_deref();
        }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        self.descend(node.right.as_deref());
        Some((&node.key, &node.value))
    }
}

#[cfg(test)]
thread_local! {
    /// Nodes built (each one a copied or new member record) on this thread.
    pub(crate) static NODES_BUILT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn height<K, V>(link: Option<&Arc<Node<K, V>>>) -> u8 {
    link.map_or(0, |node| node.height)
}

fn node<K, V>(key: K, value: V, left: Link<K, V>, right: Link<K, V>) -> Arc<Node<K, V>> {
    #[cfg(test)]
    NODES_BUILT.with(|built| built.set(built.get() + 1));
    let height = 1 + height(left.as_ref()).max(height(right.as_ref()));
    Arc::new(Node {
        key,
        value,
        height,
        left,
        right,
    })
}

/// A node over `left` and `right`, rotated back into AVL balance when their
/// heights differ by two (the most one insert or removal leaves).
fn balance<K: Clone, V: Clone>(
    key: K,
    value: V,
    left: Link<K, V>,
    right: Link<K, V>,
) -> Arc<Node<K, V>> {
    let (hl, hr) = (height(left.as_ref()), height(right.as_ref()));
    if hl > hr + 1 {
        let l = left.expect("the taller side has a node");
        if height(l.left.as_ref()) >= height(l.right.as_ref()) {
            let right = node(key, value, l.right.clone(), right);
            node(l.key.clone(), l.value.clone(), l.left.clone(), Some(right))
        } else {
            let lr = l.right.as_ref().expect("the taller side has a node");
            let left = node(
                l.key.clone(),
                l.value.clone(),
                l.left.clone(),
                lr.left.clone(),
            );
            let right = node(key, value, lr.right.clone(), right);
            node(lr.key.clone(), lr.value.clone(), Some(left), Some(right))
        }
    } else if hr > hl + 1 {
        let r = right.expect("the taller side has a node");
        if height(r.right.as_ref()) >= height(r.left.as_ref()) {
            let left = node(key, value, left, r.left.clone());
            node(r.key.clone(), r.value.clone(), Some(left), r.right.clone())
        } else {
            let rl = r.left.as_ref().expect("the taller side has a node");
            let left = node(key, value, left, rl.left.clone());
            let right = node(
                r.key.clone(),
                r.value.clone(),
                rl.right.clone(),
                r.right.clone(),
            );
            node(rl.key.clone(), rl.value.clone(), Some(left), Some(right))
        }
    } else {
        node(key, value, left, right)
    }
}

/// `link` with `value` under `key`, and whether the key is new.
fn insert<K: Ord + Clone, V: Clone>(
    link: Option<&Arc<Node<K, V>>>,
    key: K,
    value: V,
) -> (Arc<Node<K, V>>, bool) {
    let Some(n) = link else {
        return (node(key, value, None, None), true);
    };
    match key.cmp(&n.key) {
        Ordering::Less => {
            let (left, added) = insert(n.left.as_ref(), key, value);
            let root = balance(n.key.clone(), n.value.clone(), Some(left), n.right.clone());
            (root, added)
        }
        Ordering::Greater => {
            let (right, added) = insert(n.right.as_ref(), key, value);
            let root = balance(n.key.clone(), n.value.clone(), n.left.clone(), Some(right));
            (root, added)
        }
        Ordering::Equal => (node(key, value, n.left.clone(), n.right.clone()), false),
    }
}

/// `link` without `key`, or `None` when it does not hold it (nothing copied).
fn remove<K, V, Q>(link: Option<&Arc<Node<K, V>>>, key: &Q) -> Option<Link<K, V>>
where
    K: Ord + Clone + Borrow<Q>,
    V: Clone,
    Q: Ord + ?Sized,
{
    let n = link?;
    Some(match key.cmp(n.key.borrow()) {
        Ordering::Less => {
            let left = remove(n.left.as_ref(), key)?;
            Some(balance(
                n.key.clone(),
                n.value.clone(),
                left,
                n.right.clone(),
            ))
        }
        Ordering::Greater => {
            let right = remove(n.right.as_ref(), key)?;
            Some(balance(
                n.key.clone(),
                n.value.clone(),
                n.left.clone(),
                right,
            ))
        }
        Ordering::Equal => match (&n.left, &n.right) {
            (None, right) => right.clone(),
            (left, None) => left.clone(),
            (left, Some(right)) => {
                let (key, value, rest) = pop_min(right);
                Some(balance(key, value, left.clone(), rest))
            }
        },
    })
}

/// The smallest entry under `n`, and `n` without it.
fn pop_min<K: Clone, V: Clone>(n: &Arc<Node<K, V>>) -> (K, V, Link<K, V>) {
    match &n.left {
        None => (n.key.clone(), n.value.clone(), n.right.clone()),
        Some(left) => {
            let (key, value, rest) = pop_min(left);
            let root = balance(n.key.clone(), n.value.clone(), rest, n.right.clone());
            (key, value, Some(root))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// Checks the AVL invariants under `link`, returning its height and size.
    fn check<K: Ord, V>(link: Option<&Arc<Node<K, V>>>) -> (u8, usize) {
        let Some(n) = link else {
            return (0, 0);
        };
        let (hl, nl) = check(n.left.as_ref());
        let (hr, nr) = check(n.right.as_ref());
        assert!(hl.abs_diff(hr) <= 1, "unbalanced");
        assert_eq!(n.height, 1 + hl.max(hr), "stale height");
        if let Some(l) = &n.left {
            assert!(l.key < n.key, "out of order");
        }
        if let Some(r) = &n.right {
            assert!(r.key > n.key, "out of order");
        }
        (n.height, 1 + nl + nr)
    }

    /// Random inserts, overwrites and removals agree with a `BTreeMap` after
    /// every step, keep the tree balanced, and leave every earlier clone
    /// exactly as it was.
    #[test]
    fn a_persistent_map_matches_a_btreemap_and_keeps_old_versions() {
        let mut state = 7_u64;
        let mut draw = |n: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            (state >> 33) % n
        };
        let mut map: PMap<u64, u64> = PMap::default();
        let mut reference = BTreeMap::new();
        let mut versions: Vec<(PMap<u64, u64>, BTreeMap<u64, u64>)> = Vec::new();
        for step in 0..4_000 {
            let key = draw(300);
            if draw(3) == 0 {
                assert_eq!(map.remove(&key), reference.remove(&key).is_some());
            } else {
                map.insert(key, step);
                reference.insert(key, step);
            }
            assert_eq!(map.len(), reference.len());
            assert_eq!(check(map.root.as_ref()).1, reference.len());
            assert_eq!(map.get(&key), reference.get(&key));
            if step % 97 == 0 {
                versions.push((map.clone(), reference.clone()));
            }
        }
        assert!(map.iter().map(|(k, v)| (*k, *v)).eq(reference.into_iter()));
        for (old, expected) in versions {
            assert!(old.iter().map(|(k, v)| (*k, *v)).eq(expected.into_iter()));
        }
    }
}
