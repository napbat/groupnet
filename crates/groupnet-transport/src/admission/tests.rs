use super::*;

#[test]
fn open_is_explicit_and_checks_identity_and_credential_bounds() {
    let policy: &dyn Admission = &OpenAdmission;
    let node = NodeId::new("plain-uuid-or-string");
    let accepted =
        futures::executor::block_on(policy.admit(JoinRequest::new(&node, &[], None))).unwrap();
    assert_eq!(accepted.node, node);
    let credential = vec![7; MAX_CREDENTIAL_BYTES + 1];
    assert!(
        futures::executor::block_on(policy.admit(JoinRequest::new(&node, &credential, None)))
            .is_err()
    );
    assert!(
        futures::executor::block_on(policy.admit(JoinRequest::new(&NodeId::new(""), &[], None)))
            .is_err()
    );
    assert!(
        !format!("{:?}", JoinRequest::new(&node, b"private-credential", None))
            .contains("private-credential")
    );
}

#[test]
fn duplicate_and_capacity_cannot_evict_a_live_lease() {
    let registry = SessionRegistry::new(1).unwrap();
    let node = NodeId::new("one");
    let incumbent = registry.try_admit(AcceptedPeer::new(node.clone())).unwrap();
    assert_eq!(
        registry
            .try_admit(AcceptedPeer::new(node.clone()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(
        registry
            .try_admit(AcceptedPeer::new(NodeId::new("two")))
            .is_err()
    );
    assert!(incumbent.is_active());
    assert_eq!(registry.subscribe().borrow().len(), 1);
    drop(incumbent);
    assert!(registry.subscribe().borrow().is_empty());
    assert!(
        registry
            .try_admit(AcceptedPeer::new(NodeId::new("two")))
            .is_ok()
    );
}

#[test]
fn stale_cleanup_and_queued_tags_cannot_authorize_a_reconnect() {
    let registry = SessionRegistry::new(2).unwrap();
    let node = NodeId::new("reconnect");
    let first = registry.try_admit(AcceptedPeer::new(node.clone())).unwrap();
    let retained = first.clone();
    let old = first.id();
    drop(first);
    assert!(retained.is_active());
    registry.revoke(&node);
    let second = registry.try_admit(AcceptedPeer::new(node.clone())).unwrap();
    assert_ne!(old, second.id());
    assert!(!registry.is_active(&node, old));
    retained.revoke();
    drop(retained);
    assert!(second.is_active());
    second.revoke();
    assert!(!second.is_active());
    assert!(registry.subscribe().borrow().is_empty());
}

#[test]
fn simultaneous_reservations_are_atomic() {
    let registry = SessionRegistry::new(1).unwrap();
    let gate = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let registry = registry.clone();
            let gate = gate.clone();
            std::thread::spawn(move || {
                gate.wait();
                registry.try_admit(AcceptedPeer::new(NodeId::new("same")))
            })
        })
        .collect();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
}

#[test]
fn shutdown_withdraws_leases_and_permanently_fails_closed() {
    let registry = SessionRegistry::new(1).unwrap();
    let lease = registry
        .try_admit(AcceptedPeer::new(NodeId::new("peer")))
        .unwrap();
    registry.close();
    registry.close();
    assert!(!lease.is_active());
    assert!(registry.subscribe().borrow().is_empty());
    assert_eq!(
        registry
            .try_admit(AcceptedPeer::new(NodeId::new("later")))
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn closed_is_sticky_and_distinguishable_from_an_idle_registry() {
    use futures::FutureExt;

    let registry = SessionRegistry::new(1).unwrap();
    let mut neighbors = registry.subscribe();
    let mut pending = Box::pin(registry.closed());
    // Open and empty: the snapshot alone cannot tell, the lifecycle API can.
    assert!(neighbors.borrow_and_update().is_empty());
    assert!(!registry.is_closed());
    assert!(pending.as_mut().now_or_never().is_none());

    let lease = registry
        .try_admit(AcceptedPeer::new(NodeId::new("peer")))
        .unwrap();
    drop(lease);
    assert!(!registry.is_closed());
    assert!(pending.as_mut().now_or_never().is_none());

    registry.close();
    // A subscriber woken by the final empty snapshot already sees closed.
    assert!(neighbors.has_changed().unwrap());
    assert!(neighbors.borrow_and_update().is_empty());
    assert!(registry.is_closed());
    assert!(pending.now_or_never().is_some());
    // Sticky: futures created after close resolve immediately.
    assert!(registry.closed().now_or_never().is_some());
    registry.close();
    assert!(registry.clone().is_closed());
}

#[test]
fn closed_future_ends_when_every_registry_handle_is_dropped() {
    use futures::FutureExt;

    let registry = SessionRegistry::new(1).unwrap();
    let mut pending = Box::pin(registry.closed());
    assert!(pending.as_mut().now_or_never().is_none());
    drop(registry);
    assert!(pending.now_or_never().is_some());
}

#[test]
fn node_identity_bound_is_shared() {
    let registry = SessionRegistry::new(2).unwrap();
    let longest = NodeId::new("n".repeat(crate::MAX_NODE_ID_BYTES));
    assert!(registry.try_admit(AcceptedPeer::new(longest)).is_ok());
    let too_long = NodeId::new("n".repeat(crate::MAX_NODE_ID_BYTES + 1));
    assert_eq!(
        registry
            .try_admit(AcceptedPeer::new(too_long))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
}
