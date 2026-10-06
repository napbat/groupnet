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
