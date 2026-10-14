use super::*;
use std::num::NonZeroUsize;

fn registry(max_peers: usize) -> SessionRegistry {
    SessionRegistry::new(NonZeroUsize::new(max_peers).expect("nonzero test capacity"))
}

fn typed<E: error::Error + 'static>(error: &io::Error) -> &E {
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<E>())
        .expect("typed inner error")
}

#[test]
fn open_is_explicit_and_checks_identity_and_credential_bounds() {
    let policy: &dyn Admission = &OpenAdmission;
    let node = NodeId::new("plain-uuid-or-string");
    let accepted =
        futures::executor::block_on(policy.admit(JoinRequest::new(&node, &[], None))).unwrap();
    assert_eq!(accepted.node, node);
    let credential = vec![7; MAX_CREDENTIAL_BYTES + 1];
    let error =
        futures::executor::block_on(policy.admit(JoinRequest::new(&node, &credential, None)))
            .unwrap_err();
    assert!(matches!(
        error,
        AdmissionError::Request(JoinRequestError::CredentialTooLong { len })
            if len == MAX_CREDENTIAL_BYTES + 1
    ));
    let error = io::Error::from(error);
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(matches!(
        typed::<AdmissionError>(&error),
        AdmissionError::Request(JoinRequestError::CredentialTooLong { .. })
    ));
    let error =
        futures::executor::block_on(policy.admit(JoinRequest::new(&NodeId::new(""), &[], None)))
            .unwrap_err();
    assert!(matches!(
        error,
        AdmissionError::Request(JoinRequestError::Claimed(NodeIdError::Empty))
    ));
    assert!(
        !format!("{:?}", JoinRequest::new(&node, b"private-credential", None))
            .contains("private-credential")
    );
}

#[test]
fn admission_errors_keep_their_cause_and_map_to_socket_kinds() {
    let claimed = NodeId::new("claimed");
    let request = AdmissionError::from(JoinRequestError::Claimed(NodeIdError::Empty));
    assert!(
        error::Error::source(&request)
            .and_then(|source| source.downcast_ref::<JoinRequestError>())
            .is_some()
    );
    assert_eq!(io::Error::from(request).kind(), io::ErrorKind::InvalidInput);
    for denial in [
        AdmissionError::UnknownPeer {
            claimed: claimed.clone(),
        },
        AdmissionError::CredentialRejected {
            claimed: claimed.clone(),
        },
    ] {
        assert!(denial.to_string().contains("claimed"));
        assert!(error::Error::source(&denial).is_none());
        let denial = io::Error::from(denial);
        assert_eq!(denial.kind(), io::ErrorKind::PermissionDenied);
        typed::<AdmissionError>(&denial);
    }
    let unavailable = AdmissionError::from(io::Error::new(
        io::ErrorKind::TimedOut,
        "account store timed out",
    ));
    assert!(
        error::Error::source(&unavailable)
            .and_then(|source| source.downcast_ref::<io::Error>())
            .is_some_and(|cause| cause.kind() == io::ErrorKind::TimedOut)
    );
    assert_eq!(io::Error::from(unavailable).kind(), io::ErrorKind::TimedOut);
    let other = AdmissionError::Unavailable("lookup failed".into());
    assert_eq!(io::Error::from(other).kind(), io::ErrorKind::Other);
}

#[test]
fn duplicate_and_capacity_cannot_evict_a_live_lease() {
    let registry = registry(1);
    let node = NodeId::new("one");
    let incumbent = registry.try_admit(AcceptedPeer::new(node.clone())).unwrap();
    let duplicate = registry
        .try_admit(AcceptedPeer::new(node.clone()))
        .unwrap_err();
    assert_eq!(duplicate, SessionError::AlreadyAdmitted(node.clone()));
    assert_eq!(
        io::Error::from(duplicate).kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        registry
            .try_admit(AcceptedPeer::new(NodeId::new("two")))
            .unwrap_err(),
        SessionError::CapacityReached {
            max_peers: NonZeroUsize::MIN
        }
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
    let registry = registry(2);
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
    let registry = registry(1);
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
    let registry = registry(1);
    let lease = registry
        .try_admit(AcceptedPeer::new(NodeId::new("peer")))
        .unwrap();
    registry.close();
    registry.close();
    assert!(!lease.is_active());
    assert!(registry.subscribe().borrow().is_empty());
    let closed = registry
        .try_admit(AcceptedPeer::new(NodeId::new("later")))
        .unwrap_err();
    assert_eq!(closed, SessionError::Closed);
    assert_eq!(io::Error::from(closed).kind(), io::ErrorKind::BrokenPipe);
}

#[test]
fn closed_is_sticky_and_distinguishable_from_an_idle_registry() {
    use futures::FutureExt;

    let registry = registry(1);
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

    let registry = registry(1);
    let mut pending = Box::pin(registry.closed());
    assert!(pending.as_mut().now_or_never().is_none());
    drop(registry);
    assert!(pending.now_or_never().is_some());
}

#[test]
fn node_identity_bound_is_shared() {
    let registry = registry(2);
    let longest = NodeId::new("n".repeat(crate::MAX_NODE_ID_BYTES));
    assert!(registry.try_admit(AcceptedPeer::new(longest)).is_ok());
    let too_long = NodeId::new("n".repeat(crate::MAX_NODE_ID_BYTES + 1));
    let error = registry.try_admit(AcceptedPeer::new(too_long)).unwrap_err();
    assert_eq!(
        error,
        SessionError::Identity(NodeIdError::TooLong {
            len: crate::MAX_NODE_ID_BYTES + 1,
            max: crate::MAX_NODE_ID_BYTES
        })
    );
    assert_eq!(io::Error::from(error).kind(), io::ErrorKind::InvalidInput);
}
