//! Source-durable unsubscribe and exact ambiguous-result readback.

use std::time::Instant;

use super::*;
use groupnet_core::replication::{Stage, TerminalReason, TerminalReceipt, TerminalRequest};

#[tokio::test]
async fn unknown_terminal_write_is_read_back_before_retention_is_released() {
    let cluster = MemCluster::builder(&["event-terminal"])
        .group("stores")
        .spawn();
    let source = MemSource::default();
    source.unknown_terminal_once.store(true, Ordering::Release);
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
        .expect("manager")
        .with_event_complete(MemSink::default(), SubscriptionLimits::default())
        .expect("named capability");
    let handle = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            NonZeroU64::new(12).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![12],
            },
        )
        .expect("subscriber");
    eventually_within("source registration protected", SETTLE, || {
        handle.status().stage == Stage::Protected
    })
    .await;
    let receipt = handle
        .unsubscribe(vec![90], Instant::now() + SETTLE)
        .await
        .expect("exact terminal readback");
    assert!(receipt.durable);
    assert_eq!(receipt.request.reason, TerminalReason::Unsubscribed);
    assert_eq!(receipt.request.request_id, vec![90]);
    assert_eq!(handle.status().stage, Stage::TerminatedSubscriber);
    assert_eq!(handle.status().terminal, Some(receipt));
    assert_eq!(
        handle.unsubscribe(vec![92], Instant::now() + SETTLE).await,
        Err(groupnet_consistency::replication::UnsubscribeError::WrongRequest)
    );
    assert!(
        !source
            .state
            .lock()
            .expect("source lock")
            .registrations
            .contains_key(handle.key())
    );
}

#[tokio::test]
async fn dropping_unsubscribe_caller_preserves_exact_source_readback() {
    let cluster = MemCluster::builder(&["event-drop"]).group("stores").spawn();
    let source = MemSource::default();
    source.unknown_terminal_once.store(true, Ordering::Release);
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
        .expect("manager")
        .with_event_complete(MemSink::default(), SubscriptionLimits::default())
        .expect("named capability");
    let handle = Arc::new(
        manager
            .open_named(
                &scope(),
                SubscriberId {
                    name: "billing".into(),
                },
                NonZeroU64::new(17).expect("nonzero"),
                SubscriptionStart::StartAt {
                    position: 0,
                    policy: policy(),
                    request_id: vec![17],
                },
            )
            .expect("subscriber"),
    );
    eventually_within("source registration protected", SETTLE, || {
        handle.status().stage == Stage::Protected
    })
    .await;
    let caller = Arc::clone(&handle);
    let waiter =
        tokio::spawn(async move { caller.unsubscribe(vec![93], Instant::now() + SETTLE).await });
    eventually_within("terminal write issued", SETTLE, || {
        source.terminal_calls.load(Ordering::Acquire) >= 1
    })
    .await;
    waiter.abort();
    let _ = waiter.await;
    eventually_within(
        "worker readback completes after caller drop",
        SETTLE,
        || handle.status().stage == Stage::TerminatedSubscriber,
    )
    .await;
    assert_eq!(
        handle
            .status()
            .terminal
            .expect("exact source receipt")
            .request
            .request_id,
        vec![93]
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one reset scenario must observe source tombstone, rejected bare reuse, and the fenced new lineage"
)]
async fn reset_requires_exact_tombstone_and_advances_persistent_name_fence() {
    let cluster = MemCluster::builder(&["event-reset"])
        .group("stores")
        .spawn();
    let source = MemSource::default();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
        .expect("manager")
        .with_event_complete(MemSink::default(), SubscriptionLimits::default())
        .expect("named capability");
    let key = SubscriberId {
        name: "billing".into(),
    };
    let old_id = NonZeroU64::new(13).expect("nonzero");
    let old = manager
        .open_named(
            &scope(),
            key.clone(),
            old_id,
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![13],
            },
        )
        .expect("initial subscriber");
    eventually_within("source registration protected", SETTLE, || {
        old.status().stage == Stage::Protected
    })
    .await;
    let terminal = old
        .unsubscribe(vec![91], Instant::now() + SETTLE)
        .await
        .expect("source tombstone");
    assert_eq!(
        manager
            .read_current_terminal(old.key().clone(), Instant::now() + SETTLE)
            .await
            .expect("bounded source inspection"),
        Some(terminal.clone())
    );
    assert!(manager.close_named_if(old.key(), old_id));
    let before = source.register_calls.load(Ordering::Acquire);
    let mut forged = terminal.clone();
    forged.request.key.subscriber.name = "foreign".into();
    assert!(
        manager
            .open_named(
                &scope(),
                key.clone(),
                NonZeroU64::new(18).expect("nonzero"),
                SubscriptionStart::ResetAt {
                    position: 0,
                    policy: policy(),
                    request_id: vec![18],
                    prior: Box::new(forged),
                },
            )
            .is_err()
    );
    assert_eq!(source.register_calls.load(Ordering::Acquire), before);
    let bare = manager
        .open_named(
            &scope(),
            key.clone(),
            NonZeroU64::new(14).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![14],
            },
        )
        .expect("local admission cannot predict source tombstone");
    eventually_within("bare start rejected by source ledger", SETTLE, || {
        bare.status().stage == Stage::RetryExhausted
    })
    .await;
    assert!(
        !source
            .state
            .lock()
            .expect("source lock")
            .registrations
            .contains_key(bare.key())
    );
    // Remove the conclusive failed local attempt, then reset the name with
    // the exact durable predecessor and a strictly newer source ordinal.
    let failed_key = groupnet_core::replication::SubscriberKey {
        scope: scope(),
        subscriber: key.clone(),
    };
    assert!(manager.close_named_if(&failed_key, NonZeroU64::new(14).expect("nonzero")));
    let reset = manager
        .open_named(
            &scope(),
            key,
            NonZeroU64::new(15).expect("nonzero"),
            SubscriptionStart::ResetAt {
                position: 0,
                policy: policy(),
                request_id: vec![15],
                prior: Box::new(terminal.clone()),
            },
        )
        .expect("reset local admission");
    eventually_within("new source registration protected", SETTLE, || {
        reset.status().stage == Stage::Protected
    })
    .await;
    let registration = source
        .state
        .lock()
        .expect("source lock")
        .registrations
        .get(reset.key())
        .cloned()
        .expect("reset registered");
    assert!(registration.epoch.ordinal > terminal.tombstone_ordinal);
    assert_eq!(source.acknowledged(reset.key()), Some(0));
    assert_eq!(
        manager
            .read_current_terminal(reset.key().clone(), Instant::now() + SETTLE)
            .await
            .expect("new lineage inspection"),
        None
    );
}

#[tokio::test]
async fn source_expiry_stops_delivery_and_tombstone_is_separately_inspectable() {
    let cluster = MemCluster::builder(&["event-expiry"])
        .group("stores")
        .spawn();
    let source = MemSource::default();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
        .expect("manager")
        .with_event_complete(MemSink::default(), SubscriptionLimits::default())
        .expect("named capability");
    let handle = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            NonZeroU64::new(16).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![16],
            },
        )
        .expect("subscriber");
    eventually_within("source registration protected", SETTLE, || {
        handle.status().stage == Stage::Protected
    })
    .await;
    let terminal = {
        let mut state = source.state.lock().expect("source lock");
        let registration = state
            .registrations
            .remove(handle.key())
            .expect("durable registration");
        let request = TerminalRequest {
            key: handle.key().clone(),
            epoch: registration.epoch,
            acknowledged: cursor(&scope(), 0),
            request_id: vec![99],
            reason: TerminalReason::Expired,
        };
        let receipt = TerminalReceipt {
            tombstone_ordinal: request.epoch.ordinal,
            request,
            durable: true,
        };
        state
            .terminal_requests
            .insert((handle.key().clone(), vec![99]), receipt.clone());
        state.head = 1;
        state.retained = 1;
        receipt
    };
    handle.hint();
    eventually_within("source expiry stops event delivery", SETTLE, || {
        handle.status().stage == Stage::IrrecoverableGap
    })
    .await;
    assert!(handle.status().terminal.is_none());
    assert_eq!(
        manager
            .read_current_terminal(handle.key().clone(), Instant::now() + SETTLE)
            .await
            .expect("source tombstone inspection"),
        Some(terminal)
    );
}

#[tokio::test]
async fn detached_unsubscribe_releases_retention_without_rebinding_sink() {
    let cluster = MemCluster::builder(&["event-detached"])
        .group("stores")
        .spawn();
    let source = MemSource::default();
    source.unknown_terminal_once.store(true, Ordering::Release);
    let sink = MemSink::default();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
        .expect("manager")
        .with_event_complete(sink.clone(), SubscriptionLimits::default())
        .expect("named capability");
    let session_id = NonZeroU64::new(20).expect("nonzero");
    let handle = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            session_id,
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![20],
            },
        )
        .expect("subscriber");
    eventually_within("initial sink bound", SETTLE, || {
        handle.status().stage == Stage::Protected
    })
    .await;
    let key = handle.key().clone();
    let binds = sink.bind_calls.load(Ordering::Acquire);
    assert!(manager.close_named_if(&key, session_id));
    assert!(
        source
            .state
            .lock()
            .expect("source lock")
            .registrations
            .contains_key(&key)
    );
    let receipt = manager
        .unsubscribe_detached(
            key.clone(),
            NonZeroU64::new(21).expect("nonzero"),
            vec![21],
            Instant::now() + SETTLE,
        )
        .await
        .expect("source-only exact terminal readback");
    assert_eq!(receipt.request.key, key);
    assert_eq!(receipt.request.request_id, vec![21]);
    assert_eq!(sink.bind_calls.load(Ordering::Acquire), binds);
    assert!(
        !source
            .state
            .lock()
            .expect("source lock")
            .registrations
            .contains_key(&key)
    );
}

#[tokio::test]
async fn detached_unsubscribe_releases_known_gap_without_sink_or_event_bytes() {
    let cluster = MemCluster::builder(&["event-detached-gap"])
        .group("stores")
        .spawn();
    let source = MemSource::default();
    let sink = MemSink::default();
    let manager = Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
        .expect("manager")
        .with_event_complete(sink.clone(), SubscriptionLimits::default())
        .expect("named capability");
    let session_id = NonZeroU64::new(25).expect("nonzero");
    let handle = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            session_id,
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![25],
            },
        )
        .expect("subscriber");
    eventually_within("source registration protected", SETTLE, || {
        handle.status().stage == Stage::Protected
    })
    .await;
    let key = handle.key().clone();
    assert!(manager.close_named_if(&key, session_id));
    {
        let mut state = source.state.lock().expect("source lock");
        state.head = 1;
        state.retained = 1;
    }
    let binds = sink.bind_calls.load(Ordering::Acquire);
    let receipt = manager
        .unsubscribe_detached(
            key.clone(),
            NonZeroU64::new(26).expect("nonzero"),
            vec![26],
            Instant::now() + SETTLE,
        )
        .await
        .expect("retained gap does not prevent exact terminal release");
    assert_eq!(receipt.request.acknowledged, cursor(&scope(), 0));
    assert_eq!(sink.bind_calls.load(Ordering::Acquire), binds);
    assert!(
        !source
            .state
            .lock()
            .expect("source lock")
            .registrations
            .contains_key(&key)
    );
}

#[tokio::test]
async fn classified_failures_after_commit_require_exact_terminal_readback() {
    use groupnet_consistency::replication::FailureClass;

    for (index, class) in [FailureClass::Terminal, FailureClass::AuthorityLost]
        .into_iter()
        .enumerate()
    {
        let cluster = MemCluster::builder(&["event-class"])
            .group("stores")
            .spawn();
        let source = MemSource::default();
        *source.terminal_failure_once.lock().expect("failure lock") = Some(class);
        let manager =
            Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
                .expect("manager")
                .with_event_complete(MemSink::default(), SubscriptionLimits::default())
                .expect("named capability");
        let session_id =
            NonZeroU64::new(30 + u64::try_from(index).expect("bounded index")).expect("nonzero");
        let handle = manager
            .open_named(
                &scope(),
                SubscriberId {
                    name: "billing".into(),
                },
                session_id,
                SubscriptionStart::StartAt {
                    position: 0,
                    policy: policy(),
                    request_id: vec![30 + u8::try_from(index).expect("bounded index")],
                },
            )
            .expect("subscriber");
        eventually_within("source registration protected", SETTLE, || {
            handle.status().stage == Stage::Protected
        })
        .await;
        let stable = vec![40 + u8::try_from(index).expect("bounded index")];
        let receipt = handle
            .unsubscribe(stable.clone(), Instant::now() + SETTLE)
            .await
            .expect("classified failure must read back committed tombstone");
        assert_eq!(receipt.request.request_id, stable);
        assert_eq!(handle.status().stage, Stage::TerminatedSubscriber);
        assert!(source.terminal_calls.load(Ordering::Acquire) >= 1);
    }
}

#[tokio::test]
async fn detached_classified_failures_after_commit_read_back_exact_tombstone() {
    use groupnet_consistency::replication::FailureClass;

    for (index, class) in [FailureClass::Terminal, FailureClass::AuthorityLost]
        .into_iter()
        .enumerate()
    {
        let cluster = MemCluster::builder(&["event-detached-class"])
            .group("stores")
            .spawn();
        let source = MemSource::default();
        let manager =
            Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
                .expect("manager")
                .with_event_complete(MemSink::default(), SubscriptionLimits::default())
                .expect("named capability");
        let key = SubscriberId {
            name: "billing".into(),
        };
        let original_id =
            NonZeroU64::new(60 + u64::try_from(index).expect("bounded index")).expect("nonzero");
        let handle = manager
            .open_named(
                &scope(),
                key,
                original_id,
                SubscriptionStart::StartAt {
                    position: 0,
                    policy: policy(),
                    request_id: vec![60 + u8::try_from(index).expect("bounded index")],
                },
            )
            .expect("registered");
        eventually_within("source registration protected", SETTLE, || {
            handle.status().stage == Stage::Protected
        })
        .await;
        let subscriber_key = handle.key().clone();
        assert!(manager.close_named_if(&subscriber_key, original_id));
        *source.terminal_failure_once.lock().expect("failure lock") = Some(class);
        let stable = vec![70 + u8::try_from(index).expect("bounded index")];
        let receipt = manager
            .unsubscribe_detached(
                subscriber_key.clone(),
                NonZeroU64::new(70 + u64::try_from(index).expect("bounded index"))
                    .expect("nonzero"),
                stable.clone(),
                Instant::now() + SETTLE,
            )
            .await
            .expect("failed postcommit response must read back exact source receipt");
        assert_eq!(receipt.request.key, subscriber_key);
        assert_eq!(receipt.request.request_id, stable);
        assert!(receipt.durable);
    }
}

#[tokio::test]
async fn cancelled_detached_caller_can_inspect_committed_request_and_releases_admission() {
    let cluster = MemCluster::builder(&["event-detached-drop"])
        .group("stores")
        .spawn();
    let source = MemSource::default();
    let manager = Arc::new(
        Replication::new(cluster.groups[0].clone(), source.clone(), StubApp, limits())
            .expect("manager")
            .with_event_complete(MemSink::default(), SubscriptionLimits::default())
            .expect("named capability"),
    );
    let original_id = NonZeroU64::new(50).expect("nonzero");
    let opened = manager
        .open_named(
            &scope(),
            SubscriberId {
                name: "billing".into(),
            },
            original_id,
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![50],
            },
        )
        .expect("initial registration");
    eventually_within("registered", SETTLE, || {
        opened.status().stage == Stage::Protected
    })
    .await;
    let key = opened.key().clone();
    assert!(manager.close_named_if(&key, original_id));
    let gate = Arc::new(TerminalGate::default());
    *source.terminal_gate.lock().expect("terminal gate lock") = Some(Arc::clone(&gate));
    let caller = Arc::clone(&manager);
    let detached_id = NonZeroU64::new(51).expect("nonzero");
    let detached_key = key.clone();
    let pending = tokio::spawn(async move {
        caller
            .unsubscribe_detached(detached_key, detached_id, vec![51], Instant::now() + SETTLE)
            .await
    });
    eventually_within("terminal persisted before lost reply", SETTLE, || {
        gate.entered.load(Ordering::Acquire)
    })
    .await;
    assert_eq!(
        manager
            .unsubscribe_detached(
                key.clone(),
                NonZeroU64::new(52).expect("nonzero"),
                vec![52],
                Instant::now() + SETTLE,
            )
            .await,
        Err(groupnet_consistency::replication::DetachedUnsubscribeError::ActiveSession)
    );
    assert!(matches!(
        manager.open_named(
            &scope(),
            SubscriberId {
                name: "billing".into()
            },
            NonZeroU64::new(53).expect("nonzero"),
            SubscriptionStart::StartAt {
                position: 0,
                policy: policy(),
                request_id: vec![53],
            },
        ),
        Err(OpenError::AlreadyOpen)
    ));
    pending.abort();
    let _ = pending.await;
    let receipt = manager
        .read_current_terminal(key.clone(), Instant::now() + SETTLE)
        .await
        .expect("source ledger query")
        .expect("durable tombstone after caller drop");
    assert_eq!(receipt.request.request_id, vec![51]);
    assert_eq!(receipt.request.key, key);
    assert!(
        manager
            .open_named(
                &scope(),
                SubscriberId {
                    name: "billing".into(),
                },
                detached_id,
                SubscriptionStart::StartAt {
                    position: 0,
                    policy: policy(),
                    request_id: vec![52],
                },
            )
            .is_ok(),
        "cancelled detached call must release shared incarnation admission"
    );
}
