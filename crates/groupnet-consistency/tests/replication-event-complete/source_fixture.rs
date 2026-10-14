//! Durable source fixture with conditional registration and ack ledgers.

use super::*;

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the in-memory source acts synchronously while exercising the production async adapter contract"
)]
impl SourceAdapter for MemSource {
    type Position = u64;
    type Batch = Vec<u64>;
    type Error = io::Error;

    fn cursor(&self, scope: &Scope, position: &u64) -> Result<Cursor, AdapterFailure<io::Error>> {
        if let Some(gate) = self.cursor_gate.lock().expect("cursor gate lock").clone() {
            gate.entered.store(true, Ordering::Release);
            let mut released = gate.released.lock().expect("gate lock");
            while !*released {
                released = gate.wake.wait(released).expect("gate wait");
            }
        }
        Ok(cursor(scope, *position))
    }

    fn position(&self, cursor: &Cursor) -> Result<u64, AdapterFailure<io::Error>> {
        Ok(position(cursor))
    }

    fn compare(
        &self,
        left: &Cursor,
        right: &Cursor,
        proof: &SourceProof,
    ) -> Result<BoundComparison, AdapterFailure<io::Error>> {
        Ok(comparison(left, right, proof))
    }

    async fn tail(
        &self,
        scope: Scope,
        _from: Option<u64>,
        _limit: TailLimit,
    ) -> Result<SourceProof, AdapterFailure<io::Error>> {
        let state = self.state.lock().expect("source lock");
        Ok(proof(&scope, state.head, state.retained))
    }

    async fn scan_after(
        &self,
        _scope: Scope,
        from: u64,
        proof: SourceProof,
        _limit: ScanLimit,
    ) -> Result<SourceBatch<u64, Vec<u64>>, AdapterFailure<io::Error>> {
        let through = from + 1;
        Ok(SourceBatch {
            from,
            through,
            proof: proof.id,
            certificate: vec![1],
            events: 1,
            bytes: 8,
            native: vec![through],
        })
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "in-memory protocol fixture matches the async source interface"
)]
impl DurableSubscriptionSource for MemSource {
    async fn read_current_subscriber(
        &self,
        key: SubscriberKey,
    ) -> SubscriptionSourceResult<Option<SourceSubscriberState>, io::Error> {
        let state = self.state.lock().expect("source lock");
        let Some(registration) = state.registrations.get(&key) else {
            return SubscriptionSourceResult::Accepted(None);
        };
        let ack = *state.source_ack.get(&key).expect("registered ack");
        let verified = proof(&key.scope, state.head, state.retained);
        let acknowledged = cursor(&key.scope, ack);
        SubscriptionSourceResult::Accepted(Some(SourceSubscriberState {
            key,
            epoch: registration.epoch.clone(),
            acknowledged: acknowledged.clone(),
            policy_fingerprint: registration.policy_fingerprint.clone(),
            retained_to_ack: comparison(&verified.retained_from, &acknowledged, &verified),
            ack_to_head: comparison(&acknowledged, &verified.head, &verified),
            proof: verified,
        }))
    }

    async fn register_subscriber(
        &self,
        request: RegisterSubscriber,
    ) -> SubscriptionSourceResult<RegisterReceipt, io::Error> {
        self.register_calls.fetch_add(1, Ordering::AcqRel);
        let mut state = self.state.lock().expect("source lock");
        if let Some(existing) = state.registration_requests.get(&request.request_id) {
            return SubscriptionSourceResult::Accepted(existing.clone());
        }
        let prior = state.registrations.get(&request.key);
        match (&request.reset_from, request.expected_prior_ordinal, prior) {
            (None, None, None) if !state.ordinal.contains_key(&request.key) => {}
            (None, Some(expected), Some(existing))
                if expected == existing.epoch.ordinal
                    && state.source_ack.get(&request.key).copied()
                        == Some(position(&request.start)) => {}
            (Some(tombstone), Some(expected), None)
                if tombstone.tombstone_ordinal == expected
                    && state.ordinal.get(&request.key).copied() == Some(expected.get())
                    && state
                        .terminal_requests
                        .get(&(request.key.clone(), tombstone.request.request_id.clone()))
                        == Some(tombstone) => {}
            _ => return SubscriptionSourceResult::Rejected(SubscriptionError::ConditionalMismatch),
        }
        if position(&request.start) < state.retained || position(&request.start) > state.head {
            return SubscriptionSourceResult::Rejected(SubscriptionError::HistoryUnavailable);
        }
        let next = state.ordinal.get(&request.key).copied().unwrap_or(0) + 1;
        state.ordinal.insert(request.key.clone(), next);
        let verified = proof(&request.key.scope, state.head, state.retained);
        let receipt = RegisterReceipt {
            key: request.key.clone(),
            incarnation: request.incarnation,
            request_id: request.request_id.clone(),
            epoch: SubscriptionEpoch {
                ordinal: NonZeroU64::new(next).expect("monotonic ordinal"),
                native: next.to_le_bytes().to_vec(),
                history: request.start.history.clone(),
            },
            protected: request.start.clone(),
            retained_to_protected: comparison(&verified.retained_from, &request.start, &verified),
            protected_to_head: comparison(&request.start, &verified.head, &verified),
            proof: verified,
            policy_fingerprint: request.policy.fingerprint,
        };
        state
            .source_ack
            .insert(request.key.clone(), position(&request.start));
        state.registrations.insert(request.key, receipt.clone());
        state
            .registration_requests
            .insert(request.request_id, receipt.clone());
        SubscriptionSourceResult::Accepted(receipt)
    }

    async fn read_subscriber_registration(
        &self,
        _key: SubscriberKey,
        request_id: Vec<u8>,
    ) -> SubscriptionSourceResult<Option<RegisterReceipt>, io::Error> {
        SubscriptionSourceResult::Accepted(
            self.state
                .lock()
                .expect("source lock")
                .registration_requests
                .get(&request_id)
                .cloned(),
        )
    }

    async fn subscriber_tail(
        &self,
        registration: RegisterReceipt,
        from: u64,
        _limit: TailLimit,
    ) -> SubscriptionSourceResult<SourceProof, io::Error> {
        self.tails.fetch_add(1, Ordering::AcqRel);
        let state = self.state.lock().expect("source lock");
        if state.terminal_requests.values().any(|receipt| {
            receipt.request.key == registration.key
                && receipt.request.epoch == registration.epoch
                && receipt.request.reason == groupnet_core::replication::TerminalReason::Expired
        }) {
            return SubscriptionSourceResult::Rejected(SubscriptionError::Expired);
        }
        if state
            .registrations
            .get(&registration.key)
            .is_none_or(|live| live.epoch != registration.epoch)
            || state.source_ack.get(&registration.key).copied() != Some(from)
        {
            return SubscriptionSourceResult::Rejected(SubscriptionError::ConditionalMismatch);
        }
        SubscriptionSourceResult::Accepted(proof(
            &registration.key.scope,
            state.head,
            state.retained,
        ))
    }

    async fn scan_subscriber(
        &self,
        registration: RegisterReceipt,
        from: u64,
        proof: SourceProof,
        limit: ScanLimit,
    ) -> SubscriptionSourceResult<SourceBatch<u64, Vec<u64>>, io::Error> {
        let state = self.state.lock().expect("source lock");
        if state
            .registrations
            .get(&registration.key)
            .is_none_or(|live| live.epoch != registration.epoch)
            || state.source_ack.get(&registration.key).copied() != Some(from)
            || from < state.retained
        {
            return SubscriptionSourceResult::Rejected(SubscriptionError::Expired);
        }
        let through = (from + 1).min(state.head).min(position(&proof.head));
        if through == from || limit.events == 0 || limit.bytes < 8 {
            return SubscriptionSourceResult::Rejected(SubscriptionError::Backpressured);
        }
        SubscriptionSourceResult::Accepted(SourceBatch {
            from,
            through,
            proof: proof.id,
            certificate: vec![1],
            events: 1,
            bytes: 8,
            native: vec![through],
        })
    }

    async fn commit_subscriber_ack(
        &self,
        request: CommitSubscriberAck,
    ) -> SubscriptionSourceResult<SubscriberAckReceipt, io::Error> {
        let mut state = self.state.lock().expect("source lock");
        if let Some(found) = state
            .ack_requests
            .get(&(request.key.clone(), request.request_id.clone()))
        {
            return SubscriptionSourceResult::Accepted(found.clone());
        }
        if state
            .registrations
            .get(&request.key)
            .is_none_or(|live| live.epoch != request.epoch)
            || state.source_ack.get(&request.key).copied() != Some(position(&request.previous))
        {
            return SubscriptionSourceResult::Rejected(SubscriptionError::ConditionalMismatch);
        }
        state
            .source_ack
            .insert(request.key.clone(), position(&request.through));
        let receipt = SubscriberAckReceipt {
            request: request.clone(),
            durable: true,
        };
        state.ack_requests.insert(
            (request.key.clone(), request.request_id.clone()),
            receipt.clone(),
        );
        if self.unknown_ack_once.swap(false, Ordering::AcqRel) {
            SubscriptionSourceResult::Failed(AdapterFailure::Retryable(io::Error::other(
                "committed; response lost",
            )))
        } else {
            SubscriptionSourceResult::Accepted(receipt)
        }
    }

    async fn read_subscriber_ack(
        &self,
        request: CommitSubscriberAck,
    ) -> SubscriptionSourceResult<Option<SubscriberAckReceipt>, io::Error> {
        SubscriptionSourceResult::Accepted(
            self.state
                .lock()
                .expect("source lock")
                .ack_requests
                .get(&(request.key, request.request_id))
                .cloned(),
        )
    }

    async fn commit_subscriber_terminal(
        &self,
        request: groupnet_core::replication::TerminalRequest,
    ) -> SubscriptionSourceResult<groupnet_core::replication::TerminalReceipt, io::Error> {
        self.terminal_calls.fetch_add(1, Ordering::AcqRel);
        let receipt = {
            let mut state = self.state.lock().expect("source lock");
            let id = (request.key.clone(), request.request_id.clone());
            if let Some(found) = state.terminal_requests.get(&id) {
                return SubscriptionSourceResult::Accepted(found.clone());
            }
            if state
                .registrations
                .get(&request.key)
                .is_none_or(|live| live.epoch != request.epoch)
                || state.source_ack.get(&request.key).copied()
                    != Some(position(&request.acknowledged))
            {
                return SubscriptionSourceResult::Rejected(SubscriptionError::ConditionalMismatch);
            }
            let receipt = groupnet_core::replication::TerminalReceipt {
                tombstone_ordinal: request.epoch.ordinal,
                request,
                durable: true,
            };
            state.registrations.remove(&receipt.request.key);
            state.terminal_requests.insert(id, receipt.clone());
            receipt
        };
        let gate = self
            .terminal_gate
            .lock()
            .expect("terminal gate lock")
            .clone();
        if let Some(gate) = gate {
            gate.entered.store(true, Ordering::Release);
            gate.release.notified().await;
        }
        if let Some(class) = self
            .terminal_failure_once
            .lock()
            .expect("failure lock")
            .take()
        {
            let error = io::Error::other("terminal committed; classified response lost");
            return SubscriptionSourceResult::Failed(match class {
                groupnet_consistency::replication::FailureClass::Retryable => {
                    AdapterFailure::Retryable(error)
                }
                groupnet_consistency::replication::FailureClass::Terminal => {
                    AdapterFailure::Terminal(error)
                }
                groupnet_consistency::replication::FailureClass::AuthorityLost => {
                    AdapterFailure::AuthorityLost(error)
                }
            });
        }
        if self.unknown_terminal_once.swap(false, Ordering::AcqRel) {
            SubscriptionSourceResult::Failed(AdapterFailure::Retryable(io::Error::other(
                "terminal committed; response lost",
            )))
        } else {
            SubscriptionSourceResult::Accepted(receipt)
        }
    }

    async fn read_subscriber_terminal(
        &self,
        request: groupnet_core::replication::TerminalRequest,
    ) -> SubscriptionSourceResult<Option<groupnet_core::replication::TerminalReceipt>, io::Error>
    {
        SubscriptionSourceResult::Accepted(
            self.state
                .lock()
                .expect("source lock")
                .terminal_requests
                .get(&(request.key, request.request_id))
                .cloned(),
        )
    }

    async fn read_current_terminal(
        &self,
        key: SubscriberKey,
    ) -> SubscriptionSourceResult<Option<groupnet_core::replication::TerminalReceipt>, io::Error>
    {
        let state = self.state.lock().expect("source lock");
        let current = state
            .terminal_requests
            .values()
            .find(|receipt| {
                receipt.request.key == key
                    && state.ordinal.get(&key).copied() == Some(receipt.tombstone_ordinal.get())
            })
            .cloned();
        SubscriptionSourceResult::Accepted(current)
    }
}
