//! Local driver resource cleanup tests.

use super::{Checkpoint, Payload, PrivateCheckpoint, discard_stale, rejected_step};
use groupnet_core::replication::{Effect, Operation, Reject, SnapshotCleanupDisposition, Step};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::Semaphore;

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

#[test]
fn stale_effect_releases_native_batch_reserve_and_private_checkpoint() {
    let bytes = Arc::new(Semaphore::new(8));
    let permit = Arc::clone(&bytes)
        .try_acquire_many_owned(8)
        .expect("reserve");
    let checkpoint_bytes = Arc::new(Semaphore::new(8));
    let checkpoint_permit = Arc::clone(&checkpoint_bytes)
        .try_acquire_many_owned(8)
        .expect("checkpoint reserve");
    let released = Arc::new(AtomicUsize::new(0));
    let mut payloads = HashMap::from([(
        7,
        Payload {
            native: (),
            _bytes: permit,
            _snapshot_bytes: None,
        },
    )]);
    let mut checkpoints = HashMap::from([(
        8,
        PrivateCheckpoint {
            candidate: Checkpoint {
                position: (),
                native: DropCount(Arc::clone(&released)),
                bytes: 8,
            },
            _bytes: checkpoint_permit,
            _snapshot_bytes: None,
        },
    )]);
    let apply = Operation {
        session: 1,
        generation: 1,
        token: 2,
    };
    let install = Operation {
        session: 1,
        generation: 1,
        token: 3,
    };
    let mut deadlines = HashMap::from([(apply, Instant::now()), (install, Instant::now())]);
    discard_stale(
        apply,
        Some(7),
        None,
        &mut deadlines,
        &mut payloads,
        &mut checkpoints,
    );
    assert_eq!(bytes.available_permits(), 8);
    assert!(payloads.is_empty());
    discard_stale(
        install,
        None,
        Some(8),
        &mut deadlines,
        &mut payloads,
        &mut checkpoints,
    );
    assert_eq!(released.load(Ordering::Acquire), 1);
    assert_eq!(checkpoint_bytes.available_permits(), 8);
    assert!(checkpoints.is_empty());
    assert!(deadlines.is_empty());
}

#[test]
fn rejected_terminal_transition_keeps_revocation_and_tagged_disposal() {
    let attempt = Operation {
        session: 1,
        generation: u64::MAX,
        token: 1,
    };
    let cleanup = Operation {
        token: 2,
        ..attempt
    };
    let expected = vec![
        Effect::RevokeServingUnconfirmed,
        Effect::DiscardSnapshotResources {
            op: cleanup,
            attempt,
            disposition: SnapshotCleanupDisposition::Aborted,
        },
    ];
    let Step { effects, rejection } = Step {
        effects: expected.clone(),
        rejection: Some(Reject::Exhausted),
    };
    let (stop, queued) = rejected_step(rejection.expect("rejected"), true, effects);
    assert!(stop, "the shell must close its local gate");
    assert_eq!(queued, expected, "safety effects survive rejection");
}
