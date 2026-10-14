use std::error::Error;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use groupnet_core::replication::{
    Config as CoreConfig, ConfigError as CoreConfigError, SnapshotConfig,
};
use groupnet_transport::QueueCapacity;
use tokio::sync::Semaphore;

use super::{Limits, LimitsError};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("nonzero")
}

fn nz64(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("nonzero")
}

fn beyond_u32() -> Option<usize> {
    usize::try_from(u64::from(u32::MAX) + 1).ok()
}

fn snapshot(max_chunk_bytes: usize, max_candidate_bytes: u64) -> SnapshotConfig {
    SnapshotConfig {
        max_metadata_bytes: nz(512),
        max_snapshot_bytes: nz64(64),
        max_chunks: NonZeroU32::new(16).expect("nonzero"),
        max_chunk_bytes: nz(max_chunk_bytes),
        max_candidate_bytes: nz64(max_candidate_bytes),
        max_total_ms: 1_000,
    }
}

/// Small snapshot-enabled limits that pass every rule.
fn snapshot_limits() -> Limits {
    Limits {
        core: CoreConfig {
            max_batch_bytes: nz(8),
            snapshot: Some(snapshot(8, 64)),
            ..CoreConfig::default()
        },
        max_parallel_ops: nz(2),
        max_inflight_bytes: nz(16),
        max_checkpoint_bytes: nz(64),
        max_checkpoint_inflight_bytes: nz(128),
        ..Limits::default()
    }
}

#[test]
fn defaults_and_minimal_snapshot_limits_validate() {
    assert_eq!(Limits::default().validate(), Ok(()));
    assert_eq!(snapshot_limits().validate(), Ok(()));
}

#[test]
fn failed_core_config_is_carried_unchanged() {
    let core = CoreConfig {
        tail_check_ms: 0,
        ..CoreConfig::default()
    };
    let expected: CoreConfigError = core.validate().expect_err("zero tail cadence is invalid");
    let error = Limits {
        core,
        ..Limits::default()
    }
    .validate()
    .expect_err("core failure surfaces");
    assert_eq!(error, LimitsError::Core(expected));
    let source = error.source().expect("core error is the source");
    assert_eq!(
        source.downcast_ref::<CoreConfigError>(),
        Some(&expected),
        "source carries the core error unchanged"
    );
}

#[test]
fn shell_reserves_report_their_own_field() {
    let above = nz(Semaphore::MAX_PERMITS + 1);
    let limits = Limits {
        max_parallel_ops: above,
        ..Limits::default()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::ParallelOpsAbovePermits {
            max_parallel_ops: above.get()
        })
    );

    let batch = Limits::default().core.max_batch_bytes.get();
    let limits = Limits {
        max_inflight_bytes: nz(batch - 1),
        ..Limits::default()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::InflightBelowBatch {
            max_inflight_bytes: batch - 1,
            max_batch_bytes: batch,
        })
    );

    let limits = Limits {
        max_inflight_bytes: above,
        ..Limits::default()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::InflightAbovePermits {
            max_inflight_bytes: above.get()
        })
    );

    let limits = Limits {
        max_checkpoint_inflight_bytes: nz(1),
        ..Limits::default()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::CheckpointInflightBelowCheckpoint {
            max_checkpoint_inflight_bytes: 1,
            max_checkpoint_bytes: Limits::default().max_checkpoint_bytes.get(),
        })
    );

    let limits = Limits {
        max_checkpoint_inflight_bytes: above,
        ..Limits::default()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::CheckpointInflightAbovePermits {
            max_checkpoint_inflight_bytes: above.get()
        })
    );

    // The command queue's bound lives in its type: the largest capacity a
    // channel accepts is valid, and nothing above it is constructible.
    let limits = Limits {
        queue_depth: QueueCapacity::MAX,
        ..Limits::default()
    };
    assert_eq!(limits.validate(), Ok(()));
    assert!(QueueCapacity::try_from(above.get()).is_err());
}

#[test]
fn u32_permit_charges_report_their_own_field() {
    let Some(beyond) = beyond_u32() else {
        return;
    };
    let limits = Limits {
        core: CoreConfig {
            max_batch_bytes: nz(beyond),
            ..CoreConfig::default()
        },
        max_inflight_bytes: nz(beyond),
        ..Limits::default()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::BatchAboveU32 {
            max_batch_bytes: beyond
        })
    );

    let limits = Limits {
        max_checkpoint_bytes: nz(beyond),
        max_checkpoint_inflight_bytes: nz(beyond),
        ..Limits::default()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::CheckpointAboveU32 {
            max_checkpoint_bytes: beyond
        })
    );

    let mut limits = snapshot_limits();
    limits.core.snapshot = Some(snapshot(beyond, 64));
    assert_eq!(
        limits.validate(),
        Err(LimitsError::SnapshotChunkAboveU32 {
            max_chunk_bytes: beyond
        })
    );
}

#[test]
fn snapshot_reserves_report_their_own_field() {
    let limits = Limits {
        max_parallel_ops: nz(1),
        ..snapshot_limits()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::SnapshotParallelOpsBelowTwo {
            max_parallel_ops: 1
        })
    );

    let mut limits = snapshot_limits();
    limits.core.snapshot = Some(snapshot(9, 64));
    assert_eq!(
        limits.validate(),
        Err(LimitsError::SnapshotInflightBelowReserve {
            max_inflight_bytes: 16,
            max_batch_bytes: 8,
            max_chunk_bytes: 9,
        })
    );

    let mut limits = snapshot_limits();
    limits.core.snapshot = Some(snapshot(8, 65));
    assert_eq!(
        limits.validate(),
        Err(LimitsError::SnapshotCandidateAboveCheckpoint {
            max_candidate_bytes: 65,
            max_checkpoint_bytes: 64,
        })
    );

    let limits = Limits {
        max_checkpoint_inflight_bytes: nz(127),
        ..snapshot_limits()
    };
    assert_eq!(
        limits.validate(),
        Err(LimitsError::SnapshotCheckpointInflightBelowReserve {
            max_checkpoint_inflight_bytes: 127,
            max_checkpoint_bytes: 64,
            max_candidate_bytes: 64,
        })
    );
}

#[test]
fn shell_rule_errors_name_their_field_and_have_no_source() {
    let error = LimitsError::ParallelOpsAbovePermits {
        max_parallel_ops: 7,
    };
    assert!(error.source().is_none());
    assert!(error.to_string().contains("max_parallel_ops (7)"));
}
