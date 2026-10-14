//! Facade exposes typed state-sync APIs only with the opt-in feature.

#![cfg(feature = "consistency-replication")]

use std::error::Error;
use std::future::ready;
use std::io;

use groupnet::consistency::replication::{
    AckEvidenceSource, AckObservation, AckSourceFailure, AckWaitLimits, AckWaitRequest,
    AdapterFailure, ApplicationAdapter, BoundComparison, CatchUp, Checkpoint, CheckpointLimit,
    CheckpointLoadResult, CoreConfig, Cursor, InstallPermit, Limits, LimitsError, Materialized,
    NamedAckRequest, Operation, RequiredSubscriber, RevocationPermit, ScanLimit, Scope,
    SourceAdapter, SourceProof, SourceScanResult, TailLimit,
};

/// Adapters written against the tier module alone, with no `groupnet_core`
/// path: every signature type is reachable from `replication`.
#[derive(Debug)]
struct TierOnly;

fn unsupported() -> io::Error {
    io::Error::other("compile-only adapter")
}

impl SourceAdapter for TierOnly {
    type Position = u64;
    type Batch = Vec<u8>;
    type Error = io::Error;

    fn cursor(&self, _: &Scope, _: &u64) -> Result<Cursor, AdapterFailure<io::Error>> {
        Err(AdapterFailure::Terminal(unsupported()))
    }

    fn position(&self, _: &Cursor) -> Result<u64, AdapterFailure<io::Error>> {
        Err(AdapterFailure::Terminal(unsupported()))
    }

    fn compare(
        &self,
        _: &Cursor,
        _: &Cursor,
        _: &SourceProof,
    ) -> Result<BoundComparison, AdapterFailure<io::Error>> {
        Err(AdapterFailure::Terminal(unsupported()))
    }

    fn tail(
        &self,
        _: Scope,
        _: Option<u64>,
        _: TailLimit,
    ) -> impl Future<Output = Result<SourceProof, AdapterFailure<io::Error>>> + Send {
        ready(Err(AdapterFailure::Terminal(unsupported())))
    }

    fn scan_after(
        &self,
        _: Scope,
        _: u64,
        _: SourceProof,
        _: ScanLimit,
    ) -> impl Future<Output = SourceScanResult<u64, Vec<u8>, io::Error>> + Send {
        ready(Err(AdapterFailure::Terminal(unsupported())))
    }
}

impl ApplicationAdapter<u64, Vec<u8>> for TierOnly {
    type Error = io::Error;
    type Recovery = Vec<u8>;

    fn load_checkpoint(
        &self,
        _: Scope,
        _: CheckpointLimit,
    ) -> impl Future<Output = CheckpointLoadResult<u64, Vec<u8>, io::Error>> + Send {
        ready(Ok(None))
    }

    fn install_checkpoint(
        &self,
        _: Scope,
        _: Checkpoint<u64, Vec<u8>>,
        _: InstallPermit,
    ) -> impl Future<Output = Result<Materialized<u64>, AdapterFailure<io::Error>>> + Send {
        ready(Err(AdapterFailure::Terminal(unsupported())))
    }

    fn revoke_serving(
        &self,
        _: Scope,
        _: RevocationPermit,
    ) -> impl Future<Output = Result<(), AdapterFailure<io::Error>>> + Send {
        ready(Ok(()))
    }

    fn apply(
        &self,
        _: Scope,
        _: u64,
        _: u64,
        _: Vec<u8>,
        _: InstallPermit,
    ) -> impl Future<Output = Result<Materialized<u64>, AdapterFailure<io::Error>>> + Send {
        ready(Err(AdapterFailure::Terminal(unsupported())))
    }

    fn may_serve(&self, _: &Scope, _: &u64) -> bool {
        false
    }
}

impl AckEvidenceSource for TierOnly {
    fn certify(
        &self,
        _: &NamedAckRequest,
        _: AckWaitLimits,
    ) -> impl Future<Output = Result<(), AckSourceFailure>> + Send {
        ready(Err(AckSourceFailure::Unsupported))
    }

    fn observe(
        &self,
        _: &AckWaitRequest,
        _: Operation,
        _: &[RequiredSubscriber],
        _: AckWaitLimits,
    ) -> impl Future<Output = Result<AckObservation, AckSourceFailure>> + Send {
        ready(Ok(AckObservation::Pending))
    }
}

fn accepts_adapters<S, A, E>(_: &S, _: &A, _: &E)
where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
    E: AckEvidenceSource,
{
}

#[test]
fn facade_reexports_bounded_native_floor_outcomes() {
    assert_eq!(Limits::default().validate(), Ok(()));
    let outcome = CatchUp::Ready(42_u64);
    assert_eq!(outcome, CatchUp::Ready(42));
}

#[test]
fn tier_module_alone_implements_every_adapter_and_names_limit_errors() {
    accepts_adapters(&TierOnly, &TierOnly, &TierOnly);
    let limits = Limits {
        core: CoreConfig {
            tail_check_ms: 0,
            ..CoreConfig::default()
        },
        ..Limits::default()
    };
    let error = limits.validate().expect_err("zero tail cadence");
    assert!(matches!(error, LimitsError::Core(_)));
    let error: &dyn Error = &error;
    assert!(error.to_string().contains("core config"));
}
