//! Publishing state entries into the group actor's bounded inbox, and what a
//! ledger's `record` reports about it.

use groupnet_runtime::CommandRejected;

/// Attempts before giving up on an enqueue under inbox backpressure. Every
/// publisher here gossips *state*, so a later successful publication carries
/// whatever an abandoned one would have.
const PUBLISH_RETRIES: usize = 8;

/// Runs `try_enqueue` until it succeeds, yielding to the runtime between
/// attempts so the group actor can drain its inbox, and reports the last
/// refusal once [`PUBLISH_RETRIES`] attempts have all been refused.
pub(crate) async fn enqueue(
    mut try_enqueue: impl FnMut() -> Result<(), CommandRejected>,
) -> Result<(), CommandRejected> {
    let mut attempt = 1;
    loop {
        match try_enqueue() {
            Ok(()) => return Ok(()),
            Err(rejected) if attempt >= PUBLISH_RETRIES => return Err(rejected),
            Err(_) => attempt += 1,
        }
        tokio::task::yield_now().await;
    }
}

/// What recording an applied token did to a ledger — returned by
/// `AckLedger::record` (feature `acks`) and `hosted::CommitLedger::record`
/// (feature `hosted`).
///
/// It reports only the local operation: `Advanced` means the ledger changed
/// and was *enqueued* for gossip, never that any peer has seen it. A refused
/// enqueue is the record's `Err` instead.
#[cfg(any(feature = "acks", feature = "hosted"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// The ledger advanced, and the whole ledger was enqueued for gossip.
    Advanced,
    /// The token changed nothing (at or below the recorded watermark); nothing
    /// was published.
    Covered,
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use groupnet_runtime::CommandRejected;

    use super::{PUBLISH_RETRIES, enqueue};

    #[tokio::test]
    async fn a_sustained_refusal_is_reported_after_the_retry_budget() {
        let attempts = Cell::new(0);
        let outcome = enqueue(|| {
            attempts.set(attempts.get() + 1);
            Err(CommandRejected)
        })
        .await;
        assert_eq!(outcome, Err(CommandRejected));
        assert_eq!(attempts.get(), PUBLISH_RETRIES);
    }

    #[tokio::test]
    async fn a_transient_refusal_is_retried_away() {
        let attempts = Cell::new(0);
        let outcome = enqueue(|| {
            attempts.set(attempts.get() + 1);
            if attempts.get() < 3 {
                Err(CommandRejected)
            } else {
                Ok(())
            }
        })
        .await;
        assert_eq!(outcome, Ok(()));
        assert_eq!(attempts.get(), 3);
    }
}
