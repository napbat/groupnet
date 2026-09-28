//! Evidence objects returned by trusted source adapters.

use super::{Cursor, IdentityError, Scope};

/// Opaque identifier for one verified source barrier/retention statement.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProofId(pub Vec<u8>);

/// Relative order in one authoritative source history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    /// Left precedes right.
    Before,
    /// Left and right name the same source position.
    Equal,
    /// Left follows right.
    After,
    /// Adapter cannot prove a shared order.
    Incomparable,
}

/// Comparison bound to the exact operands and source proof that produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundComparison {
    /// Left operand.
    pub left: Cursor,
    /// Right operand.
    pub right: Cursor,
    /// Proof against which the adapter compared them.
    pub proof: ProofId,
    /// Verified order.
    pub order: Comparison,
}

impl BoundComparison {
    /// Returns the order only for these exact operands and proof.
    #[must_use]
    pub fn for_operands(
        &self,
        left: &Cursor,
        right: &Cursor,
        proof: &ProofId,
    ) -> Option<Comparison> {
        (self.left == *left && self.right == *right && self.proof == *proof).then_some(self.order)
    }
}

/// Verified source head and replay retention boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceProof {
    /// Unique source-proof identity.
    pub id: ProofId,
    /// Inclusive source head at the checked barrier.
    pub head: Cursor,
    /// Oldest cursor from which the source can replay a continuous suffix.
    pub retained_from: Cursor,
    /// Whether this source proof also carries local read authority.
    pub read_authority: bool,
}

impl SourceProof {
    /// Checks scope, history, and bounded proof/cursor encodings.
    ///
    /// # Errors
    /// Returns an identity error for malformed proof or cursor identity.
    pub fn validate(&self, scope: &Scope, max_bytes: usize) -> Result<(), IdentityError> {
        self.head.validate(scope, max_bytes)?;
        self.retained_from.validate(scope, max_bytes)?;
        if self.head.history != self.retained_from.history {
            return Err(IdentityError::WrongHistory);
        }
        if self.id.0.is_empty() {
            return Err(IdentityError::Empty);
        }
        if self.id.0.len() > max_bytes {
            return Err(IdentityError::TooLarge);
        }
        Ok(())
    }
}

/// Adapter proof that a batch covers every committed record in `(from, through]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coverage {
    /// Exclusive lower cursor.
    pub from: Cursor,
    /// Inclusive upper cursor.
    pub through: Cursor,
    /// Source proof used for the scan.
    pub proof: ProofId,
    /// Opaque bounded certificate validated by the source adapter.
    pub certificate: Vec<u8>,
}
