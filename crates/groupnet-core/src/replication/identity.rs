//! Scoped native cursor identity. Positions are opaque and never byte-ordered.

/// Stable named topic and payload type within a group.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Stream {
    /// Group name.
    pub group: String,
    /// Topic name.
    pub topic: String,
    /// Application-defined type/version name.
    pub kind: String,
}

/// Independently recoverable partition within a stream.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scope {
    /// Stream identity.
    pub stream: Stream,
    /// Shard, bucket, or partition name.
    pub partition: String,
}

/// Authoritative source identity and durable history generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceHistory {
    /// Application source identifier.
    pub source: String,
    /// Source generation, independent of a feed writer incarnation.
    pub generation: u64,
}

/// Source-native position encoded by the adapter in a bounded byte string.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Cursor {
    /// Scope to which the position belongs.
    pub scope: Scope,
    /// Authority that defines ordering and continuity.
    pub history: SourceHistory,
    /// Versioned native position bytes; no lexical ordering is inferred.
    pub position: Vec<u8>,
}

/// An identity, scope, or size validation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityError {
    /// A required name or position is empty.
    Empty,
    /// A name or position exceeds the configured byte cap.
    TooLarge,
    /// The cursor belongs to a different stream or partition.
    WrongScope,
    /// The cursor belongs to a different authoritative history.
    WrongHistory,
}

impl Cursor {
    /// Checks identity and encoded position bounds before retaining a cursor.
    ///
    /// # Errors
    /// Returns an identity error for empty, oversized, or wrong-scope fields.
    pub fn validate(&self, scope: &Scope, max_bytes: usize) -> Result<(), IdentityError> {
        if self.scope != *scope {
            return Err(IdentityError::WrongScope);
        }
        let names = [
            &self.scope.stream.group,
            &self.scope.stream.topic,
            &self.scope.stream.kind,
            &self.scope.partition,
            &self.history.source,
        ];
        if names.iter().any(|name| name.is_empty()) || self.position.is_empty() {
            return Err(IdentityError::Empty);
        }
        if names.iter().any(|name| name.len() > max_bytes) || self.position.len() > max_bytes {
            return Err(IdentityError::TooLarge);
        }
        Ok(())
    }
}
