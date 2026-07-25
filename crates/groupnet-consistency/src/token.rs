//! The `(epoch, seq)` write token — see the crate docs on writer restarts.

/// A write's position in one writer's feed: the feed life (`epoch`) and the
/// sequence number within it. The derived ordering is epoch-major, so any
/// token from a newer life compares above every token from an older one —
/// exactly the comparison [`Frontier`](crate::Frontier) watermarks need.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WriteToken {
    /// The feed life this write belongs to (see the crate docs on writer
    /// restarts).
    pub epoch: u64,
    /// The write's sequence number within the epoch, starting at 1.
    pub seq: u64,
}

#[cfg(test)]
mod tests {
    use super::WriteToken;

    #[test]
    fn tokens_order_epoch_major() {
        let old_life = WriteToken { epoch: 1, seq: 500 };
        let new_life = WriteToken { epoch: 2, seq: 1 };
        assert!(new_life > old_life, "any new-life token beats old-life");
    }
}
