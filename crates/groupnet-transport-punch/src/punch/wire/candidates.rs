//! Zero-allocation candidate-list views over local storage or validated wire bytes.

use super::{Candidates, Reader, SocketAddr};

#[derive(Clone, Copy, Debug)]
pub(in crate::punch) enum CandidateList<'a> {
    Local(&'a Candidates),
    Encoded(&'a [u8], u8),
}

impl<'a> CandidateList<'a> {
    pub(in crate::punch) const fn empty() -> Self {
        Self::Encoded(&[], 0)
    }

    pub(in crate::punch) fn iter(self) -> CandidateIter<'a> {
        CandidateIter {
            list: self,
            index: 0,
            offset: 0,
        }
    }

    pub(in crate::punch) fn is_empty(self) -> bool {
        match self {
            Self::Local(list) => list.is_empty(),
            Self::Encoded(bytes, _) => bytes.is_empty(),
        }
    }

    pub(in crate::punch) fn len(self) -> usize {
        match self {
            Self::Local(list) => list.len(),
            Self::Encoded(_, count) => usize::from(count),
        }
    }
}

impl<'a> From<&'a Candidates> for CandidateList<'a> {
    fn from(candidates: &'a Candidates) -> Self {
        Self::Local(candidates)
    }
}

#[derive(Debug)]
pub(in crate::punch) struct CandidateIter<'a> {
    list: CandidateList<'a>,
    index: usize,
    offset: usize,
}

impl Iterator for CandidateIter<'_> {
    type Item = SocketAddr;

    fn next(&mut self) -> Option<SocketAddr> {
        match self.list {
            CandidateList::Local(list) => {
                let address = list.get(self.index)?;
                self.index += 1;
                Some(address)
            }
            CandidateList::Encoded(bytes, count) => {
                if self.index >= usize::from(count) {
                    return None;
                }
                let mut reader = Reader {
                    bytes,
                    position: self.offset,
                };
                if !reader.flag()? {
                    return None;
                }
                let address = reader.address()?;
                self.offset = reader.position;
                self.index += 1;
                Some(address)
            }
        }
    }
}
