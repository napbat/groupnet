//! The harness's typed failure: a call that names a node the simulation
//! cannot act on.

use std::fmt;

use groupnet_core::NodeId;

/// Why a [`Simulation`](crate::Simulation) call that targets one node by id
/// was refused.
///
/// The harness hosts exactly one engine per [`NodeId`], so a node is either
/// present — added and not crashed since — or it is not. Every call that would
/// otherwise have to guess what an absent node means reports it here instead,
/// so a misspelt id or an already-crashed node fails the test loudly rather
/// than passing vacuously.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimError {
    /// [`Simulation::add`](crate::Simulation::add) was given an engine whose
    /// local id is already hosted. The engine already in place is untouched
    /// and the rejected one was never started.
    DuplicateNode(NodeId),
    /// The call named a node that is not in the simulation: never added, or
    /// removed by [`Simulation::crash`](crate::Simulation::crash).
    UnknownNode(NodeId),
}

impl fmt::Display for SimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNode(node) => {
                write!(f, "node {node} already hosts an engine in this simulation")
            }
            Self::UnknownNode(node) => write!(f, "node {node} is not in this simulation"),
        }
    }
}

impl std::error::Error for SimError {}
