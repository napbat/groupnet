//! # groupnet-testkit
//!
//! Internal test support shared across the Groupnet workspace. **Never
//! published** (`publish = false`) and never a normal dependency — crates pull
//! it in path-only, under `[dev-dependencies]`.
//!
//! It exists so the same fixtures can back both a crate's own integration
//! tests and the cross-crate ones, without duplicating frame-building
//! boilerplate or letting two copies drift apart.
//!
//! * [`frames`] — sans-IO fixtures: build a [`GroupEngine`], hand-assemble
//!   wire frames, and read effects back. Dependency-free, so
//!   `groupnet-core`'s test graph stays free of Tokio.
//!
//! The optional `cluster` feature adds the `cluster` module: an async
//! multi-node harness over [`groupnet-runtime`] and the in-memory transport —
//! all-to-all cluster bring-up plus bounded polling. It is off by default;
//! enable it only from crates that already depend on Tokio.
//!
//! [`GroupEngine`]: groupnet_core::GroupEngine
//! [`groupnet-runtime`]: https://docs.rs/groupnet-runtime

#[cfg(feature = "cluster")]
pub mod cluster;
pub mod frames;
