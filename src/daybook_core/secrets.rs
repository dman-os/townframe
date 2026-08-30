//! Secret storage for daybook moved to the shared [`secrets_rs`] crate.
//!
//! Kept as a re-export so `crate::secrets::*` call sites stay stable.

pub use secrets_rs::{RepoIdentity, SecretRepo};