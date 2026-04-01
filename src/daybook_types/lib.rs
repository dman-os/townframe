//! Daybook types crate
//!
//! This crate provides type definitions for daybook with feature-gated support
//! for automerge, uniffi, and wit bindings.

mod interlude {
    pub use serde::{Deserialize, Serialize};

    pub use utils_rs::prelude::*;
}

pub mod doc;
#[cfg(test)]
mod test;
pub mod url;

#[cfg(feature = "manifest")]
pub mod manifest;

pub mod reference;

mod macros;

#[cfg(feature = "wit")]
pub mod wit;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
custom_type_set!();
